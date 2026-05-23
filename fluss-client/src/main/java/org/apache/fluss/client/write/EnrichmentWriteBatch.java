/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.writer.AppendColumnsResult;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.memory.PreAllocatedPagedOutputView;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Phase N.3b: one batch of enrichment writes destined for a single {@link EnrichmentBatchKey}
 * (table + column group + table bucket).
 *
 * <p>Rows accumulate into a single Arrow {@link MemoryLogRecordsArrowBuilder}; on seal the builder
 * produces ONE multi-row Arrow batch. The server side stores the batch as one entry and indexes
 * each row's source offset at the batch's file position (dense, duplicated-position semantics);
 * {@code EnrichmentMerger} recovers the intra-batch row index via a backward walk in the index and
 * advances the batch iterator to extract the requested row.
 *
 * <p>Thread-safety: all mutating methods are {@code synchronized(this)}; appenders may race
 * concurrently against the batch head, but only one wins the {@link #tryAppend} call per row.
 */
@Internal
public final class EnrichmentWriteBatch {

    private final EnrichmentBatchKey key;
    private final int batchSizeLimit;
    private final long createdMs;
    private final MemoryLogRecordsArrowBuilder builder;
    private final List<MemorySegment> bufferSegments;
    private final MemorySegmentPool memorySegmentPool;
    private final List<Entry> entries = new ArrayList<>();

    private boolean sealed;

    public EnrichmentWriteBatch(
            EnrichmentBatchKey key,
            int batchSizeLimit,
            long createdMs,
            ArrowWriterPool arrowWriterPool,
            MemorySegmentPool memorySegmentPool,
            EnrichmentAccumulator.BatchEncoderInfo encoderInfo,
            int initialBufferBytes)
            throws IOException {
        this.key = key;
        this.batchSizeLimit = batchSizeLimit;
        this.createdMs = createdMs;
        this.memorySegmentPool = memorySegmentPool;
        // Share the writer's memory pool — enrichment writes go through the same bounded buffer
        // pool as base appends so the WriterClient's memory budget caps both.
        this.bufferSegments = new ArrayList<>();
        this.bufferSegments.add(memorySegmentPool.nextSegment());
        PreAllocatedPagedOutputView outputView = new PreAllocatedPagedOutputView(bufferSegments);
        int poolSchemaId = encoderInfo.writerSchemaId ^ key.getColumnGroup().hashCode();
        ArrowWriter writer =
                arrowWriterPool.getOrCreateWriter(
                        key.getTableId(),
                        poolSchemaId,
                        initialBufferBytes,
                        encoderInfo.groupRowType,
                        encoderInfo.compression);
        this.builder =
                MemoryLogRecordsArrowBuilder.builder(
                        encoderInfo.writerSchemaId, writer, outputView, true, null);
    }

    /** Memory segments held by this batch — caller returns them to the pool after ack. */
    public synchronized List<MemorySegment> pooledMemorySegments() {
        return new ArrayList<>(bufferSegments);
    }

    public EnrichmentBatchKey getKey() {
        return key;
    }

    public long getCreatedMs() {
        return createdMs;
    }

    /**
     * Try to add a row. Returns the row's future on success, or null if the batch is full / sealed
     * and the caller should start a new batch.
     *
     * <p>"Full" is the disjunction of two signals: the cumulative estimated size has hit {@code
     * batchSizeLimit}, OR the underlying {@link MemoryLogRecordsArrowBuilder#isFull()} (i.e. the
     * Arrow writer's buffer fill) reports full. The Arrow check is critical because {@code
     * arrowWriter.writeRow} throws when called past its write limit — the same fix that {@link
     * ArrowLogWriteBatch} applies on the base-append path.
     */
    public synchronized CompletableFuture<AppendColumnsResult> tryAppend(
            long sourceOffset, InternalRow enrichmentRow) {
        if (sealed) {
            return null;
        }
        // Accept the first row unconditionally so we always make progress even when one row
        // exceeds the batch size limit.
        if (!entries.isEmpty()
                && (builder.estimatedSizeInBytes() >= batchSizeLimit || builder.isFull())) {
            return null;
        }
        try {
            builder.append(ChangeType.APPEND_ONLY, enrichmentRow);
        } catch (Exception e) {
            CompletableFuture<AppendColumnsResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
        CompletableFuture<AppendColumnsResult> future = new CompletableFuture<>();
        entries.add(new Entry(sourceOffset, future));
        return future;
    }

    /** Returns true if the batch is at or above its size limit and should be drained. */
    public synchronized boolean isFull() {
        return !entries.isEmpty()
                && (builder.estimatedSizeInBytes() >= batchSizeLimit || builder.isFull());
    }

    public synchronized boolean isSealed() {
        return sealed;
    }

    public synchronized int rowCount() {
        return entries.size();
    }

    /** Seal the batch and produce the wire-format bytes and source-offset array. */
    public synchronized SealedBatch sealAndBuild() throws Exception {
        if (sealed) {
            throw new IllegalStateException("Batch is already sealed");
        }
        sealed = true;
        int observedSize = builder.estimatedSizeInBytes();
        if (entries.isEmpty()) {
            builder.close();
            memorySegmentPool.returnAll(bufferSegments);
            bufferSegments.clear();
            return new SealedBatch(
                    key,
                    /* records */ null,
                    new long[0],
                    new ArrayList<>(),
                    null,
                    new ArrayList<>(),
                    observedSize);
        }
        BytesView records;
        try {
            records = builder.build();
        } finally {
            builder.close();
        }
        long[] sourceOffsets = new long[entries.size()];
        List<CompletableFuture<AppendColumnsResult>> futures = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            sourceOffsets[i] = entries.get(i).sourceOffset;
            futures.add(entries.get(i).future);
        }
        return new SealedBatch(
                key,
                records,
                sourceOffsets,
                futures,
                memorySegmentPool,
                new ArrayList<>(bufferSegments),
                observedSize);
    }

    /** Complete all pending per-row futures with the same outcome. */
    public synchronized void completeWithError(Throwable t) {
        if (!sealed) {
            sealed = true;
            try {
                builder.close();
            } catch (Exception ignored) {
                // best-effort
            }
            memorySegmentPool.returnAll(bufferSegments);
            bufferSegments.clear();
        }
        for (Entry entry : entries) {
            if (!entry.future.isDone()) {
                entry.future.completeExceptionally(t);
            }
        }
    }

    /** Static struct that the accumulator hands off to the sender. */
    public static final class SealedBatch {
        public final EnrichmentBatchKey key;
        public final BytesView records; // nullable when entries.isEmpty()
        public final long[] sourceOffsets;
        public final List<CompletableFuture<AppendColumnsResult>> futures;
        public final int observedSizeInBytes;
        private final MemorySegmentPool memorySegmentPool;
        private final List<MemorySegment> pooledSegments;

        SealedBatch(
                EnrichmentBatchKey key,
                BytesView records,
                long[] sourceOffsets,
                List<CompletableFuture<AppendColumnsResult>> futures,
                MemorySegmentPool memorySegmentPool,
                List<MemorySegment> pooledSegments,
                int observedSizeInBytes) {
            this.key = key;
            this.records = records;
            this.sourceOffsets = sourceOffsets;
            this.futures = futures;
            this.memorySegmentPool = memorySegmentPool;
            this.pooledSegments = pooledSegments;
            this.observedSizeInBytes = observedSizeInBytes;
        }

        public boolean isEmpty() {
            return sourceOffsets.length == 0;
        }

        public void completeAll(long enrichmentWatermark) {
            for (CompletableFuture<AppendColumnsResult> future : futures) {
                if (!future.isDone()) {
                    future.complete(new AppendColumnsResult(enrichmentWatermark));
                }
            }
            releaseSegments();
        }

        public void completeAllExceptionally(Throwable t) {
            for (CompletableFuture<AppendColumnsResult> future : futures) {
                if (!future.isDone()) {
                    future.completeExceptionally(t);
                }
            }
            releaseSegments();
        }

        private void releaseSegments() {
            if (memorySegmentPool != null && !pooledSegments.isEmpty()) {
                memorySegmentPool.returnAll(pooledSegments);
                pooledSegments.clear();
            }
        }
    }

    private static final class Entry {
        final long sourceOffset;
        final CompletableFuture<AppendColumnsResult> future;

        Entry(long sourceOffset, CompletableFuture<AppendColumnsResult> future) {
            this.sourceOffset = sourceOffset;
            this.future = future;
        }
    }
}
