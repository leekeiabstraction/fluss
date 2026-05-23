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
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.MapUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Phase N.2: client-side accumulator for enrichment writes ({@code AppendWriter.appendColumns}).
 *
 * <p>Mirrors the role that {@link RecordAccumulator} plays for base appends, scoped down to what
 * enrichment writes need:
 *
 * <ul>
 *   <li>One queue per {@link EnrichmentBatchKey} (table + group + bucket).
 *   <li>The head of each queue is the open, appendable batch. When it fills up or its linger window
 *       expires it's sealed and remains in the queue tail awaiting the sender thread.
 *   <li>{@link #drainReady} returns sealed batches across keys, optionally including the still-open
 *       head when triggered by {@code flushAll}.
 * </ul>
 *
 * <p>Backpressure / memory bounds are out of scope for the first cut (PHASE_N §6.3) — the
 * accumulator is unbounded; callers' downstream consumption is the only natural guard.
 */
@Internal
public final class EnrichmentAccumulator {

    /** Per-key batch metadata held in {@link #queues}. */
    private static final class KeyQueue {
        final Deque<EnrichmentWriteBatch> batches = new ArrayDeque<>();
    }

    private final Map<EnrichmentBatchKey, KeyQueue> queues = MapUtils.newConcurrentHashMap();
    private final ReentrantLock perKeyAppendLock = new ReentrantLock();

    private final ArrowWriterPool arrowWriterPool;
    private final BufferAllocator bufferAllocator;
    private final MemorySegmentPool memorySegmentPool;
    private final BatchEncoderInfo encoderInfo;
    private final int batchSizeLimit;
    private final long lingerMs;
    private final int initialBufferBytes;
    private final TablePath tablePath;
    @Nullable private final DynamicWriteBatchSizeEstimator batchSizeEstimator;

    private volatile boolean closed;

    @GuardedBy("perKeyAppendLock")
    private boolean flushRequested;

    /**
     * @param tablePath the logical table path this accumulator writes to; used as the estimator key
     *     (per-partition keying is intentionally avoided to keep the accumulator unaware of
     *     partition catalog state).
     * @param arrowWriterPool shared with the base-append writer for Arrow-encoder reuse.
     * @param bufferAllocator shared with the base-append writer.
     * @param memorySegmentPool shared with the base-append writer ({@code LazyMemorySegmentPool})
     *     so enrichment writes draw against the same bounded budget.
     * @param batchSizeEstimator shared with the base-append writer; when non-null, batches use the
     *     dynamically-adjusted size for this table. Pass null to disable.
     * @param encoderInfo per-(table, group) encoder details. The accumulator is scoped to one such
     *     pair; callers create one accumulator per group.
     */
    public EnrichmentAccumulator(
            TablePath tablePath,
            ArrowWriterPool arrowWriterPool,
            BufferAllocator bufferAllocator,
            MemorySegmentPool memorySegmentPool,
            @Nullable DynamicWriteBatchSizeEstimator batchSizeEstimator,
            BatchEncoderInfo encoderInfo,
            int batchSizeLimit,
            long lingerMs,
            int initialBufferBytes) {
        this.tablePath = tablePath;
        this.arrowWriterPool = arrowWriterPool;
        this.bufferAllocator = bufferAllocator;
        this.memorySegmentPool = memorySegmentPool;
        this.batchSizeEstimator = batchSizeEstimator;
        this.encoderInfo = encoderInfo;
        this.batchSizeLimit = batchSizeLimit;
        this.lingerMs = lingerMs;
        this.initialBufferBytes = initialBufferBytes;
    }

    /**
     * Enqueue an enrichment row for the given key, returning a future that completes when the
     * server acknowledges (or rejects) the containing batch.
     */
    public CompletableFuture<AppendColumnsResult> append(
            EnrichmentBatchKey key, long sourceOffset, InternalRow row, long nowMs) {
        if (closed) {
            CompletableFuture<AppendColumnsResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("Accumulator is closed"));
            return failed;
        }
        KeyQueue keyQueue = queues.computeIfAbsent(key, k -> new KeyQueue());
        perKeyAppendLock.lock();
        try {
            EnrichmentWriteBatch head = keyQueue.batches.peekLast();
            if (head != null && !head.isSealed()) {
                CompletableFuture<AppendColumnsResult> future = head.tryAppend(sourceOffset, row);
                if (future != null) {
                    return future;
                }
            }
            // Need a new batch.
            EnrichmentWriteBatch fresh = newBatch(key, nowMs);
            CompletableFuture<AppendColumnsResult> future = fresh.tryAppend(sourceOffset, row);
            if (future == null) {
                // First append into a fresh batch failing means the row encode itself blew up.
                throw new IllegalStateException(
                        "Fresh enrichment batch refused first row for key " + key);
            }
            keyQueue.batches.addLast(fresh);
            return future;
        } finally {
            perKeyAppendLock.unlock();
        }
    }

    private EnrichmentWriteBatch newBatch(EnrichmentBatchKey key, long nowMs) {
        // ArrowWriter pool keyed by tableId+group hash to avoid collisions with the base writer
        // which keys on tableId+schemaId.
        int poolSchemaId = encoderInfo.writerSchemaId ^ key.getColumnGroup().hashCode();
        ArrowWriter writer =
                arrowWriterPool.getOrCreateWriter(
                        key.getTableId(),
                        poolSchemaId,
                        initialBufferBytes,
                        encoderInfo.groupRowType,
                        encoderInfo.compression);
        int sizeLimit =
                batchSizeEstimator != null
                        ? batchSizeEstimator.getEstimatedBatchSize(estimatorKey())
                        : batchSizeLimit;
        try {
            return new EnrichmentWriteBatch(
                    key, sizeLimit, nowMs, writer, memorySegmentPool, encoderInfo.writerSchemaId);
        } catch (java.io.IOException e) {
            throw new RuntimeException(
                    "Failed to allocate a memory segment for enrichment batch on key " + key, e);
        }
    }

    private PhysicalTablePath estimatorKey() {
        // Per-table (not per-partition) — enrichment accumulator is scoped per-(table, group) and
        // the estimator's job is informational. Per-partition estimation would require partition
        // catalog access that the accumulator doesn't have.
        return PhysicalTablePath.of(tablePath);
    }

    /**
     * Seal and remove all batches that are ready to ship. A batch is ready when:
     *
     * <ul>
     *   <li>it's already sealed (drained from a previous round but not picked up yet);
     *   <li>it's full;
     *   <li>its linger window has expired;
     *   <li>a {@link #flushAll()} call has marked everything ready.
     * </ul>
     *
     * @return all ready (already sealed) batches grouped per key.
     */
    public Map<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> drainReady(long nowMs)
            throws Exception {
        Map<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> result =
                new java.util.HashMap<>();
        perKeyAppendLock.lock();
        boolean forceFlush;
        try {
            forceFlush = flushRequested;
            flushRequested = false;
        } finally {
            perKeyAppendLock.unlock();
        }
        for (Map.Entry<EnrichmentBatchKey, KeyQueue> entry : queues.entrySet()) {
            KeyQueue queue = entry.getValue();
            List<EnrichmentWriteBatch.SealedBatch> sealed = new ArrayList<>();
            // Pop ready batches from the head — preserving FIFO order so source_offsets remain
            // contiguous across batches for the same key.
            perKeyAppendLock.lock();
            try {
                while (!queue.batches.isEmpty()) {
                    EnrichmentWriteBatch first = queue.batches.peekFirst();
                    if (first.isSealed()) {
                        // Already sealed in a prior round but unable to be drained then —
                        // pick it up now via a fresh sealAndBuild call would throw, so we
                        // need a different escape. Since we always remove on seal below this
                        // shouldn't happen.
                        queue.batches.pollFirst();
                        continue;
                    }
                    boolean ready =
                            forceFlush
                                    || first.isFull()
                                    || (nowMs - first.getCreatedMs()) >= lingerMs;
                    if (!ready) {
                        break;
                    }
                    queue.batches.pollFirst();
                    EnrichmentWriteBatch.SealedBatch built = first.sealAndBuild();
                    if (batchSizeEstimator != null && !built.isEmpty()) {
                        batchSizeEstimator.updateEstimation(
                                estimatorKey(), built.observedSizeInBytes);
                    }
                    sealed.add(built);
                }
            } finally {
                perKeyAppendLock.unlock();
            }
            if (!sealed.isEmpty()) {
                result.put(entry.getKey(), sealed);
            }
        }
        return result;
    }

    /** Returns true if some batch is currently ready to ship. */
    public boolean hasReady(long nowMs) {
        perKeyAppendLock.lock();
        try {
            if (flushRequested) {
                return true;
            }
        } finally {
            perKeyAppendLock.unlock();
        }
        for (KeyQueue queue : queues.values()) {
            perKeyAppendLock.lock();
            try {
                EnrichmentWriteBatch head = queue.batches.peekFirst();
                if (head != null && (head.isFull() || (nowMs - head.getCreatedMs()) >= lingerMs)) {
                    return true;
                }
            } finally {
                perKeyAppendLock.unlock();
            }
        }
        return false;
    }

    /** Mark all in-flight batches as ready to ship on the next drain. */
    public void flushAll() {
        perKeyAppendLock.lock();
        try {
            flushRequested = true;
        } finally {
            perKeyAppendLock.unlock();
        }
    }

    /**
     * Close the accumulator. After {@link #close()} returns, any pending {@link #append} will fail.
     * Callers should {@link #flushAll()} and drain before closing for clean shutdown.
     */
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    /** Returns a snapshot of queue depths per key. Test-only convenience. */
    public Map<EnrichmentBatchKey, Integer> queueDepths() {
        perKeyAppendLock.lock();
        try {
            Map<EnrichmentBatchKey, Integer> result = new java.util.HashMap<>();
            for (Map.Entry<EnrichmentBatchKey, KeyQueue> entry : queues.entrySet()) {
                result.put(entry.getKey(), entry.getValue().batches.size());
            }
            return Collections.unmodifiableMap(result);
        } finally {
            perKeyAppendLock.unlock();
        }
    }

    /** Encoding parameters shared across all batches for a single (table, group). */
    public static final class BatchEncoderInfo {
        final RowType groupRowType;
        final int writerSchemaId;
        final ArrowCompressionInfo compression;

        public BatchEncoderInfo(
                RowType groupRowType, int writerSchemaId, ArrowCompressionInfo compression) {
            this.groupRowType = groupRowType;
            this.writerSchemaId = writerSchemaId;
            this.compression = compression;
        }
    }
}
