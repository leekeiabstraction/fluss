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

package org.apache.fluss.server.log;

import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.utils.FlussPaths;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Per-(bucket, column-group) on-disk store: a {@link FileLogRecords} of Arrow batches paired with
 * an {@link OffsetIndex} mapping every enriched source offset to the file position of its
 * containing batch. The index is dense (one entry per enriched offset) because column-group writes
 * are strictly monotonic and produce one entry per source offset on the hot path.
 *
 * <p>Intended use: {@link EnrichmentSegments} owns the lifecycle (creation, recovery, sealing).
 * Direct construction is reserved for that owner; external callers should not instantiate.
 *
 * <p>Phase C scope: a single segment per (bucket, group), {@code baseOffset = 0}. Multi-segment
 * lifecycle aligned with base segments will land when tiering needs segment-granular completion
 * (Tier 3).
 */
@NotThreadSafe
final class EnrichmentSegment implements Closeable {

    private final String groupName;
    private final long baseOffset;
    private final FileLogRecords fileLogRecords;
    private final OffsetIndex offsetIndex;

    private EnrichmentSegment(
            String groupName,
            long baseOffset,
            FileLogRecords fileLogRecords,
            OffsetIndex offsetIndex) {
        this.groupName = groupName;
        this.baseOffset = baseOffset;
        this.fileLogRecords = fileLogRecords;
        this.offsetIndex = offsetIndex;
    }

    /** Open (or create) the on-disk segment for the given (bucket dir, group, baseOffset). */
    static EnrichmentSegment open(
            File logTabletDir, String groupName, long baseOffset, int maxIndexSize)
            throws IOException {
        File logFile = FlussPaths.columnGroupLogFile(logTabletDir, baseOffset, groupName);
        File indexFile = FlussPaths.columnGroupOffsetIndexFile(logTabletDir, baseOffset, groupName);
        boolean preExisting = logFile.exists();
        FileLogRecords logRecords = FileLogRecords.open(logFile, preExisting, 0, false);
        OffsetIndex index = new OffsetIndex(indexFile, baseOffset, maxIndexSize, true);
        return new EnrichmentSegment(groupName, baseOffset, logRecords, index);
    }

    String groupName() {
        return groupName;
    }

    long baseOffset() {
        return baseOffset;
    }

    /**
     * Returns the largest source offset that has been written, or {@code -1L} if the segment is
     * empty. Used to derive EWM on startup.
     */
    long lastEnrichedOffset() {
        if (offsetIndex.entries() == 0) {
            return -1L;
        }
        return offsetIndex.lastOffset();
    }

    /**
     * Append records covering the given source offsets. The wire format may carry either one Arrow
     * batch per source offset (option-a, concatenated single-row batches) or one Arrow batch with N
     * rows (option-b, multi-row batches) — both produce dense per-row index entries where rows in
     * the same batch share the batch's file position. {@link OffsetIndex} enforces strict
     * monotonicity across consecutive {@code append} calls.
     *
     * <p>For multi-row batches, {@link #lookupBatch} recovers the intra-batch row index by walking
     * backward in the index while {@code position} stays equal. The merger then advances the batch
     * iterator to that row when extracting an enrichment value.
     *
     * @return the byte position at which the first batch was written
     */
    int append(MemoryLogRecords records, long[] sourceOffsets) throws IOException {
        int basePosition = fileLogRecords.sizeInBytes();
        int written = fileLogRecords.append(records);
        if (written == 0 && records.sizeInBytes() > 0) {
            throw new IOException(
                    "FileLogRecords.append wrote 0 bytes for non-empty MemoryLogRecords ("
                            + records.sizeInBytes()
                            + " bytes pending) in enrichment segment "
                            + fileLogRecords.file().getAbsolutePath());
        }
        int rowIdx = 0;
        int relativeOffset = 0;
        for (LogRecordBatch batch : records.batches()) {
            int batchPosition = basePosition + relativeOffset;
            int recordCount = batch.getRecordCount();
            for (int i = 0; i < recordCount; i++) {
                if (rowIdx >= sourceOffsets.length) {
                    throw new IOException(
                            "Enrichment records contain more rows than sourceOffsets ("
                                    + sourceOffsets.length
                                    + ") for group "
                                    + groupName);
                }
                offsetIndex.append(sourceOffsets[rowIdx], batchPosition);
                rowIdx++;
            }
            relativeOffset += batch.sizeInBytes();
        }
        if (rowIdx != sourceOffsets.length) {
            throw new IOException(
                    "Enrichment records contain "
                            + rowIdx
                            + " total rows but sourceOffsets has "
                            + sourceOffsets.length
                            + " entries for group "
                            + groupName);
        }
        return basePosition;
    }

    /**
     * Look up the file position of the batch containing the given source offset. Returns null if
     * the segment has no entry for it. Caller is responsible for verifying the offset matches —
     * {@code OffsetPosition.getOffset()} will equal {@code sourceOffset} on a hit. The position
     * points at the START of the containing batch; for multi-row batches use {@link #lookupBatch}
     * to recover the intra-batch row index.
     */
    OffsetPosition lookup(long sourceOffset) {
        if (offsetIndex.entries() == 0) {
            return null;
        }
        OffsetPosition pos = offsetIndex.lookup(sourceOffset);
        return pos.getOffset() == sourceOffset ? pos : null;
    }

    /**
     * Look up the file position AND intra-batch row index of the given source offset. The
     * intra-batch index is recovered by walking backward in {@link OffsetIndex} while {@code
     * position} stays equal — rows in the same Arrow batch all share the batch's file position (see
     * {@link #append}). Returns null if the offset is not indexed.
     *
     * <p>Cost: O(intraIndex) entry reads from the mmap'd index. For batches with many rows accessed
     * in sequential order, this is proportional to the position within the batch.
     */
    BatchSlot lookupBatch(long sourceOffset) {
        if (offsetIndex.entries() == 0) {
            return null;
        }
        OffsetPosition pos = offsetIndex.lookup(sourceOffset);
        if (pos.getOffset() != sourceOffset) {
            return null;
        }
        int slot = Math.toIntExact(sourceOffset);
        int position = pos.getPosition();
        int intraIndex = 0;
        while (slot > 0) {
            OffsetPosition prev = offsetIndex.entry(slot - 1);
            if (prev.getPosition() != position) {
                break;
            }
            intraIndex++;
            slot--;
        }
        return new BatchSlot(position, intraIndex);
    }

    /** Position + intra-batch row index pair returned by {@link #lookupBatch}. */
    static final class BatchSlot {
        final int position;
        final int intraIndex;

        BatchSlot(int position, int intraIndex) {
            this.position = position;
            this.intraIndex = intraIndex;
        }
    }

    /** Read access to the underlying records (used by merge-on-read in Phase D). */
    FileLogRecords records() {
        return fileLogRecords;
    }

    /**
     * Return the enrichment entries in the half-open interval {@code [fromInclusive, toExclusive)},
     * subject to {@code maxBytes} on the underlying file slice (always returns at least one batch
     * if the from-bound is in range, to guarantee forward progress under tiny budgets).
     *
     * <p>Used by E.3b to ship enrichment to followers in fetch responses. Under the dense-index
     * invariant (one entry per source offset, contiguous from 0, baseOffset=0), the slot in {@link
     * OffsetIndex} for offset {@code N} is just {@code (int) N}, so the lookups here are O(1) array
     * accesses. Rows in the same Arrow batch share the batch's file position, so the slice is
     * rounded to whole-batch boundaries: if {@code fromInclusive} or {@code toExclusive} falls
     * mid-batch, the slice includes the full containing batch(es). Followers always start fetches
     * at their EWM, which aligns with a batch boundary by construction (EWM advances by {@code
     * batch.recordCount} on apply), so the rounding is a defensive no-op in practice.
     *
     * @return an {@link EnrichmentReadResult} carrying the file-backed records slice (zero-copy)
     *     and the parallel array of source offsets it covers; or {@link EnrichmentReadResult#EMPTY}
     *     if the requested range is entirely past {@link #lastEnrichedOffset()} or empty.
     */
    EnrichmentReadResult range(long fromInclusive, long toExclusive, int maxBytes)
            throws IOException {
        if (offsetIndex.entries() == 0
                || fromInclusive < 0L
                || fromInclusive >= toExclusive
                || maxBytes <= 0) {
            return EnrichmentReadResult.EMPTY;
        }
        long lastEnriched = lastEnrichedOffset();
        if (fromInclusive > lastEnriched) {
            return EnrichmentReadResult.EMPTY;
        }
        long clampedTo = Math.min(toExclusive, lastEnriched + 1L);

        int fromSlot = Math.toIntExact(fromInclusive);
        int toSlot = Math.toIntExact(clampedTo);
        int totalEntries = offsetIndex.entries();
        if (fromSlot >= totalEntries) {
            return EnrichmentReadResult.EMPTY;
        }

        // Round fromSlot DOWN to its batch's start (defensive: callers should already be at a
        // batch boundary).
        int fromBatchPosition = offsetIndex.entry(fromSlot).getPosition();
        int batchStartSlot = fromSlot;
        while (batchStartSlot > 0
                && offsetIndex.entry(batchStartSlot - 1).getPosition() == fromBatchPosition) {
            batchStartSlot--;
        }
        int startPos = fromBatchPosition;

        // Walk batches forward. Include each in turn while we haven't hit toSlot and budget
        // allows; always include the first batch even if it busts the budget (forward progress).
        int includedEndSlot = batchStartSlot;
        int currentPos = startPos;
        boolean isFirstBatch = true;
        while (includedEndSlot < totalEntries && includedEndSlot < toSlot) {
            int batchEndSlot = includedEndSlot + 1;
            while (batchEndSlot < totalEntries
                    && offsetIndex.entry(batchEndSlot).getPosition() == currentPos) {
                batchEndSlot++;
            }
            int nextBatchPos =
                    batchEndSlot < totalEntries
                            ? offsetIndex.entry(batchEndSlot).getPosition()
                            : fileLogRecords.sizeInBytes();
            if (!isFirstBatch && nextBatchPos - startPos > maxBytes) {
                break;
            }
            includedEndSlot = batchEndSlot;
            currentPos = nextBatchPos;
            isFirstBatch = false;
        }

        int endPos = currentPos;
        int sliceSize = endPos - startPos;
        FileLogRecords slice = fileLogRecords.slice(startPos, sliceSize);
        int count = includedEndSlot - batchStartSlot;
        long[] sourceOffsets = new long[count];
        for (int i = 0; i < count; i++) {
            sourceOffsets[i] = batchStartSlot + i;
        }
        return new EnrichmentReadResult(slice, sourceOffsets);
    }

    /**
     * Drop all entries with {@code source_offset >= sourceOffsetExclusive}. The on-disk {@link
     * FileLogRecords} is truncated to the file position of the first dropped entry, and the {@link
     * OffsetIndex} drops the corresponding tail. After this call, {@link #lastEnrichedOffset()} is
     * at most {@code sourceOffsetExclusive - 1}.
     *
     * <p>This is the mirror of base-log truncation: when the base log truncates to offset N,
     * enrichment for offsets {@code >= N} is no longer reachable and must be dropped to preserve
     * the contiguous-from-EWM write contract enforced by {@code Replica.appendColumnsAsLeader}.
     *
     * <p><b>Phase C invariant:</b> the index is dense — one entry per source offset, contiguous
     * from 0. Under this invariant, every {@code sourceOffsetExclusive} in {@code [0,
     * lastEnrichedOffset() + 1]} matches an indexed entry exactly. If a future writer relaxes
     * density (e.g. multi-row batches with non-contiguous offsets), this method's "first entry to
     * drop" derivation will need to walk the next-larger slot.
     *
     * @return {@code true} if any entries were dropped, {@code false} if the call was a no-op.
     */
    boolean truncateTo(long sourceOffsetExclusive) throws IOException {
        if (sourceOffsetExclusive < 0L) {
            sourceOffsetExclusive = 0L;
        }
        if (offsetIndex.entries() == 0 || sourceOffsetExclusive > lastEnrichedOffset()) {
            return false;
        }
        if (sourceOffsetExclusive == 0L) {
            // Drop everything.
            fileLogRecords.truncateTo(0);
            offsetIndex.truncate();
            fileLogRecords.flush();
            offsetIndex.flush();
            return true;
        }
        OffsetPosition firstDropped = offsetIndex.lookup(sourceOffsetExclusive);
        if (firstDropped.getOffset() != sourceOffsetExclusive) {
            // Phase C dense-index invariant violated. Rather than guess at the right
            // file position, fail loudly — this would indicate a writer that produced
            // gaps or a corrupted index.
            throw new IllegalStateException(
                    "Sparse enrichment index encountered while truncating "
                            + fileLogRecords.file().getAbsolutePath()
                            + " to "
                            + sourceOffsetExclusive
                            + ": OffsetIndex.lookup returned offset "
                            + firstDropped.getOffset()
                            + " (expected exact match under the Phase C dense-index invariant).");
        }
        fileLogRecords.truncateTo(firstDropped.getPosition());
        offsetIndex.truncateTo(sourceOffsetExclusive);
        fileLogRecords.flush();
        offsetIndex.flush();
        return true;
    }

    /** Flush in-memory writes to disk. */
    void flush() throws IOException {
        fileLogRecords.flush();
        offsetIndex.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            offsetIndex.close();
        } finally {
            fileLogRecords.close();
        }
    }
}
