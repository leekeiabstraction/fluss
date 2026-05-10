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
     * Append a batch of records covering the given source offsets (one entry per offset, all mapped
     * to the same file position — the start of this batch). The {@code OffsetIndex} enforces strict
     * monotonicity across consecutive {@code append} calls.
     *
     * @return the byte position at which the batch was written
     */
    int append(MemoryLogRecords records, long[] sourceOffsets) throws IOException {
        int position = fileLogRecords.sizeInBytes();
        int written = fileLogRecords.append(records);
        if (written == 0 && records.sizeInBytes() > 0) {
            throw new IOException(
                    "FileLogRecords.append wrote 0 bytes for non-empty MemoryLogRecords ("
                            + records.sizeInBytes()
                            + " bytes pending) in enrichment segment "
                            + fileLogRecords.file().getAbsolutePath());
        }
        for (long sourceOffset : sourceOffsets) {
            offsetIndex.append(sourceOffset, position);
        }
        return position;
    }

    /**
     * Look up the file position of the batch containing the given source offset. Returns null if
     * the segment has no entry for it. Caller is responsible for verifying the offset matches —
     * {@code OffsetPosition.getOffset()} will equal {@code sourceOffset} on a hit.
     */
    OffsetPosition lookup(long sourceOffset) {
        if (offsetIndex.entries() == 0) {
            return null;
        }
        OffsetPosition pos = offsetIndex.lookup(sourceOffset);
        return pos.getOffset() == sourceOffset ? pos : null;
    }

    /** Read access to the underlying records (used by merge-on-read in Phase D). */
    FileLogRecords records() {
        return fileLogRecords;
    }

    /**
     * Slice of an enrichment segment for replication: zero-copy {@link FileLogRecords} view of the
     * concatenated batches plus the parallel array of source offsets they cover.
     */
    static final class RangeResult {
        static final RangeResult EMPTY = new RangeResult(MemoryLogRecords.EMPTY, new long[0]);

        private final org.apache.fluss.record.LogRecords records;
        private final long[] sourceOffsets;

        RangeResult(org.apache.fluss.record.LogRecords records, long[] sourceOffsets) {
            this.records = records;
            this.sourceOffsets = sourceOffsets;
        }

        org.apache.fluss.record.LogRecords records() {
            return records;
        }

        long[] sourceOffsets() {
            return sourceOffsets;
        }

        boolean isEmpty() {
            return sourceOffsets.length == 0;
        }
    }

    /**
     * Return the enrichment entries in the half-open interval {@code [fromInclusive, toExclusive)},
     * subject to {@code maxBytes} on the underlying file slice (always returns at least one entry
     * if the from-bound is in range, to guarantee forward progress under tiny budgets).
     *
     * <p>Used by E.3b to ship enrichment to followers in fetch responses. Under the Phase C
     * dense-index invariant (one entry per source offset, contiguous from 0, baseOffset=0), the
     * slot in {@link OffsetIndex} for offset {@code N} is just {@code (int) N}, so the lookups here
     * are O(1) array accesses.
     *
     * @return a {@link RangeResult} carrying the file-backed records slice (zero-copy) and the
     *     parallel array of source offsets it covers; or {@link RangeResult#EMPTY} if the requested
     *     range is entirely past {@link #lastEnrichedOffset()} or empty.
     */
    RangeResult range(long fromInclusive, long toExclusive, int maxBytes) throws IOException {
        if (offsetIndex.entries() == 0
                || fromInclusive < 0L
                || fromInclusive >= toExclusive
                || maxBytes <= 0) {
            return RangeResult.EMPTY;
        }
        long lastEnriched = lastEnrichedOffset();
        if (fromInclusive > lastEnriched) {
            return RangeResult.EMPTY;
        }
        long clampedTo = Math.min(toExclusive, lastEnriched + 1L);

        int fromSlot = Math.toIntExact(fromInclusive);
        int toSlot = Math.toIntExact(clampedTo);
        int totalEntries = offsetIndex.entries();
        if (fromSlot >= totalEntries) {
            return RangeResult.EMPTY;
        }

        int startPos = offsetIndex.entry(fromSlot).getPosition();
        int endPos = positionForEndSlot(toSlot, totalEntries);

        // Greedy trim to maxBytes: always include at least one entry so callers make progress
        // even when a single batch is larger than the budget.
        int includedEndSlot;
        if (endPos - startPos <= maxBytes) {
            includedEndSlot = toSlot;
        } else {
            includedEndSlot = fromSlot + 1;
            for (int k = fromSlot + 2; k <= toSlot; k++) {
                int kEnd = positionForEndSlot(k, totalEntries);
                if (kEnd - startPos > maxBytes) {
                    break;
                }
                includedEndSlot = k;
            }
            endPos = positionForEndSlot(includedEndSlot, totalEntries);
        }

        int sliceSize = endPos - startPos;
        FileLogRecords slice = fileLogRecords.slice(startPos, sliceSize);
        int count = includedEndSlot - fromSlot;
        long[] sourceOffsets = new long[count];
        for (int i = 0; i < count; i++) {
            sourceOffsets[i] = fromInclusive + i;
        }
        return new RangeResult(slice, sourceOffsets);
    }

    /**
     * File position immediately past the last byte of the entry at {@code slot - 1}. When {@code
     * slot} is past the last index entry, that's the file's current end.
     */
    private int positionForEndSlot(int slot, int totalEntries) {
        if (slot >= totalEntries) {
            return fileLogRecords.sizeInBytes();
        }
        return offsetIndex.entry(slot).getPosition();
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
