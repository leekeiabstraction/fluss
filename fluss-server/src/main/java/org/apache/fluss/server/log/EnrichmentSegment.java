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
