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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.FlussPaths;

import java.io.File;
import java.util.TreeMap;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;

/**
 * Writes enrichment data as a companion file for a sealed log segment. The companion file contains
 * Arrow-format records with the same offset range as the base segment, where only enrichment
 * columns are populated and base columns are null.
 */
@Internal
public class SegmentEnrichmentWriter {

    private SegmentEnrichmentWriter() {}

    /**
     * Write enrichment data as a companion file for a sealed log segment.
     *
     * @param segmentDir the directory containing the segment files
     * @param baseOffset the base offset of the sealed segment
     * @param enrichmentByOffset map of offset to enrichment row data (full schema width)
     * @param schema the full table schema
     * @param schemaId the schema ID
     * @return the FileLogRecords for the companion file
     */
    public static FileLogRecords writeCompanionFile(
            File segmentDir,
            long baseOffset,
            TreeMap<Long, GenericRow> enrichmentByOffset,
            Schema schema,
            int schemaId)
            throws Exception {

        RowType fullRowType = schema.getRowType();

        // Build MemoryLogRecords from enrichment data
        MemoryLogRecords enrichmentRecords;
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L,
                            schemaId,
                            Integer.MAX_VALUE,
                            fullRowType,
                            ArrowCompressionInfo.NO_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseOffset,
                            LOG_MAGIC_VALUE_V0,
                            schemaId,
                            writer,
                            new UnmanagedPagedOutputView(64 * 1024));

            for (GenericRow enrichRow : enrichmentByOffset.values()) {
                builder.append(ChangeType.APPEND_ONLY, enrichRow);
            }
            builder.close();
            enrichmentRecords = MemoryLogRecords.pointToBytesView(builder.build());
        }

        // Write to companion file
        File companionFile = FlussPaths.enrichLogFile(segmentDir, baseOffset);
        FileLogRecords fileRecords = FileLogRecords.open(companionFile);
        fileRecords.append(enrichmentRecords);
        fileRecords.flush();

        return fileRecords;
    }
}
