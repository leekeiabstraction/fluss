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
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;

/**
 * Merges base log records with enrichment column group data. Given base Arrow records and an
 * in-memory map of enrichment rows keyed by offset, produces merged MemoryLogRecords containing all
 * columns.
 */
@Internal
public class ColumnGroupMerger {

    /**
     * Merge base records with enrichment data from column groups.
     *
     * @param baseRecords the base log records (may contain NULLs for enrichment columns)
     * @param enrichmentByOffset map of offset to enrichment row (only enrichment column values)
     * @param schema the full table schema (all columns including enrichment)
     * @param enrichmentColumnIndices indices of columns that belong to the enrichment group
     * @param schemaId the schema ID
     * @return merged MemoryLogRecords with enrichment columns filled in
     */
    public static MemoryLogRecords merge(
            MemoryLogRecords baseRecords,
            TreeMap<Long, GenericRow> enrichmentByOffset,
            Schema schema,
            int[] enrichmentColumnIndices,
            int schemaId)
            throws Exception {

        RowType fullRowType = schema.getRowType();
        int totalColumns = fullRowType.getFieldCount();

        // Read base records and merge with enrichment data
        List<ChangeType> mergedChangeTypes = new ArrayList<>();
        List<InternalRow> mergedRows = new ArrayList<>();
        long firstBaseOffset = 0;
        int actualSchemaId = schemaId;

        // Get the actual schema ID from the first batch
        for (LogRecordBatch firstBatch : baseRecords.batches()) {
            actualSchemaId = firstBatch.schemaId();
            break;
        }

        SchemaGetter schemaGetter = new SimpleSchemaGetter(schema, actualSchemaId);

        // Build getters for the enrichment row type
        RowType enrichmentRowType = fullRowType.project(enrichmentColumnIndices);
        InternalRow.FieldGetter[] enrichmentGetters =
                InternalRow.createFieldGetters(enrichmentRowType);
        InternalRow.FieldGetter[] baseGetters = InternalRow.createFieldGetters(fullRowType);

        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            fullRowType, actualSchemaId, schemaGetter);

            try {
                for (LogRecordBatch batch : baseRecords.batches()) {
                    firstBaseOffset = batch.baseLogOffset();

                    try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                        while (iter.hasNext()) {
                            LogRecord record = iter.next();
                            long recordOffset = record.logOffset();
                            InternalRow baseRow = record.getRow();
                            GenericRow enrichmentRow = enrichmentByOffset.get(recordOffset);

                            GenericRow merged = new GenericRow(totalColumns);

                            for (int i = 0; i < totalColumns; i++) {
                                if (enrichmentRow != null
                                        && isEnrichmentColumn(i, enrichmentColumnIndices)) {
                                    int enrichmentPos =
                                            getEnrichmentFieldIndex(i, enrichmentColumnIndices);
                                    if (enrichmentPos >= 0
                                            && !enrichmentRow.isNullAt(enrichmentPos)) {
                                        merged.setField(
                                                i,
                                                enrichmentGetters[enrichmentPos].getFieldOrNull(
                                                        enrichmentRow));
                                    }
                                } else {
                                    if (!baseRow.isNullAt(i)) {
                                        merged.setField(i, baseGetters[i].getFieldOrNull(baseRow));
                                    }
                                }
                            }

                            mergedRows.add(merged);
                            mergedChangeTypes.add(ChangeType.APPEND_ONLY);
                        }
                    }
                }
            } finally {
                readContext.close();
            }
        }

        if (mergedRows.isEmpty()) {
            return baseRecords;
        }

        // Build new MemoryLogRecords from merged rows
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L,
                            actualSchemaId,
                            Integer.MAX_VALUE,
                            fullRowType,
                            ArrowCompressionInfo.NO_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            firstBaseOffset,
                            LOG_MAGIC_VALUE_V0,
                            actualSchemaId,
                            writer,
                            new UnmanagedPagedOutputView(64 * 1024));

            for (int i = 0; i < mergedChangeTypes.size(); i++) {
                builder.append(mergedChangeTypes.get(i), mergedRows.get(i));
            }
            builder.close();
            return MemoryLogRecords.pointToBytesView(builder.build());
        }
    }

    private static boolean isEnrichmentColumn(int columnIndex, int[] enrichmentColumnIndices) {
        for (int idx : enrichmentColumnIndices) {
            if (idx == columnIndex) {
                return true;
            }
        }
        return false;
    }

    private static int getEnrichmentFieldIndex(int columnIndex, int[] enrichmentColumnIndices) {
        for (int i = 0; i < enrichmentColumnIndices.length; i++) {
            if (enrichmentColumnIndices[i] == columnIndex) {
                return i;
            }
        }
        return -1;
    }

    /** Simple SchemaGetter that returns a fixed schema for a single schema ID. */
    static class SimpleSchemaGetter implements SchemaGetter {
        private final Schema schema;
        private final int schemaId;

        SimpleSchemaGetter(Schema schema, int schemaId) {
            this.schema = schema;
            this.schemaId = schemaId;
        }

        @Override
        public Schema getSchema(int requestedSchemaId) {
            return schema;
        }

        @Override
        public CompletableFuture<SchemaInfo> getSchemaInfoAsync(int requestedSchemaId) {
            return CompletableFuture.completedFuture(new SchemaInfo(schema, schemaId));
        }

        @Override
        public SchemaInfo getLatestSchemaInfo() {
            return new SchemaInfo(schema, schemaId);
        }

        @Override
        public void release() {
            // no-op
        }
    }
}
