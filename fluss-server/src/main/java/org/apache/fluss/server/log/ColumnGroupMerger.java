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
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
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
 * Merges base log records with enrichment column group data. Given base Arrow records and
 * enrichment data (either from an in-memory map or a companion file), produces merged
 * MemoryLogRecords containing all columns.
 */
@Internal
public class ColumnGroupMerger {

    /**
     * Merge base records with enrichment data from a companion file. Reads the companion file to
     * build an offset-to-enrichment-row map, then delegates to {@link #merge}.
     *
     * @param baseRecords the base log records
     * @param companionRecords the companion enrichment file records
     * @param schema the full table schema (all columns including enrichment)
     * @param enrichmentColumnIndices indices of columns that belong to the enrichment group
     * @param schemaId the schema ID
     * @return merged MemoryLogRecords with enrichment columns filled in
     */
    public static MemoryLogRecords mergeFromCompanionFile(
            LogRecords baseRecords,
            FileLogRecords companionRecords,
            Schema schema,
            int[] enrichmentColumnIndices,
            int schemaId)
            throws Exception {

        RowType fullRowType = schema.getRowType();
        SchemaGetter schemaGetter = new SimpleSchemaGetter(schema, schemaId);

        // Read companion file into offset -> enrichment row map
        TreeMap<Long, GenericRow> enrichmentByOffset = new TreeMap<>();
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            fullRowType, schemaId, schemaGetter);
            try {
                for (LogRecordBatch batch : companionRecords.batches()) {
                    long currentOffset = batch.baseLogOffset();
                    try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                        while (iter.hasNext()) {
                            LogRecord record = iter.next();
                            InternalRow row = record.getRow();
                            int totalColumns = fullRowType.getFieldCount();
                            GenericRow enrichRow = new GenericRow(totalColumns);
                            InternalRow.FieldGetter[] getters =
                                    InternalRow.createFieldGetters(fullRowType);
                            for (int i = 0; i < totalColumns; i++) {
                                if (!row.isNullAt(i)) {
                                    enrichRow.setField(i, getters[i].getFieldOrNull(row));
                                }
                            }
                            enrichmentByOffset.put(currentOffset, enrichRow);
                            currentOffset++;
                        }
                    }
                }
            } finally {
                readContext.close();
            }
        }

        // Now read the base records and merge
        MemoryLogRecords baseMemoryRecords;
        if (baseRecords instanceof MemoryLogRecords) {
            baseMemoryRecords = (MemoryLogRecords) baseRecords;
        } else if (baseRecords instanceof FileLogRecords) {
            baseMemoryRecords = readFileLogRecordsIntoMemory((FileLogRecords) baseRecords);
        } else {
            // For other LogRecords types (e.g., BytesViewLogRecords), read via batches
            baseMemoryRecords = readLogRecordsIntoMemory(baseRecords, schema, schemaId);
        }

        return merge(
                baseMemoryRecords, enrichmentByOffset, schema, enrichmentColumnIndices, schemaId);
    }

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

        SchemaGetter schemaGetter = new SimpleSchemaGetter(schema, schemaId);

        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            fullRowType, schemaId, schemaGetter);

            try {
                for (LogRecordBatch batch : baseRecords.batches()) {
                    firstBaseOffset = batch.baseLogOffset();
                    long currentOffset = firstBaseOffset;

                    try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                        while (iter.hasNext()) {
                            LogRecord record = iter.next();
                            InternalRow baseRow = record.getRow();
                            GenericRow enrichmentRow = enrichmentByOffset.get(currentOffset);

                            GenericRow merged = new GenericRow(totalColumns);
                            InternalRow.FieldGetter[] getters =
                                    InternalRow.createFieldGetters(fullRowType);

                            for (int i = 0; i < totalColumns; i++) {
                                if (enrichmentRow != null
                                        && isEnrichmentColumn(i, enrichmentColumnIndices)) {
                                    if (!enrichmentRow.isNullAt(i)) {
                                        merged.setField(
                                                i, getters[i].getFieldOrNull(enrichmentRow));
                                    }
                                } else {
                                    if (!baseRow.isNullAt(i)) {
                                        merged.setField(i, getters[i].getFieldOrNull(baseRow));
                                    }
                                }
                            }

                            mergedRows.add(merged);
                            mergedChangeTypes.add(ChangeType.APPEND_ONLY);
                            currentOffset++;
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
                            schemaId,
                            Integer.MAX_VALUE,
                            fullRowType,
                            ArrowCompressionInfo.NO_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            firstBaseOffset,
                            LOG_MAGIC_VALUE_V0,
                            schemaId,
                            writer,
                            new UnmanagedPagedOutputView(64 * 1024));

            for (int i = 0; i < mergedChangeTypes.size(); i++) {
                builder.append(mergedChangeTypes.get(i), mergedRows.get(i));
            }
            builder.close();
            return MemoryLogRecords.pointToBytesView(builder.build());
        }
    }

    /** Read FileLogRecords into MemoryLogRecords by reading all bytes into memory. */
    private static MemoryLogRecords readFileLogRecordsIntoMemory(FileLogRecords fileRecords)
            throws Exception {
        int size = fileRecords.sizeInBytes();
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(size);
        // readInto already calls buffer.flip() internally
        fileRecords.readInto(buffer, 0);
        return MemoryLogRecords.pointToByteBuffer(buffer);
    }

    /**
     * Read any LogRecords into MemoryLogRecords by iterating batches and rebuilding. This is a
     * fallback for non-File, non-Memory LogRecords.
     */
    private static MemoryLogRecords readLogRecordsIntoMemory(
            LogRecords records, Schema schema, int schemaId) throws Exception {
        RowType fullRowType = schema.getRowType();
        SchemaGetter schemaGetter = new SimpleSchemaGetter(schema, schemaId);
        List<ChangeType> changeTypes = new ArrayList<>();
        List<InternalRow> rows = new ArrayList<>();
        long firstBaseOffset = 0;

        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            fullRowType, schemaId, schemaGetter);
            try {
                for (LogRecordBatch batch : records.batches()) {
                    firstBaseOffset = batch.baseLogOffset();
                    try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                        while (iter.hasNext()) {
                            LogRecord record = iter.next();
                            InternalRow row = record.getRow();
                            int totalColumns = fullRowType.getFieldCount();
                            GenericRow copy = new GenericRow(totalColumns);
                            InternalRow.FieldGetter[] getters =
                                    InternalRow.createFieldGetters(fullRowType);
                            for (int i = 0; i < totalColumns; i++) {
                                if (!row.isNullAt(i)) {
                                    copy.setField(i, getters[i].getFieldOrNull(row));
                                }
                            }
                            rows.add(copy);
                            changeTypes.add(ChangeType.APPEND_ONLY);
                        }
                    }
                }
            } finally {
                readContext.close();
            }
        }

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
                            firstBaseOffset,
                            LOG_MAGIC_VALUE_V0,
                            schemaId,
                            writer,
                            new UnmanagedPagedOutputView(64 * 1024));
            for (int i = 0; i < changeTypes.size(); i++) {
                builder.append(changeTypes.get(i), rows.get(i));
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
