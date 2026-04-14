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

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ColumnGroupMerger}. */
class ColumnGroupMergerTest {

    private static final int SCHEMA_ID = 1;

    /**
     * Tests that merge correctly handles non-contiguous offsets. Base records at offsets 0, 1, 2
     * with enrichment only at offsets 0 and 2. Offset 1 should get NULLs for enrichment columns.
     */
    @Test
    void testMergeWithOffsetGaps() throws Exception {
        // Schema: base_col (STRING), enrich_col (STRING)
        Schema schema =
                Schema.newBuilder()
                        .column("base_col", DataTypes.STRING())
                        .column("enrich_col", DataTypes.STRING())
                        .columnGroup("enriched")
                        .build();
        RowType fullRowType = schema.getRowType();
        int[] enrichmentIndices = new int[] {1};

        // Build base records at offsets 0, 1, 2
        List<GenericRow> baseRows = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            GenericRow row = new GenericRow(2);
            row.setField(0, BinaryString.fromString("base-" + i));
            // enrichment column is null
            baseRows.add(row);
        }
        MemoryLogRecords baseRecords = buildArrowRecords(fullRowType, 0L, baseRows);

        // Enrichment only at offsets 0 and 2 (gap at offset 1)
        TreeMap<Long, GenericRow> enrichment = new TreeMap<>();
        GenericRow enrich0 = new GenericRow(1);
        enrich0.setField(0, BinaryString.fromString("enriched-0"));
        enrichment.put(0L, enrich0);

        GenericRow enrich2 = new GenericRow(1);
        enrich2.setField(0, BinaryString.fromString("enriched-2"));
        enrichment.put(2L, enrich2);

        // Merge
        MemoryLogRecords merged =
                ColumnGroupMerger.merge(
                        baseRecords, enrichment, schema, enrichmentIndices, SCHEMA_ID);

        // Verify
        List<InternalRow> rows = readAllRows(merged, fullRowType, SCHEMA_ID, schema);
        assertThat(rows).hasSize(3);

        // Offset 0: base + enrichment
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("base-0");
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("enriched-0");

        // Offset 1: base only, enrichment is NULL
        assertThat(rows.get(1).getString(0).toString()).isEqualTo("base-1");
        assertThat(rows.get(1).isNullAt(1)).isTrue();

        // Offset 2: base + enrichment
        assertThat(rows.get(2).getString(0).toString()).isEqualTo("base-2");
        assertThat(rows.get(2).getString(1).toString()).isEqualTo("enriched-2");
    }

    /**
     * Tests that two column groups can enrich the same offsets with different columns. Sequential
     * merging should preserve both groups' data.
     */
    @Test
    void testMergeWithTwoColumnGroupsSequentially() throws Exception {
        // Schema: base_col (STRING), group_a_col (STRING), group_b_col (DOUBLE)
        Schema schema =
                Schema.newBuilder()
                        .column("base_col", DataTypes.STRING())
                        .column("group_a_col", DataTypes.STRING())
                        .columnGroup("group_a")
                        .column("group_b_col", DataTypes.DOUBLE())
                        .columnGroup("group_b")
                        .build();
        RowType fullRowType = schema.getRowType();

        // Build base records at offset 0
        List<GenericRow> baseRows = new ArrayList<>();
        GenericRow baseRow = new GenericRow(3);
        baseRow.setField(0, BinaryString.fromString("base-val"));
        baseRows.add(baseRow);
        MemoryLogRecords baseRecords = buildArrowRecords(fullRowType, 0L, baseRows);

        // Group A enrichment at offset 0
        TreeMap<Long, GenericRow> groupAData = new TreeMap<>();
        GenericRow enrichA = new GenericRow(1);
        enrichA.setField(0, BinaryString.fromString("from-group-a"));
        groupAData.put(0L, enrichA);

        // Group B enrichment at offset 0
        TreeMap<Long, GenericRow> groupBData = new TreeMap<>();
        GenericRow enrichB = new GenericRow(1);
        enrichB.setField(0, 3.14);
        groupBData.put(0L, enrichB);

        // Merge group A first, then group B (sequential)
        int[] groupAIndices = new int[] {1};
        int[] groupBIndices = new int[] {2};

        MemoryLogRecords afterGroupA =
                ColumnGroupMerger.merge(baseRecords, groupAData, schema, groupAIndices, SCHEMA_ID);
        MemoryLogRecords afterBoth =
                ColumnGroupMerger.merge(afterGroupA, groupBData, schema, groupBIndices, SCHEMA_ID);

        // Verify both groups' data present
        List<InternalRow> rows = readAllRows(afterBoth, fullRowType, SCHEMA_ID, schema);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("base-val");
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("from-group-a");
        assertThat(rows.get(0).getDouble(2)).isEqualTo(3.14);
    }

    /** Tests that merge with empty enrichment returns the base records unchanged. */
    @Test
    void testMergeWithEmptyEnrichment() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("col_a", DataTypes.STRING())
                        .column("col_b", DataTypes.STRING())
                        .columnGroup("enriched")
                        .build();
        RowType fullRowType = schema.getRowType();

        List<GenericRow> baseRows = new ArrayList<>();
        GenericRow row = new GenericRow(2);
        row.setField(0, BinaryString.fromString("hello"));
        baseRows.add(row);
        MemoryLogRecords baseRecords = buildArrowRecords(fullRowType, 0L, baseRows);

        TreeMap<Long, GenericRow> emptyEnrichment = new TreeMap<>();

        MemoryLogRecords merged =
                ColumnGroupMerger.merge(
                        baseRecords, emptyEnrichment, schema, new int[] {1}, SCHEMA_ID);

        List<InternalRow> rows = readAllRows(merged, fullRowType, SCHEMA_ID, schema);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("hello");
        assertThat(rows.get(0).isNullAt(1)).isTrue();
    }

    /**
     * Tests that enrichment data at offsets outside the base record range does not cause errors.
     */
    @Test
    void testMergeWithEnrichmentOutsideRange() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("col_a", DataTypes.STRING())
                        .column("col_b", DataTypes.STRING())
                        .columnGroup("enriched")
                        .build();
        RowType fullRowType = schema.getRowType();

        // Base records at offsets 0, 1
        List<GenericRow> baseRows = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            GenericRow row = new GenericRow(2);
            row.setField(0, BinaryString.fromString("val-" + i));
            baseRows.add(row);
        }
        MemoryLogRecords baseRecords = buildArrowRecords(fullRowType, 0L, baseRows);

        // Enrichment at offset 100 (outside base range)
        TreeMap<Long, GenericRow> enrichment = new TreeMap<>();
        GenericRow enrichRow = new GenericRow(1);
        enrichRow.setField(0, BinaryString.fromString("far-away"));
        enrichment.put(100L, enrichRow);

        // Should not crash, enrichment simply ignored
        MemoryLogRecords merged =
                ColumnGroupMerger.merge(baseRecords, enrichment, schema, new int[] {1}, SCHEMA_ID);

        List<InternalRow> rows = readAllRows(merged, fullRowType, SCHEMA_ID, schema);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getString(0).toString()).isEqualTo("val-0");
        assertThat(rows.get(0).isNullAt(1)).isTrue();
        assertThat(rows.get(1).getString(0).toString()).isEqualTo("val-1");
        assertThat(rows.get(1).isNullAt(1)).isTrue();
    }

    // ---- Helper methods ----

    /** Build Arrow-format MemoryLogRecords from a list of GenericRows. */
    private static MemoryLogRecords buildArrowRecords(
            RowType rowType, long baseOffset, List<GenericRow> rows) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                ArrowWriterPool writerPool = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    writerPool.getOrCreateWriter(
                            1L,
                            SCHEMA_ID,
                            Integer.MAX_VALUE,
                            rowType,
                            ArrowCompressionInfo.NO_COMPRESSION);
            MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            baseOffset,
                            LOG_MAGIC_VALUE_V0,
                            SCHEMA_ID,
                            writer,
                            new UnmanagedPagedOutputView(64 * 1024));

            for (GenericRow row : rows) {
                builder.append(ChangeType.APPEND_ONLY, row);
            }
            builder.close();
            return MemoryLogRecords.pointToBytesView(builder.build());
        }
    }

    /** Read all rows from MemoryLogRecords. */
    private static List<InternalRow> readAllRows(
            MemoryLogRecords records, RowType rowType, int schemaId, Schema schema)
            throws Exception {
        List<InternalRow> result = new ArrayList<>();
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        rowType,
                        schemaId,
                        new ColumnGroupMerger.SimpleSchemaGetter(schema, schemaId));
        try {
            for (LogRecordBatch batch : records.batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        LogRecord record = iter.next();
                        InternalRow row = record.getRow();
                        // Copy row data into GenericRow since Arrow rows may be invalidated
                        GenericRow copy = new GenericRow(rowType.getFieldCount());
                        InternalRow.FieldGetter[] getters = InternalRow.createFieldGetters(rowType);
                        for (int i = 0; i < rowType.getFieldCount(); i++) {
                            if (!row.isNullAt(i)) {
                                copy.setField(i, getters[i].getFieldOrNull(row));
                            }
                        }
                        result.add(copy);
                    }
                }
            }
        } finally {
            readContext.close();
        }
        return result;
    }
}
