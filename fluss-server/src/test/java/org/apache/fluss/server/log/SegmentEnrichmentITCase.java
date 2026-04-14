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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Integration test for segment-level column append (Option 3). Verifies that enrichment companion
 * files can be written for sealed segments and merged on read.
 */
class SegmentEnrichmentITCase {

    private @TempDir File tempDir;
    private LogTablet logTablet;
    private FlussScheduler scheduler;

    private static final Schema ENRICHMENT_SCHEMA =
            Schema.newBuilder()
                    .column("device_id", DataTypes.STRING())
                    .column("ip", DataTypes.STRING())
                    .column("payload", DataTypes.STRING())
                    .column("geo_region", DataTypes.STRING())
                    .columnGroup("enriched")
                    .column("risk_score", DataTypes.DOUBLE())
                    .columnGroup("enriched")
                    .build();

    private static final RowType ENRICHMENT_ROW_TYPE = ENRICHMENT_SCHEMA.getRowType();

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_enrichment");

    @BeforeEach
    void setup() throws Exception {
        File logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir, TABLE_PATH.getDatabaseName(), 1L, TABLE_PATH.getTableName());
        scheduler = new FlussScheduler(1);
        scheduler.startup();

        Configuration conf = new Configuration();
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("256kb"));

        logTablet =
                LogTablet.create(
                        PhysicalTablePath.of(TABLE_PATH),
                        logDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        scheduler,
                        LogFormat.ARROW,
                        1,
                        false,
                        SystemClock.getInstance(),
                        true);
    }

    @AfterEach
    void teardown() throws Exception {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    @Test
    void testSegmentEnrichmentCompanionFile() throws Exception {
        // 1. Verify column group metadata
        assertThat(ENRICHMENT_SCHEMA.getColumnGroupNames()).containsExactly("enriched");
        assertThat(ENRICHMENT_SCHEMA.getColumnGroups().get("enriched")).containsExactly(3, 4);

        // 2. Write base rows with NULL enrichment columns
        int numRows = 5;
        List<Object[]> baseData = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            baseData.add(new Object[] {"dev-" + i, "10.0.0." + i, "data-" + i, null, null});
        }
        MemoryLogRecords baseRecords =
                DataTestUtils.genMemoryLogRecordsByObject(
                        ENRICHMENT_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                        baseData);
        logTablet.appendAsLeader(baseRecords);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());

        // 3. Force segment roll so the segment becomes sealed
        logTablet.roll(Optional.empty());

        // 4. Verify pre-enrichment: enrichment columns should be NULL
        List<InternalRow> rowsBefore = readAllRows(0L);
        assertThat(rowsBefore).hasSize(numRows);
        for (int i = 0; i < numRows; i++) {
            InternalRow row = rowsBefore.get(i);
            assertThat(row.getString(0).toString()).isEqualTo("dev-" + i);
            assertThat(row.getString(1).toString()).isEqualTo("10.0.0." + i);
            assertThat(row.getString(2).toString()).isEqualTo("data-" + i);
            assertThat(row.isNullAt(3)).isTrue();
            assertThat(row.isNullAt(4)).isTrue();
        }

        // 5. Write companion file for the sealed segment (base offset 0)
        TreeMap<Long, GenericRow> enrichmentByOffset = new TreeMap<>();
        for (int i = 0; i < numRows; i++) {
            GenericRow enrichRow = new GenericRow(5);
            // Base columns null, enrichment columns populated
            enrichRow.setField(3, BinaryString.fromString("region-" + i));
            enrichRow.setField(4, (double) i * 0.1);
            enrichmentByOffset.put((long) i, enrichRow);
        }
        logTablet.writeEnrichmentForSegment(0L, enrichmentByOffset, ENRICHMENT_SCHEMA, 0);

        // 6. Verify post-enrichment: enrichment columns should be merged
        List<InternalRow> rowsAfter = readAllRows(0L);
        assertThat(rowsAfter).hasSize(numRows);
        for (int i = 0; i < numRows; i++) {
            InternalRow row = rowsAfter.get(i);
            assertThat(row.getString(0).toString()).isEqualTo("dev-" + i);
            assertThat(row.getString(1).toString()).isEqualTo("10.0.0." + i);
            assertThat(row.getString(2).toString()).isEqualTo("data-" + i);
            assertThat(row.getString(3).toString()).isEqualTo("region-" + i);
            assertThat(row.getDouble(4)).isCloseTo(i * 0.1, within(0.001));
        }
    }

    @Test
    void testCannotEnrichActiveSegment() throws Exception {
        // Write some data but don't roll
        List<Object[]> baseData = new ArrayList<>();
        baseData.add(new Object[] {"dev-0", "10.0.0.0", "data-0", null, null});
        MemoryLogRecords baseRecords =
                DataTestUtils.genMemoryLogRecordsByObject(
                        ENRICHMENT_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                        baseData);
        logTablet.appendAsLeader(baseRecords);

        // Try to enrich the active segment -- should fail
        TreeMap<Long, GenericRow> enrichmentByOffset = new TreeMap<>();
        GenericRow enrichRow = new GenericRow(5);
        enrichRow.setField(3, BinaryString.fromString("region-0"));
        enrichRow.setField(4, 0.5);
        enrichmentByOffset.put(0L, enrichRow);

        try {
            logTablet.writeEnrichmentForSegment(0L, enrichmentByOffset, ENRICHMENT_SCHEMA, 0);
            assertThat(false).as("Should have thrown IllegalArgumentException").isTrue();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("active segment");
        }
    }

    /**
     * Read all rows from the log tablet starting at the given offset.
     *
     * @param startOffset the offset to start reading from
     * @return list of rows read
     */
    private List<InternalRow> readAllRows(long startOffset) throws Exception {
        ColumnGroupMerger.SimpleSchemaGetter schemaGetter =
                new ColumnGroupMerger.SimpleSchemaGetter(ENRICHMENT_SCHEMA, DEFAULT_SCHEMA_ID);

        FetchDataInfo fetchInfo =
                logTablet.read(
                        startOffset,
                        Integer.MAX_VALUE,
                        FetchIsolation.HIGH_WATERMARK,
                        true,
                        null,
                        null);

        List<InternalRow> rows = new ArrayList<>();
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        ENRICHMENT_ROW_TYPE, DEFAULT_SCHEMA_ID, schemaGetter)) {
            for (LogRecordBatch batch : fetchInfo.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        LogRecord record = iter.next();
                        InternalRow row = record.getRow();
                        // Copy row data since the underlying buffers may be reused
                        int fieldCount = ENRICHMENT_ROW_TYPE.getFieldCount();
                        GenericRow copy = new GenericRow(fieldCount);
                        InternalRow.FieldGetter[] getters =
                                InternalRow.createFieldGetters(ENRICHMENT_ROW_TYPE);
                        for (int i = 0; i < fieldCount; i++) {
                            if (!row.isNullAt(i)) {
                                copy.setField(i, getters[i].getFieldOrNull(row));
                            }
                        }
                        rows.add(copy);
                    }
                }
            }
        }
        return rows;
    }
}
