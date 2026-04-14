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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link MergedLogScanner}. */
class MergedLogScannerITCase extends ClientToServerITCaseBase {

    private static final Schema PARENT_SCHEMA =
            Schema.newBuilder()
                    .column("device_id", DataTypes.INT())
                    .column("ip", DataTypes.STRING())
                    .column("payload", DataTypes.STRING())
                    .build();

    private static final Schema OVERLAY_SCHEMA =
            Schema.newBuilder()
                    .column("parent_offset", DataTypes.BIGINT())
                    .column("geo_region", DataTypes.STRING())
                    .column("risk_score", DataTypes.DOUBLE())
                    .build();

    @Test
    void testMergedScanWithFullOverlay() throws Exception {
        TablePath parentPath = TablePath.of("test_overlay_db", "parent_full");
        TablePath overlayPath = TablePath.of("test_overlay_db", "overlay_full");

        TableDescriptor parentDesc =
                TableDescriptor.builder().schema(PARENT_SCHEMA).distributedBy(1).build();
        TableDescriptor overlayDesc =
                TableDescriptor.builder().schema(OVERLAY_SCHEMA).distributedBy(1).build();

        createTable(parentPath, parentDesc, false);
        createTable(overlayPath, overlayDesc, true);

        int recordCount = 10;

        try (Table parentTable = conn.getTable(parentPath);
                Table overlayTable = conn.getTable(overlayPath)) {

            // Write parent records and capture offsets
            AppendWriter parentWriter = parentTable.newAppend().createWriter();
            long[] parentOffsets = new long[recordCount];
            for (int i = 0; i < recordCount; i++) {
                AppendResult result =
                        parentWriter.append(row(i, "192.168.1." + i, "payload_" + i)).get();
                parentOffsets[i] = result.getOffset();
            }

            // Write overlay records referencing parent offsets
            AppendWriter overlayWriter = overlayTable.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                overlayWriter
                        .append(row(parentOffsets[i], "region_" + (i % 3), (double) (i * 10)))
                        .get();
            }

            // Set up merged scanner
            RowType parentRowType = PARENT_SCHEMA.getRowType();
            RowType overlayRowType = OVERLAY_SCHEMA.getRowType();

            LogScanner parentScanner = createLogScanner(parentTable);
            LogScanner overlayScanner = createLogScanner(overlayTable);
            subscribeFromBeginning(parentScanner, parentTable);
            subscribeFromBeginning(overlayScanner, overlayTable);

            MergedLogScanner mergedScanner =
                    new MergedLogScanner(
                            parentScanner,
                            overlayScanner,
                            parentRowType,
                            overlayRowType,
                            MergeMode.WAIT);

            // Poll until all merged records are collected
            List<ScanRecord> results = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < recordCount && System.currentTimeMillis() < deadline) {
                results.addAll(mergedScanner.poll(Duration.ofSeconds(1)));
            }

            assertThat(results).hasSize(recordCount);

            // Verify merged schema: 3 parent + 2 enrichment = 5
            RowType mergedType = mergedScanner.getMergedRowType();
            assertThat(mergedType.getFieldCount()).isEqualTo(5);

            // Verify each merged row
            for (ScanRecord rec : results) {
                InternalRow mergedRow = rec.getRow();
                assertThat(mergedRow.getFieldCount()).isEqualTo(5);

                int deviceId = mergedRow.getInt(0);
                assertThat(mergedRow.getString(1).toString()).isEqualTo("192.168.1." + deviceId);
                assertThat(mergedRow.getString(2).toString()).isEqualTo("payload_" + deviceId);
                assertThat(mergedRow.getString(3).toString()).isEqualTo("region_" + (deviceId % 3));
                assertThat(mergedRow.getDouble(4)).isEqualTo((double) (deviceId * 10));
            }

            mergedScanner.close();
        }
    }

    @Test
    void testMergedScanBestEffortWithLag() throws Exception {
        TablePath parentPath = TablePath.of("test_overlay_db", "parent_lag");
        TablePath overlayPath = TablePath.of("test_overlay_db", "overlay_lag");

        TableDescriptor parentDesc =
                TableDescriptor.builder().schema(PARENT_SCHEMA).distributedBy(1).build();
        TableDescriptor overlayDesc =
                TableDescriptor.builder().schema(OVERLAY_SCHEMA).distributedBy(1).build();

        createTable(parentPath, parentDesc, false);
        createTable(overlayPath, overlayDesc, true);

        int totalRecords = 10;
        int enrichedRecords = 5;

        try (Table parentTable = conn.getTable(parentPath);
                Table overlayTable = conn.getTable(overlayPath)) {

            // Write all parent records
            AppendWriter parentWriter = parentTable.newAppend().createWriter();
            long[] parentOffsets = new long[totalRecords];
            for (int i = 0; i < totalRecords; i++) {
                AppendResult result =
                        parentWriter.append(row(i, "192.168.1." + i, "payload_" + i)).get();
                parentOffsets[i] = result.getOffset();
            }

            // Write overlay records for only the first half
            AppendWriter overlayWriter = overlayTable.newAppend().createWriter();
            for (int i = 0; i < enrichedRecords; i++) {
                overlayWriter
                        .append(row(parentOffsets[i], "region_" + (i % 3), (double) (i * 10)))
                        .get();
            }

            // Set up merged scanner in BEST_EFFORT mode
            RowType parentRowType = PARENT_SCHEMA.getRowType();
            RowType overlayRowType = OVERLAY_SCHEMA.getRowType();

            LogScanner parentScanner = createLogScanner(parentTable);
            LogScanner overlayScanner = createLogScanner(overlayTable);
            subscribeFromBeginning(parentScanner, parentTable);
            subscribeFromBeginning(overlayScanner, overlayTable);

            MergedLogScanner mergedScanner =
                    new MergedLogScanner(
                            parentScanner,
                            overlayScanner,
                            parentRowType,
                            overlayRowType,
                            MergeMode.BEST_EFFORT);

            // Poll until all records collected
            List<ScanRecord> results = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < totalRecords && System.currentTimeMillis() < deadline) {
                results.addAll(mergedScanner.poll(Duration.ofSeconds(1)));
            }

            assertThat(results).hasSize(totalRecords);

            // Separate enriched and non-enriched records
            int enrichedCount = 0;
            int nullEnrichmentCount = 0;
            for (ScanRecord rec : results) {
                InternalRow mergedRow = rec.getRow();
                assertThat(mergedRow.getFieldCount()).isEqualTo(5);

                int deviceId = mergedRow.getInt(0);
                // Parent fields always present
                assertThat(mergedRow.getString(1).toString()).isEqualTo("192.168.1." + deviceId);
                assertThat(mergedRow.getString(2).toString()).isEqualTo("payload_" + deviceId);

                if (!mergedRow.isNullAt(3)) {
                    // Enrichment present
                    assertThat(mergedRow.getString(3).toString())
                            .isEqualTo("region_" + (deviceId % 3));
                    assertThat(mergedRow.getDouble(4)).isEqualTo((double) (deviceId * 10));
                    enrichedCount++;
                } else {
                    // Enrichment missing (overlay lagging)
                    assertThat(mergedRow.isNullAt(4)).isTrue();
                    nullEnrichmentCount++;
                }
            }

            assertThat(enrichedCount).isEqualTo(enrichedRecords);
            assertThat(nullEnrichmentCount).isEqualTo(totalRecords - enrichedRecords);

            mergedScanner.close();
        }
    }
}
