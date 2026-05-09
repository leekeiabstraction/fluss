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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendColumnsResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.log.ColumnGroupStore;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for Column Group Enrichment with Enrichment Watermark (EWM) tracking.
 *
 * <p>This test verifies:
 *
 * <ul>
 *   <li>Schema with column groups can be created and used
 *   <li>Base rows can be written and read via standard AppendWriter/LogScanner
 *   <li>Enrichment data can be stored on the server via ColumnGroupStore
 *   <li>Enrichment Watermark (EWM) tracks progress correctly
 *   <li>Column group metadata is correctly reflected in the Schema
 * </ul>
 */
public class ColumnGroupEWMITCase extends ClientToServerITCaseBase {

    private static final String DB_NAME = "test_db";
    private static final String TABLE_NAME = "device_logs_enriched";
    private static final TablePath TABLE_PATH = TablePath.of(DB_NAME, TABLE_NAME);

    // Schema: 4 base columns + 2 enrichment columns
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("device_id", DataTypes.STRING())
                    .column("ip", DataTypes.STRING())
                    .column("payload", DataTypes.STRING())
                    .column("ts", DataTypes.BIGINT())
                    .column("geo_region", DataTypes.STRING())
                    .columnGroup("enriched")
                    .column("risk_score", DataTypes.DOUBLE())
                    .columnGroup("enriched")
                    .build();

    private static final TableDescriptor TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(SCHEMA).distributedBy(1).build();

    @Test
    void testSchemaColumnGroupMetadata() {
        // Verify column group metadata in schema
        Map<String, List<Integer>> groups = SCHEMA.getColumnGroups();
        assertThat(groups).containsKey("enriched");
        assertThat(groups.get("enriched")).containsExactly(4, 5);

        // Verify default group indices
        int[] defaultIndices = SCHEMA.getDefaultGroupColumnIndices();
        assertThat(defaultIndices).containsExactly(0, 1, 2, 3);

        // Verify column group names
        assertThat(SCHEMA.getColumnGroupNames()).containsExactly("enriched");

        // Verify individual column metadata
        assertThat(SCHEMA.getColumn("geo_region").getColumnGroup()).hasValue("enriched");
        assertThat(SCHEMA.getColumn("risk_score").getColumnGroup()).hasValue("enriched");
        assertThat(SCHEMA.getColumn("device_id").getColumnGroup()).isEmpty();
    }

    @Test
    void testWriteBaseRowsAndReadBack() throws Exception {
        long tableId = createTable(TABLE_PATH, TABLE_DESCRIPTOR, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        int recordCount = 5;
        List<GenericRow> expectedRows = new ArrayList<>();

        try (Table table = conn.getTable(TABLE_PATH)) {
            // Write 5 base rows (enrichment columns as NULL)
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                GenericRow baseRow =
                        row(
                                "device-" + i,
                                "10.0.0." + i,
                                "payload-" + i,
                                (long) (1000 + i),
                                null,
                                null);
                expectedRows.add(baseRow);
                writer.append(baseRow).get();
            }

            // Read back via LogScanner
            LogScanner scanner = createLogScanner(table);
            subscribeFromBeginning(scanner, table);

            List<InternalRow> readRows = new ArrayList<>();
            while (readRows.size() < recordCount) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    readRows.add(record.getRow());
                }
            }

            // Verify base columns are present and enrichment columns are NULL
            assertThat(readRows).hasSize(recordCount);
            for (int i = 0; i < recordCount; i++) {
                InternalRow row = readRows.get(i);
                assertThat(row.getString(0).toString()).isEqualTo("device-" + i);
                assertThat(row.getString(1).toString()).isEqualTo("10.0.0." + i);
                assertThat(row.getString(2).toString()).isEqualTo("payload-" + i);
                assertThat(row.getLong(3)).isEqualTo(1000 + i);
                // Enrichment columns should be NULL
                assertThat(row.isNullAt(4)).isTrue();
                assertThat(row.isNullAt(5)).isTrue();
            }
        }
    }

    @Test
    void testEnrichmentStorageAndEWM() throws Exception {
        long tableId =
                createTable(TablePath.of(DB_NAME, "device_logs_ewm"), TABLE_DESCRIPTOR, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        TablePath tablePath = TablePath.of(DB_NAME, "device_logs_ewm");

        int recordCount = 5;

        try (Table table = conn.getTable(tablePath)) {
            // Write base rows
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                GenericRow baseRow =
                        row(
                                "device-" + i,
                                "10.0.0." + i,
                                "payload-" + i,
                                (long) (1000 + i),
                                null,
                                null);
                writer.append(baseRow).get();
            }

            // Read rows to ensure they are committed
            LogScanner scanner = createLogScanner(table);
            subscribeFromBeginning(scanner, table);
            int readCount = 0;
            while (readCount < recordCount) {
                readCount += scanner.poll(Duration.ofSeconds(1)).count();
            }

            // Get the LogTablet for bucket 0 via direct server access
            TableBucket tableBucket = new TableBucket(tableId, 0);
            LogTablet logTablet = getLeaderLogTablet(tableBucket);

            // Register the enrichment column group
            logTablet.registerColumnGroupIfAbsent("enriched");

            // Verify EWM starts at -1
            assertThat(logTablet.getEnrichmentWatermark("enriched")).isEqualTo(-1L);

            // Store enrichment for offsets 0, 1, 2
            for (int i = 0; i < 3; i++) {
                GenericRow enrichment =
                        GenericRow.of(
                                BinaryString.fromString("US-WEST-" + i), (double) (0.5 + i * 0.1));
                logTablet.putEnrichment("enriched", i, enrichment);
            }

            // Advance EWM to 3 (offsets 0, 1, 2 are enriched)
            logTablet.advanceEnrichmentWatermark("enriched", 3);
            assertThat(logTablet.getEnrichmentWatermark("enriched")).isEqualTo(3L);

            // Verify enrichment data can be retrieved
            ColumnGroupStore store = logTablet.getColumnGroupStore("enriched");
            assertThat(store).isNotNull();
            assertThat(store.size()).isEqualTo(3);

            GenericRow enrichRow0 = store.get(0L);
            assertThat(enrichRow0).isNotNull();
            assertThat(enrichRow0.getString(0).toString()).isEqualTo("US-WEST-0");
            assertThat(enrichRow0.getDouble(1))
                    .isCloseTo(0.5, org.assertj.core.data.Offset.offset(0.001));

            GenericRow enrichRow2 = store.get(2L);
            assertThat(enrichRow2).isNotNull();
            assertThat(enrichRow2.getString(0).toString()).isEqualTo("US-WEST-2");
            assertThat(enrichRow2.getDouble(1))
                    .isCloseTo(0.7, org.assertj.core.data.Offset.offset(0.001));

            // Offsets 3 and 4 should have no enrichment
            assertThat(store.get(3L)).isNull();
            assertThat(store.get(4L)).isNull();

            // Enrich remaining offsets
            for (int i = 3; i < recordCount; i++) {
                GenericRow enrichment =
                        GenericRow.of(
                                BinaryString.fromString("US-EAST-" + i), (double) (0.9 + i * 0.05));
                logTablet.putEnrichment("enriched", i, enrichment);
            }
            logTablet.advanceEnrichmentWatermark("enriched", recordCount);

            // Verify EWM advanced
            assertThat(logTablet.getEnrichmentWatermark("enriched")).isEqualTo((long) recordCount);
            assertThat(store.size()).isEqualTo(recordCount);

            // Verify all enrichment watermarks
            Map<String, Long> allEwm = logTablet.getAllEnrichmentWatermarks();
            assertThat(allEwm).containsEntry("enriched", (long) recordCount);

            // Verify range query
            assertThat(store.getRange(0, 3)).hasSize(3);
            assertThat(store.getRange(2, 5)).hasSize(3);
        }
    }

    @Test
    void testMultipleColumnGroups() throws Exception {
        // Schema with two separate enrichment groups
        Schema multiGroupSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.STRING())
                        .column("geo", DataTypes.STRING())
                        .columnGroup("geo_enriched")
                        .column("fraud_score", DataTypes.DOUBLE())
                        .columnGroup("fraud_enriched")
                        .build();

        // Verify two independent groups
        Map<String, List<Integer>> groups = multiGroupSchema.getColumnGroups();
        assertThat(groups).hasSize(2);
        assertThat(groups.get("geo_enriched")).containsExactly(2);
        assertThat(groups.get("fraud_enriched")).containsExactly(3);
        assertThat(multiGroupSchema.getDefaultGroupColumnIndices()).containsExactly(0, 1);
    }

    @Test
    void testSchemaJsonRoundTrip() {
        // Verify column group metadata survives JSON serialization
        byte[] json = SCHEMA.toJsonBytes();
        Schema deserialized = Schema.fromJsonBytes(json);

        assertThat(deserialized.getColumnGroups()).isEqualTo(SCHEMA.getColumnGroups());
        assertThat(deserialized.getColumn("geo_region").getColumnGroup()).hasValue("enriched");
        assertThat(deserialized.getColumn("device_id").getColumnGroup()).isEmpty();
    }

    @Test
    void testAppendColumnsOverRpcAdvancesEwm() throws Exception {
        TablePath tablePath = TablePath.of(DB_NAME, "device_logs_rpc_columns");
        long tableId = createTable(tablePath, TABLE_DESCRIPTOR, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        int recordCount = 4;
        TableBucket bucket = new TableBucket(tableId, 0);

        try (Table table = conn.getTable(tablePath)) {
            // Phase 1: append base rows so source offsets exist.
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                GenericRow baseRow =
                        row(
                                "device-" + i,
                                "10.0.0." + i,
                                "payload-" + i,
                                (long) (1000 + i),
                                null,
                                null);
                writer.append(baseRow).get();
            }
            // Drain the log so we know offsets are durable.
            try (LogScanner scanner = createLogScanner(table)) {
                subscribeFromBeginning(scanner, table);
                int read = 0;
                while (read < recordCount) {
                    read += scanner.poll(Duration.ofSeconds(1)).count();
                }
            }

            // Phase 2: enrich offsets 0, 1, 2 (skip 3) over the wire.
            for (int i = 0; i < 3; i++) {
                GenericRow enrichmentRow =
                        GenericRow.of(
                                BinaryString.fromString("US-WEST-" + i), (double) (0.5 + i * 0.1));
                AppendColumnsResult result =
                        writer.appendColumns("enriched", bucket, i, enrichmentRow).get();
                // After contiguous fill of [0..i], EWM should be i+1.
                assertThat(result.getEnrichmentWatermark()).isEqualTo(i + 1L);
            }

            // Verify the leader's in-memory store reflects what the RPC delivered.
            LogTablet leader = getLeaderLogTablet(bucket);
            ColumnGroupStore store = leader.getColumnGroupStore("enriched");
            assertThat(store).isNotNull();
            assertThat(store.size()).isEqualTo(3);
            assertThat(store.getEnrichmentWatermark()).isEqualTo(3L);
            assertThat(store.get(0L).getString(0).toString()).isEqualTo("US-WEST-0");
            assertThat(store.get(2L).getDouble(1))
                    .isCloseTo(0.7, org.assertj.core.data.Offset.offset(0.001));

            // Phase 3: enrich a non-contiguous offset (3 is missing, jump to... wait, 3 is next
            // contiguous). Skip 3, enrich nothing else: send a duplicate to confirm idempotence
            // of EWM advancement. Then fill 3 to confirm EWM jumps past it.
            GenericRow lateRow = GenericRow.of(BinaryString.fromString("US-EAST-3"), 1.05);
            AppendColumnsResult fillThree =
                    writer.appendColumns("enriched", bucket, 3L, lateRow).get();
            assertThat(fillThree.getEnrichmentWatermark()).isEqualTo(4L);
            assertThat(store.size()).isEqualTo(4);
        }
    }

    @Test
    void testAppendColumnsRejectsOutOfOrder() throws Exception {
        TablePath tablePath = TablePath.of(DB_NAME, "device_logs_strict_order");
        long tableId = createTable(tablePath, TABLE_DESCRIPTOR, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        int recordCount = 4;
        TableBucket bucket = new TableBucket(tableId, 0);

        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                writer.append(
                                row(
                                        "device-" + i,
                                        "10.0.0." + i,
                                        "payload-" + i,
                                        (long) (1000 + i),
                                        null,
                                        null))
                        .get();
            }
            try (LogScanner scanner = createLogScanner(table)) {
                subscribeFromBeginning(scanner, table);
                int read = 0;
                while (read < recordCount) {
                    read += scanner.poll(Duration.ofSeconds(1)).count();
                }
            }

            // EWM starts at -1; first valid offset is 0. Skipping to 1 must fail.
            GenericRow enrich = GenericRow.of(BinaryString.fromString("US-WEST-1"), 0.6);
            assertThatThrownBy(() -> writer.appendColumns("enriched", bucket, 1L, enrich).get())
                    .hasMessageContaining("Out-of-order column-group write")
                    .hasMessageContaining("expected source_offset 0");

            // Filling 0 first then 2 (skipping 1) must also fail on the second call.
            writer.appendColumns(
                            "enriched",
                            bucket,
                            0L,
                            GenericRow.of(BinaryString.fromString("US-WEST-0"), 0.5))
                    .get();
            GenericRow skipOne = GenericRow.of(BinaryString.fromString("US-WEST-2"), 0.7);
            assertThatThrownBy(() -> writer.appendColumns("enriched", bucket, 2L, skipOne).get())
                    .hasMessageContaining("Out-of-order column-group write")
                    .hasMessageContaining("expected source_offset 1");

            // EWM advanced exactly once (only offset 0 was successfully enriched).
            LogTablet leader = getLeaderLogTablet(bucket);
            assertThat(leader.getEnrichmentWatermark("enriched")).isEqualTo(1L);
        }
    }

    @Test
    void testProjectionGatedVisibility() throws Exception {
        TablePath tablePath = TablePath.of(DB_NAME, "device_logs_gate");
        long tableId = createTable(tablePath, TABLE_DESCRIPTOR, false);
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        int recordCount = 6;
        TableBucket bucket = new TableBucket(tableId, 0);

        try (Table table = conn.getTable(tablePath)) {
            // Append 6 base rows so offsets 0..5 exist.
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < recordCount; i++) {
                GenericRow baseRow =
                        row(
                                "device-" + i,
                                "10.0.0." + i,
                                "payload-" + i,
                                (long) (1000 + i),
                                null,
                                null);
                writer.append(baseRow).get();
            }
            // Drain via a non-projecting scanner so we know all 6 rows are durable.
            try (LogScanner scanner = createLogScanner(table)) {
                subscribeFromBeginning(scanner, table);
                int read = 0;
                while (read < recordCount) {
                    read += scanner.poll(Duration.ofSeconds(1)).count();
                }
            }

            // Enrich offsets 0, 1, 2 only — advance EWM to 3.
            for (int i = 0; i < 3; i++) {
                GenericRow enrichmentRow =
                        GenericRow.of(
                                BinaryString.fromString("US-WEST-" + i), (double) (0.5 + i * 0.1));
                AppendColumnsResult result =
                        writer.appendColumns("enriched", bucket, i, enrichmentRow).get();
                assertThat(result.getEnrichmentWatermark()).isEqualTo(i + 1L);
            }

            LogTablet leader = getLeaderLogTablet(bucket);
            assertThat(leader.getEnrichmentWatermark("enriched")).isEqualTo(3L);

            // Sanity: a non-projecting scanner still sees all 6 (option-(b) gate semantics).
            try (LogScanner fullScanner = createLogScanner(table)) {
                subscribeFromBeginning(fullScanner, table);
                int seen = 0;
                long deadline = System.currentTimeMillis() + 5_000L;
                while (seen < recordCount && System.currentTimeMillis() < deadline) {
                    seen += fullScanner.poll(Duration.ofSeconds(1)).count();
                }
                assertThat(seen).isEqualTo(recordCount);
            }

            // Projecting scanner that includes the "enriched" group's geo_region (idx 4)
            // must see only offsets [0..2] (capped by EWM=3).
            int[] projection = new int[] {0, 4};
            try (LogScanner gatedScanner = createLogScanner(table, projection)) {
                subscribeFromBeginning(gatedScanner, table);
                List<ScanRecord> seen = new ArrayList<>();
                long deadline = System.currentTimeMillis() + 5_000L;
                while (seen.size() < 3 && System.currentTimeMillis() < deadline) {
                    ScanRecords records = gatedScanner.poll(Duration.ofSeconds(1));
                    for (ScanRecord r : records) {
                        seen.add(r);
                    }
                }
                // Try one more poll to make sure no extras dribble in.
                ScanRecords extra = gatedScanner.poll(Duration.ofMillis(500));
                for (ScanRecord r : extra) {
                    seen.add(r);
                }
                assertThat(seen).hasSize(3);
                for (int i = 0; i < 3; i++) {
                    assertThat(seen.get(i).logOffset()).isEqualTo(i);
                }
            }
        }
    }

    /** Find the leader LogTablet for a given table bucket across all tablet servers. */
    private LogTablet getLeaderLogTablet(TableBucket tableBucket) {
        for (TabletServer ts : FLUSS_CLUSTER_EXTENSION.getTabletServers()) {
            ReplicaManager replicaManager = ts.getReplicaManager();
            ReplicaManager.HostedReplica hosted = replicaManager.getReplica(tableBucket);
            if (hosted instanceof ReplicaManager.OnlineReplica) {
                Replica replica = ((ReplicaManager.OnlineReplica) hosted).getReplica();
                if (replica.isLeader()) {
                    return replica.getLogTablet();
                }
            }
        }
        throw new IllegalStateException("No leader LogTablet found for bucket " + tableBucket);
    }
}
