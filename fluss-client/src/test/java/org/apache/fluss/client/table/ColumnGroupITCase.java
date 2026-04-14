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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for column groups. Verifies that enrichment columns can be written at existing
 * offsets and merged on read.
 */
class ColumnGroupITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initConfig())
                    .build();

    private Connection conn;
    private Admin admin;

    @BeforeEach
    void setup() throws Exception {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterEach
    void teardown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    void testColumnGroupSchemaDefinition() {
        Schema schema =
                Schema.newBuilder()
                        .column("device_id", DataTypes.STRING())
                        .column("ip", DataTypes.STRING())
                        .column("geo_region", DataTypes.STRING())
                        .columnGroup("enriched")
                        .column("risk_score", DataTypes.DOUBLE())
                        .columnGroup("enriched")
                        .build();

        assertThat(schema.getColumnGroupNames()).containsExactly("enriched");
        assertThat(schema.getColumnGroups().get("enriched")).containsExactly(2, 3);
        assertThat(schema.getDefaultGroupColumnIndices()).containsExactly(0, 1);

        // Verify column group is preserved in Column
        assertThat(schema.getColumns().get(0).getColumnGroup()).isNotPresent();
        assertThat(schema.getColumns().get(1).getColumnGroup()).isNotPresent();
        assertThat(schema.getColumns().get(2).getColumnGroup()).hasValue("enriched");
        assertThat(schema.getColumns().get(3).getColumnGroup()).hasValue("enriched");
    }

    @Test
    void testColumnGroupEnrichmentMerge() throws Exception {
        // 1. Create table with column groups
        Schema schema =
                Schema.newBuilder()
                        .column("device_id", DataTypes.STRING())
                        .column("ip", DataTypes.STRING())
                        .column("payload", DataTypes.STRING())
                        .column("geo_region", DataTypes.STRING())
                        .columnGroup("enriched")
                        .column("risk_score", DataTypes.DOUBLE())
                        .columnGroup("enriched")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();

        TablePath tablePath = TablePath.of("test_db", "test_column_groups");
        admin.createDatabase("test_db", DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(tablePath, descriptor, false).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();

        // Wait for table to be ready
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        try (Table table = conn.getTable(tablePath)) {
            // 2. Write base rows (enrichment columns will be NULL)
            AppendWriter writer = table.newAppend().createWriter();

            GenericRow baseRow1 = new GenericRow(5);
            baseRow1.setField(0, BinaryString.fromString("dev-001"));
            baseRow1.setField(1, BinaryString.fromString("192.168.1.1"));
            baseRow1.setField(2, BinaryString.fromString("sensor-data-1"));
            // geo_region and risk_score are null
            writer.append(baseRow1).get();

            GenericRow baseRow2 = new GenericRow(5);
            baseRow2.setField(0, BinaryString.fromString("dev-002"));
            baseRow2.setField(1, BinaryString.fromString("10.0.0.1"));
            baseRow2.setField(2, BinaryString.fromString("sensor-data-2"));
            writer.append(baseRow2).get();

            // 3. Write enrichment columns at existing offsets via client API (RPC)
            // Enrich offset 0: geo_region="us-west-2", risk_score=0.85
            GenericRow enrichRow0 = new GenericRow(2);
            enrichRow0.setField(0, BinaryString.fromString("us-west-2"));
            enrichRow0.setField(1, 0.85);
            writer.appendColumns("enriched", 0, 0L, enrichRow0).get();

            // Enrich offset 1: geo_region="eu-central-1", risk_score=0.32
            GenericRow enrichRow1 = new GenericRow(2);
            enrichRow1.setField(0, BinaryString.fromString("eu-central-1"));
            enrichRow1.setField(1, 0.32);
            writer.appendColumns("enriched", 0, 1L, enrichRow1).get();

            // 4. Read back and verify merged rows
            LogScanner scanner = table.newScan().createLogScanner();
            scanner.subscribeFromBeginning(0);

            List<InternalRow> rows = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (rows.size() < 2 && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = scanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : scanRecords) {
                    rows.add(record.getRow());
                }
            }

            assertThat(rows).hasSize(2);

            // Verify first row: base + enrichment merged
            InternalRow row0 = rows.get(0);
            assertThat(row0.getString(0).toString()).isEqualTo("dev-001");
            assertThat(row0.getString(1).toString()).isEqualTo("192.168.1.1");
            assertThat(row0.getString(2).toString()).isEqualTo("sensor-data-1");
            assertThat(row0.getString(3).toString()).isEqualTo("us-west-2");
            assertThat(row0.getDouble(4)).isEqualTo(0.85);

            // Verify second row: base + enrichment merged
            InternalRow row1 = rows.get(1);
            assertThat(row1.getString(0).toString()).isEqualTo("dev-002");
            assertThat(row1.getString(1).toString()).isEqualTo("10.0.0.1");
            assertThat(row1.getString(2).toString()).isEqualTo("sensor-data-2");
            assertThat(row1.getString(3).toString()).isEqualTo("eu-central-1");
            assertThat(row1.getDouble(4)).isEqualTo(0.32);
        }
    }

    /**
     * Tests the full enrichment pipeline pattern: edge device writes base columns, enrichment job
     * reads records with offsets, performs lookup, and writes enrichment columns back at those
     * offsets via the client API.
     */
    @Test
    void testEnrichmentPipeline() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("sensor_id", DataTypes.STRING())
                        .column("temperature", DataTypes.DOUBLE())
                        .column("location", DataTypes.STRING())
                        .columnGroup("enriched")
                        .column("alert_level", DataTypes.INT())
                        .columnGroup("enriched")
                        .build();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();

        TablePath tablePath = TablePath.of("test_db", "test_enrichment_pipeline");
        admin.createDatabase("test_db", DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(tablePath, descriptor, false).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();

            // === Stage 1: Edge devices write sensor data ===
            for (int i = 0; i < 5; i++) {
                GenericRow row = new GenericRow(4);
                row.setField(0, BinaryString.fromString("sensor-" + i));
                row.setField(1, 20.0 + i * 0.5);
                // enrichment columns null initially
                writer.append(row).get();
            }

            // === Stage 2: Enrichment job reads base records ===
            LogScanner enrichmentScanner = table.newScan().createLogScanner();
            enrichmentScanner.subscribeFromBeginning(0);

            List<long[]> offsetsAndBuckets = new ArrayList<>();
            List<Double> temperatures = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (offsetsAndBuckets.size() < 5 && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = enrichmentScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : scanRecords) {
                    offsetsAndBuckets.add(new long[] {record.logOffset(), 0});
                    temperatures.add(record.getRow().getDouble(1));
                }
            }
            assertThat(offsetsAndBuckets).hasSize(5);

            // === Stage 3: Enrichment job writes back enrichment columns ===
            // Simulate enrichment: determine location and alert level from temperature
            for (int i = 0; i < offsetsAndBuckets.size(); i++) {
                long offset = offsetsAndBuckets.get(i)[0];
                double temp = temperatures.get(i);

                GenericRow enrichRow = new GenericRow(2);
                enrichRow.setField(
                        0, BinaryString.fromString(temp > 21.0 ? "factory-floor" : "cold-room"));
                enrichRow.setField(1, temp > 21.5 ? 2 : (temp > 21.0 ? 1 : 0));

                writer.appendColumns("enriched", 0, offset, enrichRow).get();
            }

            // === Stage 4: Consumer reads fully enriched records ===
            LogScanner consumerScanner = table.newScan().createLogScanner();
            consumerScanner.subscribeFromBeginning(0);

            List<InternalRow> enrichedRows = new ArrayList<>();
            deadline = System.currentTimeMillis() + 10_000;
            while (enrichedRows.size() < 5 && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = consumerScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : scanRecords) {
                    enrichedRows.add(record.getRow());
                }
            }

            assertThat(enrichedRows).hasSize(5);

            // Verify all records have enrichment data
            for (int i = 0; i < 5; i++) {
                InternalRow row = enrichedRows.get(i);
                assertThat(row.getString(0).toString()).isEqualTo("sensor-" + i);

                double temp = 20.0 + i * 0.5;
                assertThat(row.getDouble(1)).isEqualTo(temp);

                // Enrichment columns should be filled
                String expectedLocation = temp > 21.0 ? "factory-floor" : "cold-room";
                assertThat(row.getString(2).toString()).isEqualTo(expectedLocation);

                int expectedAlert = temp > 21.5 ? 2 : (temp > 21.0 ? 1 : 0);
                assertThat(row.getInt(3)).isEqualTo(expectedAlert);
            }
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS, 1);
        return conf;
    }
}
