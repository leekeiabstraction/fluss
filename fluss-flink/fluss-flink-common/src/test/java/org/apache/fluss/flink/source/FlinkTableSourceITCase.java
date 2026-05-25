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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendColumnsResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.clock.ManualClock;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertQueryResultExactOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.flink.utils.FlinkTestBase.waitUntilPartitions;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRows;
import static org.apache.fluss.flink.utils.FlinkTestBase.writeRowsToPartition;
import static org.apache.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT case for using flink sql to read fluss table. */
abstract class FlinkTableSourceITCase extends AbstractTestBase {
    protected static final ManualClock CLOCK = new ManualClock();

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new Configuration()
                                    // not to clean snapshots for test purpose
                                    .set(
                                            ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS,
                                            Integer.MAX_VALUE))
                    .setNumOfTabletServers(3)
                    .setClock(CLOCK)
                    .build();

    static final String CATALOG_NAME = "testcatalog";
    static final String DEFAULT_DB = "defaultdb";
    protected StreamExecutionEnvironment execEnv;
    protected StreamTableEnvironment tEnv;
    protected static Connection conn;
    protected static Admin admin;

    protected static Configuration clientConf;
    protected static String bootstrapServers;

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        bootstrapServers = FLUSS_CLUSTER_EXTENSION.getBootstrapServers();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    void before() {
        // initialize env and table env
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());

        // initialize catalog and database
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, ConfigOptions.BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @Test
    public void testCreateTableLike() throws Exception {
        tEnv.executeSql(
                        "CREATE TEMPORARY TABLE Orders (\n"
                                + "a int not null primary key not enforced, "
                                + "b varchar"
                                + ") WITH ( \n"
                                + "    'connector' = 'datagen',\n"
                                + "    'rows-per-second' = '10'"
                                + ");")
                .await();
        tEnv.executeSql("create table like_test LIKE Orders (EXCLUDING OPTIONS)").await();
        TablePath tablePath = TablePath.of(DEFAULT_DB, "like_test");

        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        // write records
        writeRows(conn, tablePath, rows, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");

        assertResultsIgnoreOrder(
                tEnv.executeSql("select * from like_test").collect(), expectedRows, true);
    }

    @Test
    void testPkTableReadOnlySnapshot() throws Exception {
        tEnv.executeSql(
                "create table read_snapshot_test (a int not null primary key not enforced, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "read_snapshot_test");

        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        // write records
        writeRows(conn, tablePath, rows, false);

        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");

        assertResultsIgnoreOrder(
                tEnv.executeSql(
                                // the options is just used to check option with prefix 'client.fs'
                                // should
                                // pass Flink validation
                                "select * from read_snapshot_test /*+ OPTIONS('client.fs.oss.endpoint' = 'test') */")
                        .collect(),
                expectedRows,
                true);
    }

    @Test
    void testNonPkTableRead() throws Exception {
        tEnv.executeSql("create table non_pk_table_test (a int, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "non_pk_table_test");

        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        // write records
        writeRows(conn, tablePath, rows, true);

        List<String> expected = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");
        assertQueryResultExactOrder(tEnv, "select * from non_pk_table_test", expected);
    }

    @Test
    void testColumnGroupTableViaFlinkDdl() throws Exception {
        // Phase K.3: column groups declared via Flink CREATE TABLE WITH (...) DDL produce a
        // server-side schema identical to one declared via the Java admin API. Verifies the
        // round-trip: parse `column-groups.<g>` on createTable, internalize into the schema,
        // re-synthesize the property on getTable so SHOW CREATE TABLE preserves the DDL.
        tEnv.executeSql(
                "create table cg_ddl ("
                        + "  device_id int, "
                        + "  payload string, "
                        + "  geo_region string, "
                        + "  risk_score double"
                        + ") with ('column-groups.enriched' = 'geo_region, risk_score',"
                        + " 'bucket.key' = 'device_id', 'bucket.num' = '1')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "cg_ddl");
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        TableBucket bucket = new TableBucket(tableId, 0);

        // The Fluss schema should now have an "enriched" group with both columns.
        Map<String, List<Integer>> groups =
                admin.getTableInfo(tablePath).get().getSchema().getColumnGroups();
        assertThat(groups).containsOnlyKeys("enriched");
        assertThat(groups.get("enriched")).containsExactly(2, 3);

        // Write 10 base + enrich first 5 — same setup as Phase I.2.
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 10; i++) {
                writer.append(row(i, "p" + i, null, null)).get();
            }
            writer.flush();
            for (int i = 0; i < 5; i++) {
                writer.appendColumns(
                                "enriched",
                                bucket,
                                i,
                                GenericRow.of(
                                        BinaryString.fromString("US-WEST-" + i), 0.5 + i * 0.1))
                        .get();
            }
        }
        retry(
                java.time.Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .waitAndGetLeaderReplica(bucket)
                                                .getLogTablet()
                                                .getCommittedEnrichmentWatermark("enriched"))
                                .isEqualTo(5L));

        // Explicit enrichment projection: gated at CEW = 5, merged values present.
        assertQueryResultExactOrder(
                tEnv,
                "select device_id, geo_region from cg_ddl",
                Arrays.asList(
                        "+I[0, US-WEST-0]",
                        "+I[1, US-WEST-1]",
                        "+I[2, US-WEST-2]",
                        "+I[3, US-WEST-3]",
                        "+I[4, US-WEST-4]"));

        // Round-trip: SHOW CREATE TABLE re-emits the column-groups.<g> property.
        try (CloseableIterator<Row> showIter =
                tEnv.executeSql("show create table cg_ddl").collect()) {
            assertThat(showIter.hasNext()).isTrue();
            String ddl = (String) showIter.next().getField(0);
            assertThat(ddl)
                    .as("SHOW CREATE TABLE output: %s", ddl)
                    .contains("'column-groups.enriched'")
                    .containsAnyOf("geo_region, risk_score", "geo_region,risk_score");
        }
    }

    @Test
    void testColumnGroupTableViaFlinkDdlRejectsMalformed() {
        // Phase K.3 negative cases — every malformed CREATE TABLE must throw at create time,
        // not later at write/read time. The ValidationException is wrapped by the Flink
        // catalog layer; assert against the root-cause message rather than the top one.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_empty_value (a int, b string)"
                                                + " with ('column-groups.x' = '')"))
                .getRootCause()
                .hasMessageContaining("must list at least one column");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_unknown_col (a int, b string)"
                                                + " with ('column-groups.x' = 'no_such_col')"))
                .getRootCause()
                .hasMessageContaining("does not exist");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_dup (a int, b string)"
                                                + " with ('column-groups.x' = 'a, a')"))
                .getRootCause()
                .hasMessageContaining("listed twice");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_two_groups (a int, b string)"
                                                + " with ('column-groups.x' = 'a',"
                                                + " 'column-groups.y' = 'a')"))
                .getRootCause()
                .hasMessageContaining("multiple column groups");

        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_pk (a int, b string,"
                                                + " primary key (a) not enforced)"
                                                + " with ('column-groups.x' = 'b')"))
                .getRootCause()
                .hasMessageContaining("not supported on tables with a primary key");

        // Phase M.3: partition key inside a non-default group is rejected at CREATE TABLE.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_pkey_in_group ("
                                                + "  dt string, device_id int, geo_region string"
                                                + ") partitioned by (dt) "
                                                + "with ('column-groups.enriched' = 'dt')"))
                .getRootCause()
                .hasMessageContaining("Partition keys must belong to the default group");
    }

    @Test
    void testPartitionedColumnGroupTableCreate() throws Exception {
        // Phase M.3: declaring a partitioned table whose partition key stays in the default
        // group and whose enrichment group covers other columns succeeds. (Read/write paths
        // are exercised in later M phases.)
        tEnv.executeSql(
                "create table cg_partitioned ("
                        + "  dt string, device_id int, payload string,"
                        + "  geo_region string, risk_score double"
                        + ") partitioned by (dt) with ("
                        + " 'column-groups.enriched' = 'geo_region, risk_score',"
                        + " 'bucket.key' = 'device_id', 'bucket.num' = '1')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "cg_partitioned");
        org.apache.fluss.metadata.TableInfo info = admin.getTableInfo(tablePath).get();
        assertThat(info.getPartitionKeys()).containsExactly("dt");
        assertThat(info.getSchema().getColumnGroups()).containsOnlyKeys("enriched");
        assertThat(info.getSchema().getColumnGroups().get("enriched")).containsExactly(3, 4);
    }

    @Test
    void testAutoPartitionedColumnGroupTable() throws Exception {
        // Mirrors the FIP "SQL DDL — declare column groups" snippet: a partitioned base
        // table with auto-partition enabled and two independent column groups. Verifies
        // the DDL parses, the schema captures both groups at the correct column indices,
        // the partition key stays in the default group, auto-partition pre-creates
        // partitions, and SHOW CREATE TABLE preserves the relevant properties.
        tEnv.executeSql(
                "create table device_logs ("
                        + "  dt string, device_id string, ip string, payload string,"
                        + "  geo_region string, risk_score double, risk_classification string"
                        + ") partitioned by (dt) with ("
                        + "  'bucket.num' = '16',"
                        + "  'bucket.key' = 'device_id',"
                        + "  'table.auto-partition.enabled' = 'true',"
                        + "  'table.auto-partition.time-unit' = 'DAY',"
                        + "  'column-groups.enriched_geo' = 'geo_region',"
                        + "  'column-groups.enriched_risk' = 'risk_score, risk_classification')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "device_logs");
        org.apache.fluss.metadata.TableInfo info = admin.getTableInfo(tablePath).get();

        // dt is the partition key and stays in the default group (the invariant —
        // partition keys must not appear in any named column group).
        assertThat(info.getPartitionKeys()).containsExactly("dt");

        // 0=dt, 1=device_id, 2=ip, 3=payload, 4=geo_region, 5=risk_score, 6=risk_classification.
        Map<String, List<Integer>> groups = info.getSchema().getColumnGroups();
        assertThat(groups).containsOnlyKeys("enriched_geo", "enriched_risk");
        assertThat(groups.get("enriched_geo")).containsExactly(4);
        assertThat(groups.get("enriched_risk")).containsExactly(5, 6);

        // Auto-partition pre-creates partitions (default num-precreate = 2 → today + tomorrow).
        FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);

        // SHOW CREATE TABLE round-trip preserves both groups and the auto-partition setting.
        try (CloseableIterator<Row> showIter =
                tEnv.executeSql("show create table device_logs").collect()) {
            assertThat(showIter.hasNext()).isTrue();
            String ddl = (String) showIter.next().getField(0);
            assertThat(ddl)
                    .as("SHOW CREATE TABLE output: %s", ddl)
                    .contains("'column-groups.enriched_geo'")
                    .contains("'column-groups.enriched_risk'")
                    .contains("'table.auto-partition.enabled'");
        }
    }

    @Test
    void testEnrichmentSinkPartitionedTarget() throws Exception {
        // Phase M.5: end-to-end enrichment writes against a partitioned column-group target.
        // Sink row layout grows by one leading STRING column (partition name). The writer
        // resolves partition name → partition ID via cached Admin lookup.
        tEnv.executeSql(
                "create table cg_m5_base ("
                        + "  dt string, device_id int, payload string,"
                        + "  geo_region string, risk_score double,"
                        + "  _partition string metadata from 'partition' virtual,"
                        + "  _bucket bigint metadata from 'bucket' virtual,"
                        + "  _offset bigint metadata from 'offset' virtual"
                        + ") partitioned by (dt) with ("
                        + " 'column-groups.enriched' = 'geo_region, risk_score',"
                        + " 'bucket.key' = 'device_id', 'bucket.num' = '1')");
        TablePath basePath = TablePath.of(DEFAULT_DB, "cg_m5_base");
        long tableId = admin.getTableInfo(basePath).get().getTableId();

        tEnv.executeSql("alter table cg_m5_base add partition (dt='2025-02-01')");
        tEnv.executeSql("alter table cg_m5_base add partition (dt='2025-02-02')");

        // Write 2 base rows into each partition via the Java client.
        try (Table table = conn.getTable(basePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 2; i++) {
                writer.append(row("2025-02-01", i, "p" + i, null, null)).get();
            }
            for (int i = 0; i < 2; i++) {
                writer.append(row("2025-02-02", i, "q" + i, null, null)).get();
            }
            writer.flush();
        }

        // Declare a partitioned write-only enrichment table: leading STRING for partition.
        tEnv.executeSql(
                "create table cg_m5_writes ("
                        + "  src_partition string, src_bucket bigint, src_offset bigint,"
                        + "  geo_region string, risk_score double"
                        + ") with ('enrichment.target' = 'cg_m5_base',"
                        + " 'enrichment.group' = 'enriched')");

        org.apache.flink.table.api.TableResult insertResult =
                tEnv.executeSql(
                        "insert into cg_m5_writes "
                                + "select _partition, _bucket, _offset,"
                                + "       _partition || '/' || cast(device_id as string),"
                                + "       cast(device_id as double) / 10.0 "
                                + "from cg_m5_base");

        try {
            // Wait for CEW=2 on each partition's bucket.
            long partitionId1 = lookupPartitionId(basePath, "2025-02-01");
            long partitionId2 = lookupPartitionId(basePath, "2025-02-02");
            TableBucket bucket1 = new TableBucket(tableId, partitionId1, 0);
            TableBucket bucket2 = new TableBucket(tableId, partitionId2, 0);
            retry(
                    java.time.Duration.ofMinutes(1),
                    () -> {
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .waitAndGetLeaderReplica(bucket1)
                                                .getLogTablet()
                                                .getCommittedEnrichmentWatermark("enriched"))
                                .isEqualTo(2L);
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .waitAndGetLeaderReplica(bucket2)
                                                .getLogTablet()
                                                .getCommittedEnrichmentWatermark("enriched"))
                                .isEqualTo(2L);
                    });
        } finally {
            insertResult.getJobClient().ifPresent(jc -> jc.cancel());
        }

        // Verify enrichment values present across both partitions via SQL read.
        List<String> expected =
                Arrays.asList(
                        "+I[2025-02-01, 0, 2025-02-01/0, 0.0]",
                        "+I[2025-02-01, 1, 2025-02-01/1, 0.1]",
                        "+I[2025-02-02, 0, 2025-02-02/0, 0.0]",
                        "+I[2025-02-02, 1, 2025-02-02/1, 0.1]");
        try (org.apache.flink.util.CloseableIterator<Row> iter =
                tEnv.executeSql("select dt, device_id, geo_region, risk_score from cg_m5_base")
                        .collect()) {
            assertResultsIgnoreOrder(iter, expected, true);
        }
    }

    private long lookupPartitionId(TablePath path, String partitionName) throws Exception {
        for (org.apache.fluss.metadata.PartitionInfo info : admin.listPartitionInfos(path).get()) {
            if (info.getPartitionName().equals(partitionName)) {
                return info.getPartitionId();
            }
        }
        throw new IllegalStateException("Partition '" + partitionName + "' not found on " + path);
    }

    @Test
    void testDropPartitionOnColumnGroupTable() throws Exception {
        // Phase M.8: drop-partition releases EWM state and enrichment segments along with the
        // log-segment cleanup. Confirm:
        //   (a) Drop on a partition with base + enrichment writes succeeds.
        //   (b) listPartitionInfos no longer returns the dropped partition.
        // Test only exercises the path; the cleanup itself piggybacks on existing
        // log-segment cleanup with no new code in Phase M.
        tEnv.executeSql(
                "create table cg_m8_drop ("
                        + "  dt string, device_id int, payload string,"
                        + "  geo_region string, risk_score double"
                        + ") partitioned by (dt) with ("
                        + " 'column-groups.enriched' = 'geo_region, risk_score',"
                        + " 'bucket.key' = 'device_id', 'bucket.num' = '1')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "cg_m8_drop");
        long tableId = admin.getTableInfo(tablePath).get().getTableId();

        tEnv.executeSql("alter table cg_m8_drop add partition (dt='2025-04-01')");
        tEnv.executeSql("alter table cg_m8_drop add partition (dt='2025-04-02')");

        long pidDropped = lookupPartitionId(tablePath, "2025-04-01");

        // Write a few rows + enrichment to the partition being dropped.
        TableBucket bucket = new TableBucket(tableId, pidDropped, 0);
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 2; i++) {
                writer.append(row("2025-04-01", i, "p" + i, null, null)).get();
            }
            writer.flush();
            for (int i = 0; i < 2; i++) {
                writer.appendColumns(
                                "enriched",
                                bucket,
                                i,
                                GenericRow.of(BinaryString.fromString("US-" + i), 0.1 * i))
                        .get();
            }
            retry(
                    java.time.Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            FLUSS_CLUSTER_EXTENSION
                                                    .waitAndGetLeaderReplica(bucket)
                                                    .getLogTablet()
                                                    .getCommittedEnrichmentWatermark("enriched"))
                                    .isEqualTo(2L));
        }

        // Drop the partition via Flink DDL.
        tEnv.executeSql("alter table cg_m8_drop drop partition (dt='2025-04-01')");

        // The dropped partition is no longer listed; only the surviving one remains.
        retry(
                java.time.Duration.ofMinutes(1),
                () -> {
                    List<String> remaining = new ArrayList<>();
                    for (org.apache.fluss.metadata.PartitionInfo info :
                            admin.listPartitionInfos(tablePath).get()) {
                        remaining.add(info.getPartitionName());
                    }
                    assertThat(remaining).containsExactly("2025-04-02");
                });
    }

    @Test
    void testPartitionedColumnGroupTableMetadataColumns() throws Exception {
        // Phase M.4: bucket/offset/partition METADATA columns on a partitioned column-group
        // table. Source-side splice reads the partition name from the LogSplit (no per-record
        // catalog lookup needed) and emits it as a STRING METADATA column.
        tEnv.executeSql(
                "create table cg_partitioned_meta ("
                        + "  dt string, device_id int, payload string,"
                        + "  geo_region string, risk_score double,"
                        + "  _partition string metadata from 'partition' virtual,"
                        + "  _bucket bigint metadata from 'bucket' virtual,"
                        + "  _offset bigint metadata from 'offset' virtual"
                        + ") partitioned by (dt) with ("
                        + " 'column-groups.enriched' = 'geo_region, risk_score',"
                        + " 'bucket.key' = 'device_id', 'bucket.num' = '1')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "cg_partitioned_meta");
        long tableId = admin.getTableInfo(tablePath).get().getTableId();

        // Add two partitions and wait for them to be ready.
        tEnv.executeSql("alter table cg_partitioned_meta add partition (dt='2025-01-01')");
        tEnv.executeSql("alter table cg_partitioned_meta add partition (dt='2025-01-02')");

        // Write rows into both partitions via the Java client.
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 3; i++) {
                writer.append(row("2025-01-01", i, "p" + i, null, null)).get();
            }
            for (int i = 0; i < 2; i++) {
                writer.append(row("2025-01-02", i, "q" + i, null, null)).get();
            }
            writer.flush();
        }

        // Project (partition, bucket, offset, device_id) — assert each row carries the
        // partition name it was written to and offsets are 0-based per partition.
        // Order across partitions is not guaranteed; use ignore-order assertion.
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            expected.add(String.format("+I[2025-01-01, 0, %d, %d]", i, i));
        }
        for (int i = 0; i < 2; i++) {
            expected.add(String.format("+I[2025-01-02, 0, %d, %d]", i, i));
        }
        try (org.apache.flink.util.CloseableIterator<Row> iter =
                tEnv.executeSql(
                                "select _partition, _bucket, _offset, device_id "
                                        + "from cg_partitioned_meta")
                        .collect()) {
            assertResultsIgnoreOrder(iter, expected, true);
        }
    }

    @Test
    void testColumnGroupTableMetadataColumns() throws Exception {
        // Phase L.2: bucket/offset exposed as Flink METADATA columns. Opt-in via the user's
        // CREATE TABLE — if no METADATA columns are declared, the existing read path is
        // unchanged.
        tEnv.executeSql(
                "create table cg_metadata ("
                        + "  device_id int, "
                        + "  payload string, "
                        + "  geo_region string, "
                        + "  risk_score double,"
                        + "  _bucket bigint metadata from 'bucket' virtual,"
                        + "  _offset bigint metadata from 'offset' virtual"
                        + ") with ('column-groups.enriched' = 'geo_region, risk_score',"
                        + " 'bucket.key' = 'device_id', 'bucket.num' = '1')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "cg_metadata");
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        // Write 10 base rows. Single bucket → bucket id is always 0; offsets 0..9.
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 10; i++) {
                writer.append(row(i, "p" + i, null, null)).get();
            }
            writer.flush();
        }

        // Metadata projection in arbitrary position — outputs (_bucket, _offset, device_id).
        List<String> withMetadata = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            withMetadata.add(String.format("+I[0, %d, %d]", i, i));
        }
        assertQueryResultExactOrder(
                tEnv, "select _bucket, _offset, device_id from cg_metadata", withMetadata);

        // Regression: a query that does not request any metadata column behaves as before.
        List<String> noMetadata = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            noMetadata.add(String.format("+I[%d, p%d]", i, i));
        }
        assertQueryResultExactOrder(tEnv, "select device_id, payload from cg_metadata", noMetadata);
    }

    @Test
    void testEnrichmentSinkViaSql() throws Exception {
        // Phase L.3: end-to-end SQL enrichment pipeline (Option B). User reads base rows from
        // cg_l3_base, projects bucket/offset + computed enrichment values, and writes them to a
        // write-only enrichment-target table; the CEW on the base table advances.
        tEnv.executeSql(
                "create table cg_l3_base ("
                        + "  device_id int, "
                        + "  payload string, "
                        + "  geo_region string, "
                        + "  risk_score double,"
                        + "  _bucket bigint metadata from 'bucket' virtual,"
                        + "  _offset bigint metadata from 'offset' virtual"
                        + ") with ('column-groups.enriched' = 'geo_region, risk_score',"
                        + " 'bucket.key' = 'device_id', 'bucket.num' = '1')");
        TablePath basePath = TablePath.of(DEFAULT_DB, "cg_l3_base");
        long tableId = admin.getTableInfo(basePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        TableBucket bucket = new TableBucket(tableId, 0);

        // Write 5 base rows.
        try (Table table = conn.getTable(basePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 5; i++) {
                writer.append(row(i, "p" + i, null, null)).get();
            }
            writer.flush();
        }

        // Declare the write-only enrichment sink.
        tEnv.executeSql(
                "create table cg_l3_writes ("
                        + "  src_bucket bigint, "
                        + "  src_offset bigint, "
                        + "  geo_region string, "
                        + "  risk_score double"
                        + ") with ('enrichment.target' = 'cg_l3_base',"
                        + " 'enrichment.group' = 'enriched')");

        // SELECT from a write-only enrichment table must be rejected at plan time.
        assertThatThrownBy(() -> tEnv.executeSql("select * from cg_l3_writes"))
                .getRootCause()
                .hasMessageContaining("write-only enrichment target");

        // Run the streaming INSERT (it will write rows then keep running).
        org.apache.flink.table.api.TableResult insertResult =
                tEnv.executeSql(
                        "insert into cg_l3_writes "
                                + "select _bucket, _offset, "
                                + "       'US-' || cast(device_id as string), "
                                + "       cast(device_id as double) / 10.0 "
                                + "from cg_l3_base");

        try {
            retry(
                    java.time.Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            FLUSS_CLUSTER_EXTENSION
                                                    .waitAndGetLeaderReplica(bucket)
                                                    .getLogTablet()
                                                    .getCommittedEnrichmentWatermark("enriched"))
                                    .isEqualTo(5L));
        } finally {
            insertResult.getJobClient().ifPresent(jc -> jc.cancel());
        }

        // Verify via SELECT: enrichment cols filled on base read.
        assertQueryResultExactOrder(
                tEnv,
                "select device_id, geo_region, risk_score from cg_l3_base",
                Arrays.asList(
                        "+I[0, US-0, 0.0]",
                        "+I[1, US-1, 0.1]",
                        "+I[2, US-2, 0.2]",
                        "+I[3, US-3, 0.3]",
                        "+I[4, US-4, 0.4]"));
    }

    @Test
    void testEnrichmentSinkRejectsMalformed() throws Exception {
        // Phase L.4: validation negatives. CREATE-TABLE-time rejections (pairing, PK) come from
        // FlinkConversions.validateEnrichmentPairing; INSERT-time rejections (target/group/schema
        // mismatch) come from EnrichmentTableSink.resolveTarget.

        // Catalog-time rejections — these don't need a target table to exist.

        // (a) enrichment.target without enrichment.group.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_no_group ("
                                                + "src_bucket bigint, src_offset bigint, c string"
                                                + ") with ('enrichment.target' = 'cg_l4_base')"))
                .getRootCause()
                .hasMessageContaining("'enrichment.target' requires 'enrichment.group'");

        // (b) enrichment.group without enrichment.target.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_no_target ("
                                                + "src_bucket bigint, src_offset bigint, c string"
                                                + ") with ('enrichment.group' = 'enriched')"))
                .getRootCause()
                .hasMessageContaining("'enrichment.group' requires 'enrichment.target'");

        // (c) PRIMARY KEY on enrichment sink table.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "create table cg_bad_pk_sink ("
                                                + "src_bucket bigint not null, src_offset bigint, c string,"
                                                + " primary key (src_bucket) not enforced"
                                                + ") with ('enrichment.target' = 'cg_l4_base',"
                                                + " 'enrichment.group' = 'enriched')"))
                .getRootCause()
                .hasMessageContaining("must be log-only");

        // INSERT-time rejections — set up a real base column-group table to validate against.
        tEnv.executeSql(
                "create table cg_l4_base ("
                        + "  device_id int, payload string, "
                        + "  geo_region string, risk_score double,"
                        + "  _bucket bigint metadata from 'bucket' virtual,"
                        + "  _offset bigint metadata from 'offset' virtual"
                        + ") with ('column-groups.enriched' = 'geo_region, risk_score',"
                        + " 'bucket.key' = 'device_id', 'bucket.num' = '1')");

        // (d) enrichment.target points at a non-existent table.
        tEnv.executeSql(
                "create table cg_l4_bad_target ("
                        + "  src_bucket bigint, src_offset bigint,"
                        + "  geo_region string, risk_score double"
                        + ") with ('enrichment.target' = 'no_such_table',"
                        + " 'enrichment.group' = 'enriched')");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "insert into cg_l4_bad_target "
                                                + "select cast(0 as bigint), cast(0 as bigint),"
                                                + " cast(null as string), cast(null as double)"))
                .getRootCause()
                .hasMessageContaining("does not exist");

        // (e) target exists but group doesn't.
        tEnv.executeSql(
                "create table cg_l4_bad_group ("
                        + "  src_bucket bigint, src_offset bigint,"
                        + "  geo_region string, risk_score double"
                        + ") with ('enrichment.target' = 'cg_l4_base',"
                        + " 'enrichment.group' = 'no_such_group')");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "insert into cg_l4_bad_group "
                                                + "select cast(0 as bigint), cast(0 as bigint),"
                                                + " cast(null as string), cast(null as double)"))
                .hasMessageContaining("not declared on enrichment target");

        // (f) schema mismatch — column count.
        tEnv.executeSql(
                "create table cg_l4_bad_count ("
                        + "  src_bucket bigint, src_offset bigint,"
                        + "  geo_region string"
                        + ") with ('enrichment.target' = 'cg_l4_base',"
                        + " 'enrichment.group' = 'enriched')");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "insert into cg_l4_bad_count "
                                                + "select cast(0 as bigint), cast(0 as bigint),"
                                                + " cast(null as string)"))
                .hasMessageContaining("expects 4 columns");

        // (g) schema mismatch — value column type.
        tEnv.executeSql(
                "create table cg_l4_bad_type ("
                        + "  src_bucket bigint, src_offset bigint,"
                        + "  geo_region int, risk_score double"
                        + ") with ('enrichment.target' = 'cg_l4_base',"
                        + " 'enrichment.group' = 'enriched')");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "insert into cg_l4_bad_type "
                                                + "select cast(0 as bigint), cast(0 as bigint),"
                                                + " cast(0 as int), cast(null as double)"))
                .hasMessageContaining("does not match target group");

        // (h) leading column not BIGINT.
        tEnv.executeSql(
                "create table cg_l4_bad_leading ("
                        + "  src_bucket int, src_offset bigint,"
                        + "  geo_region string, risk_score double"
                        + ") with ('enrichment.target' = 'cg_l4_base',"
                        + " 'enrichment.group' = 'enriched')");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "insert into cg_l4_bad_leading "
                                                + "select cast(0 as int), cast(0 as bigint),"
                                                + " cast(null as string), cast(null as double)"))
                .hasMessageContaining("must be BIGINT");
    }

    @Test
    void testColumnGroupTableSqlRead() throws Exception {
        // Phase I.2: verify Flink SQL reads of a column-group table mirror direct Java-client
        // semantics — projection touching enrichment columns is gated at CEW; base-only
        // projection is gated at HW (PHASE_I §3.3).
        //
        // Setup mirrors PaimonTieringITCase#testTieringForColumnGroupLogTable: 10 base rows,
        // enrichment on offsets 0..4, so HW=10 and CEW=5.
        TablePath tablePath = TablePath.of(DEFAULT_DB, "cg_sql_read");
        String groupName = "enriched";
        // Use Java API to create the column-group table — Flink DDL has no column-group syntax
        // (PHASE_I §4.3). Column groups are server-side metadata; the Flink catalog reads
        // tableInfo.getRowType() and exposes the columns as regular Table columns.
        org.apache.fluss.metadata.Schema flussSchema =
                org.apache.fluss.metadata.Schema.newBuilder()
                        .column("device_id", org.apache.fluss.types.DataTypes.INT())
                        .column("payload", org.apache.fluss.types.DataTypes.STRING())
                        .column("geo_region", org.apache.fluss.types.DataTypes.STRING())
                        .columnGroup(groupName)
                        .column("risk_score", org.apache.fluss.types.DataTypes.DOUBLE())
                        .columnGroup(groupName)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(1, "device_id").build();
        admin.createTable(tablePath, descriptor, false).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        TableBucket bucket = new TableBucket(tableId, 0);

        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 10; i++) {
                writer.append(row(i, "p" + i, null, null)).get();
            }
            writer.flush();
            for (int i = 0; i < 5; i++) {
                GenericRow enrichmentRow =
                        GenericRow.of(BinaryString.fromString("US-WEST-" + i), 0.5 + i * 0.1);
                AppendColumnsResult result =
                        writer.appendColumns(groupName, bucket, i, enrichmentRow).get();
                assertThat(result.getEnrichmentWatermark()).isEqualTo(i + 1L);
            }
        }
        // Wait for CEW to catch up across ISR so projecting reads see all 5 enriched rows.
        retry(
                java.time.Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .waitAndGetLeaderReplica(bucket)
                                                .getLogTablet()
                                                .getCommittedEnrichmentWatermark(groupName))
                                .isEqualTo(5L));

        // Base-only projection: gated at HW = 10. All 10 rows are visible; enrichment columns
        // are not in the projection so the server merger never fires.
        List<String> baseOnlyExpected =
                Arrays.asList(
                        "+I[0, p0]",
                        "+I[1, p1]",
                        "+I[2, p2]",
                        "+I[3, p3]",
                        "+I[4, p4]",
                        "+I[5, p5]",
                        "+I[6, p6]",
                        "+I[7, p7]",
                        "+I[8, p8]",
                        "+I[9, p9]");
        assertQueryResultExactOrder(
                tEnv, "select device_id, payload from cg_sql_read", baseOnlyExpected);

        // Projection touching enrichment: gated at CEW = 5. Only 5 rows visible, with the
        // enrichment values merged in.
        List<String> enrichedProjectionExpected =
                Arrays.asList(
                        "+I[0, US-WEST-0]",
                        "+I[1, US-WEST-1]",
                        "+I[2, US-WEST-2]",
                        "+I[3, US-WEST-3]",
                        "+I[4, US-WEST-4]");
        assertQueryResultExactOrder(
                tEnv, "select device_id, geo_region from cg_sql_read", enrichedProjectionExpected);

        // SELECT * implicitly projects every column including enrichment ones. Flink's planner
        // doesn't push a projection when nothing is pruned, so for column-group tables
        // FlinkSourceSplitReader#forceFullProjectionForColumnGroups synthesizes an identity
        // projection — without that the server would return raw base-log rows with null
        // enrichment columns. Note: Flink's Row#toString rounds doubles to the shortest
        // representation, so 0.5 + 3*0.1 prints "0.8" here even though Java's
        // Double.toString yields "0.7999999999999999".
        List<String> selectStarExpected =
                Arrays.asList(
                        "+I[0, p0, US-WEST-0, 0.5]",
                        "+I[1, p1, US-WEST-1, 0.6]",
                        "+I[2, p2, US-WEST-2, 0.7]",
                        "+I[3, p3, US-WEST-3, 0.8]",
                        "+I[4, p4, US-WEST-4, 0.9]");
        assertQueryResultExactOrder(tEnv, "select * from cg_sql_read", selectStarExpected);
    }

    @Test
    void testColumnGroupTableSchemaEvolutionDuringStreamingQuery() throws Exception {
        // Phase I.4: a streaming Flink SQL query against a column-group table survives a
        // mid-query alterTable. The query keeps emitting rows under the projection that was
        // synthesized at job-start time; new columns are out of projection and not visible
        // until the job restarts. No crash, no malformed rows (PHASE_I §3.3, §4.2, §6.2).
        TablePath tablePath = TablePath.of(DEFAULT_DB, "cg_streaming_alter");
        String groupName = "enriched";
        org.apache.fluss.metadata.Schema flussSchema =
                org.apache.fluss.metadata.Schema.newBuilder()
                        .column("device_id", org.apache.fluss.types.DataTypes.INT())
                        .column("payload", org.apache.fluss.types.DataTypes.STRING())
                        .column("geo_region", org.apache.fluss.types.DataTypes.STRING())
                        .columnGroup(groupName)
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(flussSchema).distributedBy(1, "device_id").build();
        admin.createTable(tablePath, descriptor, false).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        TableBucket bucket = new TableBucket(tableId, 0);

        // Phase v1: write 5 base + enrich each one so CEW reaches 5.
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 5; i++) {
                writer.append(row(i, "p" + i, (Object) null)).get();
            }
            writer.flush();
            for (int i = 0; i < 5; i++) {
                writer.appendColumns(
                                groupName,
                                bucket,
                                i,
                                GenericRow.of(BinaryString.fromString("US-WEST-" + i)))
                        .get();
            }
        }
        retry(
                java.time.Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .waitAndGetLeaderReplica(bucket)
                                                .getLogTablet()
                                                .getCommittedEnrichmentWatermark(groupName))
                                .isEqualTo(5L));

        // Start a streaming query whose projection (synthesized at job-start) is
        // [device_id, geo_region]. Collect the first 5 rows pre-alter.
        try (CloseableIterator<Row> rowIter =
                tEnv.executeSql("select device_id, geo_region from cg_streaming_alter").collect()) {
            List<String> preAlter = collectRowsWithTimeout(rowIter, 5, false);
            assertThat(preAlter)
                    .containsExactly(
                            "+I[0, US-WEST-0]",
                            "+I[1, US-WEST-1]",
                            "+I[2, US-WEST-2]",
                            "+I[3, US-WEST-3]",
                            "+I[4, US-WEST-4]");

            // Bump the schema: add risk_score to the "enriched" group. The running query's
            // projection still references columns by their v1 indices (0 = device_id,
            // 2 = geo_region). Those indices remain stable in v2 (risk_score is appended at
            // index 3), so the query keeps working under the v1 projection — risk_score is
            // simply not visible until the user restarts the job.
            admin.alterTable(
                            tablePath,
                            Collections.singletonList(
                                    TableChange.addColumn(
                                            "risk_score",
                                            org.apache.fluss.types.DataTypes.DOUBLE(),
                                            null,
                                            TableChange.ColumnPosition.last(),
                                            groupName)),
                            false)
                    .get();

            // Phase v2: write 5 more base + 5 more enrichment under the new schema.
            try (Table table = conn.getTable(tablePath)) {
                AppendWriter writer = table.newAppend().createWriter();
                for (int i = 5; i < 10; i++) {
                    writer.append(row(i, "p" + i, null, null)).get();
                }
                writer.flush();
                for (int i = 5; i < 10; i++) {
                    writer.appendColumns(
                                    groupName,
                                    bucket,
                                    i,
                                    GenericRow.of(
                                            BinaryString.fromString("US-WEST-" + i), 0.5 + i * 0.1))
                            .get();
                }
            }
            retry(
                    java.time.Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            FLUSS_CLUSTER_EXTENSION
                                                    .waitAndGetLeaderReplica(bucket)
                                                    .getLogTablet()
                                                    .getCommittedEnrichmentWatermark(groupName))
                                    .isEqualTo(10L));

            // Collect the next 5 rows. The streaming query continues without crashing; v2
            // segments decode under the v1-shaped projection (Phase H per-(group, batchSchemaId)
            // dispatch). risk_score is not in projection, so it's not in the output rows.
            List<String> postAlter = collectRowsWithTimeout(rowIter, 5, false);
            assertThat(postAlter)
                    .containsExactly(
                            "+I[5, US-WEST-5]",
                            "+I[6, US-WEST-6]",
                            "+I[7, US-WEST-7]",
                            "+I[8, US-WEST-8]",
                            "+I[9, US-WEST-9]");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"ARROW", "INDEXED"})
    void testAppendTableProjectPushDown(String logFormat) throws Exception {
        String tableName = "append_table_project_push_down_" + logFormat;
        tEnv.executeSql(
                String.format(
                        "create table %s (a int, b varchar, c bigint, d int, e int, f bigint) with"
                                + " ('connector' = 'fluss', 'table.log.format' = '%s')",
                        tableName, logFormat));
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        List<InternalRow> rows =
                Arrays.asList(
                        row(1, "v1", 100L, 1000, 100, 1000L),
                        row(2, "v2", 200L, 2000, 200, 2000L),
                        row(3, "v3", 300L, 3000, 300, 3000L),
                        row(4, "v4", 400L, 4000, 400, 4000L),
                        row(5, "v5", 500L, 5000, 500, 5000L),
                        row(6, "v6", 600L, 6000, 600, 6000L),
                        row(7, "v7", 700L, 7000, 700, 7000L),
                        row(8, "v8", 800L, 8000, 800, 8000L),
                        row(9, "v9", 900L, 9000, 900, 9000L),
                        row(10, "v10", 1000L, 10000, 1000, 10000L));
        writeRows(conn, tablePath, rows, true);

        // projection + reorder.
        String query = "select b, d, c from " + tableName;
        // make sure the plan has pushed down the projection into source
        assertThat(tEnv.explainSql(query))
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, "
                                + tableName
                                + ", project=[b, d, c], metadata=[]]], fields=[b, d, c])");

        List<String> expected =
                Arrays.asList(
                        "+I[v1, 1000, 100]",
                        "+I[v2, 2000, 200]",
                        "+I[v3, 3000, 300]",
                        "+I[v4, 4000, 400]",
                        "+I[v5, 5000, 500]",
                        "+I[v6, 6000, 600]",
                        "+I[v7, 7000, 700]",
                        "+I[v8, 8000, 800]",
                        "+I[v9, 9000, 900]",
                        "+I[v10, 10000, 1000]");
        assertQueryResultExactOrder(tEnv, query, expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"PK_SNAPSHOT", "PK_LOG", "LOG"})
    void testTableProjectPushDown(String mode) throws Exception {
        boolean isPkTable = mode.startsWith("PK");
        boolean testPkLog = mode.equals("PK_LOG");
        String tableName = "table_" + mode;
        String pkDDL = isPkTable ? ", primary key (a) not enforced" : "";
        tEnv.executeSql(
                String.format(
                        "create table %s (a int, b varchar, c bigint, d int %s) with ('connector' = 'fluss')",
                        tableName, pkDDL));
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        List<InternalRow> rows =
                Arrays.asList(
                        row(1, "v1", 100L, 1000),
                        row(2, "v2", 200L, 2000),
                        row(3, "v3", 300L, 3000),
                        row(4, "v4", 400L, 4000),
                        row(5, "v5", 500L, 5000),
                        row(6, "v6", 600L, 6000),
                        row(7, "v7", 700L, 7000),
                        row(8, "v8", 800L, 8000),
                        row(9, "v9", 900L, 9000),
                        row(10, "v10", 1000L, 10000));

        if (isPkTable) {
            if (!testPkLog) {
                // write records and wait snapshot before collect job start,
                // to make sure reading from kv snapshot
                writeRows(conn, tablePath, rows, false);
                FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(TablePath.of(DEFAULT_DB, tableName));
            }
        } else {
            writeRows(conn, tablePath, rows, true);
        }

        String query = "select b, a, c from " + tableName;
        // make sure the plan has pushed down the projection into source
        assertThat(tEnv.explainSql(query))
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, "
                                + tableName
                                + ", project=[b, a, c], metadata=[]]], fields=[b, a, c])");

        List<String> expected =
                Arrays.asList(
                        "+I[v1, 1, 100]",
                        "+I[v2, 2, 200]",
                        "+I[v3, 3, 300]",
                        "+I[v4, 4, 400]",
                        "+I[v5, 5, 500]",
                        "+I[v6, 6, 600]",
                        "+I[v7, 7, 700]",
                        "+I[v8, 8, 800]",
                        "+I[v9, 9, 900]",
                        "+I[v10, 10, 1000]");
        org.apache.flink.util.CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();
        if (testPkLog) {
            // delay the write after collect job start,
            // to make sure reading from log instead of snapshot
            writeRows(conn, tablePath, rows, false);
        }
        int expectRecords = expected.size();
        List<String> actual = collectRowsWithTimeout(rowIter, expectRecords);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void testPkTableReadMixSnapshotAndLog() throws Exception {
        tEnv.executeSql(
                "create table mix_snapshot_log_test (a int not null primary key not enforced, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "mix_snapshot_log_test");

        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        // write records
        writeRows(conn, tablePath, rows, false);

        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");

        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from mix_snapshot_log_test").collect();
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // now, we put rows to the table again, should read the log
        expectedRows =
                Arrays.asList(
                        "-U[1, v1]",
                        "+U[1, v1]",
                        "-U[2, v2]",
                        "+U[2, v2]",
                        "-U[3, v3]",
                        "+U[3, v3]");
        writeRows(conn, tablePath, rows, false);
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testPkTableReadWithKvSnapshotLease() throws Exception {
        tEnv.executeSql(
                "create table pk_table_with_kv_snapshot_lease (a int not null primary key not enforced, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pk_table_with_kv_snapshot_lease");

        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        // write records
        writeRows(conn, tablePath, rows, false);

        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        // enable checkpoint to make sure the kv snapshot lease will be cleared.
        execEnv.enableCheckpointing(100);

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");
        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from pk_table_with_kv_snapshot_lease").collect();
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // now, we put rows to the table again, should read the log
        expectedRows =
                Arrays.asList(
                        "-U[1, v1]",
                        "+U[1, v1]",
                        "-U[2, v2]",
                        "+U[2, v2]",
                        "-U[3, v3]",
                        "+U[3, v3]");
        writeRows(conn, tablePath, rows, false);
        assertResultsIgnoreOrder(rowIter, expectedRows, true);

        // check lease will be dropped after job finished.
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zkClient.getKvSnapshotLeasesList().isEmpty()).isTrue());
    }

    @Test
    void testReadWithKvSnapshotLeaseNoCheckpoint() throws Exception {
        tEnv.executeSql(
                "create table pk_table_with_kv_snapshot_lease2 (a int not null primary key not enforced, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "pk_table_with_kv_snapshot_lease2");

        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        // write records
        writeRows(conn, tablePath, rows, false);

        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        List<String> expectedRows = Arrays.asList("+I[1, v1]", "+I[2, v2]", "+I[3, v3]");
        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from pk_table_with_kv_snapshot_lease2").collect();
        assertResultsIgnoreOrder(rowIter, expectedRows, true);

        // check lease will be dropped after job finished.
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zkClient.getKvSnapshotLeasesList().isEmpty()).isTrue());
    }

    // -------------------------------------------------------------------------------------
    // Fluss scan start mode tests
    // -------------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadLogTableWithDifferentScanStartupMode(boolean isPartitioned) throws Exception {
        String tableName = "tab1_" + (isPartitioned ? "partitioned" : "non_partitioned");
        String partitionName = null;
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        if (!isPartitioned) {
            tEnv.executeSql(
                    String.format(
                            "create table %s (a int, b varchar, c bigint, d int ) "
                                    + "with ('connector' = 'fluss')",
                            tableName));
        } else {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "a int, b varchar, c bigint, d int, p varchar"
                                    + ") partitioned by (p) "
                                    + "with ("
                                    + "'connector' = 'fluss',"
                                    + "'table.auto-partition.enabled' = 'true',"
                                    + "'table.auto-partition.time-unit' = 'year',"
                                    + "'table.auto-partition.num-precreate' = '1')",
                            tableName));
            Map<Long, String> partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1);
            // just pick one partition
            partitionName = partitionNameById.values().iterator().next();
        }
        List<InternalRow> rows1 =
                Arrays.asList(
                        rowWithPartition(new Object[] {1, "v1", 100L, 1000}, partitionName),
                        rowWithPartition(new Object[] {2, "v2", 200L, 2000}, partitionName),
                        rowWithPartition(new Object[] {3, "v3", 300L, 3000}, partitionName),
                        rowWithPartition(new Object[] {4, "v4", 400L, 4000}, partitionName),
                        rowWithPartition(new Object[] {5, "v5", 500L, 5000}, partitionName));

        writeRows(conn, tablePath, rows1, true);
        CLOCK.advanceTime(Duration.ofMillis(100L));
        long timestamp = CLOCK.milliseconds();

        List<InternalRow> rows2 =
                Arrays.asList(
                        rowWithPartition(new Object[] {6, "v6", 600L, 6000}, partitionName),
                        rowWithPartition(new Object[] {7, "v7", 700L, 7000}, partitionName),
                        rowWithPartition(new Object[] {8, "v8", 800L, 8000}, partitionName),
                        rowWithPartition(new Object[] {9, "v9", 900L, 9000}, partitionName),
                        rowWithPartition(new Object[] {10, "v10", 1000L, 10000}, partitionName));
        // for second batch, we don't wait snapshot finish.
        writeRows(conn, tablePath, rows2, true);

        // 1. read log table with scan.startup.mode='full'
        String options = " /*+ OPTIONS('scan.startup.mode' = 'full') */";
        String query = "select a, b, c, d from " + tableName + options;
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1, 100, 1000]",
                        "+I[2, v2, 200, 2000]",
                        "+I[3, v3, 300, 3000]",
                        "+I[4, v4, 400, 4000]",
                        "+I[5, v5, 500, 5000]",
                        "+I[6, v6, 600, 6000]",
                        "+I[7, v7, 700, 7000]",
                        "+I[8, v8, 800, 8000]",
                        "+I[9, v9, 900, 9000]",
                        "+I[10, v10, 1000, 10000]");
        assertQueryResultExactOrder(tEnv, query, expected);

        // 2. read kv table with scan.startup.mode='earliest'
        options = " /*+ OPTIONS('scan.startup.mode' = 'earliest') */";
        query = "select a, b, c, d from " + tableName + options;
        assertQueryResultExactOrder(tEnv, query, expected);

        // 3. read log table with scan.startup.mode='timestamp'
        expected =
                Arrays.asList(
                        "+I[6, v6, 600, 6000]",
                        "+I[7, v7, 700, 7000]",
                        "+I[8, v8, 800, 8000]",
                        "+I[9, v9, 900, 9000]",
                        "+I[10, v10, 1000, 10000]");
        options =
                String.format(
                        " /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' ='%d') */",
                        timestamp);
        query = "select a, b, c, d from " + tableName + options;
        assertQueryResultExactOrder(tEnv, query, expected);
    }

    @Test
    void testReadKvTableWithScanStartupModeEqualsFull() throws Exception {
        tEnv.executeSql(
                "create table read_full_test (a int not null primary key not enforced, b varchar)");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "read_full_test");

        List<InternalRow> rows1 =
                Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"), row(3, "v33"));

        // write records and wait generate snapshot.
        writeRows(conn, tablePath, rows1, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        List<InternalRow> rows2 = Arrays.asList(row(1, "v11"), row(2, "v22"), row(4, "v4"));

        String options = " /*+ OPTIONS('scan.startup.mode' = 'full') */";
        String query = "select a, b from read_full_test " + options;
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1]",
                        "+I[2, v2]",
                        "+I[3, v33]",
                        "-U[1, v1]",
                        "+U[1, v11]",
                        "-U[2, v2]",
                        "+U[2, v22]",
                        "+I[4, v4]");
        org.apache.flink.util.CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();
        int expectRecords = 8;
        // delay to write after collect job start, to make sure reading from log instead of
        // snapshot
        writeRows(conn, tablePath, rows2, false);
        List<String> actual = collectRowsWithTimeout(rowIter, expectRecords);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private static Stream<Arguments> readKvTableScanStartupModeArgs() {
        return Stream.of(
                Arguments.of("earliest", true),
                Arguments.of("earliest", false),
                Arguments.of("timestamp", true),
                Arguments.of("timestamp", false));
    }

    @ParameterizedTest
    @MethodSource("readKvTableScanStartupModeArgs")
    void testReadKvTableWithEarliestAndTimestampScanStartupMode(String mode, boolean isPartitioned)
            throws Exception {
        long timestamp = CLOCK.milliseconds();
        String tableName = mode + "_test_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String partitionName = null;
        if (!isPartitioned) {
            tEnv.executeSql(
                    String.format(
                            "create table %s (a int not null primary key not enforced, b varchar)",
                            tableName));
        } else {
            tEnv.executeSql(
                    String.format(
                            "create table %s (a int not null, b varchar, c varchar, primary key (a, c) NOT ENFORCED) partitioned by (c) "
                                    + "with ("
                                    + " 'table.auto-partition.enabled' = 'true',"
                                    + " 'table.auto-partition.time-unit' = 'year',"
                                    + " 'table.auto-partition.num-precreate' = '1')",
                            tableName));
            Map<Long, String> partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1);
            // just pick one partition
            partitionName = partitionNameById.values().iterator().next();
        }

        List<InternalRow> rows1 =
                Arrays.asList(
                        rowWithPartition(new Object[] {1, "v1"}, partitionName),
                        rowWithPartition(new Object[] {2, "v2"}, partitionName),
                        rowWithPartition(new Object[] {3, "v3"}, partitionName),
                        rowWithPartition(new Object[] {3, "v33"}, partitionName));

        // write records and wait generate snapshot.
        writeRows(conn, tablePath, rows1, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        CLOCK.advanceTime(Duration.ofMillis(100));

        List<InternalRow> rows2 =
                Arrays.asList(
                        rowWithPartition(new Object[] {1, "v11"}, partitionName),
                        rowWithPartition(new Object[] {2, "v22"}, partitionName),
                        rowWithPartition(new Object[] {4, "v4"}, partitionName));
        writeRows(conn, tablePath, rows2, false);
        CLOCK.advanceTime(Duration.ofMillis(100));

        String options =
                String.format(
                        " /*+ OPTIONS('scan.startup.mode' = '%s', 'scan.startup.timestamp' = '%s') */",
                        mode, timestamp);
        String query = "select a, b from " + tableName + options;
        List<String> expected =
                Arrays.asList(
                        "+I[1, v1]",
                        "+I[2, v2]",
                        "+I[3, v3]",
                        "-U[3, v3]",
                        "+U[3, v33]",
                        "-U[1, v1]",
                        "+U[1, v11]",
                        "-U[2, v2]",
                        "+U[2, v22]",
                        "+I[4, v4]");
        assertQueryResultExactOrder(tEnv, query, expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReadPrimaryKeyPartitionedTable(boolean isAutoPartition) throws Exception {
        String tableName = "read_primary_key_partitioned_table" + (isAutoPartition ? "_auto" : "");
        String createTableDdl;
        if (isAutoPartition) {
            createTableDdl =
                    String.format(
                            "create table %s"
                                    + " (a int not null, b varchar, c string, primary key (a, c) NOT ENFORCED) partitioned by (c) "
                                    + "with ('table.auto-partition.enabled' = 'true', 'table.auto-partition.time-unit' = 'year')",
                            tableName);
        } else {
            createTableDdl =
                    String.format(
                            "create table %s"
                                    + " (a int not null, b varchar, c string, primary key (a, c) NOT ENFORCED) partitioned by (c) ",
                            tableName);
        }
        tEnv.executeSql(createTableDdl);
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        // write data into partitions and wait snapshot is done
        Map<Long, String> partitionNameById;
        if (isAutoPartition) {
            partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
        } else {
            int currentYear = LocalDate.now().getYear();
            tEnv.executeSql(
                    String.format(
                            "alter table %s add partition (c = '%s')", tableName, currentYear));
            partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1);
        }

        List<String> expectedRowValues =
                writeRowsToPartition(conn, tablePath, partitionNameById.values());
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        // This test requires dynamically discovering newly created partitions, so
        // 'scan.partition.discovery.interval' needs to be set to 2s (default is 1 minute),
        // otherwise the test may hang for 1 minute.
        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(
                                String.format(
                                        "select * from %s /*+ OPTIONS('scan.partition.discovery.interval' = '2s') */",
                                        tableName))
                        .collect();
        assertResultsIgnoreOrder(rowIter, expectedRowValues, false);
        String partition1 =
                LocalDateTime.now().minusYears(1).format(DateTimeFormatter.ofPattern("yyyy"));
        String partition2 =
                LocalDateTime.now().minusYears(2).format(DateTimeFormatter.ofPattern("yyyy"));

        // then create some new partitions, and write rows to the new partitions
        tEnv.executeSql(
                String.format("alter table %s add partition (c = '%s')", tableName, partition1));
        tEnv.executeSql(
                String.format("alter table %s add partition (c = '%s')", tableName, partition2));
        // write data to the new partitions
        expectedRowValues =
                writeRowsToPartition(conn, tablePath, Arrays.asList(partition1, partition2));
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    @Test
    void testReadTimestampGreaterThanMaxTimestamp() throws Exception {
        tEnv.executeSql("create table timestamp_table (a int, b varchar) ");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "timestamp_table");

        // write first bath records
        List<InternalRow> rows = Arrays.asList(row(1, "v1"), row(2, "v2"), row(3, "v3"));

        writeRows(conn, tablePath, rows, true);
        CLOCK.advanceTime(Duration.ofMillis(100L));
        // startup time between write first and second batch records.
        long currentTimeMillis = CLOCK.milliseconds();

        // startup timestamp is larger than current time.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                                String.format(
                                                        "select * from timestamp_table /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' = '%s') */ ",
                                                        currentTimeMillis
                                                                + Duration.ofMinutes(5).toMillis()))
                                        .await())
                .hasStackTraceContaining(
                        String.format(
                                "the fetch timestamp %s is larger than the current timestamp",
                                currentTimeMillis + Duration.ofMinutes(5).toMillis()));

        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(
                                String.format(
                                        "select * from timestamp_table /*+ OPTIONS('scan.startup.mode' = 'timestamp', 'scan.startup.timestamp' = '%s') */ ",
                                        currentTimeMillis))
                        .collect();
        CLOCK.advanceTime(Duration.ofMillis(100L));
        // write second batch record.
        rows = Arrays.asList(row(4, "v4"), row(5, "v5"), row(6, "v6"));
        writeRows(conn, tablePath, rows, true);
        List<String> expected = Arrays.asList("+I[4, v4]", "+I[5, v5]", "+I[6, v6]");
        int expectRecords = expected.size();
        List<String> actual = collectRowsWithTimeout(rowIter, expectRecords);
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    // -------------------------------------------------------------------------------------
    // Fluss look source tests
    // -------------------------------------------------------------------------------------

    private static Stream<Arguments> lookupArgs() {
        return Stream.of(
                Arguments.of(Caching.ENABLE_CACHE, false),
                Arguments.of(Caching.DISABLE_CACHE, false),
                Arguments.of(Caching.ENABLE_CACHE, true),
                Arguments.of(Caching.DISABLE_CACHE, true));
    }

    /** lookup table with one pk, one join condition. */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup1PkTable(Caching caching, boolean async) throws Exception {
        String dim = prepareDimTableAndSourceTable(caching, async, new String[] {"id"}, null, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, c, h.name FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected =
                Arrays.asList("+I[1, 11, name1]", "+I[2, 2, name2]", "+I[3, 33, name3]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookupWithProjection(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(caching, async, new String[] {"name"}, null, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, c, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected =
                Arrays.asList("+I[1, 11, address5]", "+I[2, 2, address2]", "+I[10, 44, address4]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * lookup table with one pk, two join condition and one of the join condition is constant value.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup1PkTableWith2Conditions(Caching caching, boolean async) throws Exception {
        String dim = prepareDimTableAndSourceTable(caching, async, new String[] {"id"}, null, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.name FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id AND h.name = 'name3'",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Collections.singletonList("+I[3, name33, name3]");
        assertResultsIgnoreOrder(collected, expected, true);

        // project all columns from dim table
        String dimJoinQuery2 =
                String.format(
                        "SELECT a, b FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id AND h.name = 'name3'",
                        dim);

        CloseableIterator<Row> collected2 = tEnv.executeSql(dimJoinQuery2).collect();
        List<String> expected2 = Collections.singletonList("+I[3, name33]");
        assertResultsIgnoreOrder(collected2, expected2, true);
    }

    /**
     * lookup table with one pk, two join condition and one of the join condition is constant value.
     */
    @Test
    void testLookupWithFilterPushDown() throws Exception {
        String dim =
                prepareDimTableAndSourceTable(
                        Caching.DISABLE_CACHE, false, new String[] {"id"}, null, "p_date");

        Map<Long, String> partitionNameById =
                waitUntilPartitions(
                        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                        TablePath.of(DEFAULT_DB, dim));

        // pick the first partition to do filter
        String filteredPartition = partitionNameById.values().stream().sorted().iterator().next();

        String dimJoinQuery =
                String.format(
                        "SELECT a, h.id, h.name, h.address FROM src "
                                + " LEFT JOIN %s FOR SYSTEM_TIME AS OF src.proc as h "
                                + " ON src.a = h.id and src.p_date = h.p_date and h.p_date <> '%s'",
                        dim, filteredPartition);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[1, null, null, null]",
                        "+I[2, null, null, null]",
                        "+I[3, null, null, null]",
                        "+I[10, null, null, null]",
                        "+I[1, null, null, null]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * lookup table with one pk, 3 join condition on dim fields, 1st for variable non-pk, 2nd for
     * pk, 3rd for constant value.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup1PkTableWith3Conditions(Caching caching, boolean async) throws Exception {
        String dim = prepareDimTableAndSourceTable(caching, async, new String[] {"id"}, null, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, c, h.address FROM src LEFT JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name AND src.a = h.id AND h.address= 'address2'",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[1, name1, 11, null]",
                        "+I[2, name2, 2, address2]",
                        "+I[3, name33, 33, null]",
                        "+I[10, name0, 44, null]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /** lookup table with two pk, join condition contains all the pks. */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup2PkTable(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(
                        caching, async, new String[] {"id", "name"}, null, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name AND src.a = h.id",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Arrays.asList("+I[1, name1, address1]", "+I[2, name2, address2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * lookup table with two pk, but the defined key are in reserved order. The result should
     * exactly the same with {@link #testLookup2PkTable(Caching, boolean)}.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup2PkTableWithUnorderedKey(Caching caching, boolean async) throws Exception {
        // the primary key is (name, id) but the schema order is (id, name)
        String dim =
                prepareDimTableAndSourceTable(
                        caching, async, new String[] {"name", "id"}, null, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name AND src.a = h.id",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Arrays.asList("+I[1, name1, address1]", "+I[2, name2, address2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    /**
     * lookup table with two pk, only one key is in the join condition. The result should throw
     * exception.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup2PkTableWith1KeyInCondition(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(
                        caching, async, new String[] {"id", "name"}, null, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id",
                        dim);
        assertThatThrownBy(() -> tEnv.executeSql(dimJoinQuery))
                .hasStackTraceContaining(
                        "The Fluss lookup function supports lookup tables where"
                                + " the lookup keys include all primary keys or all bucket keys."
                                + " Can't find expected key 'name' in lookup keys [id]");
    }

    /**
     * lookup table with two pk, 3 join condition on dim fields, 1st for variable non-pk, 2nd for
     * pk, 3rd for constant value.
     */
    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookup2PkTableWith3Conditions(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(
                        caching, async, new String[] {"id", "name"}, null, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, h.name, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON 'name2' = h.name AND src.a = h.id AND h.address= 'address' || CAST(src.c AS STRING)",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Collections.singletonList("+I[2, name2, address2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testLookupPartitionedTable(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(caching, async, new String[] {"id"}, null, "p_date");

        String dimJoinQuery =
                String.format(
                        "SELECT a, h.name, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.a = h.id AND src.p_date = h.p_date",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Arrays.asList("+I[1, name1, address1]", "+I[2, name2, address2]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testPrefixLookup(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(
                        caching, async, new String[] {"name", "id"}, new String[] {"name"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected =
                Arrays.asList(
                        "+I[1, name1, address1]",
                        "+I[1, name1, address5]",
                        "+I[2, name2, address2]",
                        "+I[10, name0, address4]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testPrefixLookupPartitionedTable(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(
                        caching,
                        async,
                        new String[] {"name", "id"},
                        new String[] {"name"},
                        "p_date");
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name AND src.p_date = h.p_date",
                        dim);

        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Arrays.asList("+I[1, name1, address1]", "+I[1, name1, address5]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @ParameterizedTest
    @MethodSource("lookupArgs")
    void testPrefixLookupWithCondition(Caching caching, boolean async) throws Exception {
        String dim =
                prepareDimTableAndSourceTable(
                        caching, async, new String[] {"name", "id"}, new String[] {"name"}, null);
        String dimJoinQuery =
                String.format(
                        "SELECT a, b, h.address FROM src JOIN %s FOR SYSTEM_TIME AS OF src.proc as h"
                                + " ON src.b = h.name AND h.address = 'address5'",
                        dim);
        CloseableIterator<Row> collected = tEnv.executeSql(dimJoinQuery).collect();
        List<String> expected = Collections.singletonList("+I[1, name1, address5]");
        assertResultsIgnoreOrder(collected, expected, true);
    }

    @Test
    void testLookupFullCacheThrowException() {
        tEnv.executeSql(
                "create table lookup_join_throw_table"
                        + " (a int not null primary key not enforced, b varchar)"
                        + " with ('lookup.cache' = 'FULL')");
        // should throw exception
        assertThatThrownBy(() -> tEnv.executeSql("select * from lookup_join_throw_table"))
                .cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Full lookup caching is not supported yet.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLookupInsertIfNotExists(boolean async) throws Exception {
        // use single parallelism to make result ordering stable
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        String uidMappingTable = String.format("uid_mapping_%s", async ? "async" : "sync");
        // Create uid_mapping table: uid (STRING, PK), uid_int32 (INT, auto-increment)
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " uid varchar not null,"
                                + " uid_int32 int,"
                                + " primary key (uid) NOT ENFORCED"
                                + ") with ('auto-increment.fields' = 'uid_int32', 'bucket.num' = '1')",
                        uidMappingTable));

        // Create ods_table as a Flink temporary source table
        List<Row> odsData =
                Arrays.asList(
                        Row.of("CN", "Beijing", "Haidian", "2025-01-01", "user_a"),
                        Row.of("CN", "Shanghai", "Pudong", "2025-01-02", "user_b"),
                        Row.of("US", "California", "LA", "2025-01-03", "user_a"),
                        Row.of("JP", "Tokyo", "Shibuya", "2025-01-04", "user_c"));

        Schema odsSchema =
                Schema.newBuilder()
                        .column("country", DataTypes.STRING())
                        .column("prov", DataTypes.STRING())
                        .column("city", DataTypes.STRING())
                        .column("ymd", DataTypes.STRING())
                        .column("uid", DataTypes.STRING())
                        .columnByExpression("proctime", "PROCTIME()")
                        .build();
        RowTypeInfo odsTypeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {
                            Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING
                        },
                        new String[] {"country", "prov", "city", "ymd", "uid"});
        DataStream<Row> odsDs = execEnv.fromCollection(odsData).returns(odsTypeInfo);
        tEnv.dropTemporaryView("ods_src");
        tEnv.createTemporaryView("ods_src", tEnv.fromDataStream(odsDs, odsSchema));

        // Perform lookup join with insertIfNotExists hint
        String lookupAsyncOption = async ? "'lookup.async' = 'true'" : "'lookup.async' = 'false'";
        String joinQuery =
                String.format(
                        "SELECT ods.country, ods.prov, ods.city, ods.ymd, ods.uid, dim.uid_int32 "
                                + "FROM ods_src AS ods "
                                + "JOIN %s /*+ OPTIONS('lookup.insert-if-not-exists' = 'true', %s) */ "
                                + "FOR SYSTEM_TIME AS OF ods.proctime AS dim "
                                + "ON dim.uid = ods.uid",
                        uidMappingTable, lookupAsyncOption);

        CloseableIterator<Row> joinResult = tEnv.executeSql(joinQuery).collect();

        // user_a appears twice, user_b and user_c appear once each
        // With insertIfNotExists, uid_mapping should auto-create entries:
        // user_a -> uid_int32=1, user_b -> uid_int32=2, user_a (2nd) reuses uid_int32=1,
        // user_c -> uid_int32=3
        List<String> expectedJoinResults =
                Arrays.asList(
                        "+I[CN, Beijing, Haidian, 2025-01-01, user_a, 1]",
                        "+I[CN, Shanghai, Pudong, 2025-01-02, user_b, 2]",
                        "+I[US, California, LA, 2025-01-03, user_a, 1]",
                        "+I[JP, Tokyo, Shibuya, 2025-01-04, user_c, 3]");
        assertResultsIgnoreOrder(joinResult, expectedJoinResults, true);

        // Verify uid_mapping table has the correct data
        List<String> expectedUidMappingResults =
                Arrays.asList("+I[user_a, 1]", "+I[user_b, 2]", "+I[user_c, 3]");
        assertQueryResultExactOrder(
                tEnv,
                String.format(
                        "SELECT uid, uid_int32 FROM %s /*+ OPTIONS('scan.startup.mode' = 'earliest') */",
                        uidMappingTable),
                expectedUidMappingResults);
    }

    @Test
    void testStreamingReadSinglePartitionPushDown() throws Exception {
        tEnv.executeSql(
                "create table partitioned_table"
                        + " (a int not null, b varchar, c string, primary key (a, c) NOT ENFORCED) partitioned by (c) ");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "partitioned_table");
        tEnv.executeSql("alter table partitioned_table add partition (c=2025)");
        tEnv.executeSql("alter table partitioned_table add partition (c=2026)");

        List<String> expectedRowValues =
                writeRowsToPartition(conn, tablePath, Arrays.asList("2025", "2026")).stream()
                        .filter(s -> s.contains("2025"))
                        .collect(Collectors.toList());
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        String plan = tEnv.explainSql("select * from partitioned_table where c ='2025'");
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, partitioned_table, "
                                + "filter=[=(c, _UTF-16LE'2025':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\")]]], "
                                + "fields=[a, b, c])");

        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from partitioned_table where c ='2025'").collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    @Test
    void testStreamingReadAllPartitionTypePushDown() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE all_type_partitioned_table"
                        + " (id INT, "
                        + "  p_bool BOOLEAN, p_int INT, p_bigint BIGINT, "
                        + "  p_bytes BYTES, p_string STRING, "
                        + "  p_float FLOAT, p_double DOUBLE, "
                        + "  p_date DATE, p_time TIME, p_ts_ntz TIMESTAMP, "
                        + "  p_ts_ltz TIMESTAMP WITH LOCAL TIME ZONE) "
                        + "PARTITIONED BY (p_bool, p_int, p_bigint, p_bytes, p_string, "
                        + "  p_float, p_double, p_date, p_time, p_ts_ntz, p_ts_ltz) ");
        tEnv.executeSql(
                        "INSERT INTO all_type_partitioned_table VALUES "
                                + "(1, false, 10, 99999, CAST('Hi' AS VARBINARY), 'hello', 12.5,  7.88,   DATE '2025-10-12', TIME '12:55:00', TIMESTAMP '2025-10-12 12:55:00.001', TO_TIMESTAMP_LTZ(4001, 3)), "
                                + "(2, true,  12, 99998, CAST('Hi' AS VARBINARY), 'world', 13.6,  8.99,   DATE '2025-10-11', TIME '12:55:12', TIMESTAMP '2025-10-12 12:55:01.001', TO_TIMESTAMP_LTZ(5001, 3)), "
                                + "(3, false, 13, 99997, CAST('Hi' AS VARBINARY), 'Hi',    17.44, 4.444,  DATE '2025-10-10', TIME '12:55:32', TIMESTAMP '2025-10-12 12:55:02.001', TO_TIMESTAMP_LTZ(6001, 3)), "
                                + "(4, true,  14, 99996, CAST('Hi' AS VARBINARY), 'Ciao',  11.0,  9.4211, DATE '2025-10-09', TIME '12:00:44', TIMESTAMP '2025-10-12 12:55:03.001', TO_TIMESTAMP_LTZ(7001, 3)); ")
                .await();

        List<String> expectedShowPartitionsResult =
                Arrays.asList(
                        "+I[p_bool=false/p_int=10/p_bigint=99999/p_bytes=4869/p_string=hello/p_float=12_5/p_double=7_88/p_date=2025-10-12/p_time=12-55-00_000/p_ts_ntz=2025-10-12-12-55-00_001/p_ts_ltz=1970-01-01-00-00-04_001]",
                        "+I[p_bool=true/p_int=12/p_bigint=99998/p_bytes=4869/p_string=world/p_float=13_6/p_double=8_99/p_date=2025-10-11/p_time=12-55-12_000/p_ts_ntz=2025-10-12-12-55-01_001/p_ts_ltz=1970-01-01-00-00-05_001]",
                        "+I[p_bool=false/p_int=13/p_bigint=99997/p_bytes=4869/p_string=Hi/p_float=17_44/p_double=4_444/p_date=2025-10-10/p_time=12-55-32_000/p_ts_ntz=2025-10-12-12-55-02_001/p_ts_ltz=1970-01-01-00-00-06_001]",
                        "+I[p_bool=true/p_int=14/p_bigint=99996/p_bytes=4869/p_string=Ciao/p_float=11_0/p_double=9_4211/p_date=2025-10-09/p_time=12-00-44_000/p_ts_ntz=2025-10-12-12-55-03_001/p_ts_ltz=1970-01-01-00-00-07_001]");
        CloseableIterator<Row> showPartitionIterator =
                tEnv.executeSql("show partitions all_type_partitioned_table").collect();
        assertResultsIgnoreOrder(showPartitionIterator, expectedShowPartitionsResult, true);

        String query =
                "select * from all_type_partitioned_table where "
                        + "p_bool is false and p_int not in (12) and p_bigint in (99999) and p_bytes=CAST('Hi' AS VARBINARY) "
                        + "and p_string='hello' and p_float=CAST(12.5 AS FLOAT) and p_double=7.88 and p_date=DATE '2025-10-12' "
                        + "and p_time=TIME '12:55:00' and p_ts_ntz=TIMESTAMP '2025-10-12 12:55:00.001' "
                        + "and p_ts_ltz=TO_TIMESTAMP_LTZ(4001, 3)";
        String plan = tEnv.explainSql(query);
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, all_type_partitioned_table, "
                                + "filter=[and(and(and(and(and(and(and(and(and(and(<>(p_int, 12), "
                                + "=(p_bigint, 99999:BIGINT)), =(p_bytes, X'4869':VARBINARY(2147483647))), "
                                + "=(p_string, _UTF-16LE'hello':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\")), "
                                + "=(p_float, 1.25E1:FLOAT)), =(p_double, 7.88E0:DOUBLE)), =(p_date, 2025-10-12)), "
                                + "=(p_time, 12:55:00)), =(p_ts_ntz, 2025-10-12 12:55:00.001:TIMESTAMP(6))), "
                                + "=(p_ts_ltz, 1970-01-01 00:00:04.001:TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))), NOT(p_bool))]]], "
                                + "fields=[id, p_bool, p_int, p_bigint, p_bytes, p_string, p_float, p_double, p_date, p_time, p_ts_ntz, p_ts_ltz])")
                // although all filter conditions are pushed down into source, they are still
                // retained in the plan
                .contains("where=");

        List<String> expectedRowValues =
                Collections.singletonList(
                        "+I[1, false, 10, 99999, [72, 105], hello, 12.5, 7.88, 2025-10-12, 12:55, 2025-10-12T12:55:00.001, 1970-01-01T00:00:04.001Z]");
        org.apache.flink.util.CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    @Test
    void testStreamingReadMultiPartitionPushDown() throws Exception {
        tEnv.executeSql(
                "create table multi_partitioned_table"
                        + " (a int not null, b varchar, c string,d string, primary key (a, c, d) NOT ENFORCED) partitioned by (c,d) "
                        + "with ('table.auto-partition.enabled' = 'true', 'table.auto-partition.time-unit' = 'year','table.auto-partition.key'= 'c','table.auto-partition.num-precreate' = '0')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "multi_partitioned_table");
        tEnv.executeSql("alter table multi_partitioned_table add partition (c=2025,d=1)");
        tEnv.executeSql("alter table multi_partitioned_table add partition (c=2025,d=2)");
        tEnv.executeSql("alter table multi_partitioned_table add partition (c=2026,d=1)");

        List<String> expectedRowValues =
                writeRowsToTwoPartition(
                                tablePath, Arrays.asList("c=2025,d=1", "c=2025,d=2", "c=2026,d=1"))
                        .stream()
                        .filter(s -> s.contains("2025"))
                        .collect(Collectors.toList());
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        String plan = tEnv.explainSql("select * from multi_partitioned_table where c ='2025'");
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, multi_partitioned_table, "
                                + "filter=[=(c, _UTF-16LE'2025':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\")]]], "
                                + "fields=[a, b, c, d])");

        // test partition key prefix match
        // This test requires dynamically discovering newly created partitions, so
        // 'scan.partition.discovery.interval' needs to be set to 2s (default is 1 minute),
        // otherwise the test may hang for 1 minute.
        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(
                                "select * from multi_partitioned_table /*+ OPTIONS('scan.partition.discovery.interval' = '2s') */ where c ='2025'")
                        .collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues, false);

        tEnv.executeSql("alter table multi_partitioned_table add partition (c=2025,d=3)");
        tEnv.executeSql("alter table multi_partitioned_table add partition (c=2026,d=2)");
        expectedRowValues =
                writeRowsToTwoPartition(tablePath, Arrays.asList("c=2025,d=3", "c=2026,d=2"))
                        .stream()
                        .filter(s -> s.contains("2025"))
                        .collect(Collectors.toList());
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);

        String plan2 =
                tEnv.explainSql("select * from multi_partitioned_table where c ='2025' and d ='3'");
        assertThat(plan2)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, multi_partitioned_table, "
                                + "filter=[and(=(c, _UTF-16LE'2025':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"), "
                                + "=(d, _UTF-16LE'3':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"))]]], "
                                + "fields=[a, b, c, d])");

        // test all partition key match
        rowIter =
                tEnv.executeSql("select * from multi_partitioned_table where c ='2025' and d ='3'")
                        .collect();
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    @Test
    void testStreamingReadWithCombinedFilters1() throws Exception {
        tEnv.executeSql(
                "create table combined_filters_table"
                        + " (a int not null, b varchar, c string, d int, primary key (a, c) NOT ENFORCED) partitioned by (c) ");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "combined_filters_table");
        tEnv.executeSql("alter table combined_filters_table add partition (c=2025)");
        tEnv.executeSql("alter table combined_filters_table add partition (c=2026)");

        List<InternalRow> rows = new ArrayList<>();
        List<String> expectedRowValues1 = new ArrayList<>();
        List<String> expectedRowValues2 = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            rows.add(row(i, "v" + i, "2025", i * 100));
            if (i % 2 == 0) {
                expectedRowValues1.add(String.format("+I[%d, 2025, %d]", i, i * 100));
            }
            if (i == 2) {
                expectedRowValues2.add(String.format("+I[%d, 2025, %d]", i, i * 100));
            }
        }
        writeRows(conn, tablePath, rows, false);

        for (int i = 0; i < 10; i++) {
            rows.add(row(i, "v" + i, "2026", i * 100));
        }

        writeRows(conn, tablePath, rows, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        String plan =
                tEnv.explainSql(
                        "select a,c,d from combined_filters_table where c ='2025' and d % 200 = 0");
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, combined_filters_table, "
                                + "filter=[=(c, _UTF-16LE'2025':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\")], "
                                + "project=[a, c, d], metadata=[]]], fields=[a, c, d])");

        // test column filter, partition filter and flink runtime filter
        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(
                                "select a,c,d from combined_filters_table where c ='2025' and d % 200 = 0")
                        .collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues1, true);

        plan =
                tEnv.explainSql(
                        "select a,c,d from combined_filters_table where c ='2025' and d = 200");
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, combined_filters_table, "
                                + "filter=[=(c, _UTF-16LE'2025':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\")], "
                                + "project=[a, c, d], metadata=[]]], fields=[a, c, d])");

        // test column filter, partition filter and flink runtime filter
        rowIter =
                tEnv.executeSql(
                                "select a,c,d from combined_filters_table where c ='2025' and d = 200")
                        .collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues2, true);
    }

    @Test
    void testNonPartitionPushDown() throws Exception {
        tEnv.executeSql(
                "create table partitioned_table_no_filter"
                        + " (a int not null, b varchar, c string, primary key (a, c) NOT ENFORCED) partitioned by (c) ");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "partitioned_table_no_filter");
        tEnv.executeSql("alter table partitioned_table_no_filter add partition (c=2025)");
        tEnv.executeSql("alter table partitioned_table_no_filter add partition (c=2026)");

        List<String> expectedRowValues =
                writeRowsToPartition(conn, tablePath, Arrays.asList("2025", "2026"));
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql("select * from partitioned_table_no_filter").collect();
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    @Test
    void testStreamingReadNonPKTableWithCombinedFilters() throws Exception {
        tEnv.executeSql(
                "create table combined_filters_table"
                        + " (a int not null, b varchar, c string, d int) partitioned by (c)"
                        + " with ('table.statistics.columns' = 'd')");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "combined_filters_table");
        tEnv.executeSql("alter table combined_filters_table add partition (c=2025)");
        tEnv.executeSql("alter table combined_filters_table add partition (c=2026)");

        List<InternalRow> rows = new ArrayList<>();
        List<String> expectedRowValues = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            rows.add(row(i, "v" + i, "2025", i * 100));
            if (i > 2) {
                expectedRowValues.add(String.format("+I[%d, 2025, %d]", i, i * 100));
            }
        }
        writeRows(conn, tablePath, rows, true);

        List<InternalRow> rows2 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rows2.add(row(i, "v" + i, "2026", i * 100));
        }

        writeRows(conn, tablePath, rows2, true);

        String plan =
                tEnv.explainSql(
                        "select a,c,d from combined_filters_table where c ='2025' and d > 200");

        // assert both partition filter and record batch filter are pushed down
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, combined_filters_table, "
                                + "filter=[and(=(c, _UTF-16LE'2025':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"), >(d, 200))], "
                                + "project=[a, c, d], metadata=[]]], fields=[a, c, d])");
        // assert predicates are still retained in the Calc operator (FLINK-38635 safety net)
        assertThat(plan).contains("where=");

        // test column filter, partition filter and flink runtime filter
        org.apache.flink.util.CloseableIterator<Row> rowIter =
                tEnv.executeSql(
                                "select a,c,d from combined_filters_table where c ='2025' and d > 200;")
                        .collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    /**
     * Test RecordBatchFilter push down with large dataset and multiple range filters to verify
     * correctness when log segments have gaps due to filtering.
     */
    @Test
    void testRecordBatchFilterPushDownWithLogGaps() throws Exception {
        // Create a log table for testing record batch filter push down with specific batch size
        // configurations
        // to ensure predictable RecordBatch sizes during writing and reading
        tEnv.executeSql(
                "create table record_batch_filter_test "
                        + "(id int, sequence_num int, name varchar, score double) "
                        + "with ("
                        + "'table.log.format' = 'ARROW', "
                        + "'table.statistics.columns' = 'sequence_num', "
                        // Writer configuration: aim for ~500 records per RecordBatch
                        // Each record has: int(4) + int(4) + varchar(~12) + double(8) ≈ 28 bytes
                        // So 500 records ≈ 14KB, set batch size to 32KB to account for overhead
                        + "'client.writer.batch-size' = '32kb', "
                        // Shorter timeout for predictable batching
                        + "'client.writer.batch-timeout' = '10ms', "
                        // limit read batch size to 500 records max
                        + "'client.scanner.log.max-poll-records' = '500', "
                        + "'client.scanner.log.fetch.max-bytes-for-bucket' = '32kb' "
                        + ")");

        TablePath tablePath = TablePath.of(DEFAULT_DB, "record_batch_filter_test");

        // Write 10,000 ordered rows with sequential sequence_num in controlled batches
        // to ensure predictable RecordBatch sizes
        int totalRecords = 10000;
        int batchSize = 500;

        for (int batchStart = 0; batchStart < totalRecords; batchStart += batchSize) {
            List<InternalRow> batchRows = new ArrayList<>();
            int batchEnd = Math.min(batchStart + batchSize, totalRecords);

            for (int i = batchStart; i < batchEnd; i++) {
                batchRows.add(row(i, i, "value_" + i, i * 0.1));
            }

            // Write each batch separately to ensure proper RecordBatch formation
            writeRows(conn, tablePath, batchRows, true);
        }

        // Define three ranges to filter: 5500-5999, 8350-8400, 9400-9999
        String query =
                "select id, sequence_num, name from record_batch_filter_test "
                        + "where (sequence_num >= 5500 and sequence_num < 6000) "
                        + "or (sequence_num >= 8350 and sequence_num <= 8400) "
                        + "or (sequence_num >= 9400 and sequence_num < 10000)";

        // Verify that record batch filters are pushed down
        String plan = tEnv.explainSql(query);
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, record_batch_filter_test, "
                                + "filter=[OR(OR(AND(>=(sequence_num, 5500), <(sequence_num, 6000)), "
                                + "AND(>=(sequence_num, 8350), <=(sequence_num, 8400))), "
                                + "AND(>=(sequence_num, 9400), <(sequence_num, 10000)))]");

        // Collect results and verify correctness
        List<String> expectedResults = new ArrayList<>();

        // Range 1: 5500-5999 (500 records)
        for (int i = 5500; i < 6000; i++) {
            expectedResults.add(String.format("+I[%d, %d, value_%d]", i, i, i));
        }

        // Range 2: 8350-8400 (51 records)
        for (int i = 8350; i <= 8400; i++) {
            expectedResults.add(String.format("+I[%d, %d, value_%d]", i, i, i));
        }

        // Range 3: 9400-9999 (600 records)
        for (int i = 9400; i < 10000; i++) {
            expectedResults.add(String.format("+I[%d, %d, value_%d]", i, i, i));
        }

        try (CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect()) {
            assertResultsIgnoreOrder(rowIter, expectedResults, true);
        }
    }

    /**
     * Tests that ALTER TABLE can dynamically change statistics columns configuration, and filter
     * pushdown works correctly across different statistics modes: disabled → subset → wildcard.
     *
     * <p>Each phase writes data, then verifies filter pushdown behavior. Since {@code writeRows}
     * re-creates the Table instance each time, the writer picks up the latest TableInfo after ALTER
     * TABLE, so new batches are written with the updated statistics configuration.
     */
    @Test
    void testAlterTableStatisticsColumnsWithFilterPushDown() throws Exception {
        // Create table WITHOUT statistics columns (disabled mode)
        tEnv.executeSql(
                "create table alter_stats_test "
                        + "(id int, amount bigint, region varchar, score double) "
                        + "with ("
                        + "'table.log.format' = 'ARROW', "
                        + "'client.writer.batch-size' = '16kb', "
                        + "'client.writer.batch-timeout' = '10ms'"
                        + ")");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "alter_stats_test");

        // ========== Phase 1: Statistics DISABLED ==========
        // Write data with amount range [0, 900], step=100
        List<InternalRow> phase1Rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            phase1Rows.add(row(i, (long) (i * 100), "HangZhou", i * 1.5));
        }
        writeRows(conn, tablePath, phase1Rows, true);

        // Without statistics columns configured, filter is NOT pushed to source for record
        // batch filtering — it only appears in the Calc operator
        String query1 = "select id, amount from alter_stats_test where amount > 500";
        String plan1 = tEnv.explainSql(query1);
        assertThat(plan1).contains("filter=[]");
        assertThat(plan1).contains("where=");

        // Verify correctness: amount > 500 means amount in {600, 700, 800, 900}
        List<String> expected1 = new ArrayList<>();
        for (int i = 6; i < 10; i++) {
            expected1.add(String.format("+I[%d, %d]", i, i * 100));
        }
        try (CloseableIterator<Row> iter = tEnv.executeSql(query1).collect()) {
            assertResultsIgnoreOrder(iter, expected1, true);
        }

        // ========== Phase 2: ALTER TABLE to enable statistics on subset ('amount') ==========
        tEnv.executeSql("alter table alter_stats_test set ('table.statistics.columns' = 'amount')");

        // Write new data with amount range [1000, 1900], step=100
        List<InternalRow> phase2Rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            phase2Rows.add(row(10 + i, (long) (1000 + i * 100), "Shanghai", (10 + i) * 1.5));
        }
        writeRows(conn, tablePath, phase2Rows, true);

        // Filter on 'amount' column — statistics should be collected for new batches
        String query2 = "select id, amount from alter_stats_test where amount > 1500";
        String plan2 = tEnv.explainSql(query2);
        assertThat(plan2).contains("filter=[>(amount, 1500)]");

        // Verify correctness: amount > 1500 means amount in {1600, 1700, 1800, 1900}
        List<String> expected2 = new ArrayList<>();
        for (int i = 6; i < 10; i++) {
            expected2.add(String.format("+I[%d, %d]", 10 + i, 1000 + i * 100));
        }
        try (CloseableIterator<Row> iter = tEnv.executeSql(query2).collect()) {
            assertResultsIgnoreOrder(iter, expected2, true);
        }

        // Filter on 'score' column — no statistics for 'score' in subset mode,
        // filter is NOT pushed to source for record batch filtering, only in Calc operator.
        // Select integer columns to avoid fragile double formatting in assertions.
        String query2b = "select id, amount from alter_stats_test where score > 20.0";
        String plan2b = tEnv.explainSql(query2b);
        assertThat(plan2b).contains("filter=[]");
        assertThat(plan2b).contains("where=");

        List<String> expected2b = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            double score = (10 + i) * 1.5;
            if (score > 20.0) {
                expected2b.add(String.format("+I[%d, %d]", 10 + i, 1000 + i * 100));
            }
        }
        try (CloseableIterator<Row> iter = tEnv.executeSql(query2b).collect()) {
            assertResultsIgnoreOrder(iter, expected2b, true);
        }

        // ========== Phase 3: ALTER TABLE to wildcard '*' (all supported columns) ==========
        tEnv.executeSql("alter table alter_stats_test set ('table.statistics.columns' = '*')");

        // Write new data with amount range [2000, 2900], step=100
        List<InternalRow> phase3Rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            phase3Rows.add(row(20 + i, (long) (2000 + i * 100), "Beijing", (20 + i) * 1.5));
        }
        writeRows(conn, tablePath, phase3Rows, true);

        // Filter on 'amount' — statistics available
        String query3 = "select id, amount, region from alter_stats_test where amount >= 2500";
        String plan3 = tEnv.explainSql(query3);
        assertThat(plan3).contains("filter=[>=(amount, 2500)]");

        // Verify correctness: amount >= 2500 means amount in {2500, 2600, 2700, 2800, 2900}
        List<String> expected3 = new ArrayList<>();
        for (int i = 5; i < 10; i++) {
            expected3.add(String.format("+I[%d, %d, Beijing]", 20 + i, 2000 + i * 100));
        }
        try (CloseableIterator<Row> iter = tEnv.executeSql(query3).collect()) {
            assertResultsIgnoreOrder(iter, expected3, true);
        }

        // Combined filter on 'amount' and 'region' — both have statistics in wildcard mode,
        // so the filter should be pushed to source (not empty)
        String query3b =
                "select id, amount, region from alter_stats_test "
                        + "where amount >= 1000 and region = 'Beijing'";
        String plan3b = tEnv.explainSql(query3b);
        assertThat(plan3b)
                .contains(
                        "filter=[and(>=(amount, 1000), "
                                + "=(region, _UTF-16LE'Beijing':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"))]");

        // Verify: amount >= 1000 AND region = 'Beijing' → only phase3 rows
        // (phase2 rows have region='Shanghai', so they are excluded by the region filter)
        List<String> expected3b = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expected3b.add(String.format("+I[%d, %d, Beijing]", 20 + i, 2000 + i * 100));
        }
        try (CloseableIterator<Row> iter = tEnv.executeSql(query3b).collect()) {
            assertResultsIgnoreOrder(iter, expected3b, true);
        }

        // ========== Phase 4: Cross-phase query — all data, mixed statistics coverage ==========
        // Query across all phases: phase1 batches have no stats, phase2 has partial (amount only),
        // phase3 has full stats. The filter should still be pushed down because the latest
        // table config has statistics enabled, and batches without stats are conservatively
        // included.
        String query4 =
                "select id, amount from alter_stats_test "
                        + "where amount >= 500 and amount < 1500";
        String plan4 = tEnv.explainSql(query4);
        assertThat(plan4).contains("filter=[and(>=(amount, 500), <(amount, 1500))]");

        List<String> expected4 = new ArrayList<>();
        // Phase 1 rows: amount in {500, 600, 700, 800, 900}
        for (int i = 5; i < 10; i++) {
            expected4.add(String.format("+I[%d, %d]", i, i * 100));
        }
        // Phase 2 rows: amount in {1000, 1100, 1200, 1300, 1400}
        for (int i = 0; i < 5; i++) {
            expected4.add(String.format("+I[%d, %d]", 10 + i, 1000 + i * 100));
        }
        try (CloseableIterator<Row> iter = tEnv.executeSql(query4).collect()) {
            assertResultsIgnoreOrder(iter, expected4, true);
        }
    }

    /** Tests that ALTER TABLE rejects statistics columns on primary key tables. */
    @Test
    void testAlterTableStatisticsColumnsOnPkTableShouldFail() {
        // Create a PK table without statistics columns
        tEnv.executeSql(
                "create table pk_stats_reject_test "
                        + "(id int not null, amount bigint, primary key (id) not enforced)");

        // ALTER TABLE to add statistics columns should fail for PK tables
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "alter table pk_stats_reject_test "
                                                + "set ('table.statistics.columns' = 'amount')"))
                .rootCause()
                .hasMessageContaining(
                        "Statistics columns are not supported for primary key tables");
    }

    /** Tests that ALTER TABLE rejects invalid statistics columns on log tables. */
    @Test
    void testAlterTableInvalidStatisticsColumnsOnLogTableShouldFail() {
        // Create a PK table without statistics columns
        tEnv.executeSql(
                "create table log_stats_reject_test "
                        + "(id int not null, amount bigint, content bytes)");

        // ALTER TABLE to add statistics columns should fail for PK tables
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "alter table log_stats_reject_test "
                                                + "set ('table.statistics.columns' = 'content')"))
                .rootCause()
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "Column 'content' of type 'BYTES' is not supported for statistics collection.");
    }

    private List<String> writeRowsToTwoPartition(TablePath tablePath, Collection<String> partitions)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<String> expectedRowValues = new ArrayList<>();

        for (String partition : partitions) {
            String[] keyValuePairs = partition.split(",");
            String[] values = new String[2];
            values[0] = keyValuePairs[0].split("=")[1];
            values[1] = keyValuePairs[1].split("=")[1];

            for (int i = 0; i < 10; i++) {
                rows.add(row(i, "v" + i, values[0], values[1]));
                expectedRowValues.add(
                        String.format("+I[%d, v%d, %s, %s]", i, i, values[0], values[1]));
            }
        }

        writeRows(conn, tablePath, rows, false);

        return expectedRowValues;
    }

    @Test
    void testStreamingReadPartitionPushDownWithInExpr() throws Exception {

        tEnv.executeSql(
                "create table partitioned_table_in"
                        + " (a int not null, b varchar, c string, primary key (a, c) NOT ENFORCED) partitioned by (c) ");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "partitioned_table_in");
        tEnv.executeSql("alter table partitioned_table_in add partition (c=2025)");
        tEnv.executeSql("alter table partitioned_table_in add partition (c=2026)");
        tEnv.executeSql("alter table partitioned_table_in add partition (c=2027)");

        List<String> expectedRowValues =
                writeRowsToPartition(conn, tablePath, Arrays.asList("2025", "2026", "2027"))
                        .stream()
                        .filter(s -> s.contains("2025") || s.contains("2026"))
                        .collect(Collectors.toList());
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        String query1 = "select * from partitioned_table_in where c in ('2025','2026')";
        String plan = tEnv.explainSql(query1);
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, partitioned_table_in, filter=[OR(=(c, _UTF-16LE'2025'), =(c, _UTF-16LE'2026'))]]], fields=[a, b, c])");

        org.apache.flink.util.CloseableIterator<Row> rowIter = tEnv.executeSql(query1).collect();
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);

        String query2 = "select * from partitioned_table_in where c ='2025' or  c ='2026'";
        plan = tEnv.explainSql(query2);
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, partitioned_table_in, filter=[OR(=(c, _UTF-16LE'2025':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"), =(c, _UTF-16LE'2026':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\"))]]], fields=[a, b, c])");
        rowIter = tEnv.executeSql(query2).collect();
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    @Test
    void testStreamingReadWithCombinedFiltersAndInExpr() throws Exception {
        tEnv.executeSql(
                "create table combined_filters_table_in"
                        + " (a int not null, b varchar, c string, d int, primary key (a, c) NOT ENFORCED) partitioned by (c) ");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "combined_filters_table_in");
        tEnv.executeSql("alter table combined_filters_table_in add partition (c=2025)");
        tEnv.executeSql("alter table combined_filters_table_in add partition (c=2026)");
        tEnv.executeSql("alter table combined_filters_table_in add partition (c=2027)");

        List<InternalRow> rows = new ArrayList<>();
        List<String> expectedRowValues = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            rows.add(row(i, "v" + i, "2025", i * 100));
            if (i % 2 == 0) {
                expectedRowValues.add(String.format("+I[%d, 2025, %d]", i, i * 100));
            }
        }
        for (int i = 0; i < 10; i++) {
            rows.add(row(i, "v" + i, "2026", i * 100));
            if (i % 2 == 0) {
                expectedRowValues.add(String.format("+I[%d, 2026, %d]", i, i * 100));
            }
        }

        for (int i = 0; i < 10; i++) {
            rows.add(row(i, "v" + i, "2027", i * 100));
        }

        writeRows(conn, tablePath, rows, false);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        String query1 =
                "select a,c,d from combined_filters_table_in where c in ('2025','2026') and d % 200 = 0";
        String plan = tEnv.explainSql(query1);
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, combined_filters_table_in, filter=[OR(=(c, _UTF-16LE'2025'), =(c, _UTF-16LE'2026'))], project=[a, c, d], metadata=[]]], fields=[a, c, d])");

        // test column filter, partition filter and flink runtime filter
        org.apache.flink.util.CloseableIterator<Row> rowIter = tEnv.executeSql(query1).collect();
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);

        rowIter =
                tEnv.executeSql(
                                "select a,c,d from combined_filters_table_in where (c ='2025' or  c ='2026') "
                                        + "and d % 200 = 0")
                        .collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    @Test
    void testStreamingReadPartitionPushDownWithLikeExpr() throws Exception {

        tEnv.executeSql(
                "create table partitioned_table_like"
                        + " (a int not null, b varchar, c string, primary key (a, c) NOT ENFORCED) partitioned by (c) ");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "partitioned_table_like");
        tEnv.executeSql("alter table partitioned_table_like add partition (c=2025)");
        tEnv.executeSql("alter table partitioned_table_like add partition (c=2026)");
        tEnv.executeSql("alter table partitioned_table_like add partition (c=3026)");

        List<String> allData =
                writeRowsToPartition(conn, tablePath, Arrays.asList("2025", "2026", "3026"));
        List<String> expectedRowValues =
                allData.stream()
                        .filter(s -> s.contains("2025") || s.contains("2026"))
                        .collect(Collectors.toList());
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        String query1 = "select * from partitioned_table_like where c like '202%'";
        String plan = tEnv.explainSql(query1);
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, partitioned_table_like, filter=[LIKE(c, _UTF-16LE'202%')]]], fields=[a, b, c])");

        org.apache.flink.util.CloseableIterator<Row> rowIter = tEnv.executeSql(query1).collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
        expectedRowValues =
                allData.stream()
                        .filter(s -> s.contains("2026") || s.contains("3026"))
                        .collect(Collectors.toList());
        String query2 = "select * from partitioned_table_like where c like '%026'";
        plan = tEnv.explainSql(query2);
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, partitioned_table_like, filter=[LIKE(c, _UTF-16LE'%026')]]], fields=[a, b, c])");
        rowIter = tEnv.executeSql(query2).collect();
        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);

        expectedRowValues =
                allData.stream().filter(s -> s.contains("3026")).collect(Collectors.toList());
        String query3 = "select * from partitioned_table_like where c like '%3026%'";
        plan = tEnv.explainSql(query3);
        assertThat(plan)
                .contains(
                        "TableSourceScan(table=[[testcatalog, defaultdb, partitioned_table_like, filter=[LIKE(c, _UTF-16LE'%3026%')]]], fields=[a, b, c])");
        rowIter = tEnv.executeSql(query3).collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    @Test
    void testStreamingReadPartitionComplexPushDown() throws Exception {

        tEnv.executeSql(
                "create table partitioned_table_complex"
                        + " (a int not null, b varchar, c string,d string, primary key (a, c, d) NOT ENFORCED) partitioned by (c,d) ");
        TablePath tablePath = TablePath.of(DEFAULT_DB, "partitioned_table_complex");
        tEnv.executeSql("alter table partitioned_table_complex add partition (c=2025,d=1)");
        tEnv.executeSql("alter table partitioned_table_complex add partition (c=2025,d=2)");
        tEnv.executeSql("alter table partitioned_table_complex add partition (c=2026,d=1)");

        List<String> allData =
                writeRowsToTwoPartition(
                        tablePath, Arrays.asList("c=2025,d=1", "c=2025,d=2", "c=2026,d=1"));
        List<String> expectedRowValues =
                allData.stream()
                        .filter(s -> s.contains("v3") && !s.contains("2025, 2"))
                        .collect(Collectors.toList());
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshot(tablePath);

        String query =
                "select * from partitioned_table_complex where  a = 3\n"
                        + "    and (c in ('2026')  or d like '%1%') "
                        + "    and     b like '%v3%'";
        String plan = tEnv.explainSql(query);
        assertThat(plan)
                .contains(
                        "Calc(select=[3 AS a, b, c, d], where=[((a = 3) AND ((c = '2026') OR LIKE(d, '%1%')) AND LIKE(b, '%v3%'))])\n"
                                + "+- TableSourceScan(table=[[testcatalog, defaultdb, partitioned_table_complex, filter=[OR(=(c, _UTF-16LE'2026'), LIKE(d, _UTF-16LE'%1%'))]]], fields=[a, b, c, d])");

        org.apache.flink.util.CloseableIterator<Row> rowIter = tEnv.executeSql(query).collect();

        assertResultsIgnoreOrder(rowIter, expectedRowValues, true);
    }

    private enum Caching {
        ENABLE_CACHE,
        DISABLE_CACHE
    }

    /**
     * Creates dim table in Fluss and source table in Flink, and generates data for them.
     *
     * @return the table name of the dim table
     */
    private String prepareDimTableAndSourceTable(
            Caching caching,
            boolean async,
            String[] primaryKeys,
            @Nullable String[] bucketKeys,
            @Nullable String partitionedKey)
            throws Exception {
        String options = async ? "'lookup.async' = 'true'" : "'lookup.async' = 'false'";
        if (caching == Caching.ENABLE_CACHE) {
            options +=
                    ",'lookup.cache' = 'PARTIAL'"
                            + ",'lookup.partial-cache.max-rows' = '1000'"
                            + ",'lookup.partial-cache.expire-after-write' = '10min'";
        }
        String bucketOptions =
                bucketKeys == null
                        ? ""
                        : ", 'bucket.num' = '1', 'bucket.key' = '"
                                + String.join(",", bucketKeys)
                                + "'";

        // create dim table
        String tableName =
                String.format(
                        "lookup_test_%s_%s_pk_%s_%s",
                        caching.name().toLowerCase(),
                        async ? "async" : "sync",
                        String.join("_", primaryKeys),
                        RandomUtils.nextInt());
        if (partitionedKey == null) {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "  id int not null,"
                                    + "  address varchar,"
                                    + "  name varchar,"
                                    + "  primary key (%s) NOT ENFORCED) with (%s %s)",
                            tableName, String.join(",", primaryKeys), options, bucketOptions));
        } else {
            tEnv.executeSql(
                    String.format(
                            "create table %s ("
                                    + "  id int not null,"
                                    + "  address varchar,"
                                    + "  name varchar,"
                                    + "  %s varchar , "
                                    + "  primary key (%s, %s) NOT ENFORCED) partitioned by (%s) with (%s , "
                                    + " 'table.auto-partition.enabled' = 'true', 'table.auto-partition.time-unit' = 'year'"
                                    + " %s)",
                            tableName,
                            partitionedKey,
                            String.join(",", primaryKeys),
                            partitionedKey,
                            partitionedKey,
                            options,
                            bucketOptions));
        }

        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        String partition1 = null;
        String partition2 = null;
        if (partitionedKey != null) {
            Map<Long, String> partitionNameById =
                    waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath);
            // just pick one partition to insert data
            Iterator<String> partitionIterator = partitionNameById.values().iterator();
            partition1 = partitionIterator.next();
            partition2 = partitionIterator.next();
        }

        // prepare dim table data
        try (Table dimTable = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = dimTable.newUpsert().createWriter();
            for (int i = 1; i <= 5; i++) {
                Object[] values =
                        partition1 == null
                                ? new Object[] {i, "address" + i, "name" + i % 4}
                                : new Object[] {i, "address" + i, "name" + i % 4, partition1};
                upsertWriter.upsert(row(values));
            }
            upsertWriter.flush();
        }

        // prepare a source table
        List<Row> testData =
                partition1 == null
                        ? Arrays.asList(
                                Row.of(1, "name1", 11),
                                Row.of(2, "name2", 2),
                                Row.of(3, "name33", 33),
                                Row.of(10, "name0", 44))
                        : Arrays.asList(
                                Row.of(1, "name1", 11, partition1),
                                Row.of(2, "name2", 2, partition1),
                                Row.of(3, "name33", 33, partition2),
                                Row.of(10, "name0", 44, partition2),
                                Row.of(1, "name1", 11, "empty-partition"));
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.INT())
                        .columnByExpression("proc", "PROCTIME()");
        if (partitionedKey != null) {
            builder.column(partitionedKey, DataTypes.STRING());
        }
        Schema srcSchema = builder.build();
        RowTypeInfo srcTestTypeInfo =
                partitionedKey == null
                        ? new RowTypeInfo(
                                new TypeInformation[] {Types.INT, Types.STRING, Types.INT},
                                new String[] {"a", "b", "c"})
                        : new RowTypeInfo(
                                new TypeInformation[] {
                                    Types.INT, Types.STRING, Types.INT, Types.STRING
                                },
                                new String[] {"a", "b", "c", partitionedKey});
        DataStream<Row> srcDs = execEnv.fromCollection(testData).returns(srcTestTypeInfo);
        tEnv.dropTemporaryView("src");
        tEnv.createTemporaryView("src", tEnv.fromDataStream(srcDs, srcSchema));

        return tableName;
    }

    private GenericRow rowWithPartition(Object[] values, @Nullable String partition) {
        if (partition == null) {
            return row(values);
        } else {
            Object[] newValues = new Object[values.length + 1];
            System.arraycopy(values, 0, newValues, 0, values.length);
            newValues[values.length] = partition;
            return row(newValues);
        }
    }
}
