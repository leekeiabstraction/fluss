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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end ITCase for follower-side enrichment replication (E.3c). Writes base + column-group
 * data on the leader and asserts that followers persist the enrichment files, EWM, and CEW the
 * leader propagates back via fetch responses.
 */
class FollowerEnrichmentReplicationITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(2)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 2);
        return conf;
    }

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "device_logs_replicated");
    private static final int DEFAULT_SCHEMA_ID = 0;

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("payload", DataTypes.STRING())
                    .column("geo", DataTypes.STRING())
                    .columnGroup("enriched")
                    .build();

    private static final RowType BASE_ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                    new String[] {"id", "payload"});

    private static final RowType ENRICHMENT_ROW_TYPE =
            RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"geo"});

    private ZooKeeperClient zkClient;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testFollowerReplicatesEnrichmentSegmentsAndCew() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        TableDescriptor descriptor =
                TableDescriptor.builder().schema(SCHEMA).distributedBy(1).build();
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, TABLE_PATH, descriptor);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        ReplicaManager leaderRm =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderId).getReplicaManager();
        Replica leaderReplica = leaderRm.getReplicaOrException(tb);
        LogTablet leaderLog = leaderReplica.getLogTablet();
        String groupName = "enriched";

        // Base log: 5 rows on the leader. Wait for the follower to mirror — that's the precondition
        // for enrichment replication, since base-first is the rule (see ReplicaFetcherThread).
        for (int i = 0; i < 5; i++) {
            leaderReplica.appendRecordsToLeader(
                    genMemoryLogRecordsByObject(
                            BASE_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            CURRENT_LOG_MAGIC_VALUE,
                            Collections.singletonList(new Object[] {i, "payload-" + i})),
                    0);
        }
        assertThat(leaderLog.localLogEndOffset()).isEqualTo(5L);

        LeaderAndIsr leaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        List<Integer> followerIds =
                leaderAndIsr.isr().stream()
                        .filter(id -> id != leaderId)
                        .collect(Collectors.toList());
        assertThat(followerIds).hasSize(1);

        for (int followerId : followerIds) {
            ReplicaManager followerRm =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(followerId).getReplicaManager();
            Replica followerReplica = followerRm.getReplicaOrException(tb);
            LogTablet followerLog = followerReplica.getLogTablet();

            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(followerLog.localLogEndOffset()).isEqualTo(5L));

            // Now write enrichment for offsets 0..4 on the leader. With E.3d wired up the
            // leader's CEW will auto-advance from 0 to 5 once the follower's fetch round-trip
            // completes — we don't need to set CEW manually.
            leaderLog.registerColumnGroupIfAbsent(groupName);
            for (int i = 0; i < 5; i++) {
                leaderLog.appendColumnsAsLeader(
                        groupName, buildSingleRowEnrichment("US-WEST-" + i), new long[] {(long) i});
            }
            assertThat(leaderLog.getEnrichmentWatermark(groupName)).isEqualTo(5L);

            // Follower picks up enrichment via its periodic fetch. Wait for it.
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(followerLog.getEnrichmentWatermark(groupName)).isEqualTo(5L));

            // CEW must converge to 5 too — that's what unblocks the future read-side gate
            // (E.4). The leader auto-advances CEW as the follower's cursor catches up; the
            // follower then learns the new value through the next fetch response.
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(followerLog.getCommittedEnrichmentWatermark(groupName))
                                    .isEqualTo(5L));
            assertThat(leaderLog.getCommittedEnrichmentWatermark(groupName)).isEqualTo(5L);

            // E.5c: ongoing catch-up. Append a second round of base + enrichment and verify
            // the follower picks up the new tail without manual intervention. Same fetcher loop
            // as the first round — the cursor advances from 5 to 10, and the leader serves
            // [5, 10) in response.
            for (int i = 5; i < 10; i++) {
                leaderReplica.appendRecordsToLeader(
                        genMemoryLogRecordsByObject(
                                BASE_ROW_TYPE,
                                DEFAULT_SCHEMA_ID,
                                CURRENT_LOG_MAGIC_VALUE,
                                Collections.singletonList(new Object[] {i, "payload-" + i})),
                        0);
                leaderLog.appendColumnsAsLeader(
                        groupName, buildSingleRowEnrichment("US-WEST-" + i), new long[] {(long) i});
            }
            assertThat(leaderLog.getEnrichmentWatermark(groupName)).isEqualTo(10L);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(followerLog.getEnrichmentWatermark(groupName)).isEqualTo(10L));
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(followerLog.getCommittedEnrichmentWatermark(groupName))
                                    .isEqualTo(10L));

            // On-disk: the enrichment log + index files must exist in the follower's bucket dir.
            File followerTabletDir = followerLog.getLogDir();
            File[] colFiles = followerTabletDir.listFiles((d, name) -> name.contains(".col."));
            assertThat(colFiles).as("follower bucket dir at %s", followerTabletDir).isNotNull();
            assertThat(colFiles)
                    .as("follower must have one *.col.<g>.log + one *.col.<g>.index for the group")
                    .hasSize(2);
        }
    }

    private static MemoryLogRecords buildSingleRowEnrichment(String value) throws Exception {
        return DataTestUtils.createBasicMemoryLogRecords(
                ENRICHMENT_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                0L,
                System.currentTimeMillis(),
                CURRENT_LOG_MAGIC_VALUE,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                Collections.singletonList(ChangeType.APPEND_ONLY),
                Collections.singletonList(new Object[] {value}),
                LogFormat.ARROW,
                ArrowCompressionInfo.NO_COMPRESSION);
    }
}
