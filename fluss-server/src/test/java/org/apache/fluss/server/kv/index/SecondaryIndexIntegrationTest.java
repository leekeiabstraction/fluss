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

package org.apache.fluss.server.kv.index;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.index.IndexDescriptor;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the secondary index extension system. Creates a real KvTablet with a hash
 * index wired in, writes records, flushes, and verifies search results.
 */
class SecondaryIndexIntegrationTest {

    private static final short SCHEMA_ID = (short) DEFAULT_SCHEMA_ID;
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .withComment("primary key")
                    .column("name", DataTypes.STRING())
                    .withComment("indexed column")
                    .primaryKey("id")
                    .build();
    private static final RowType ROW_TYPE = SCHEMA.getRowType();

    private @TempDir File tempLogDir;
    private @TempDir File tmpKvDir;

    private TestingSchemaGetter schemaGetter;
    private LogTablet logTablet;
    private KvTablet kvTablet;

    @BeforeEach
    void setUp() throws Exception {
        Configuration conf = new Configuration();
        PhysicalTablePath tablePath = PhysicalTablePath.of(TablePath.of("testDb", "indexedTable"));
        schemaGetter = new TestingSchemaGetter(new SchemaInfo(SCHEMA, SCHEMA_ID));

        // Create log tablet
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(tempLogDir, "testDb", 0L, "indexedTable");
        logTablet =
                LogTablet.create(
                        tablePath,
                        logTabletDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        new FlussScheduler(1),
                        LogFormat.ARROW,
                        1,
                        true,
                        SystemClock.getInstance(),
                        true);

        // Parse index descriptors from table properties
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("table.index.name_idx.type", "hash");
        tableProps.put("table.index.name_idx.columns", "name");
        List<IndexDescriptor> descriptors = IndexDescriptor.fromProperties(tableProps);

        // Create SecondaryIndexManager
        File indexBaseDir = new File(tmpKvDir, "indexes");
        SecondaryIndexManager indexManager =
                new SecondaryIndexManager(
                        descriptors,
                        ROW_TYPE,
                        KvFormat.COMPACTED,
                        schemaGetter,
                        indexBaseDir,
                        null);

        // Create KvTablet with the index manager
        TableConfig tableConf = new TableConfig(new Configuration());
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        tablePath.getTablePath(),
                        new TableConfig(new Configuration()),
                        new TestingSequenceGeneratorFactory());
        TableBucket tableBucket = logTablet.getTableBucket();
        kvTablet =
                KvTablet.create(
                        tablePath,
                        tableBucket,
                        logTablet,
                        tmpKvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED,
                        rowMerger,
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        tableConf.getChangelogImage(),
                        indexManager,
                        KvManager.getDefaultRateLimiter(),
                        autoIncrementManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (kvTablet != null) {
            kvTablet.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }
    }

    @Test
    void testHashIndexSearchAfterFlush() throws Exception {
        KvRecordTestUtils.KvRecordBatchFactory batchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
        KvRecordTestUtils.KvRecordFactory recordFactory =
                KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE);

        // Insert three records: two with name "alice", one with name "bob"
        KvRecordBatch batch =
                batchFactory.ofRecords(
                        recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "alice"}),
                        recordFactory.ofRecord("k2".getBytes(), new Object[] {2, "bob"}),
                        recordFactory.ofRecord("k3".getBytes(), new Object[] {3, "alice"}));

        kvTablet.putAsLeader(batch, null);

        // Flush to trigger index updates
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // Search for "alice" — should find 2 keys
        List<byte[]> aliceResults = kvTablet.searchIndex("name_idx", "alice".getBytes(), 10);
        assertThat(aliceResults).hasSize(2);

        // Search for "bob" — should find 1 key
        List<byte[]> bobResults = kvTablet.searchIndex("name_idx", "bob".getBytes(), 10);
        assertThat(bobResults).hasSize(1);

        // Search for "charlie" — should find 0 keys
        List<byte[]> charlieResults = kvTablet.searchIndex("name_idx", "charlie".getBytes(), 10);
        assertThat(charlieResults).isEmpty();
    }

    @Test
    void testHashIndexHandlesUpdates() throws Exception {
        KvRecordTestUtils.KvRecordBatchFactory batchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
        KvRecordTestUtils.KvRecordFactory recordFactory =
                KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE);

        // Insert: k1 -> alice
        KvRecordBatch batch1 =
                batchFactory.ofRecords(
                        recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "alice"}));
        kvTablet.putAsLeader(batch1, null);
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // Verify: alice has 1 result
        assertThat(kvTablet.searchIndex("name_idx", "alice".getBytes(), 10)).hasSize(1);

        // Update: k1 -> bob (same key, different name)
        KvRecordBatch batch2 =
                batchFactory.ofRecords(
                        recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "bob"}));
        kvTablet.putAsLeader(batch2, null);
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // alice should now have 0 results, bob should have 1
        assertThat(kvTablet.searchIndex("name_idx", "alice".getBytes(), 10)).isEmpty();
        assertThat(kvTablet.searchIndex("name_idx", "bob".getBytes(), 10)).hasSize(1);
    }

    @Test
    void testHashIndexHandlesDeletes() throws Exception {
        KvRecordTestUtils.KvRecordBatchFactory batchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
        KvRecordTestUtils.KvRecordFactory recordFactory =
                KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE);

        // Insert: k1 -> alice
        KvRecordBatch batch1 =
                batchFactory.ofRecords(
                        recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "alice"}));
        kvTablet.putAsLeader(batch1, null);
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        assertThat(kvTablet.searchIndex("name_idx", "alice".getBytes(), 10)).hasSize(1);

        // Delete: k1
        KvRecordBatch batch2 =
                batchFactory.ofRecords(recordFactory.ofRecord("k1".getBytes(), null));
        kvTablet.putAsLeader(batch2, null);
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // alice should now have 0 results
        assertThat(kvTablet.searchIndex("name_idx", "alice".getBytes(), 10)).isEmpty();
    }

    @Test
    void testRebuildSecondaryIndexes() throws Exception {
        KvRecordTestUtils.KvRecordBatchFactory batchFactory =
                KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
        KvRecordTestUtils.KvRecordFactory recordFactory =
                KvRecordTestUtils.KvRecordFactory.of(ROW_TYPE);

        // Insert records and flush
        KvRecordBatch batch =
                batchFactory.ofRecords(
                        recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "alice"}),
                        recordFactory.ofRecord("k2".getBytes(), new Object[] {2, "bob"}));
        kvTablet.putAsLeader(batch, null);
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);

        // Verify search works
        assertThat(kvTablet.searchIndex("name_idx", "alice".getBytes(), 10)).hasSize(1);
        assertThat(kvTablet.searchIndex("name_idx", "bob".getBytes(), 10)).hasSize(1);

        // Simulate what recovery does: clear and rebuild
        kvTablet.getSecondaryIndexManager().clear();

        // After clear, search should return nothing
        assertThat(kvTablet.searchIndex("name_idx", "alice".getBytes(), 10)).isEmpty();

        // Rebuild from RocksDB scan
        kvTablet.rebuildSecondaryIndexes();

        // After rebuild, search should work again
        assertThat(kvTablet.searchIndex("name_idx", "alice".getBytes(), 10)).hasSize(1);
        assertThat(kvTablet.searchIndex("name_idx", "bob".getBytes(), 10)).hasSize(1);
    }
}
