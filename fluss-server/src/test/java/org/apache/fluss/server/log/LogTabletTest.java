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
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.FlussPaths.offsetFromFile;
import static org.apache.fluss.utils.FlussPaths.writerSnapshotFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LogTablet}. */
final class LogTabletTest extends LogTestBase {
    private @TempDir File tempDir;
    private LogTablet logTablet;
    private FlussScheduler scheduler;
    private File logDir;

    // TODO add more tests refer to kafka's UnifiedLogTest.

    @BeforeEach
    public void setup() throws Exception {
        super.before();
        logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_ID,
                        DATA1_TABLE_PATH.getTableName());
        scheduler = new FlussScheduler(1);
        scheduler.startup();
        logTablet =
                LogTablet.create(
                        PhysicalTablePath.of(DATA1_TABLE_PATH),
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
    public void teardown() throws Exception {
        scheduler.shutdown();
    }

    @Test
    void testHighWatermarkMetadataUpdatedAfterSegmentRoll() throws Exception {
        MemoryLogRecords mr =
                genMemoryLogRecordsByObject(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}));
        logTablet.appendAsLeader(mr);
        assertFetchSizeAndOffsets(0L, 0, Collections.emptyList(), baseRowType);
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
        assertFetchSizeAndOffsets(0L, mr.sizeInBytes(), Arrays.asList(0L, 1L, 2L), baseRowType);
        logTablet.roll(Optional.empty());
        assertFetchSizeAndOffsets(0L, mr.sizeInBytes(), Arrays.asList(0L, 1L, 2L), baseRowType);
        logTablet.appendAsLeader(mr);
        assertFetchSizeAndOffsets(3L, 0, Collections.emptyList(), baseRowType);
    }

    @Test
    void testAppendInfoFirstOffset() throws Exception {
        MemoryLogRecords mr1 =
                genMemoryLogRecordsByObject(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}));

        LogAppendInfo firstAppendInfo = logTablet.appendAsLeader(mr1);
        assertThat(firstAppendInfo.firstOffset()).isEqualTo(0L);

        MemoryLogRecords mr2 =
                genMemoryLogRecordsByObject(
                        Arrays.asList(
                                new Object[] {4, "d"},
                                new Object[] {5, "e"},
                                new Object[] {6, "f"}));
        LogAppendInfo secondAppendInfo = logTablet.appendAsLeader(mr2);
        assertThat(secondAppendInfo.firstOffset()).isEqualTo(3L);

        logTablet.roll(Optional.empty());
        MemoryLogRecords mr3 =
                genMemoryLogRecordsByObject(
                        Arrays.asList(new Object[] {1, "g"}, new Object[] {2, "h"}));
        LogAppendInfo afterRollAppendInfo = logTablet.appendAsLeader(mr3);
        assertThat(afterRollAppendInfo.firstOffset()).isEqualTo(6L);
    }

    @Test
    void testHighWatermarkMaintenance() throws Exception {
        MemoryLogRecords mr1 =
                genMemoryLogRecordsByObject(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}));
        assertHighWatermark(0L);
        // HighWatermark not changed by append.
        logTablet.appendAsLeader(mr1);
        assertHighWatermark(0L);
        // Update highWatermark as leader.
        logTablet.maybeIncrementHighWatermark(new LogOffsetMetadata(1L));
        assertHighWatermark(1L);
        // Cannot update past the log end offset.
        logTablet.updateHighWatermark(5L);
        assertHighWatermark(3L);
    }

    @Test
    void testFetchUpToLogEndOffset() throws Exception {
        MemoryLogRecords mr1 =
                genMemoryLogRecordsByObject(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}));
        logTablet.appendAsLeader(mr1);

        MemoryLogRecords mr2 =
                genMemoryLogRecordsByObject(
                        Arrays.asList(
                                new Object[] {4, "d"},
                                new Object[] {5, "e"},
                                new Object[] {6, "f"}));
        logTablet.appendAsLeader(mr2);

        for (long i = logTablet.localLogEndOffset(); i < logTablet.localLogEndOffset(); i = i + 1) {
            assertNonEmptyFetch(i, FetchIsolation.LOG_END);
        }
    }

    @Test
    void testFetchUpToHighWatermark() throws Exception {
        MemoryLogRecords mr1 =
                genMemoryLogRecordsByObject(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}));
        logTablet.appendAsLeader(mr1);
        MemoryLogRecords mr2 =
                genMemoryLogRecordsByObject(
                        Arrays.asList(new Object[] {4, "a"}, new Object[] {5, "b"}));
        logTablet.appendAsLeader(mr2);
        assertHighWatermarkBoundedFetches();
        logTablet.updateHighWatermark(3L);
        assertHighWatermarkBoundedFetches();
        logTablet.updateHighWatermark(5L);
        assertHighWatermarkBoundedFetches();
    }

    @Test
    void testActiveWriters() throws Exception {
        long writerId1 = 1L;
        MemoryLogRecords records = genMemoryLogRecordsWithWriterId(DATA1, writerId1, 0, 0L);
        logTablet.appendAsLeader(records);
        WriterStateEntry entry = logTablet.activeWriters().get(writerId1);
        assertThat(entry).isNotNull();
        assertThat(entry.lastBatchSequence()).isEqualTo(0);
        assertThat(entry.firstDataOffset()).isEqualTo(0L);

        long writerId2 = 2L;
        records = genMemoryLogRecordsWithWriterId(DATA1, writerId2, 0, 0L);
        logTablet.appendAsLeader(records);
        entry = logTablet.activeWriters().get(writerId2);
        assertThat(entry).isNotNull();
        assertThat(entry.lastBatchSequence()).isEqualTo(0);
        assertThat(entry.firstDataOffset()).isEqualTo(10L);
    }

    @Test
    void testOffsetFromWriterSnapshotFile() {
        long offset = 23423423L;
        File snapshotFile = writerSnapshotFile(tempDir, offset);
        assertThat(offsetFromFile(snapshotFile)).isEqualTo(offset);
    }

    @Test
    void testNonSequentialAppend() throws Exception {
        long writerId = 1L;
        MemoryLogRecords records = genMemoryLogRecordsWithWriterId(DATA1, writerId, 0, 0);
        logTablet.appendAsLeader(records);

        MemoryLogRecords nextRecords = genMemoryLogRecordsWithWriterId(DATA1, writerId, 2, 0);
        assertThatThrownBy(() -> logTablet.appendAsLeader(nextRecords))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        String.format(
                                "Out of order batch sequence for writer 1 at offset 19 in table-bucket "
                                        + "TableBucket{tableId=150001, bucket=%s} : 2 (incoming batch seq.), 0 (current batch seq.)",
                                logTablet.getTableBucket().getBucket()));
    }

    @Test
    void testWriterExpireCheckAfterDelete() {
        // Test that "PeriodicWriterExpirationCheck" scheduled task gets canceled after log is
        // deleted.
        ScheduledFuture<?> writerExpireCheck = logTablet.writerExpireCheck();
        assertThat(scheduler.taskRunning(writerExpireCheck)).isTrue();

        logTablet.drop();
        assertThat(scheduler.taskRunning(writerExpireCheck)).isFalse();
    }

    @Test
    void testWriterStateOffsetUpdatedForNonIdempotentData() throws Exception {
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        logTablet.appendAsLeader(records);
        takeWriterSnapshot(logTablet);
        assertThat(latestWriterSnapshotOffset(logTablet).get()).isEqualTo(10L);
    }

    @Test
    void testWriterStateTruncateTo() throws Exception {
        logTablet.appendAsLeader(
                genMemoryLogRecordsByObject(Collections.singletonList(new Object[] {1, "a"})));
        logTablet.appendAsLeader(
                genMemoryLogRecordsByObject(Collections.singletonList(new Object[] {2, "b"})));
        takeWriterSnapshot(logTablet);

        logTablet.appendAsLeader(
                genMemoryLogRecordsByObject(Collections.singletonList(new Object[] {3, "c"})));
        takeWriterSnapshot(logTablet);

        logTablet.truncateTo(2L);
        assertThat(latestWriterSnapshotOffset(logTablet).get()).isEqualTo(2L);
        assertThat(latestWriterStateEndOffset(logTablet)).isEqualTo(2);

        logTablet.truncateTo(1L);
        assertThat(latestWriterSnapshotOffset(logTablet).get()).isEqualTo(1L);
        assertThat(latestWriterStateEndOffset(logTablet)).isEqualTo(1);

        logTablet.truncateTo(0L);
        assertThat(latestWriterSnapshotOffset(logTablet)).isNotPresent();
        assertThat(latestWriterStateEndOffset(logTablet)).isEqualTo(0);
    }

    @Test
    void testWriterStateTruncateToWithNoSnapshots() throws Exception {
        // This ensures that the upgrade optimization path cannot be hit after initial loading.
        long writerId = 1L;

        logTablet.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {1, "a"}), writerId, 0, 0L));
        logTablet.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {1, "b"}), writerId, 1, 0L));

        LogTestUtils.deleteWriterSnapshotFiles(logDir);

        logTablet.truncateTo(1L);
        assertThat(activeWritersWithLastBatchSequence(logTablet).size()).isEqualTo(1);

        Integer lastBatchSeq = activeWritersWithLastBatchSequence(logTablet).get(writerId);
        assertThat(lastBatchSeq).isEqualTo(0);
    }

    @Test
    void testWriterStateTruncateFullyAndStartAt() throws Exception {
        MemoryLogRecords records =
                genMemoryLogRecordsByObject(Collections.singletonList(new Object[] {1, "a"}));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, new MemorySize(records.sizeInBytes()));
        LogTablet log = createLogTablet(conf);
        log.appendAsLeader(records);
        takeWriterSnapshot(log);

        log.appendAsLeader(
                genMemoryLogRecordsByObject(Collections.singletonList(new Object[] {2, "b"})));
        log.appendAsLeader(
                genMemoryLogRecordsByObject(Collections.singletonList(new Object[] {3, "c"})));
        takeWriterSnapshot(log);

        assertThat(log.logSegments().size()).isEqualTo(3);
        assertThat(latestWriterStateEndOffset(log)).isEqualTo(3);
        assertThat(latestWriterSnapshotOffset(log)).isEqualTo(Optional.of(3L));

        log.truncateFullyAndStartAt(29);
        assertThat(log.logSegments().size()).isEqualTo(1);
        assertThat(latestWriterStateEndOffset(log)).isEqualTo(29);
        assertThat(latestWriterSnapshotOffset(log).get()).isEqualTo(29);
    }

    @Test
    void testWriterIdExpirationOnSegmentDeletion() throws Exception {
        long writerId1 = 1L;
        MemoryLogRecords records =
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {1, "a"}), writerId1, 0, 0L);
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, new MemorySize(records.sizeInBytes()));
        LogTablet log = createLogTablet(conf);
        log.appendAsLeader(records);
        takeWriterSnapshot(log);

        long writerId2 = 2L;
        log.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {2, "b"}), writerId2, 0, 0L));
        log.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {2, "b"}), writerId2, 1, 0L));
        takeWriterSnapshot(log);

        assertThat(log.logSegments().size()).isEqualTo(3);
        assertThat(log.activeWriters().keySet())
                .isEqualTo(new HashSet<>(Arrays.asList(writerId1, writerId2)));

        log.updateHighWatermark(log.localLogEndOffset());

        // TODO add delete log segment logic.
    }

    @Test
    void testWriterSnapshotAfterSegmentRollOnAppend() throws Exception {
        long writerId = 1L;
        MemoryLogRecords records =
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {1, "a"}), writerId, 0, 0L);
        conf.set(
                ConfigOptions.LOG_SEGMENT_FILE_SIZE,
                new MemorySize(records.sizeInBytes() * 2L - 1));
        LogTablet log = createLogTablet(conf);

        log.appendAsLeader(records);

        // The next append should overflow the segment and cause it to roll.
        log.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {1, "a"}), writerId, 1, 0L));

        assertThat(log.logSegments().size()).isEqualTo(2);
        assertThat(log.activeLogSegment().getBaseOffset()).isEqualTo(1L);
        assertThat(latestWriterSnapshotOffset(log)).isEqualTo(Optional.of(1L));

        // Force a reload from the snapshot to check its consistency.
        log.truncateTo(1L);

        assertThat(log.logSegments().size()).isEqualTo(2);
        assertThat(log.activeLogSegment().getBaseOffset()).isEqualTo(1L);
        assertThat(latestWriterSnapshotOffset(log)).isEqualTo(Optional.of(1L));

        Optional<WriterStateEntry> lastEntry = log.writerStateManager().lastEntry(writerId);
        assertThat(lastEntry).isPresent();
        assertThat(lastEntry.get().firstDataOffset()).isEqualTo(0L);
        assertThat(lastEntry.get().lastDataOffset()).isEqualTo(0L);
    }

    @Test
    void testPeriodicWriterIdExpiration() throws Exception {
        conf.set(ConfigOptions.WRITER_ID_EXPIRATION_TIME, Duration.ofMillis(1000));
        conf.set(ConfigOptions.WRITER_ID_EXPIRATION_CHECK_INTERVAL, Duration.ofMillis(1000));
        long writerId1 = 23L;
        LogTablet log = createLogTablet(conf);
        log.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {1, "a"}), writerId1, 0, 0L));
        assertThat(log.activeWriters().keySet()).isEqualTo(Collections.singleton(writerId1));
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(log.activeWriters().keySet()).isEqualTo(Collections.emptySet()));
    }

    @Test
    void testDuplicateAppends() throws Exception {
        long writerId = 1L;
        // Pad the beginning of the log.
        for (int i = 0; i <= 5; i++) {
            logTablet.appendAsLeader(
                    genMemoryLogRecordsWithWriterId(
                            Collections.singletonList(new Object[] {1, "a"}), writerId, i, 0L));
        }

        // Append an entry with multiple log records.
        LogAppendInfo multiEntryAppendInfo =
                logTablet.appendAsLeader(genMemoryLogRecordsWithWriterId(DATA1, writerId, 6, 0L));
        assertThat(multiEntryAppendInfo.lastOffset() - multiEntryAppendInfo.firstOffset() + 1)
                .isEqualTo(DATA1.size());

        // Append a Duplicate of the tail, when the entry at the tail has multiple records.
        LogAppendInfo dupMultiEntryAppendInfo =
                logTablet.appendAsLeader(genMemoryLogRecordsWithWriterId(DATA1, writerId, 6, 0L));
        assertThat(dupMultiEntryAppendInfo.firstOffset())
                .isEqualTo(multiEntryAppendInfo.firstOffset());
        assertThat(dupMultiEntryAppendInfo.lastOffset())
                .isEqualTo(multiEntryAppendInfo.lastOffset());

        // Append a partial duplicate of the tail. This is not allowed.
        assertThatThrownBy(
                        () ->
                                logTablet.appendAsLeader(
                                        genMemoryLogRecordsWithWriterId(DATA1, writerId, 8, 0L)))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        String.format(
                                "Out of order batch sequence for writer 1 at offset 25 in table-bucket "
                                        + "TableBucket{tableId=150001, bucket=%s} : 8 (incoming batch seq.), 6 (current batch seq.)",
                                logTablet.getTableBucket().getBucket()));

        // Append a duplicate of the batch which is 4th from the tail. This should succeed without
        // error since we retain the batch metadata of the last 5 batches.
        logTablet.appendAsLeader(genMemoryLogRecordsWithWriterId(DATA1, writerId, 4, 0L));

        // Duplicates at older entries are reported as OutOfOrderSequence errors. batch 1 is removed
        // from writerState.
        assertThatThrownBy(
                        () ->
                                logTablet.appendAsLeader(
                                        genMemoryLogRecordsWithWriterId(DATA1, writerId, 1, 0L)))
                .isInstanceOf(OutOfOrderSequenceException.class)
                .hasMessageContaining(
                        String.format(
                                "Out of order batch sequence for writer 1 at offset 25 in table-bucket "
                                        + "TableBucket{tableId=150001, bucket=%s} : 1 (incoming batch seq.), 6 (current batch seq.)",
                                logTablet.getTableBucket().getBucket()));
    }

    private LogTablet createLogTablet(Configuration config) throws Exception {
        File logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_ID,
                        DATA1_TABLE_PATH.getTableName());
        Scheduler scheduler = new FlussScheduler(1);
        scheduler.startup();
        return LogTablet.create(
                PhysicalTablePath.of(DATA1_TABLE_PATH),
                logDir,
                config,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0,
                scheduler,
                LogFormat.ARROW,
                1,
                false,
                SystemClock.getInstance(),
                true);
    }

    @Test
    void testEnrichmentRecoversAcrossRestart() throws Exception {
        // Append base records so localLogEndOffset >= 2; otherwise the recovery clamp
        // (added in E.5b) will pull EWM back to 0 because the disk state would represent
        // an enrichment-ahead-of-base condition that production never enters.
        for (int i = 0; i < 2; i++) {
            logTablet.appendAsLeader(
                    genMemoryLogRecordsByObject(
                            Collections.singletonList(new Object[] {i, "v" + i})));
        }

        String groupName = "enriched_test";
        logTablet.registerColumnGroupIfAbsent(groupName);
        assertThat(logTablet.getEnrichmentWatermark(groupName)).isEqualTo(0L);

        for (int i = 0; i < 2; i++) {
            logTablet.appendColumnsAsLeader(
                    groupName, buildSingleRowEnrichment("US-WEST-" + i), new long[] {(long) i});
        }
        assertThat(logTablet.getEnrichmentWatermark(groupName)).isEqualTo(2L);

        // Capture the dir before close so we can reopen on it.
        File tabletDir = logTablet.getLogDir();
        logTablet.close();

        // Phase 2: a fresh LogTablet on the same dir must rediscover the segment + EWM.
        Scheduler scheduler2 = new FlussScheduler(1);
        scheduler2.startup();
        try {
            LogTablet recovered =
                    LogTablet.create(
                            PhysicalTablePath.of(DATA1_TABLE_PATH),
                            tabletDir,
                            conf,
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            0,
                            scheduler2,
                            LogFormat.ARROW,
                            1,
                            false,
                            SystemClock.getInstance(),
                            true);
            try {
                assertThat(recovered.getEnrichmentWatermark(groupName)).isEqualTo(2L);
                assertThat(recovered.getAllEnrichmentWatermarks()).containsEntry(groupName, 2L);
            } finally {
                recovered.close();
            }
        } finally {
            scheduler2.shutdown();
        }
    }

    @Test
    void testTruncateToClampsEnrichment() throws Exception {
        // 5 base records → localLogEndOffset = 5.
        for (int i = 0; i < 5; i++) {
            logTablet.appendAsLeader(
                    genMemoryLogRecordsByObject(
                            Collections.singletonList(new Object[] {i, "v" + i})));
        }

        String groupName = "enriched_test";
        logTablet.registerColumnGroupIfAbsent(groupName);
        for (int i = 0; i < 5; i++) {
            logTablet.appendColumnsAsLeader(
                    groupName, buildSingleRowEnrichment("US-WEST-" + i), new long[] {(long) i});
        }
        assertThat(logTablet.getEnrichmentWatermark(groupName)).isEqualTo(5L);
        EnrichmentSegment seg = logTablet.getEnrichmentSegment(groupName);
        int sizeBefore = seg.records().sizeInBytes();
        assertThat(sizeBefore).isPositive();

        // Truncate base log to offset 3. Enrichment for offsets {3, 4} must be dropped.
        logTablet.truncateTo(3L);

        assertThat(logTablet.getEnrichmentWatermark(groupName)).isEqualTo(3L);
        assertThat(seg.lastEnrichedOffset()).isEqualTo(2L);
        assertThat(seg.lookup(2L)).isNotNull();
        assertThat(seg.lookup(3L)).isNull();
        assertThat(seg.lookup(4L)).isNull();
        assertThat(seg.records().sizeInBytes()).isLessThan(sizeBefore);
    }

    @Test
    void testTruncateFullyAndStartAtClearsEnrichment() throws Exception {
        for (int i = 0; i < 3; i++) {
            logTablet.appendAsLeader(
                    genMemoryLogRecordsByObject(
                            Collections.singletonList(new Object[] {i, "v" + i})));
        }
        String groupA = "geo";
        String groupB = "device";
        logTablet.registerColumnGroupIfAbsent(groupA);
        logTablet.registerColumnGroupIfAbsent(groupB);
        for (int i = 0; i < 3; i++) {
            logTablet.appendColumnsAsLeader(
                    groupA, buildSingleRowEnrichment("US-WEST-" + i), new long[] {(long) i});
            logTablet.appendColumnsAsLeader(
                    groupB, buildSingleRowEnrichment("dev-" + i), new long[] {(long) i});
        }
        assertThat(logTablet.getEnrichmentWatermark(groupA)).isEqualTo(3L);
        assertThat(logTablet.getEnrichmentWatermark(groupB)).isEqualTo(3L);

        // truncateFullyAndStartAt(5) clears the base log and starts at 5; every enrichment entry
        // is now orphaned and must be dropped from every registered group.
        logTablet.truncateFullyAndStartAt(5L);

        assertThat(logTablet.getEnrichmentWatermark(groupA)).isEqualTo(0L);
        assertThat(logTablet.getEnrichmentWatermark(groupB)).isEqualTo(0L);
        assertThat(logTablet.getEnrichmentSegment(groupA).records().sizeInBytes()).isZero();
        assertThat(logTablet.getEnrichmentSegment(groupB).records().sizeInBytes()).isZero();
        assertThat(logTablet.getEnrichmentSegment(groupA).lastEnrichedOffset()).isEqualTo(-1L);
        assertThat(logTablet.getEnrichmentSegment(groupB).lastEnrichedOffset()).isEqualTo(-1L);
    }

    @Test
    void testRecoveryClampsEnrichmentAheadOfBaseLog() throws Exception {
        // 2 base records → localLogEndOffset = 2.
        for (int i = 0; i < 2; i++) {
            logTablet.appendAsLeader(
                    genMemoryLogRecordsByObject(
                            Collections.singletonList(new Object[] {i, "v" + i})));
        }

        // Register and append enrichment for source_offsets 0..3 — beyond localLogEndOffset.
        // LogTablet.appendColumnsAsLeader accepts this; the in-range check lives in Replica.
        // This simulates the crash window left by base-first / enrichment-second truncation
        // (PHASE_E_REPLICATION §4.4).
        String groupName = "enriched_test";
        logTablet.registerColumnGroupIfAbsent(groupName);
        for (int i = 0; i < 4; i++) {
            logTablet.appendColumnsAsLeader(
                    groupName, buildSingleRowEnrichment("US-WEST-" + i), new long[] {(long) i});
        }
        assertThat(logTablet.getEnrichmentWatermark(groupName)).isEqualTo(4L);

        File tabletDir = logTablet.getLogDir();
        logTablet.close();

        // Recovery must clamp the enrichment tail to localLogEndOffset = 2.
        Scheduler scheduler2 = new FlussScheduler(1);
        scheduler2.startup();
        try {
            LogTablet recovered =
                    LogTablet.create(
                            PhysicalTablePath.of(DATA1_TABLE_PATH),
                            tabletDir,
                            conf,
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            0,
                            scheduler2,
                            LogFormat.ARROW,
                            1,
                            false,
                            SystemClock.getInstance(),
                            true);
            try {
                assertThat(recovered.localLogEndOffset()).isEqualTo(2L);
                assertThat(recovered.getEnrichmentWatermark(groupName)).isEqualTo(2L);
                EnrichmentSegment recoveredSeg = recovered.getEnrichmentSegment(groupName);
                assertThat(recoveredSeg.lastEnrichedOffset()).isEqualTo(1L);
                assertThat(recoveredSeg.lookup(2L)).isNull();
                assertThat(recoveredSeg.lookup(3L)).isNull();
            } finally {
                recovered.close();
            }
        } finally {
            scheduler2.shutdown();
        }
    }

    private MemoryLogRecords buildSingleRowEnrichment(String value) throws Exception {
        RowType groupRowType =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {
                            org.apache.fluss.types.DataTypes.STRING()
                        },
                        new String[] {"geo"});
        return org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords(
                groupRowType,
                DEFAULT_SCHEMA_ID,
                0L,
                System.currentTimeMillis(),
                LogRecordBatch.CURRENT_LOG_MAGIC_VALUE,
                org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID,
                org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE,
                Collections.singletonList(org.apache.fluss.record.ChangeType.APPEND_ONLY),
                Collections.singletonList(new Object[] {value}),
                LogFormat.ARROW,
                org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION);
    }

    private void assertFetchSizeAndOffsets(
            long fetchOffset, int expectedSize, List<Long> expectedOffsets, RowType rowType)
            throws Exception {
        FetchDataInfo fetchInfo =
                readLog(logTablet, fetchOffset, 2048, FetchIsolation.HIGH_WATERMARK, false);
        assertThat(fetchInfo.getRecords().sizeInBytes()).isEqualTo(expectedSize);
        List<Long> actualOffsets = new ArrayList<>();
        Iterable<LogRecordBatch> batchIterable = fetchInfo.getRecords().batches();
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        baseRowType, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            for (LogRecordBatch batch : batchIterable) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        actualOffsets.add(iter.next().logOffset());
                    }
                }
            }
        }
        assertThat(actualOffsets).isEqualTo(expectedOffsets);
    }

    private void assertHighWatermark(long offset) throws IOException {
        assertThat(logTablet.getHighWatermark()).isEqualTo(offset);
        assertValidLogOffsetMetadata(logTablet.fetchHighWatermarkMetadata());
    }

    private void assertHighWatermarkBoundedFetches() throws Exception {
        for (long offset = 0; offset < logTablet.getHighWatermark(); offset++) {
            assertNonEmptyFetch(offset, FetchIsolation.HIGH_WATERMARK);
        }
        for (long offset = logTablet.getHighWatermark();
                offset <= logTablet.localLogEndOffset();
                offset++) {
            assertEmptyFetch(offset, FetchIsolation.HIGH_WATERMARK);
        }
    }

    private void assertNonEmptyFetch(long offset, FetchIsolation isolation) throws Exception {
        FetchDataInfo readInfo = readLog(logTablet, offset, Integer.MAX_VALUE, isolation, true);

        assertThat(readInfo.getRecords().sizeInBytes() > 0).isTrue();

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        baseRowType, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            for (LogRecordBatch batch : readInfo.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        LogRecord record = iter.next();
                        long upperBoundOffset;
                        if (isolation == FetchIsolation.HIGH_WATERMARK) {
                            upperBoundOffset = logTablet.getHighWatermark();
                        } else {
                            upperBoundOffset = logTablet.localLogEndOffset();
                        }
                        assertThat(record.logOffset() < upperBoundOffset).isTrue();
                    }
                }
            }
        }

        assertThat(readInfo.getFetchOffsetMetadata().getMessageOffset()).isEqualTo(offset);
        assertValidLogOffsetMetadata(readInfo.getFetchOffsetMetadata());
    }

    private void assertEmptyFetch(long offset, FetchIsolation isolation) throws Exception {
        FetchDataInfo readInfo = readLog(logTablet, offset, Integer.MAX_VALUE, isolation, true);
        assertThat(readInfo.getRecords().sizeInBytes() == 0).isTrue();
        assertThat(readInfo.getFetchOffsetMetadata().getMessageOffset()).isEqualTo(offset);
        assertValidLogOffsetMetadata(readInfo.getFetchOffsetMetadata());
    }

    private FetchDataInfo readLog(
            LogTablet logTablet,
            long offset,
            int maxLength,
            FetchIsolation isolation,
            boolean minOneMessage)
            throws Exception {
        return logTablet.read(offset, maxLength, isolation, minOneMessage, null, null);
    }

    private void assertValidLogOffsetMetadata(LogOffsetMetadata offsetMetadata) throws IOException {
        assertThat(offsetMetadata.messageOffsetOnly()).isFalse();

        long segmentBaseOffset = offsetMetadata.getSegmentBaseOffset();
        List<LogSegment> logSegments =
                logTablet.logSegments(segmentBaseOffset, segmentBaseOffset + 1);
        assertThat(logSegments.isEmpty()).isFalse();
        LogSegment segment = logSegments.get(0);
        assertThat(segment.getBaseOffset()).isEqualTo(segmentBaseOffset);
        assertThat(offsetMetadata.getRelativePositionInSegment() <= segment.getSizeInBytes())
                .isTrue();

        FetchDataInfo readInfo =
                segment.read(
                        offsetMetadata.getMessageOffset(), 2048, segment.getSizeInBytes(), false);

        if (offsetMetadata.getRelativePositionInSegment() < segment.getSizeInBytes()) {
            assertThat(readInfo.getFetchOffsetMetadata()).isEqualTo(offsetMetadata);
        } else {
            assertThat(readInfo).isNull();
        }
    }

    private Map<Long, Integer> activeWritersWithLastBatchSequence(LogTablet logTablet) {
        Map<Long, Integer> result = new HashMap<>();
        logTablet
                .writerStateManager()
                .activeWriters()
                .forEach((writerId, entry) -> result.put(writerId, entry.lastBatchSequence()));
        return result;
    }

    private void takeWriterSnapshot(LogTablet logTablet) throws IOException {
        logTablet.writerStateManager().takeSnapshot();
    }

    private Optional<Long> latestWriterSnapshotOffset(LogTablet logTablet) {
        return logTablet.writerStateManager().latestSnapshotOffset();
    }

    private long latestWriterStateEndOffset(LogTablet logTablet) {
        return logTablet.writerStateManager().mapEndOffset();
    }
}
