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

package org.apache.fluss.client.write;

import org.apache.fluss.client.table.writer.AppendColumnsResult;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Phase N.2 unit tests for {@link EnrichmentAccumulator} and {@link EnrichmentWriteBatch}. */
public class EnrichmentAccumulatorTest {

    private static final long TABLE_ID = 7L;
    private static final String GROUP = "enriched";
    private static final RowType GROUP_ROW_TYPE =
            RowType.of(
                    new org.apache.fluss.types.DataType[] {DataTypes.STRING(), DataTypes.DOUBLE()},
                    new String[] {"geo_region", "risk_score"});
    private static final int SCHEMA_ID = 1;
    private static final int BATCH_SIZE = 4 * 1024;
    private static final int BUFFER_BYTES = 4 * 1024;

    private BufferAllocator allocator;
    private ArrowWriterPool writerPool;
    private TestingMemorySegmentPool memorySegmentPool;
    private EnrichmentAccumulator accumulator;

    @BeforeEach
    void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        writerPool = new ArrowWriterPool(allocator);
        memorySegmentPool = new TestingMemorySegmentPool(BUFFER_BYTES);
        accumulator =
                new EnrichmentAccumulator(
                        TablePath.of("test_db", "test_table"),
                        writerPool,
                        allocator,
                        memorySegmentPool,
                        /* batchSizeEstimator */ null,
                        new EnrichmentAccumulator.BatchEncoderInfo(
                                GROUP_ROW_TYPE, SCHEMA_ID, DEFAULT_COMPRESSION),
                        BATCH_SIZE,
                        /* lingerMs */ 50L,
                        BUFFER_BYTES);
    }

    @AfterEach
    void teardown() {
        accumulator.close();
        writerPool.close();
        allocator.close();
    }

    @Test
    void appendsToOneKeyMergeIntoOneBatch() throws Exception {
        EnrichmentBatchKey key = key(0);
        long now = 1000L;
        CompletableFuture<AppendColumnsResult> f0 = accumulator.append(key, 0, row("US", 0.1), now);
        CompletableFuture<AppendColumnsResult> f1 = accumulator.append(key, 1, row("CA", 0.2), now);
        CompletableFuture<AppendColumnsResult> f2 = accumulator.append(key, 2, row("UK", 0.3), now);

        assertThat(accumulator.queueDepths().get(key)).isEqualTo(1);
        assertThat(f0).isNotDone();
        assertThat(f1).isNotDone();
        assertThat(f2).isNotDone();

        // Below linger window — not ready.
        assertThat(accumulator.hasReady(now + 10)).isFalse();
        // Past linger window — ready.
        assertThat(accumulator.hasReady(now + 100)).isTrue();

        Map<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> drained =
                accumulator.drainReady(now + 100);
        assertThat(drained).containsOnlyKeys(key);
        assertThat(drained.get(key)).hasSize(1);
        EnrichmentWriteBatch.SealedBatch sealed = drained.get(key).get(0);
        assertThat(sealed.sourceOffsets).containsExactly(0L, 1L, 2L);
        assertThat(sealed.futures).hasSize(3);

        // After drain, the queue is empty (entry remains in the map for reuse, depth=0).
        assertThat(accumulator.queueDepths().get(key)).isEqualTo(0);

        sealed.completeAll(3L);
        assertThat(f0.get().getEnrichmentWatermark()).isEqualTo(3L);
        assertThat(f1.get().getEnrichmentWatermark()).isEqualTo(3L);
        assertThat(f2.get().getEnrichmentWatermark()).isEqualTo(3L);
    }

    @Test
    void differentKeysProduceDifferentBatches() throws Exception {
        EnrichmentBatchKey k0 = key(0);
        EnrichmentBatchKey k1 = key(1);
        long now = 2000L;
        accumulator.append(k0, 0, row("US", 0.1), now);
        accumulator.append(k1, 0, row("CA", 0.2), now);

        accumulator.flushAll();
        Map<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> drained =
                accumulator.drainReady(now);
        assertThat(drained).containsOnlyKeys(k0, k1);
        assertThat(drained.get(k0).get(0).sourceOffsets).containsExactly(0L);
        assertThat(drained.get(k1).get(0).sourceOffsets).containsExactly(0L);
    }

    @Test
    void flushAllForcesReadinessRegardlessOfLinger() throws Exception {
        EnrichmentBatchKey key = key(0);
        long now = 3000L;
        CompletableFuture<AppendColumnsResult> f = accumulator.append(key, 0, row("US", 0.1), now);

        // Linger not yet expired.
        assertThat(accumulator.hasReady(now + 5)).isFalse();
        accumulator.flushAll();
        // Now ready.
        assertThat(accumulator.hasReady(now + 5)).isTrue();

        Map<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> drained =
                accumulator.drainReady(now + 5);
        assertThat(drained.get(key)).hasSize(1);
        drained.get(key).get(0).completeAll(1L);
        assertThat(f).isDone();
    }

    @Test
    void completeWithErrorFansOutToFutures() throws Exception {
        EnrichmentBatchKey key = key(0);
        long now = 4000L;
        CompletableFuture<AppendColumnsResult> f0 = accumulator.append(key, 0, row("US", 0.1), now);
        CompletableFuture<AppendColumnsResult> f1 = accumulator.append(key, 1, row("CA", 0.2), now);

        accumulator.flushAll();
        Map<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> drained =
                accumulator.drainReady(now);
        EnrichmentWriteBatch.SealedBatch sealed = drained.get(key).get(0);

        RuntimeException ex = new RuntimeException("boom");
        sealed.completeAllExceptionally(ex);

        assertThat(f0).isCompletedExceptionally();
        assertThat(f1).isCompletedExceptionally();
    }

    @Test
    void closedAccumulatorRejectsAppends() {
        accumulator.close();
        CompletableFuture<AppendColumnsResult> failed =
                accumulator.append(key(0), 0, row("US", 0.1), 0L);
        assertThat(failed).isCompletedExceptionally();
        assertThatThrownBy(failed::get).hasMessageContaining("Accumulator is closed");
    }

    private static EnrichmentBatchKey key(int bucketId) {
        return new EnrichmentBatchKey(TABLE_ID, GROUP, new TableBucket(TABLE_ID, bucketId));
    }

    private static GenericRow row(String geo, double risk) {
        GenericRow r = new GenericRow(2);
        r.setField(0, org.apache.fluss.row.BinaryString.fromString(geo));
        r.setField(1, risk);
        return r;
    }
}
