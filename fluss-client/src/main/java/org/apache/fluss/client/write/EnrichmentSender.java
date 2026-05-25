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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogColumnsReqForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogColumnsRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogColumnsRequest;
import org.apache.fluss.rpc.protocol.Errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Phase N.3: daemon-thread driver that polls one or more {@link EnrichmentAccumulator}s for ready
 * batches and ships them via {@link TabletServerGateway#produceLogColumns}.
 *
 * <p>One sender owns one or more accumulators (the {@link
 * org.apache.fluss.client.table.writer.AppendWriter} hosts them, one per column group it sees). The
 * sender keeps running as long as {@link #running} is true; calls to {@link #initiateClose()} drain
 * any remaining batches, complete their futures, then exit.
 */
@Internal
public final class EnrichmentSender implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentSender.class);

    private final TablePath tablePath;
    private final MetadataUpdater metadataUpdater;
    private volatile List<EnrichmentAccumulator> accumulators;
    private final long pollIntervalMs;

    private volatile boolean running = true;

    /**
     * Replace the accumulator list. Used when {@link
     * org.apache.fluss.client.table.writer.AppendWriter#appendColumns} encounters a new column
     * group after the sender has already started.
     */
    public void updateAccumulators(List<EnrichmentAccumulator> accumulators) {
        this.accumulators = accumulators;
    }

    public EnrichmentSender(
            TablePath tablePath,
            MetadataUpdater metadataUpdater,
            List<EnrichmentAccumulator> accumulators,
            long pollIntervalMs) {
        this.tablePath = tablePath;
        this.metadataUpdater = metadataUpdater;
        this.accumulators = accumulators;
        this.pollIntervalMs = pollIntervalMs;
    }

    @Override
    public void run() {
        try {
            while (running) {
                long now = System.currentTimeMillis();
                boolean didWork = drainAndSend(now);
                if (!didWork) {
                    try {
                        Thread.sleep(pollIntervalMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            // Drain pass on shutdown — flush everything still buffered.
            for (EnrichmentAccumulator accumulator : accumulators) {
                accumulator.flushAll();
            }
            drainAndSend(System.currentTimeMillis());
        } catch (Throwable t) {
            LOG.error("EnrichmentSender for {} terminated unexpectedly", tablePath, t);
        }
    }

    private boolean drainAndSend(long nowMs) {
        boolean didWork = false;
        for (EnrichmentAccumulator accumulator : accumulators) {
            Map<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> drained;
            try {
                drained = accumulator.drainReady(nowMs);
            } catch (Exception e) {
                LOG.warn("EnrichmentSender drain failed for {}", tablePath, e);
                continue;
            }
            for (Map.Entry<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> entry :
                    drained.entrySet()) {
                for (EnrichmentWriteBatch.SealedBatch sealed : entry.getValue()) {
                    if (sealed.isEmpty()) {
                        continue;
                    }
                    didWork = true;
                    sendOne(entry.getKey(), sealed);
                }
            }
        }
        return didWork;
    }

    private void sendOne(EnrichmentBatchKey key, EnrichmentWriteBatch.SealedBatch sealed) {
        TableBucket bucket = key.getBucket();
        int leader;
        try {
            leader = metadataUpdater.leaderFor(tablePath, bucket);
        } catch (Exception e) {
            sealed.completeAllExceptionally(
                    new FlussRuntimeException(
                            "Failed to resolve leader for enrichment write on " + bucket, e));
            return;
        }
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            sealed.completeAllExceptionally(
                    new FlussRuntimeException(
                            "No tablet server gateway for leader "
                                    + leader
                                    + " (bucket="
                                    + bucket
                                    + ")"));
            return;
        }
        ProduceLogColumnsRequest request =
                new ProduceLogColumnsRequest()
                        .setTableId(key.getTableId())
                        .setColumnGroup(key.getColumnGroup());
        PbProduceLogColumnsReqForBucket bucketReq = request.addBucketsReq();
        bucketReq.setBucketId(bucket.getBucket());
        if (bucket.getPartitionId() != null) {
            bucketReq.setPartitionId(bucket.getPartitionId());
        }
        BytesView records = sealed.records;
        bucketReq.setRecordsBytesView(records);
        for (long sourceOffset : sealed.sourceOffsets) {
            bucketReq.addSourceOffset(sourceOffset);
        }
        gateway.produceLogColumns(request)
                .whenComplete(
                        (response, error) -> {
                            if (error != null) {
                                sealed.completeAllExceptionally(error);
                                return;
                            }
                            if (response.getBucketsRespsCount() == 0) {
                                sealed.completeAllExceptionally(
                                        new IOException(
                                                "Empty produceLogColumns response for bucket "
                                                        + bucket));
                                return;
                            }
                            PbProduceLogColumnsRespForBucket resp = response.getBucketsRespAt(0);
                            if (resp.hasErrorCode() && resp.getErrorCode() != 0) {
                                Errors err = Errors.forCode((short) resp.getErrorCode());
                                String msg =
                                        "Server rejected enrichment write for bucket "
                                                + bucket
                                                + ": "
                                                + resp.getErrorMessage();
                                sealed.completeAllExceptionally(err.exception(msg));
                            } else {
                                sealed.completeAll(resp.getEnrichmentWatermark());
                            }
                        });
    }

    /** Stop accepting new work and drain remaining batches before the thread exits. */
    public void initiateClose() {
        running = false;
    }

    public boolean isRunning() {
        return running;
    }
}
