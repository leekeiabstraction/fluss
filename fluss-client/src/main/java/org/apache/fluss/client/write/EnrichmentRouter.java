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
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Phase N.3a: per-(tablePath) routing of enrichment writes through a shared {@link
 * EnrichmentSender} daemon thread.
 *
 * <p>Each {@link WriterClient} caches one router per {@link TablePath}. The router lazily creates
 * per-group accumulators (one {@link EnrichmentAccumulator} per column group) and one sender thread
 * that drains across all of them. Closing the router flushes pending batches, stops the sender, and
 * joins the thread.
 */
@Internal
public final class EnrichmentRouter {

    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentRouter.class);

    /**
     * Default sender poll cadence. Small enough that batches don't sit idle past linger; large
     * enough that an empty workload doesn't burn CPU.
     */
    private static final long DEFAULT_POLL_INTERVAL_MS = 2L;

    private final TablePath tablePath;
    private final WriterClient writerClient;
    private final MetadataUpdater metadataUpdater;
    private final Map<String, EnrichmentAccumulator> accumulatorsByGroup =
            MapUtils.newConcurrentHashMap();

    private volatile EnrichmentSender sender;
    private volatile Thread senderThread;
    private volatile boolean closed;

    EnrichmentRouter(
            TablePath tablePath, WriterClient writerClient, MetadataUpdater metadataUpdater) {
        this.tablePath = tablePath;
        this.writerClient = writerClient;
        this.metadataUpdater = metadataUpdater;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    /**
     * Get-or-create an accumulator for the given column group. The first call also starts the
     * sender thread; subsequent calls update the sender's accumulator list. Synchronized so the
     * sender starts exactly once even under concurrent first-call races.
     *
     * <p>Sizing and linger are read from the {@link WriterClient}'s configuration (Phase N.4 —
     * shared with base appends).
     */
    public synchronized EnrichmentAccumulator getOrCreateAccumulator(
            String columnGroup, EnrichmentAccumulator.BatchEncoderInfo encoderInfo) {
        if (closed) {
            throw new IllegalStateException("EnrichmentRouter for " + tablePath + " is closed");
        }
        EnrichmentAccumulator existing = accumulatorsByGroup.get(columnGroup);
        if (existing != null) {
            return existing;
        }
        EnrichmentAccumulator fresh = writerClient.newEnrichmentAccumulator(tablePath, encoderInfo);
        accumulatorsByGroup.put(columnGroup, fresh);
        ensureSenderStarted();
        return fresh;
    }

    private void ensureSenderStarted() {
        List<EnrichmentAccumulator> accumulators = new ArrayList<>(accumulatorsByGroup.values());
        if (sender == null) {
            sender =
                    new EnrichmentSender(
                            tablePath, metadataUpdater, accumulators, DEFAULT_POLL_INTERVAL_MS);
            senderThread = new Thread(sender, "fluss-enrichment-sender-" + tablePath);
            senderThread.setDaemon(true);
            senderThread.start();
        } else {
            sender.updateAccumulators(accumulators);
        }
    }

    /** Flush all in-flight batches, stop the sender, and wait for it to drain. Idempotent. */
    public synchronized void close(Duration timeout) {
        if (closed) {
            return;
        }
        closed = true;
        for (EnrichmentAccumulator accumulator : accumulatorsByGroup.values()) {
            accumulator.flushAll();
        }
        if (sender != null) {
            sender.initiateClose();
            try {
                senderThread.join(timeout.toMillis());
                if (senderThread.isAlive()) {
                    LOG.warn(
                            "Enrichment sender for {} did not stop within {}; interrupting.",
                            tablePath,
                            timeout);
                    senderThread.interrupt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                senderThread.interrupt();
            }
        }
        for (EnrichmentAccumulator accumulator : accumulatorsByGroup.values()) {
            accumulator.close();
        }
        accumulatorsByGroup.clear();
    }
}
