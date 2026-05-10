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

import org.apache.fluss.annotation.Internal;

/** Structure used for lower level reads. */
@Internal
public class LogReadInfo {

    private final FetchDataInfo fetchedData;
    private final long highWatermark;
    private final long logEndOffset;
    /**
     * Per-group enrichment payload to ship to a follower in this fetch's response (E.3b). Empty for
     * client fetches, non-enrichment tables, and pre-Phase-E peers. Each entry carries a zero-copy
     * {@link org.apache.fluss.record.FileLogRecords} slice plus the parallel source offsets it
     * covers; the wire encoder ({@code makeFetchLogResponse}) materializes the bytes.
     */
    private final java.util.Map<String, EnrichmentReadResult> enrichmentPayloadPerGroup;
    /**
     * Leader's view of CEW per group on this bucket, propagated to the follower so it can advance
     * its own committedEnrichmentWatermarks (E.3c). Empty when no enrichment payload is being
     * shipped.
     */
    private final java.util.Map<String, Long> committedEwms;

    public LogReadInfo(FetchDataInfo fetchedData, long highWatermark, long logEndOffset) {
        this(
                fetchedData,
                highWatermark,
                logEndOffset,
                java.util.Collections.emptyMap(),
                java.util.Collections.emptyMap());
    }

    public LogReadInfo(
            FetchDataInfo fetchedData,
            long highWatermark,
            long logEndOffset,
            java.util.Map<String, EnrichmentReadResult> enrichmentPayloadPerGroup,
            java.util.Map<String, Long> committedEwms) {
        this.fetchedData = fetchedData;
        this.highWatermark = highWatermark;
        this.logEndOffset = logEndOffset;
        this.enrichmentPayloadPerGroup = enrichmentPayloadPerGroup;
        this.committedEwms = committedEwms;
    }

    public FetchDataInfo getFetchedData() {
        return fetchedData;
    }

    public long getHighWatermark() {
        return highWatermark;
    }

    public long getLogEndOffset() {
        return logEndOffset;
    }

    public java.util.Map<String, EnrichmentReadResult> getEnrichmentPayloadPerGroup() {
        return enrichmentPayloadPerGroup;
    }

    public java.util.Map<String, Long> getCommittedEwms() {
        return committedEwms;
    }

    @Override
    public String toString() {
        return "LogReadInfo("
                + "fetchedData="
                + fetchedData
                + ", highWatermark="
                + highWatermark
                + ", logEndOffset="
                + logEndOffset
                + ')';
    }
}
