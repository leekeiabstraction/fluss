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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Result of {@link FetchLogRequest} for each table bucket. */
@Internal
public class FetchLogResultForBucket extends ResultForBucket {
    private final @Nullable RemoteLogFetchInfo remoteLogFetchInfo;
    private final @Nullable LogRecords records;
    private final long highWatermark;
    private final long filteredEndOffset;
    /**
     * Per-group enrichment payload for replication (E.3b). Empty for client fetches. Each entry
     * carries the records bytes plus the source offsets they cover. The wire encoder reads this to
     * populate {@code PbFetchLogRespForBucket.enrichment_payload_per_group}.
     */
    private final List<EnrichmentPayload> enrichmentPayloadPerGroup;
    /**
     * Leader's view of CEW per group on this bucket, sent to the follower in the response so it can
     * advance its own CEW (E.3c). Empty when no enrichment payload is shipping.
     */
    private final Map<String, Long> committedEwms;

    public FetchLogResultForBucket(
            TableBucket tableBucket, LogRecords records, long highWatermark) {
        this(
                tableBucket,
                null,
                checkNotNull(records, "records can not be null"),
                highWatermark,
                -1L,
                ApiError.NONE,
                Collections.emptyList(),
                Collections.emptyMap());
    }

    public FetchLogResultForBucket(
            TableBucket tableBucket,
            LogRecords records,
            long highWatermark,
            long filteredEndOffset) {
        this(
                tableBucket,
                null,
                checkNotNull(records, "records can not be null"),
                highWatermark,
                filteredEndOffset,
                ApiError.NONE,
                Collections.emptyList(),
                Collections.emptyMap());
    }

    public FetchLogResultForBucket(TableBucket tableBucket, ApiError error) {
        this(
                tableBucket,
                null,
                null,
                -1L,
                -1L,
                error,
                Collections.emptyList(),
                Collections.emptyMap());
    }

    public FetchLogResultForBucket(
            TableBucket tableBucket, RemoteLogFetchInfo remoteLogFetchInfo, long highWatermark) {
        this(
                tableBucket,
                checkNotNull(remoteLogFetchInfo, "remote log fetch info can not be null"),
                null,
                highWatermark,
                -1L,
                ApiError.NONE,
                Collections.emptyList(),
                Collections.emptyMap());
    }

    /**
     * Create a filtered empty response with the correct next fetch offset. This is used when all
     * batches are filtered out but we need to inform the client about the correct offset to
     * continue fetching from.
     */
    public FetchLogResultForBucket(
            TableBucket tableBucket, long highWatermark, long filteredEndOffset) {
        this(
                tableBucket,
                null,
                null,
                highWatermark,
                filteredEndOffset,
                ApiError.NONE,
                Collections.emptyList(),
                Collections.emptyMap());
    }

    private FetchLogResultForBucket(
            TableBucket tableBucket,
            @Nullable RemoteLogFetchInfo remoteLogFetchInfo,
            @Nullable LogRecords records,
            long highWatermark,
            long filteredEndOffset,
            ApiError error,
            List<EnrichmentPayload> enrichmentPayloadPerGroup,
            Map<String, Long> committedEwms) {
        super(tableBucket, error);
        this.remoteLogFetchInfo = remoteLogFetchInfo;
        this.records = records;
        this.highWatermark = highWatermark;
        this.filteredEndOffset = filteredEndOffset;
        this.enrichmentPayloadPerGroup = enrichmentPayloadPerGroup;
        this.committedEwms = committedEwms;
    }

    /**
     * Returns a copy of this result with enrichment replication payload attached. Used by the
     * leader fetch handler in E.3b to layer per-group enrichment + CEW snapshots onto an
     * already-built local-fetch result before serialization.
     */
    public FetchLogResultForBucket withEnrichment(
            List<EnrichmentPayload> enrichmentPayloadPerGroup, Map<String, Long> committedEwms) {
        return new FetchLogResultForBucket(
                getTableBucket(),
                remoteLogFetchInfo,
                records,
                highWatermark,
                filteredEndOffset,
                getError(),
                enrichmentPayloadPerGroup,
                committedEwms);
    }

    /**
     * The fetch result currently supporting only fetch from remote or fetch from local. It means
     * that if remoteLogFetchInfo is not null, the records should be null. Otherwise, the records
     * should not be null.
     *
     * @return {@code true} if the log is fetched from remote.
     */
    public boolean fetchFromRemote() {
        return remoteLogFetchInfo != null;
    }

    public @Nullable LogRecords records() {
        return records;
    }

    public LogRecords recordsOrEmpty() {
        if (records == null) {
            return MemoryLogRecords.EMPTY;
        } else {
            return records;
        }
    }

    public @Nullable RemoteLogFetchInfo remoteLogFetchInfo() {
        return remoteLogFetchInfo;
    }

    public long getHighWatermark() {
        return highWatermark;
    }

    /**
     * Returns whether a filtered end offset is set, indicating that server-side filtering was
     * applied and all batches were filtered out.
     */
    public boolean hasFilteredEndOffset() {
        return filteredEndOffset >= 0;
    }

    /**
     * Returns the offset up to which server-side filtering has been applied. Only meaningful when
     * {@link #hasFilteredEndOffset()} returns {@code true}.
     */
    public long getFilteredEndOffset() {
        return filteredEndOffset;
    }

    /** Per-group enrichment payload to ship to a follower in this fetch's response. */
    public List<EnrichmentPayload> getEnrichmentPayloadPerGroup() {
        return enrichmentPayloadPerGroup;
    }

    /** Leader's view of CEW per group on this bucket. */
    public Map<String, Long> getCommittedEwms() {
        return committedEwms;
    }

    /**
     * Entity-layer counterpart of {@code PbEnrichmentBatchForGroup}: a column group's name plus the
     * {@link LogRecords} bytes (typically a {@link org.apache.fluss.record.FileLogRecords} slice
     * for zero-copy) and the parallel array of source offsets they cover.
     */
    public static final class EnrichmentPayload {
        private final String groupName;
        private final LogRecords records;
        private final long[] sourceOffsets;

        public EnrichmentPayload(String groupName, LogRecords records, long[] sourceOffsets) {
            this.groupName = checkNotNull(groupName, "groupName");
            this.records = checkNotNull(records, "records");
            this.sourceOffsets = checkNotNull(sourceOffsets, "sourceOffsets");
        }

        public String getGroupName() {
            return groupName;
        }

        public LogRecords getRecords() {
            return records;
        }

        public long[] getSourceOffsets() {
            return sourceOffsets;
        }
    }
}
