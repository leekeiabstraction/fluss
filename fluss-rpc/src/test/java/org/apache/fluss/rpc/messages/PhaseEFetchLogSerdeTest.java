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

package org.apache.fluss.rpc.messages;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip serde for the Phase E.3a wire-format additions to {@link PbFetchLogReqForBucket} and
 * {@link PbFetchLogRespForBucket}: per-group follower EWM cursor on the request, and replicated
 * enrichment payload + committed EWM on the response. The fields are inert today (E.3b/c/d wires
 * them into the fetch handler), but locking in the wire format here lets future commits change
 * behaviour without touching the bytes.
 */
class PhaseEFetchLogSerdeTest {

    @Test
    void testFollowerEwmRequestsRoundTrip() {
        PbFetchLogReqForBucket original =
                new PbFetchLogReqForBucket()
                        .setBucketId(1)
                        .setFetchOffset(100L)
                        .setMaxFetchBytes(1024);
        original.addFollowerEwmRequest().setGroupName("geo").setCurrentEwm(50L);
        original.addFollowerEwmRequest().setGroupName("device").setCurrentEwm(75L);

        PbFetchLogReqForBucket parsed = roundTrip(original, new PbFetchLogReqForBucket());

        assertThat(parsed.getBucketId()).isEqualTo(1);
        assertThat(parsed.getFetchOffset()).isEqualTo(100L);
        assertThat(parsed.getMaxFetchBytes()).isEqualTo(1024);
        assertThat(parsed.getFollowerEwmRequestsCount()).isEqualTo(2);
        assertThat(parsed.getFollowerEwmRequestAt(0).getGroupName()).isEqualTo("geo");
        assertThat(parsed.getFollowerEwmRequestAt(0).getCurrentEwm()).isEqualTo(50L);
        assertThat(parsed.getFollowerEwmRequestAt(1).getGroupName()).isEqualTo("device");
        assertThat(parsed.getFollowerEwmRequestAt(1).getCurrentEwm()).isEqualTo(75L);
    }

    @Test
    void testEnrichmentResponseRoundTrip() {
        byte[] geoBytes = new byte[] {1, 2, 3, 4};
        byte[] deviceBytes = new byte[] {9, 9, 9};

        PbFetchLogRespForBucket original =
                new PbFetchLogRespForBucket()
                        .setBucketId(1)
                        .setHighWatermark(200L)
                        .setLogStartOffset(0L);

        PbEnrichmentBatchForGroup geoBatch = original.addEnrichmentPayloadPerGroup();
        geoBatch.setGroupName("geo").setRecords(geoBytes);
        geoBatch.addSourceOffset(10L);
        geoBatch.addSourceOffset(11L);

        PbEnrichmentBatchForGroup deviceBatch = original.addEnrichmentPayloadPerGroup();
        deviceBatch.setGroupName("device").setRecords(deviceBytes);
        deviceBatch.addSourceOffset(42L);

        original.addCommittedEwm().setGroupName("geo").setCew(10L);
        original.addCommittedEwm().setGroupName("device").setCew(42L);

        PbFetchLogRespForBucket parsed = roundTrip(original, new PbFetchLogRespForBucket());

        assertThat(parsed.getBucketId()).isEqualTo(1);
        assertThat(parsed.getHighWatermark()).isEqualTo(200L);

        assertThat(parsed.getEnrichmentPayloadPerGroupsCount()).isEqualTo(2);
        PbEnrichmentBatchForGroup parsedGeo = parsed.getEnrichmentPayloadPerGroupAt(0);
        assertThat(parsedGeo.getGroupName()).isEqualTo("geo");
        assertThat(parsedGeo.getRecords()).isEqualTo(geoBytes);
        assertThat(parsedGeo.getSourceOffsetsCount()).isEqualTo(2);
        assertThat(parsedGeo.getSourceOffsetAt(0)).isEqualTo(10L);
        assertThat(parsedGeo.getSourceOffsetAt(1)).isEqualTo(11L);

        PbEnrichmentBatchForGroup parsedDevice = parsed.getEnrichmentPayloadPerGroupAt(1);
        assertThat(parsedDevice.getGroupName()).isEqualTo("device");
        assertThat(parsedDevice.getRecords()).isEqualTo(deviceBytes);
        assertThat(parsedDevice.getSourceOffsetsCount()).isEqualTo(1);
        assertThat(parsedDevice.getSourceOffsetAt(0)).isEqualTo(42L);

        assertThat(parsed.getCommittedEwmsCount()).isEqualTo(2);
        assertThat(parsed.getCommittedEwmAt(0).getGroupName()).isEqualTo("geo");
        assertThat(parsed.getCommittedEwmAt(0).getCew()).isEqualTo(10L);
        assertThat(parsed.getCommittedEwmAt(1).getGroupName()).isEqualTo("device");
        assertThat(parsed.getCommittedEwmAt(1).getCew()).isEqualTo(42L);
    }

    @Test
    void testBackwardsCompatibleEmptyDefaults() {
        // Pre-Phase-E peer: request and response without any enrichment fields. Round-trip must
        // produce empty lists (proto2 repeated default), not throw or carry stale state.
        PbFetchLogReqForBucket reqOriginal =
                new PbFetchLogReqForBucket()
                        .setBucketId(2)
                        .setFetchOffset(0L)
                        .setMaxFetchBytes(512);
        PbFetchLogReqForBucket reqParsed = roundTrip(reqOriginal, new PbFetchLogReqForBucket());
        assertThat(reqParsed.getFollowerEwmRequestsCount()).isZero();
        assertThat(reqParsed.getFollowerEwmRequestsList()).isEmpty();

        PbFetchLogRespForBucket respOriginal =
                new PbFetchLogRespForBucket()
                        .setBucketId(2)
                        .setHighWatermark(0L)
                        .setLogStartOffset(0L);
        PbFetchLogRespForBucket respParsed = roundTrip(respOriginal, new PbFetchLogRespForBucket());
        assertThat(respParsed.getEnrichmentPayloadPerGroupsCount()).isZero();
        assertThat(respParsed.getEnrichmentPayloadPerGroupsList()).isEmpty();
        assertThat(respParsed.getCommittedEwmsCount()).isZero();
        assertThat(respParsed.getCommittedEwmsList()).isEmpty();
    }

    private static PbFetchLogReqForBucket roundTrip(
            PbFetchLogReqForBucket original, PbFetchLogReqForBucket destination) {
        ByteBuf buf = Unpooled.buffer(original.totalSize());
        original.writeTo(buf);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        destination.parseFrom(bytes);
        return destination;
    }

    private static PbFetchLogRespForBucket roundTrip(
            PbFetchLogRespForBucket original, PbFetchLogRespForBucket destination) {
        ByteBuf buf = Unpooled.buffer(original.totalSize());
        original.writeTo(buf);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        destination.parseFrom(bytes);
        return destination;
    }
}
