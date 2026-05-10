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
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.protocol.ApiError;

/** Result of {@link ListOffsetsRequest} for each table bucket. */
@Internal
public class ListOffsetsResultForBucket extends ResultForBucket {
    /**
     * The visible offset for this table bucket. if the {@link ListOffsetsRequest} is from follower,
     * the offset is LogEndOffset(LEO), otherwise, the request is from client, it will be
     * HighWatermark(HW).
     */
    private final long offset;

    /**
     * Tier-safe upper bound for tiering jobs reading column-group tables (Phase F.3). When
     * non-negative, equals {@code min(HW, min(CEW_g) across all groups on this bucket)} — tiering
     * past this offset would spin because the read-side gate blocks records past CEW. {@code -1}
     * means the table has no column groups (or this offset type does not carry the cap, e.g.
     * EARLIEST_OFFSET / LEADER_END_OFFSET_SNAPSHOT) and the tiering caller should use {@link
     * #getOffset()} unmodified.
     */
    private final long tierSafeEndOffset;

    public ListOffsetsResultForBucket(TableBucket tableBucket, long offset) {
        this(tableBucket, offset, -1L, ApiError.NONE);
    }

    public ListOffsetsResultForBucket(
            TableBucket tableBucket, long offset, long tierSafeEndOffset) {
        this(tableBucket, offset, tierSafeEndOffset, ApiError.NONE);
    }

    public ListOffsetsResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, -1, -1L, error);
    }

    private ListOffsetsResultForBucket(
            TableBucket tableBucket, long offset, long tierSafeEndOffset, ApiError error) {
        super(tableBucket, error);
        this.offset = offset;
        this.tierSafeEndOffset = tierSafeEndOffset;
    }

    public long getOffset() {
        return offset;
    }

    /**
     * Tier-safe end offset, or {@code -1L} when not applicable (no column groups, or an offset type
     * other than the latest-HW one). See javadoc of the field.
     */
    public long getTierSafeEndOffset() {
        return tierSafeEndOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ListOffsetsResultForBucket that = (ListOffsetsResultForBucket) o;
        return offset == that.offset && tierSafeEndOffset == that.tierSafeEndOffset;
    }

    @Override
    public String toString() {
        return "ListOffsetsResultForBucket{"
                + "offset="
                + offset
                + ", tierSafeEndOffset="
                + tierSafeEndOffset
                + ", tableBucket="
                + tableBucket
                + ", error="
                + error
                + '}';
    }
}
