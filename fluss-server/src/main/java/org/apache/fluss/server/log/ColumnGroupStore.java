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

import org.apache.fluss.row.GenericRow;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory store for enrichment column group data. Stores enrichment rows keyed by their base log
 * offset, and tracks the enrichment watermark (EWM) which represents the highest offset up to which
 * enrichment is contiguously complete.
 *
 * <p>Thread-safe: uses a ConcurrentSkipListMap for data and volatile for the watermark.
 */
@ThreadSafe
public class ColumnGroupStore {

    private final String groupName;
    private final ConcurrentSkipListMap<Long, GenericRow> data;
    private volatile long enrichmentWatermark = -1L;

    public ColumnGroupStore(String groupName) {
        this.groupName = groupName;
        this.data = new ConcurrentSkipListMap<>();
    }

    public String getGroupName() {
        return groupName;
    }

    /** Store enrichment data for a specific offset. */
    public void put(long offset, GenericRow enrichmentRow) {
        data.put(offset, enrichmentRow);
    }

    /** Get enrichment data for a specific offset, or null if not yet enriched. */
    @Nullable
    public GenericRow get(long offset) {
        return data.get(offset);
    }

    /**
     * Get all enrichment data within the given offset range [fromOffset, toOffset).
     *
     * @return a navigable map of offset to enrichment row
     */
    public NavigableMap<Long, GenericRow> getRange(long fromOffset, long toOffset) {
        return data.subMap(fromOffset, true, toOffset, false);
    }

    /** Returns the current enrichment watermark. */
    public long getEnrichmentWatermark() {
        return enrichmentWatermark;
    }

    /**
     * Advance the enrichment watermark to the new value if it is greater than the current value.
     * The EWM represents: "all offsets up to (but not including) this value have enrichment data."
     */
    public void advanceEnrichmentWatermark(long newEwm) {
        synchronized (this) {
            if (newEwm > enrichmentWatermark) {
                enrichmentWatermark = newEwm;
            }
        }
    }

    /** Returns the number of enrichment entries stored. */
    public int size() {
        return data.size();
    }
}
