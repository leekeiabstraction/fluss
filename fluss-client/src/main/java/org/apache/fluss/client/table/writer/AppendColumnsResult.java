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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * The result of appending enrichment columns ({@link AppendWriter#appendColumns}).
 *
 * <p>Carries the per-bucket enrichment watermark (EWM) for the column group after the put succeeded
 * on the leader.
 */
@PublicEvolving
public final class AppendColumnsResult {

    private final long enrichmentWatermark;

    public AppendColumnsResult(long enrichmentWatermark) {
        this.enrichmentWatermark = enrichmentWatermark;
    }

    /**
     * Returns the enrichment watermark for the column group on the bucket targeted by this append,
     * after the put. The EWM is the highest contiguous offset (exclusive upper bound) for which
     * enrichment has been filled, starting from offset 0.
     */
    public long getEnrichmentWatermark() {
        return enrichmentWatermark;
    }
}
