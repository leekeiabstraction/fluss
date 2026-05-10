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
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;

/**
 * Slice of an {@link EnrichmentSegment} for replication: zero-copy {@link LogRecords} view of the
 * concatenated batches plus the parallel array of source offsets they cover. Returned by {@link
 * LogTablet#readEnrichmentForFollower}.
 */
@Internal
public final class EnrichmentReadResult {

    public static final EnrichmentReadResult EMPTY =
            new EnrichmentReadResult(MemoryLogRecords.EMPTY, new long[0]);

    private final LogRecords records;
    private final long[] sourceOffsets;

    public EnrichmentReadResult(LogRecords records, long[] sourceOffsets) {
        this.records = records;
        this.sourceOffsets = sourceOffsets;
    }

    public LogRecords records() {
        return records;
    }

    public long[] sourceOffsets() {
        return sourceOffsets;
    }

    public boolean isEmpty() {
        return sourceOffsets.length == 0;
    }
}
