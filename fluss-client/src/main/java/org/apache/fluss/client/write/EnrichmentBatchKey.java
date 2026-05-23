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
import org.apache.fluss.metadata.TableBucket;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Phase N.2: identifies an enrichment-write batch slot.
 *
 * <p>An accumulator holds one queue of {@link EnrichmentWriteBatch} per key. Two enrichment writes
 * target the same batch slot when they share a table id, column group, and table bucket (including
 * partition id) — the server's enrichment-write RPC also addresses on this triple.
 *
 * <p>Keying on {@link TableBucket} (rather than just {@code (table, group, bucket-int)}) lets the
 * accumulator carry the partition_id forward into the wire request without an extra lookup at send
 * time.
 */
@Internal
public final class EnrichmentBatchKey {

    private final long tableId;
    private final String columnGroup;
    private final TableBucket bucket;
    private final int hash;

    public EnrichmentBatchKey(long tableId, String columnGroup, TableBucket bucket) {
        this.tableId = tableId;
        this.columnGroup = checkNotNull(columnGroup, "columnGroup must not be null");
        this.bucket = checkNotNull(bucket, "bucket must not be null");
        this.hash = Objects.hash(tableId, columnGroup, bucket);
    }

    public long getTableId() {
        return tableId;
    }

    public String getColumnGroup() {
        return columnGroup;
    }

    public TableBucket getBucket() {
        return bucket;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EnrichmentBatchKey)) {
            return false;
        }
        EnrichmentBatchKey that = (EnrichmentBatchKey) o;
        return tableId == that.tableId
                && columnGroup.equals(that.columnGroup)
                && bucket.equals(that.bucket);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "EnrichmentBatchKey{tableId="
                + tableId
                + ", columnGroup='"
                + columnGroup
                + '\''
                + ", bucket="
                + bucket
                + '}';
    }
}
