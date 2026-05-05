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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;

import java.util.concurrent.CompletableFuture;

/**
 * The writer to write data to the log table.
 *
 * @since 0.2
 */
@PublicEvolving
public interface AppendWriter extends TableWriter {

    /**
     * Append a record into a Log Table.
     *
     * @param record the record to append.
     * @return A {@link CompletableFuture} that always returns append result when complete normally.
     */
    CompletableFuture<AppendResult> append(InternalRow record);

    /**
     * Write enrichment columns for the given column group at an existing source offset on a
     * specific bucket. The enrichment row must contain only the columns of the named column group,
     * in the order they appear in the table schema.
     *
     * <p>This is a single-row, leader-routed RPC that bypasses the regular append batching
     * pipeline. After the put succeeds, the per-bucket enrichment watermark advances to the highest
     * contiguous offset that has been filled (starting from 0).
     *
     * @param columnGroup the name of the column group being filled.
     * @param bucket the bucket whose log contains the source offset.
     * @param sourceOffset the base log offset to enrich; must already exist in the log.
     * @param enrichmentRow the enrichment column values, in column-group order.
     * @return a future that completes with the enrichment watermark after the put.
     */
    CompletableFuture<AppendColumnsResult> appendColumns(
            String columnGroup, TableBucket bucket, long sourceOffset, InternalRow enrichmentRow);
}
