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
     * Append enrichment columns at an existing offset in a column group. The enrichment row should
     * contain only the columns belonging to the specified column group, in the order they appear in
     * the schema.
     *
     * @param columnGroup the column group name to write to
     * @param bucketId the bucket ID where the base record exists
     * @param targetOffset the offset of the base record to enrich
     * @param row the enrichment column values (only columns in the column group)
     * @return A {@link CompletableFuture} that completes when the enrichment is acknowledged.
     */
    CompletableFuture<AppendResult> appendColumns(
            String columnGroup, int bucketId, long targetOffset, InternalRow row);
}
