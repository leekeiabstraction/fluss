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

package org.apache.fluss.index;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * A secondary index instance, one per (table-bucket, index). The lifecycle is tied to the owning
 * KvTablet.
 *
 * <p>Index mutations ({@link #onPut}, {@link #onDelete}) are called during the KvPreWriteBuffer
 * flush, after the data has been persisted to RocksDB. The {@link #flush()} method is called after
 * a batch of mutations completes.
 *
 * @since 0.10
 */
@PublicEvolving
public interface SecondaryIndex extends Closeable {

    /**
     * Called for each entry flushed from the KvPreWriteBuffer to RocksDB.
     *
     * @param key the primary key bytes
     * @param value the encoded value bytes (schema ID + row data)
     */
    void onPut(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException;

    /**
     * Called for each delete flushed from the KvPreWriteBuffer.
     *
     * @param key the primary key bytes
     */
    void onDelete(@Nonnull byte[] key) throws IOException;

    /** Called after a batch of {@link #onPut}/{@link #onDelete} calls completes. */
    void flush() throws IOException;

    /**
     * Search the index for entries matching the given query value.
     *
     * @param queryValue the value to search for (interpretation is index-type specific)
     * @param maxResults the maximum number of results to return
     * @return a list of matching primary key bytes
     */
    List<byte[]> search(@Nonnull byte[] queryValue, int maxResults) throws IOException;

    /**
     * Snapshot the index state to the given directory.
     *
     * @param targetDir the directory to write snapshot files into
     */
    void snapshot(@Nonnull File targetDir) throws IOException;

    /** Clear all index data. Used before rebuilding the index during recovery. */
    void clear() throws IOException;
}
