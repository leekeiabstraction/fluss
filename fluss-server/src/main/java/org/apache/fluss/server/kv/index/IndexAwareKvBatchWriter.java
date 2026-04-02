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

package org.apache.fluss.server.kv.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.server.kv.KvBatchWriter;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * A {@link KvBatchWriter} decorator that intercepts put/delete calls and forwards them to a {@link
 * SecondaryIndexManager} in addition to the delegate writer.
 */
@Internal
public class IndexAwareKvBatchWriter implements KvBatchWriter {

    private final KvBatchWriter delegate;
    private final SecondaryIndexManager indexManager;

    public IndexAwareKvBatchWriter(KvBatchWriter delegate, SecondaryIndexManager indexManager) {
        this.delegate = delegate;
        this.indexManager = indexManager;
    }

    @Override
    public void put(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException {
        delegate.put(key, value);
        indexManager.onPut(key, value);
    }

    @Override
    public void delete(@Nonnull byte[] key) throws IOException {
        delegate.delete(key);
        indexManager.onDelete(key);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
        indexManager.flush();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
