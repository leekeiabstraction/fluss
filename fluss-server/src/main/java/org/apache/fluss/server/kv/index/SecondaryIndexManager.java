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
import org.apache.fluss.index.IndexContext;
import org.apache.fluss.index.IndexDescriptor;
import org.apache.fluss.index.IndexExtensionPlugin;
import org.apache.fluss.index.IndexExtensionPluginSetUp;
import org.apache.fluss.index.SecondaryIndex;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Manages all secondary indexes for a single KvTablet. Handles lifecycle (create, close),
 * delegation of mutations, search, snapshot, and recovery.
 */
@Internal
public class SecondaryIndexManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SecondaryIndexManager.class);

    private final Map<String, SecondaryIndex> indexes;
    private final File indexBaseDir;

    public SecondaryIndexManager(
            List<IndexDescriptor> descriptors,
            RowType rowType,
            KvFormat kvFormat,
            SchemaGetter schemaGetter,
            File indexBaseDir,
            @Nullable PluginManager pluginManager)
            throws IOException {
        this.indexBaseDir = checkNotNull(indexBaseDir, "indexBaseDir must not be null");
        this.indexes = new HashMap<>();

        for (IndexDescriptor descriptor : descriptors) {
            IndexExtensionPlugin plugin =
                    IndexExtensionPluginSetUp.fromIdentifier(descriptor.getType(), pluginManager);

            int[] columnIndices = resolveColumnIndices(descriptor.getColumns(), rowType);
            File dataDir = new File(indexBaseDir, descriptor.getName());
            if (!dataDir.exists() && !dataDir.mkdirs()) {
                throw new IOException("Failed to create index data directory: " + dataDir);
            }

            IndexContext context =
                    new IndexContext(
                            descriptor.getName(),
                            columnIndices,
                            descriptor.getExtraProperties(),
                            rowType,
                            kvFormat,
                            schemaGetter,
                            dataDir);

            SecondaryIndex index = plugin.createIndex(context);
            indexes.put(descriptor.getName(), index);
            LOG.info(
                    "Created secondary index '{}' of type '{}'",
                    descriptor.getName(),
                    descriptor.getType());
        }
    }

    /** Returns true if this manager has any indexes configured. */
    public boolean hasIndexes() {
        return !indexes.isEmpty();
    }

    /** Called for each entry flushed from KvPreWriteBuffer to RocksDB. */
    public void onPut(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException {
        for (SecondaryIndex index : indexes.values()) {
            index.onPut(key, value);
        }
    }

    /** Called for each delete flushed from KvPreWriteBuffer. */
    public void onDelete(@Nonnull byte[] key) throws IOException {
        for (SecondaryIndex index : indexes.values()) {
            index.onDelete(key);
        }
    }

    /** Called after a batch of mutations completes. */
    public void flush() throws IOException {
        for (SecondaryIndex index : indexes.values()) {
            index.flush();
        }
    }

    /** Search a named index. */
    public List<byte[]> search(String indexName, byte[] queryValue, int maxResults)
            throws IOException {
        SecondaryIndex index = indexes.get(indexName);
        if (index == null) {
            throw new IllegalArgumentException("Unknown index: " + indexName);
        }
        return index.search(queryValue, maxResults);
    }

    /** Snapshot all indexes into subdirectories under snapshotDir. */
    public void snapshot(File snapshotDir) throws IOException {
        for (Map.Entry<String, SecondaryIndex> entry : indexes.entrySet()) {
            File indexSnapshotDir = new File(snapshotDir, entry.getKey());
            entry.getValue().snapshot(indexSnapshotDir);
        }
    }

    /** Clear all indexes (for recovery). */
    public void clear() throws IOException {
        for (SecondaryIndex index : indexes.values()) {
            index.clear();
        }
    }

    @Override
    public void close() throws IOException {
        List<IOException> exceptions = new ArrayList<>();
        for (Map.Entry<String, SecondaryIndex> entry : indexes.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                LOG.warn("Failed to close secondary index '{}'", entry.getKey(), e);
                exceptions.add(e);
            }
        }
        indexes.clear();
        if (!exceptions.isEmpty()) {
            IOException combined = new IOException("Failed to close one or more secondary indexes");
            for (IOException e : exceptions) {
                combined.addSuppressed(e);
            }
            throw combined;
        }
    }

    private static int[] resolveColumnIndices(List<String> columnNames, RowType rowType) {
        int[] indices = new int[columnNames.size()];
        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < columnNames.size(); i++) {
            int idx = fieldNames.indexOf(columnNames.get(i));
            if (idx < 0) {
                throw new IllegalArgumentException(
                        "Column '" + columnNames.get(i) + "' not found in row type: " + rowType);
            }
            indices[i] = idx;
        }
        return indices;
    }
}
