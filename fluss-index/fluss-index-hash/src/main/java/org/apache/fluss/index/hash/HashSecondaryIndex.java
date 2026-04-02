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

package org.apache.fluss.index.hash;

import org.apache.fluss.index.IndexContext;
import org.apache.fluss.index.SecondaryIndex;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueDecoder;

import javax.annotation.Nonnull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An in-memory hash-based secondary index. Maps indexed column values (converted to string) to the
 * set of primary keys that have that value.
 *
 * <p>This is a POC implementation to prove the extension architecture. Production implementations
 * would use more efficient storage and serialization.
 */
public class HashSecondaryIndex implements SecondaryIndex {

    private static final String SNAPSHOT_FILE_NAME = "hash-index.dat";

    private final ValueDecoder valueDecoder;
    private final int[] indexedColumnIndices;
    private final InternalRow.FieldGetter[] fieldGetters;

    /** Maps stringified indexed column value -> set of primary keys. */
    private final Map<String, Set<ByteBuffer>> index;

    /** Reverse map: primary key -> current indexed value (for updates/deletes). */
    private final Map<ByteBuffer, String> reverseIndex;

    public HashSecondaryIndex(IndexContext context) {
        this.valueDecoder = new ValueDecoder(context.getSchemaGetter(), context.getKvFormat());
        this.indexedColumnIndices = context.getIndexedColumnIndices();
        this.fieldGetters = new InternalRow.FieldGetter[indexedColumnIndices.length];
        for (int i = 0; i < indexedColumnIndices.length; i++) {
            this.fieldGetters[i] =
                    InternalRow.createFieldGetter(
                            context.getRowType().getTypeAt(indexedColumnIndices[i]),
                            indexedColumnIndices[i]);
        }
        this.index = new HashMap<>();
        this.reverseIndex = new HashMap<>();
    }

    @Override
    public void onPut(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException {
        ByteBuffer keyBuf = ByteBuffer.wrap(key.clone());
        String indexedValue = extractIndexedValue(value);

        // Remove old mapping if this key was previously indexed
        String oldValue = reverseIndex.get(keyBuf);
        if (oldValue != null) {
            Set<ByteBuffer> keys = index.get(oldValue);
            if (keys != null) {
                keys.remove(keyBuf);
                if (keys.isEmpty()) {
                    index.remove(oldValue);
                }
            }
        }

        // Add new mapping
        index.computeIfAbsent(indexedValue, k -> new HashSet<>()).add(keyBuf);
        reverseIndex.put(keyBuf, indexedValue);
    }

    @Override
    public void onDelete(@Nonnull byte[] key) throws IOException {
        ByteBuffer keyBuf = ByteBuffer.wrap(key.clone());
        String oldValue = reverseIndex.remove(keyBuf);
        if (oldValue != null) {
            Set<ByteBuffer> keys = index.get(oldValue);
            if (keys != null) {
                keys.remove(keyBuf);
                if (keys.isEmpty()) {
                    index.remove(oldValue);
                }
            }
        }
    }

    @Override
    public void flush() throws IOException {
        // In-memory index, nothing to flush
    }

    @Override
    public List<byte[]> search(@Nonnull byte[] queryValue, int maxResults) throws IOException {
        String queryStr = new String(queryValue);
        Set<ByteBuffer> matches = index.get(queryStr);
        if (matches == null || matches.isEmpty()) {
            return new ArrayList<>();
        }

        List<byte[]> result = new ArrayList<>();
        int count = 0;
        for (ByteBuffer buf : matches) {
            if (count >= maxResults) {
                break;
            }
            byte[] keyBytes = new byte[buf.remaining()];
            buf.duplicate().get(keyBytes);
            result.add(keyBytes);
            count++;
        }
        return result;
    }

    @Override
    public void snapshot(@Nonnull File targetDir) throws IOException {
        if (!targetDir.exists() && !targetDir.mkdirs()) {
            throw new IOException("Failed to create snapshot directory: " + targetDir);
        }
        File snapshotFile = new File(targetDir, SNAPSHOT_FILE_NAME);
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(snapshotFile))) {
            // Write reverse index size
            out.writeInt(reverseIndex.size());
            for (Map.Entry<ByteBuffer, String> entry : reverseIndex.entrySet()) {
                // Write key
                ByteBuffer keyBuf = entry.getKey();
                byte[] keyBytes = new byte[keyBuf.remaining()];
                keyBuf.duplicate().get(keyBytes);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                // Write indexed value
                byte[] valueBytes = entry.getValue().getBytes();
                out.writeInt(valueBytes.length);
                out.write(valueBytes);
            }
        }
    }

    @Override
    public void clear() throws IOException {
        index.clear();
        reverseIndex.clear();
    }

    @Override
    public void close() throws IOException {
        clear();
    }

    /** Restores index state from a snapshot directory. */
    void restoreFromSnapshot(File snapshotDir) throws IOException {
        File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
        if (!snapshotFile.exists()) {
            return;
        }
        clear();
        try (DataInputStream in = new DataInputStream(new FileInputStream(snapshotFile))) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                // Read key
                int keyLen = in.readInt();
                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);
                ByteBuffer keyBuf = ByteBuffer.wrap(keyBytes);
                // Read indexed value
                int valueLen = in.readInt();
                byte[] valueBytes = new byte[valueLen];
                in.readFully(valueBytes);
                String indexedValue = new String(valueBytes);

                index.computeIfAbsent(indexedValue, k -> new HashSet<>()).add(keyBuf);
                reverseIndex.put(keyBuf, indexedValue);
            }
        }
    }

    private String extractIndexedValue(byte[] valueBytes) {
        BinaryValue binaryValue = valueDecoder.decodeValue(valueBytes);
        InternalRow row = binaryValue.row;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fieldGetters.length; i++) {
            if (i > 0) {
                sb.append('\0');
            }
            Object fieldValue = fieldGetters[i].getFieldOrNull(row);
            if (fieldValue != null) {
                sb.append(fieldValue.toString());
            } else {
                sb.append("__NULL__");
            }
        }
        return sb.toString();
    }
}
