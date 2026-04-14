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
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

/**
 * Persistent store for column group enrichment data. Uses a simple append-only binary file format
 * per column group. Each entry stores: target offset (8 bytes) + serialized enrichment row.
 *
 * <p>On startup, the file is replayed to rebuild the in-memory enrichment map.
 */
@Internal
public class ColumnGroupStore implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnGroupStore.class);
    private static final String FILE_PREFIX = "enrichment.";
    private static final String FILE_SUFFIX = ".dat";
    private static final byte VERSION = 1;

    private final File storeFile;
    private final RowType enrichmentRowType;
    private DataOutputStream output;

    /**
     * Create a new column group store.
     *
     * @param tabletDir the tablet directory
     * @param groupName the column group name
     * @param enrichmentRowType the row type for enrichment columns
     */
    public ColumnGroupStore(File tabletDir, String groupName, RowType enrichmentRowType)
            throws IOException {
        this.storeFile = new File(tabletDir, FILE_PREFIX + groupName + FILE_SUFFIX);
        this.enrichmentRowType = enrichmentRowType;
        this.output = new DataOutputStream(new FileOutputStream(storeFile, true));
    }

    /** Append an enrichment row for the given target offset. */
    public void append(long targetOffset, GenericRow row) throws IOException {
        output.writeByte(VERSION);
        output.writeLong(targetOffset);
        writeRow(output, row, enrichmentRowType);
        output.flush();
    }

    /**
     * Load all enrichment data from the store file into the provided map.
     *
     * @param tabletDir the tablet directory
     * @param groupName the column group name
     * @param enrichmentRowType the row type for enrichment columns
     * @return a TreeMap of offset -> enrichment row, or empty map if no file exists
     */
    public static TreeMap<Long, GenericRow> load(
            File tabletDir, String groupName, RowType enrichmentRowType) {
        TreeMap<Long, GenericRow> result = new TreeMap<>();
        File storeFile = new File(tabletDir, FILE_PREFIX + groupName + FILE_SUFFIX);
        if (!storeFile.exists()) {
            return result;
        }

        try (DataInputStream input = new DataInputStream(new FileInputStream(storeFile))) {
            while (true) {
                byte version;
                try {
                    version = input.readByte();
                } catch (java.io.EOFException eof) {
                    break; // End of file
                }
                if (version != VERSION) {
                    LOG.warn(
                            "Unknown version {} in column group store {}, skipping rest",
                            version,
                            storeFile);
                    break;
                }
                long offset = input.readLong();
                GenericRow row = readRow(input, enrichmentRowType);
                result.put(offset, row);
            }
        } catch (IOException e) {
            LOG.warn("Error loading column group store {}", storeFile, e);
        }

        LOG.info(
                "Loaded {} enrichment entries from column group store {}",
                result.size(),
                storeFile);
        return result;
    }

    /** Check if a column group store file exists for the given group. */
    public static boolean exists(File tabletDir, String groupName) {
        return new File(tabletDir, FILE_PREFIX + groupName + FILE_SUFFIX).exists();
    }

    @Override
    public void close() throws IOException {
        if (output != null) {
            output.close();
            output = null;
        }
    }

    /** Serialize a GenericRow to the output stream. */
    private static void writeRow(DataOutputStream out, GenericRow row, RowType rowType)
            throws IOException {
        List<DataType> fieldTypes = rowType.getChildren();
        out.writeInt(fieldTypes.size());
        for (int i = 0; i < fieldTypes.size(); i++) {
            if (row.isNullAt(i)) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                writeField(out, row, i, fieldTypes.get(i));
            }
        }
    }

    /** Deserialize a GenericRow from the input stream. */
    private static GenericRow readRow(DataInputStream in, RowType rowType) throws IOException {
        int fieldCount = in.readInt();
        List<DataType> fieldTypes = rowType.getChildren();
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            boolean isNull = in.readBoolean();
            if (!isNull) {
                row.setField(i, readField(in, fieldTypes.get(i)));
            }
        }
        return row;
    }

    /** Write a single field value based on its data type. */
    private static void writeField(DataOutputStream out, GenericRow row, int index, DataType type)
            throws IOException {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                out.writeBoolean(row.getBoolean(index));
                break;
            case INTEGER:
                out.writeInt(row.getInt(index));
                break;
            case BIGINT:
                out.writeLong(row.getLong(index));
                break;
            case FLOAT:
                out.writeFloat(row.getFloat(index));
                break;
            case DOUBLE:
                out.writeDouble(row.getDouble(index));
                break;
            case STRING:
                BinaryString str = row.getString(index);
                byte[] strBytes = str.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(strBytes.length);
                out.write(strBytes);
                break;
            case BYTES:
                byte[] bytes = row.getBytes(index);
                out.writeInt(bytes.length);
                out.write(bytes);
                break;
            default:
                throw new IOException("Unsupported data type for column group store: " + root);
        }
    }

    /** Read a single field value based on its data type. */
    private static Object readField(DataInputStream in, DataType type) throws IOException {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return in.readBoolean();
            case INTEGER:
                return in.readInt();
            case BIGINT:
                return in.readLong();
            case FLOAT:
                return in.readFloat();
            case DOUBLE:
                return in.readDouble();
            case STRING:
                int strLen = in.readInt();
                byte[] strBytes = new byte[strLen];
                in.readFully(strBytes);
                return BinaryString.fromString(
                        new String(strBytes, java.nio.charset.StandardCharsets.UTF_8));
            case BYTES:
                int bytesLen = in.readInt();
                byte[] bytes = new byte[bytesLen];
                in.readFully(bytes);
                return bytes;
            default:
                throw new IOException("Unsupported data type for column group store: " + root);
        }
    }
}
