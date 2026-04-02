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
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.types.RowType;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Runtime context provided to {@link IndexExtensionPlugin#createIndex(IndexContext)} when creating
 * a secondary index instance.
 *
 * @since 0.10
 */
@PublicEvolving
public class IndexContext {

    private final String indexName;
    private final int[] indexedColumnIndices;
    private final Map<String, String> indexProperties;
    private final RowType rowType;
    private final KvFormat kvFormat;
    private final SchemaGetter schemaGetter;
    private final File dataDir;

    public IndexContext(
            String indexName,
            int[] indexedColumnIndices,
            Map<String, String> indexProperties,
            RowType rowType,
            KvFormat kvFormat,
            SchemaGetter schemaGetter,
            File dataDir) {
        this.indexName = checkNotNull(indexName, "indexName must not be null");
        this.indexedColumnIndices =
                checkNotNull(indexedColumnIndices, "indexedColumnIndices must not be null");
        this.indexProperties =
                Collections.unmodifiableMap(
                        checkNotNull(indexProperties, "indexProperties must not be null"));
        this.rowType = checkNotNull(rowType, "rowType must not be null");
        this.kvFormat = checkNotNull(kvFormat, "kvFormat must not be null");
        this.schemaGetter = checkNotNull(schemaGetter, "schemaGetter must not be null");
        this.dataDir = checkNotNull(dataDir, "dataDir must not be null");
    }

    /** Returns the name of this index. */
    public String getIndexName() {
        return indexName;
    }

    /** Returns the column ordinals (in the table schema) that this index covers. */
    public int[] getIndexedColumnIndices() {
        return indexedColumnIndices;
    }

    /** Returns additional properties for this index (from table properties). */
    public Map<String, String> getIndexProperties() {
        return indexProperties;
    }

    /** Returns the row type of the table. */
    public RowType getRowType() {
        return rowType;
    }

    /** Returns the KV format used by the table. */
    public KvFormat getKvFormat() {
        return kvFormat;
    }

    /** Returns the schema getter for decoding values. */
    public SchemaGetter getSchemaGetter() {
        return schemaGetter;
    }

    /** Returns the local directory for this index to store its data files. */
    public File getDataDir() {
        return dataDir;
    }
}
