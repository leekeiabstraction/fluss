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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Describes a secondary index declared via table properties.
 *
 * <p>Convention: indexes are declared via table properties with the following keys:
 *
 * <ul>
 *   <li>{@code table.index.<name>.type} - the index type identifier (e.g. "hash", "hnsw")
 *   <li>{@code table.index.<name>.columns} - comma-separated list of column names
 *   <li>{@code table.index.<name>.<key>} - additional index-specific properties
 * </ul>
 *
 * @since 0.10
 */
@PublicEvolving
public class IndexDescriptor {

    private static final String INDEX_PREFIX = "table.index.";

    private final String name;
    private final String type;
    private final List<String> columns;
    private final Map<String, String> extraProperties;

    public IndexDescriptor(
            String name, String type, List<String> columns, Map<String, String> extraProperties) {
        this.name = checkNotNull(name, "name must not be null");
        this.type = checkNotNull(type, "type must not be null");
        checkArgument(!columns.isEmpty(), "columns must not be empty");
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.extraProperties = Collections.unmodifiableMap(new HashMap<>(extraProperties));
    }

    /** Returns the index name. */
    public String getName() {
        return name;
    }

    /** Returns the index type identifier (e.g. "hash", "hnsw"). */
    public String getType() {
        return type;
    }

    /** Returns the list of indexed column names. */
    public List<String> getColumns() {
        return columns;
    }

    /** Returns additional index-specific properties beyond type and columns. */
    public Map<String, String> getExtraProperties() {
        return extraProperties;
    }

    /**
     * Parses all index descriptors from a table properties map.
     *
     * @param properties the table properties
     * @return a list of parsed index descriptors (empty if no indexes are declared)
     */
    public static List<IndexDescriptor> fromProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return Collections.emptyList();
        }

        // Collect all distinct index names
        Set<String> indexNames = new HashSet<>();
        for (String key : properties.keySet()) {
            if (key.startsWith(INDEX_PREFIX)) {
                String remainder = key.substring(INDEX_PREFIX.length());
                int dotIdx = remainder.indexOf('.');
                if (dotIdx > 0) {
                    indexNames.add(remainder.substring(0, dotIdx));
                }
            }
        }

        if (indexNames.isEmpty()) {
            return Collections.emptyList();
        }

        List<IndexDescriptor> descriptors = new ArrayList<>();
        for (String indexName : indexNames) {
            String prefix = INDEX_PREFIX + indexName + ".";
            String type = properties.get(prefix + "type");
            String columnsStr = properties.get(prefix + "columns");

            if (type == null || type.isEmpty()) {
                throw new IllegalArgumentException(
                        "Missing required property '"
                                + prefix
                                + "type' for index '"
                                + indexName
                                + "'");
            }
            if (columnsStr == null || columnsStr.isEmpty()) {
                throw new IllegalArgumentException(
                        "Missing required property '"
                                + prefix
                                + "columns' for index '"
                                + indexName
                                + "'");
            }

            String[] parts = columnsStr.split(",");
            List<String> columns = new ArrayList<>(parts.length);
            for (String part : parts) {
                columns.add(part.trim());
            }

            // Collect extra properties (everything except type and columns)
            Map<String, String> extras = new HashMap<>();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (entry.getKey().startsWith(prefix)
                        && !entry.getKey().equals(prefix + "type")
                        && !entry.getKey().equals(prefix + "columns")) {
                    String extraKey = entry.getKey().substring(prefix.length());
                    extras.put(extraKey, entry.getValue());
                }
            }

            descriptors.add(new IndexDescriptor(indexName, type, columns, extras));
        }

        return descriptors;
    }

    @Override
    public String toString() {
        return "IndexDescriptor{"
                + "name='"
                + name
                + '\''
                + ", type='"
                + type
                + '\''
                + ", columns="
                + columns
                + ", extraProperties="
                + extraProperties
                + '}';
    }
}
