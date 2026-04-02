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
import org.apache.fluss.plugin.Plugin;

import java.io.File;
import java.io.IOException;

/**
 * A Plugin to create instances of {@link SecondaryIndex}. Each implementation provides a specific
 * index type (e.g., hash, HNSW vector, R-tree spatial).
 *
 * @since 0.10
 */
@PublicEvolving
public interface IndexExtensionPlugin extends Plugin {

    /**
     * Returns a unique identifier among {@link IndexExtensionPlugin} implementations.
     *
     * @return the identifier, e.g. "hash", "hnsw", "rtree"
     */
    String identifier();

    /**
     * Creates a new, empty index instance for a single tablet.
     *
     * @param context the runtime context for this index
     * @return a new secondary index instance
     */
    SecondaryIndex createIndex(IndexContext context) throws IOException;

    /**
     * Restores an index instance from a snapshot directory.
     *
     * @param context the runtime context for this index
     * @param snapshotDir the directory containing the snapshot data
     * @return the restored secondary index instance
     */
    SecondaryIndex restoreIndex(IndexContext context, File snapshotDir) throws IOException;
}
