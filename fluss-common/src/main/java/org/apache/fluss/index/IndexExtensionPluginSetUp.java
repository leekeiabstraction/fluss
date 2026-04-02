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

import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Discovers and loads {@link IndexExtensionPlugin} implementations via ServiceLoader and
 * PluginManager.
 */
public class IndexExtensionPluginSetUp {

    /**
     * Finds an {@link IndexExtensionPlugin} by its identifier.
     *
     * @param identifier the plugin identifier (e.g. "hash", "hnsw")
     * @param pluginManager the plugin manager, or null if only classpath plugins should be used
     * @return the matching plugin
     * @throws UnsupportedOperationException if no plugin matches the identifier
     */
    public static IndexExtensionPlugin fromIdentifier(
            final String identifier, @Nullable final PluginManager pluginManager) {
        Iterator<IndexExtensionPlugin> plugins = getAllPlugins(pluginManager);

        while (plugins.hasNext()) {
            IndexExtensionPlugin plugin = plugins.next();
            if (Objects.equals(plugin.identifier(), identifier)) {
                return plugin;
            }
        }

        throw new UnsupportedOperationException(
                "No IndexExtensionPlugin found for identifier: " + identifier);
    }

    private static Iterator<IndexExtensionPlugin> getAllPlugins(
            @Nullable PluginManager pluginManager) {
        final Iterator<IndexExtensionPlugin> pluginIteratorSPI =
                ServiceLoader.load(
                                IndexExtensionPlugin.class,
                                IndexExtensionPlugin.class.getClassLoader())
                        .iterator();
        if (pluginManager == null) {
            return pluginIteratorSPI;
        } else {
            return Iterators.concat(
                    pluginManager.load(IndexExtensionPlugin.class), pluginIteratorSPI);
        }
    }
}
