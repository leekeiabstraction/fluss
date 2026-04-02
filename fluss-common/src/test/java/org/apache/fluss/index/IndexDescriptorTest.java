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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IndexDescriptor}. */
class IndexDescriptorTest {

    @Test
    void testParseEmptyProperties() {
        List<IndexDescriptor> descriptors = IndexDescriptor.fromProperties(new HashMap<>());
        assertThat(descriptors).isEmpty();
    }

    @Test
    void testParseNullProperties() {
        List<IndexDescriptor> descriptors = IndexDescriptor.fromProperties(null);
        assertThat(descriptors).isEmpty();
    }

    @Test
    void testParseSingleIndex() {
        Map<String, String> props = new HashMap<>();
        props.put("table.index.myidx.type", "hash");
        props.put("table.index.myidx.columns", "name");

        List<IndexDescriptor> descriptors = IndexDescriptor.fromProperties(props);
        assertThat(descriptors).hasSize(1);

        IndexDescriptor desc = descriptors.get(0);
        assertThat(desc.getName()).isEqualTo("myidx");
        assertThat(desc.getType()).isEqualTo("hash");
        assertThat(desc.getColumns()).containsExactly("name");
        assertThat(desc.getExtraProperties()).isEmpty();
    }

    @Test
    void testParseMultipleColumns() {
        Map<String, String> props = new HashMap<>();
        props.put("table.index.idx1.type", "hash");
        props.put("table.index.idx1.columns", "col1, col2, col3");

        List<IndexDescriptor> descriptors = IndexDescriptor.fromProperties(props);
        assertThat(descriptors).hasSize(1);
        assertThat(descriptors.get(0).getColumns()).containsExactly("col1", "col2", "col3");
    }

    @Test
    void testParseExtraProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("table.index.vecidx.type", "hnsw");
        props.put("table.index.vecidx.columns", "embedding");
        props.put("table.index.vecidx.m", "16");
        props.put("table.index.vecidx.ef_construction", "200");

        List<IndexDescriptor> descriptors = IndexDescriptor.fromProperties(props);
        assertThat(descriptors).hasSize(1);

        IndexDescriptor desc = descriptors.get(0);
        assertThat(desc.getType()).isEqualTo("hnsw");
        assertThat(desc.getExtraProperties()).containsEntry("m", "16");
        assertThat(desc.getExtraProperties()).containsEntry("ef_construction", "200");
    }

    @Test
    void testParseMultipleIndexes() {
        Map<String, String> props = new HashMap<>();
        props.put("table.index.idx1.type", "hash");
        props.put("table.index.idx1.columns", "name");
        props.put("table.index.idx2.type", "rtree");
        props.put("table.index.idx2.columns", "lat,lng");

        List<IndexDescriptor> descriptors = IndexDescriptor.fromProperties(props);
        assertThat(descriptors).hasSize(2);
    }

    @Test
    void testMissingTypeFails() {
        Map<String, String> props = new HashMap<>();
        props.put("table.index.myidx.columns", "name");

        assertThatThrownBy(() -> IndexDescriptor.fromProperties(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type");
    }

    @Test
    void testMissingColumnsFails() {
        Map<String, String> props = new HashMap<>();
        props.put("table.index.myidx.type", "hash");

        assertThatThrownBy(() -> IndexDescriptor.fromProperties(props))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("columns");
    }

    @Test
    void testNonIndexPropertiesIgnored() {
        Map<String, String> props = new HashMap<>();
        props.put("table.replication.factor", "3");
        props.put("table.log.format", "indexed");

        List<IndexDescriptor> descriptors = IndexDescriptor.fromProperties(props);
        assertThat(descriptors).isEmpty();
    }
}
