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

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ColumnGroupStore} persistence and recovery. */
class ColumnGroupStoreTest {

    @TempDir File tempDir;

    @Test
    void testWriteAndLoadEnrichmentData() throws Exception {
        RowType enrichmentRowType =
                new RowType(
                        Arrays.asList(
                                new DataField("geo_region", DataTypes.STRING()),
                                new DataField("risk_score", DataTypes.DOUBLE())));

        // Write enrichment data
        try (ColumnGroupStore store =
                new ColumnGroupStore(tempDir, "enriched", enrichmentRowType)) {
            GenericRow row0 = new GenericRow(2);
            row0.setField(0, BinaryString.fromString("us-west-2"));
            row0.setField(1, 0.85);
            store.append(0L, row0);

            GenericRow row1 = new GenericRow(2);
            row1.setField(0, BinaryString.fromString("eu-central-1"));
            row1.setField(1, 0.32);
            store.append(1L, row1);

            // Row with null field
            GenericRow row2 = new GenericRow(2);
            row2.setField(0, BinaryString.fromString("ap-northeast-1"));
            // risk_score is null
            store.append(5L, row2);
        }

        // Verify file exists
        assertThat(ColumnGroupStore.exists(tempDir, "enriched")).isTrue();
        assertThat(ColumnGroupStore.exists(tempDir, "nonexistent")).isFalse();

        // Load and verify
        TreeMap<Long, GenericRow> loaded =
                ColumnGroupStore.load(tempDir, "enriched", enrichmentRowType);
        assertThat(loaded).hasSize(3);

        // Verify row 0
        GenericRow loadedRow0 = loaded.get(0L);
        assertThat(loadedRow0).isNotNull();
        assertThat(loadedRow0.getString(0).toString()).isEqualTo("us-west-2");
        assertThat(loadedRow0.getDouble(1)).isEqualTo(0.85);

        // Verify row 1
        GenericRow loadedRow1 = loaded.get(1L);
        assertThat(loadedRow1).isNotNull();
        assertThat(loadedRow1.getString(0).toString()).isEqualTo("eu-central-1");
        assertThat(loadedRow1.getDouble(1)).isEqualTo(0.32);

        // Verify row 5 (with null risk_score)
        GenericRow loadedRow5 = loaded.get(5L);
        assertThat(loadedRow5).isNotNull();
        assertThat(loadedRow5.getString(0).toString()).isEqualTo("ap-northeast-1");
        assertThat(loadedRow5.isNullAt(1)).isTrue();
    }

    @Test
    void testLoadFromNonExistentFile() {
        RowType rowType = new RowType(Arrays.asList(new DataField("col", DataTypes.STRING())));

        TreeMap<Long, GenericRow> loaded = ColumnGroupStore.load(tempDir, "missing", rowType);
        assertThat(loaded).isEmpty();
    }

    @Test
    void testAppendAfterReopen() throws Exception {
        RowType rowType = new RowType(Arrays.asList(new DataField("value", DataTypes.INT())));

        // First write session
        try (ColumnGroupStore store = new ColumnGroupStore(tempDir, "data", rowType)) {
            GenericRow row = new GenericRow(1);
            row.setField(0, 42);
            store.append(0L, row);
        }

        // Second write session (append mode)
        try (ColumnGroupStore store = new ColumnGroupStore(tempDir, "data", rowType)) {
            GenericRow row = new GenericRow(1);
            row.setField(0, 99);
            store.append(1L, row);
        }

        // Load and verify both entries
        TreeMap<Long, GenericRow> loaded = ColumnGroupStore.load(tempDir, "data", rowType);
        assertThat(loaded).hasSize(2);
        assertThat(loaded.get(0L).getInt(0)).isEqualTo(42);
        assertThat(loaded.get(1L).getInt(0)).isEqualTo(99);
    }
}
