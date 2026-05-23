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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SchemaUpdate}. */
class SchemaUpdateTest {

    @Test
    void addColumnWithoutGroupJoinsDefaultGroup() {
        Schema initial =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        Schema updated =
                SchemaUpdate.applySchemaChanges(
                        initial,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "c",
                                        DataTypes.INT(),
                                        null,
                                        TableChange.ColumnPosition.last())));

        assertThat(updated.getColumns()).hasSize(3);
        assertThat(updated.getColumns().get(2).getName()).isEqualTo("c");
        // No user-declared column groups exist.
        assertThat(updated.getColumnGroups()).isEmpty();
        // The new column is in the default base group.
        assertThat(updated.getDefaultGroupColumnIndices()).contains(0, 1, 2);
    }

    @Test
    void addColumnToExistingGroup() {
        Schema initial =
                Schema.newBuilder()
                        .column("device_id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .column("geo_region", DataTypes.STRING())
                        .columnGroup("enriched")
                        .build();
        Schema updated =
                SchemaUpdate.applySchemaChanges(
                        initial,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "risk_score",
                                        DataTypes.DOUBLE(),
                                        null,
                                        TableChange.ColumnPosition.last(),
                                        "enriched")));

        assertThat(updated.getColumns()).hasSize(4);
        assertThat(updated.getColumns().get(3).getName()).isEqualTo("risk_score");

        Map<String, List<Integer>> groups = updated.getColumnGroups();
        assertThat(groups).containsKey("enriched");
        // Pre-existing geo_region (idx 2) plus newly added risk_score (idx 3).
        assertThat(groups.get("enriched")).containsExactly(2, 3);
    }

    @Test
    void addColumnCreatingNewGroup() {
        Schema initial =
                Schema.newBuilder()
                        .column("device_id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .build();
        Schema updated =
                SchemaUpdate.applySchemaChanges(
                        initial,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "geo_region",
                                        DataTypes.STRING(),
                                        null,
                                        TableChange.ColumnPosition.last(),
                                        "enriched")));

        assertThat(updated.getColumns()).hasSize(3);
        Map<String, List<Integer>> groups = updated.getColumnGroups();
        assertThat(groups).containsOnlyKeys("enriched");
        assertThat(groups.get("enriched")).containsExactly(2);
    }

    @Test
    void rejectsEmptyColumnGroupName() {
        Schema initial =
                Schema.newBuilder()
                        .column("device_id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .build();
        assertThatThrownBy(
                        () ->
                                SchemaUpdate.applySchemaChanges(
                                        initial,
                                        Collections.singletonList(
                                                TableChange.addColumn(
                                                        "bad",
                                                        DataTypes.STRING(),
                                                        null,
                                                        TableChange.ColumnPosition.last(),
                                                        ""))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("Column group name must not be empty");
    }

    @Test
    void cannotAddColumnWithSameNameAsExistingColumn() {
        // Phase M.6: a new column being added cannot share a name with an existing column.
        // This is the only practical way M.1 (partition keys ⊂ default group) could be violated
        // via schema evolution — by re-adding the partition-key column with a group tag — and the
        // existing "Column already exists" check transitively prevents it. Partition keys
        // themselves are fixed at table-create time and cannot be added or removed via
        // TableChange today (M.6 §3.2 future-work note).
        Schema initial =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("device_id", DataTypes.INT())
                        .build();
        assertThatThrownBy(
                        () ->
                                SchemaUpdate.applySchemaChanges(
                                        initial,
                                        Collections.singletonList(
                                                TableChange.addColumn(
                                                        "dt",
                                                        DataTypes.STRING(),
                                                        null,
                                                        TableChange.ColumnPosition.last(),
                                                        "enriched"))))
                .isInstanceOf(InvalidAlterTableException.class)
                .hasMessageContaining("already exists");
    }

    @Test
    void preservesPreExistingGroupsWhenAddingDefaultGroupColumn() {
        Schema initial =
                Schema.newBuilder()
                        .column("device_id", DataTypes.INT())
                        .column("geo_region", DataTypes.STRING())
                        .columnGroup("enriched")
                        .build();
        Schema updated =
                SchemaUpdate.applySchemaChanges(
                        initial,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "ts",
                                        DataTypes.BIGINT(),
                                        null,
                                        TableChange.ColumnPosition.last())));

        assertThat(updated.getColumns()).hasSize(3);
        assertThat(updated.getColumnGroups().get("enriched")).containsExactly(1);
        // The default group still owns the unassigned columns: device_id and ts.
        assertThat(updated.getDefaultGroupColumnIndices()).contains(0, 2);
    }
}
