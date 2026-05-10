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
import org.apache.fluss.exception.SchemaChangeException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;

import java.util.List;

/** Schema update. */
public class SchemaUpdate {

    /** Apply schema changes to the given table info and return the updated schema. */
    public static Schema applySchemaChanges(Schema initialSchema, List<TableChange> changes) {
        SchemaUpdate schemaUpdate = new SchemaUpdate(initialSchema);
        for (TableChange change : changes) {
            schemaUpdate = schemaUpdate.applySchemaChange(change);
        }
        return schemaUpdate.getSchema();
    }

    // Now we only maintain the Builder
    private final Schema.Builder builder;

    public SchemaUpdate(Schema initialSchema) {
        // Initialize builder from the current table schema
        this.builder = Schema.newBuilder().fromSchema(initialSchema);
    }

    public Schema getSchema() {
        // Validation and building are now delegated
        return builder.build();
    }

    private SchemaUpdate applySchemaChange(TableChange columnChange) {
        if (columnChange instanceof TableChange.AddColumn) {
            return addColumn((TableChange.AddColumn) columnChange);
        } else if (columnChange instanceof TableChange.ModifyColumn) {
            return modifiedColumn((TableChange.ModifyColumn) columnChange);
        } else if (columnChange instanceof TableChange.RenameColumn) {
            return renameColumn((TableChange.RenameColumn) columnChange);
        } else if (columnChange instanceof TableChange.DropColumn) {
            return dropColumn((TableChange.DropColumn) columnChange);
        }
        throw new IllegalArgumentException(
                "Unknown column change type " + columnChange.getClass().getName());
    }

    private SchemaUpdate addColumn(TableChange.AddColumn addColumn) {
        // Use the builder to check if column exists
        Schema.Column existingColumn = builder.getColumn(addColumn.getName()).orElse(null);

        if (existingColumn != null) {
            throw new InvalidAlterTableException(
                    "Column " + addColumn.getName() + " already exists.");
        }

        if (addColumn.getPosition() != TableChange.ColumnPosition.last()) {
            throw new IllegalArgumentException("Only support addColumn column at last now.");
        }

        if (!addColumn.getDataType().isNullable()) {
            throw new IllegalArgumentException(
                    "Column " + addColumn.getName() + " must be nullable.");
        }

        // Delegate the actual addition to the builder
        builder.column(addColumn.getName(), addColumn.getDataType());

        // Fixed: Use null check for the String comment
        String comment = addColumn.getComment();
        if (comment != null) {
            builder.withComment(comment);
        }

        // Phase H: tag the column to a column group when requested. Reuses
        // Schema.Builder#columnGroup, which assigns the most recently added column. If the named
        // group doesn't exist on the table yet, it is created with this column as its sole member
        // (PHASE_H §3.1, §4.2).
        String columnGroup = addColumn.getColumnGroup();
        if (columnGroup != null) {
            // Reject malformed group names early (PHASE_H §6.1). null means "default group" and
            // is the long-standing addColumn behaviour; an empty string is never a valid group
            // identifier and would silently land as a literal "" group otherwise.
            if (columnGroup.isEmpty()) {
                throw new InvalidAlterTableException(
                        "Column group name must not be empty (got empty string for column "
                                + addColumn.getName()
                                + ").");
            }
            builder.columnGroup(columnGroup);
        }

        return this;
    }

    private SchemaUpdate dropColumn(TableChange.DropColumn dropColumn) {
        throw new SchemaChangeException("Not support drop column now.");
    }

    private SchemaUpdate modifiedColumn(TableChange.ModifyColumn modifyColumn) {
        throw new SchemaChangeException("Not support modify column now.");
    }

    private SchemaUpdate renameColumn(TableChange.RenameColumn renameColumn) {
        throw new SchemaChangeException("Not support rename column now.");
    }
}
