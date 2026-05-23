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

package org.apache.fluss.flink.sink;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;

/**
 * Phase L.3: write-only Flink table sink that translates {@code INSERT INTO} statements into
 * column-group enrichment writes against another Fluss table.
 *
 * <p>Activated by the {@code enrichment.target} / {@code enrichment.group} table properties in the
 * user's {@code CREATE TABLE ... WITH (...)}. The sink's row schema must be {@code (BIGINT
 * src_bucket, BIGINT src_offset, <group-cols...>)} — validated against the target table's current
 * schema at plan time.
 */
public class EnrichmentTableSink implements DynamicTableSink {

    private final TablePath sinkTablePath;
    private final TablePath targetTablePath;
    private final Configuration flussConfig;
    private final String groupName;
    private final RowType sinkRowType;

    public EnrichmentTableSink(
            TablePath sinkTablePath,
            TablePath targetTablePath,
            Configuration flussConfig,
            String groupName,
            RowType sinkRowType) {
        this.sinkTablePath = sinkTablePath;
        this.targetTablePath = targetTablePath;
        this.flussConfig = flussConfig;
        this.groupName = groupName;
        this.sinkRowType = sinkRowType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        ResolvedTarget resolved = resolveTarget();
        EnrichmentSink sink =
                new EnrichmentSink(
                        targetTablePath,
                        flussConfig,
                        resolved.tableId,
                        groupName,
                        resolved.enrichmentValueRowType);
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                return dataStream
                        .sinkTo(sink)
                        .name("EnrichmentSink(" + targetTablePath + "/" + groupName + ")")
                        .setParallelism(dataStream.getParallelism());
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new EnrichmentTableSink(
                sinkTablePath, targetTablePath, flussConfig, groupName, sinkRowType);
    }

    @Override
    public String asSummaryString() {
        return "FlussEnrichmentSink(target=" + targetTablePath + ", group=" + groupName + ")";
    }

    /**
     * Plan-time resolution: fetch the target's current schema, validate the sink row against the
     * named group, and return the value-only row type to pass to the runtime writer.
     */
    private ResolvedTarget resolveTarget() {
        TableInfo targetInfo;
        try (Connection conn = ConnectionFactory.createConnection(flussConfig);
                Admin admin = conn.getAdmin()) {
            try {
                targetInfo = admin.getTableInfo(targetTablePath).get();
            } catch (Exception e) {
                throw new ValidationException(
                        "Enrichment target table "
                                + targetTablePath
                                + " (declared in "
                                + sinkTablePath
                                + "'s `enrichment.target`) cannot be resolved: "
                                + rootMessage(e),
                        e);
            }
        } catch (Exception e) {
            throw new ValidationException(
                    "Failed to connect to Fluss while resolving enrichment target "
                            + targetTablePath
                            + ": "
                            + rootMessage(e),
                    e);
        }

        Schema targetSchema = targetInfo.getSchema();
        Map<String, List<Integer>> groups = targetSchema.getColumnGroups();
        List<Integer> groupIndexes = groups.get(groupName);
        if (groupIndexes == null) {
            throw new ValidationException(
                    "Column group '"
                            + groupName
                            + "' not declared on enrichment target "
                            + targetTablePath
                            + ". Available groups: "
                            + groups.keySet());
        }

        validateSinkLayout(targetSchema, groupIndexes);

        // Build value-only Flink RowType from the sink row's trailing fields (positions 2..N-1).
        List<RowType.RowField> sinkFields = sinkRowType.getFields();
        List<RowType.RowField> valueFields = sinkFields.subList(2, sinkFields.size());
        RowType valueRowType = new RowType(false, valueFields);
        return new ResolvedTarget(targetInfo.getTableId(), valueRowType);
    }

    private void validateSinkLayout(Schema targetSchema, List<Integer> groupIndexes) {
        List<RowType.RowField> sinkFields = sinkRowType.getFields();
        if (sinkFields.size() != groupIndexes.size() + 2) {
            throw new ValidationException(
                    "Enrichment sink "
                            + sinkTablePath
                            + " expects "
                            + (groupIndexes.size() + 2)
                            + " columns (2 addressing + "
                            + groupIndexes.size()
                            + " enrichment) for target group '"
                            + groupName
                            + "' on "
                            + targetTablePath
                            + ", but got "
                            + sinkFields.size()
                            + ".");
        }

        // Leading two fields must be BIGINT (src_bucket, src_offset).
        for (int i = 0; i < 2; i++) {
            LogicalType t = sinkFields.get(i).getType();
            if (t.getTypeRoot() != LogicalTypeRoot.BIGINT) {
                throw new ValidationException(
                        "Enrichment sink "
                                + sinkTablePath
                                + " column at position "
                                + i
                                + " ('"
                                + sinkFields.get(i).getName()
                                + "') must be BIGINT (positions 0 and 1 carry the bucket and "
                                + "offset respectively); got "
                                + t.asSummaryString());
            }
        }

        // Trailing fields must match the target group's column types in order, by Flink-mapped
        // type — names are user-chosen and not required to match.
        List<Schema.Column> targetColumns = targetSchema.getColumns();
        for (int i = 0; i < groupIndexes.size(); i++) {
            int targetIdx = groupIndexes.get(i);
            Schema.Column targetCol = targetColumns.get(targetIdx);
            RowType.RowField sinkField = sinkFields.get(i + 2);
            LogicalType expected =
                    FlinkConversions.toFlinkType(targetCol.getDataType()).getLogicalType();
            if (!sameType(sinkField.getType(), expected)) {
                throw new ValidationException(
                        "Enrichment sink "
                                + sinkTablePath
                                + " column at position "
                                + (i + 2)
                                + " ('"
                                + sinkField.getName()
                                + "', "
                                + sinkField.getType().asSummaryString()
                                + ") does not match target group '"
                                + groupName
                                + "' column '"
                                + targetCol.getName()
                                + "' ("
                                + expected.asSummaryString()
                                + ").");
            }
        }
    }

    private static boolean sameType(LogicalType a, LogicalType b) {
        // Match on the structural/typeRoot identity ignoring NOT NULL — Flink's planner widens
        // nullability when projecting from a target column.
        return a.copy(true).equals(b.copy(true));
    }

    private static String rootMessage(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getMessage();
    }

    private static final class ResolvedTarget {
        final long tableId;
        final RowType enrichmentValueRowType;

        ResolvedTarget(long tableId, RowType enrichmentValueRowType) {
            this.tableId = tableId;
            this.enrichmentValueRowType = enrichmentValueRowType;
        }
    }
}
