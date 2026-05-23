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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.SinkAdapter;
import org.apache.fluss.flink.sink.writer.EnrichmentSinkWriter;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Phase L.3: Flink Sink V2 wrapper around {@link EnrichmentSinkWriter}. The sink is the
 * serializable description that ships to TaskManagers; {@link #createWriter} instantiates the
 * per-subtask writer.
 */
public class EnrichmentSink extends SinkAdapter<RowData> {

    private static final long serialVersionUID = 1L;

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final long tableId;
    private final String groupName;
    private final RowType enrichmentValueRowType;

    public EnrichmentSink(
            TablePath tablePath,
            Configuration flussConfig,
            long tableId,
            String groupName,
            RowType enrichmentValueRowType) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableId = tableId;
        this.groupName = groupName;
        this.enrichmentValueRowType = enrichmentValueRowType;
    }

    @Override
    protected SinkWriter<RowData> createWriter(
            MailboxExecutor mailboxExecutor, SinkWriterMetricGroup metricGroup) {
        EnrichmentSinkWriter writer =
                new EnrichmentSinkWriter(
                        tablePath, flussConfig, tableId, groupName, enrichmentValueRowType);
        writer.initialize();
        return writer;
    }
}
