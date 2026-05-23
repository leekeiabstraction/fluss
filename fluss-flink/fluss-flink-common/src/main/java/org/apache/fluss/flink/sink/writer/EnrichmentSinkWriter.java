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

package org.apache.fluss.flink.sink.writer;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.utils.FlinkRowToFlussRowConverter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Phase L.3: Flink Sink V2 writer that translates {@code (src_bucket, src_offset,
 * <enrichment-values...>)} rows into {@link AppendWriter#appendColumns} calls on the target
 * column-group table.
 *
 * <p>The sink-side row schema is fixed: positions {@code 0} and {@code 1} carry the {@code BIGINT}
 * bucket and offset respectively; positions {@code 2..N-1} carry the enrichment group's columns in
 * declared order. Plan-time validation in {@link org.apache.fluss.flink.sink.EnrichmentTableSink}
 * ensures the sink schema matches the target group before this writer ever runs.
 */
public class EnrichmentSinkWriter implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentSinkWriter.class);

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final long tableId;
    private final String groupName;
    private final RowType enrichmentValueRowType;

    private transient Connection connection;
    private transient Table table;
    private transient AppendWriter appendWriter;
    private transient FlinkRowToFlussRowConverter valuesConverter;
    private transient ProjectedRowData valuesView;

    private volatile Throwable asyncError;

    public EnrichmentSinkWriter(
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

    public void initialize() {
        connection = ConnectionFactory.createConnection(flussConfig);
        table = connection.getTable(tablePath);
        appendWriter = table.newAppend().createWriter();
        valuesConverter = FlinkRowToFlussRowConverter.create(enrichmentValueRowType);
        // Sink-side row layout: [src_bucket BIGINT, src_offset BIGINT, <values...>].
        int totalFields = enrichmentValueRowType.getFieldCount() + 2;
        int[] valuesProjection = new int[enrichmentValueRowType.getFieldCount()];
        for (int i = 0; i < valuesProjection.length; i++) {
            valuesProjection[i] = i + 2;
        }
        valuesView = ProjectedRowData.from(valuesProjection);
        LOG.info(
                "Opened enrichment sink writer for table {} group '{}' (tableId={}, fields={}).",
                tablePath,
                groupName,
                tableId,
                totalFields);
    }

    @Override
    public void write(RowData row, Context context) throws IOException {
        rethrowIfAsyncError();
        int bucketId = (int) row.getLong(0);
        long sourceOffset = row.getLong(1);
        InternalRow valuesRow = valuesConverter.toInternalRow(valuesView.replaceRow(row));
        TableBucket bucket = new TableBucket(tableId, bucketId);
        CompletableFuture<?> future =
                appendWriter.appendColumns(groupName, bucket, sourceOffset, valuesRow);
        future.whenComplete(
                (ignored, throwable) -> {
                    if (throwable != null && asyncError == null) {
                        asyncError = throwable;
                    }
                });
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        if (appendWriter != null) {
            appendWriter.flush();
        }
        rethrowIfAsyncError();
    }

    @Override
    public void close() throws Exception {
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception closing Fluss Table for enrichment sink.", e);
        } finally {
            table = null;
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception closing Fluss Connection for enrichment sink.", e);
        } finally {
            connection = null;
        }
        try {
            if (valuesConverter != null) {
                valuesConverter.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception closing FlinkRowToFlussRowConverter.", e);
        } finally {
            valuesConverter = null;
        }
        rethrowIfAsyncError();
    }

    private void rethrowIfAsyncError() throws IOException {
        Throwable t = asyncError;
        if (t != null) {
            asyncError = null;
            throw new IOException("Async enrichment append failed: " + t.getMessage(), t);
        }
    }
}
