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
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.utils.FlinkRowToFlussRowConverter;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.MapUtils;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Phase L.3 / M.5: Flink Sink V2 writer that translates {@code (src_bucket, src_offset,
 * <enrichment-values...>)} (non-partitioned target) or {@code (src_partition, src_bucket,
 * src_offset, <enrichment-values...>)} (partitioned target) rows into {@link
 * AppendWriter#appendColumns} calls on the target column-group table.
 *
 * <p>When the target table is partitioned, the leading column carries the partition name as a
 * {@code STRING}; the writer resolves the name to the internal partition ID via a cached {@link
 * Admin#listPartitionInfos} lookup. Plan-time validation in {@link
 * org.apache.fluss.flink.sink.EnrichmentTableSink} ensures the sink row layout matches the target's
 * partitioning + group shape before this writer ever runs.
 */
public class EnrichmentSinkWriter implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentSinkWriter.class);

    private final TablePath tablePath;
    private final Configuration flussConfig;
    private final long tableId;
    private final String groupName;
    private final RowType enrichmentValueRowType;
    private final boolean partitioned;

    private transient Connection connection;
    private transient Admin admin;
    private transient Table table;
    private transient AppendWriter appendWriter;
    private transient FlinkRowToFlussRowConverter valuesConverter;
    private transient ProjectedRowData valuesView;
    private transient Map<String, Long> partitionIdCache;

    private volatile Throwable asyncError;

    public EnrichmentSinkWriter(
            TablePath tablePath,
            Configuration flussConfig,
            long tableId,
            String groupName,
            RowType enrichmentValueRowType,
            boolean partitioned) {
        this.tablePath = tablePath;
        this.flussConfig = flussConfig;
        this.tableId = tableId;
        this.groupName = groupName;
        this.enrichmentValueRowType = enrichmentValueRowType;
        this.partitioned = partitioned;
    }

    public void initialize() {
        connection = ConnectionFactory.createConnection(flussConfig);
        table = connection.getTable(tablePath);
        appendWriter = table.newAppend().createWriter();
        valuesConverter = FlinkRowToFlussRowConverter.create(enrichmentValueRowType);

        // Sink-side row layout:
        //   non-partitioned: [src_bucket BIGINT, src_offset BIGINT, <values...>]
        //   partitioned:     [src_partition STRING, src_bucket BIGINT, src_offset BIGINT,
        //                     <values...>]
        int addressingArity = partitioned ? 3 : 2;
        int[] valuesProjection = new int[enrichmentValueRowType.getFieldCount()];
        for (int i = 0; i < valuesProjection.length; i++) {
            valuesProjection[i] = i + addressingArity;
        }
        valuesView = ProjectedRowData.from(valuesProjection);

        if (partitioned) {
            admin = connection.getAdmin();
            partitionIdCache = MapUtils.newConcurrentHashMap();
        }

        LOG.info(
                "Opened enrichment sink writer for table {} group '{}' "
                        + "(tableId={}, partitioned={}, addressingArity={}).",
                tablePath,
                groupName,
                tableId,
                partitioned,
                addressingArity);
    }

    @Override
    public void write(RowData row, Context context) throws IOException {
        rethrowIfAsyncError();
        TableBucket bucket;
        long sourceOffset;
        if (partitioned) {
            String partitionName = row.getString(0).toString();
            int bucketId = (int) row.getLong(1);
            sourceOffset = row.getLong(2);
            long partitionId = resolvePartitionId(partitionName);
            bucket = new TableBucket(tableId, partitionId, bucketId);
        } else {
            int bucketId = (int) row.getLong(0);
            sourceOffset = row.getLong(1);
            bucket = new TableBucket(tableId, bucketId);
        }
        InternalRow valuesRow = valuesConverter.toInternalRow(valuesView.replaceRow(row));
        CompletableFuture<?> future =
                appendWriter.appendColumns(groupName, bucket, sourceOffset, valuesRow);
        future.whenComplete(
                (ignored, throwable) -> {
                    if (throwable != null && asyncError == null) {
                        asyncError = throwable;
                    }
                });
    }

    /**
     * Resolves a partition name to its internal numeric ID. Misses fall back to a fresh {@link
     * Admin#listPartitionInfos} call; if the name still doesn't resolve the partition doesn't exist
     * (or was dropped between scan and write).
     */
    private long resolvePartitionId(String partitionName) throws IOException {
        Long cached = partitionIdCache.get(partitionName);
        if (cached != null) {
            return cached;
        }
        try {
            for (PartitionInfo info : admin.listPartitionInfos(tablePath).get()) {
                partitionIdCache.put(info.getPartitionName(), info.getPartitionId());
            }
        } catch (Exception e) {
            throw new IOException(
                    "Failed to list partitions of enrichment target "
                            + tablePath
                            + " while resolving partition '"
                            + partitionName
                            + "': "
                            + e.getMessage(),
                    e);
        }
        cached = partitionIdCache.get(partitionName);
        if (cached == null) {
            throw new IOException(
                    "Partition '"
                            + partitionName
                            + "' does not exist on enrichment target "
                            + tablePath
                            + ". The partition may have been dropped or never created.");
        }
        return cached;
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
            if (admin != null) {
                admin.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception closing Fluss Admin for enrichment sink.", e);
        } finally {
            admin = null;
        }
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
