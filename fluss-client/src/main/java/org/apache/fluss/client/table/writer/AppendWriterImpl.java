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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.PreAllocatedPagedOutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.IndexedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogColumnsReqForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogColumnsRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogColumnsRequest;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** The writer to write data to the log table. */
class AppendWriterImpl extends AbstractTableWriter implements AppendWriter {
    private static final AppendResult APPEND_SUCCESS = new AppendResult();

    private final @Nullable KeyEncoder bucketKeyEncoder;

    private final LogFormat logFormat;
    private final IndexedRowEncoder indexedRowEncoder;
    private final CompactedRowEncoder compactedRowEncoder;
    private final FieldGetter[] fieldGetters;
    private final TableInfo tableInfo;

    AppendWriterImpl(TablePath tablePath, TableInfo tableInfo, WriterClient writerClient) {
        super(tablePath, tableInfo, writerClient);
        List<String> bucketKeys = tableInfo.getBucketKeys();
        if (bucketKeys.isEmpty()) {
            this.bucketKeyEncoder = null;
        } else {
            this.bucketKeyEncoder =
                    KeyEncoder.ofBucketKeyEncoder(
                            tableInfo.getRowType(),
                            tableInfo.getBucketKeys(),
                            tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        }

        DataType[] fieldDataTypes =
                tableInfo.getSchema().getRowType().getChildren().toArray(new DataType[0]);

        this.logFormat = tableInfo.getTableConfig().getLogFormat();
        this.indexedRowEncoder = new IndexedRowEncoder(tableInfo.getRowType());
        this.compactedRowEncoder = new CompactedRowEncoder(fieldDataTypes);
        this.fieldGetters = InternalRow.createFieldGetters(tableInfo.getRowType());
        this.tableInfo = tableInfo;
    }

    /**
     * Append row into Fluss non-pk table.
     *
     * @param row the row to append.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    public CompletableFuture<AppendResult> append(InternalRow row) {
        checkFieldCount(row);

        PhysicalTablePath physicalPath = getPhysicalPath(row);
        byte[] bucketKey = bucketKeyEncoder != null ? bucketKeyEncoder.encodeKey(row) : null;

        final WriteRecord record;
        if (logFormat == LogFormat.INDEXED) {
            IndexedRow indexedRow = encodeIndexedRow(row);
            record = WriteRecord.forIndexedAppend(tableInfo, physicalPath, indexedRow, bucketKey);
        } else if (logFormat == LogFormat.COMPACTED) {
            CompactedRow compactedRow = encodeCompactedRow(row);
            record =
                    WriteRecord.forCompactedAppend(
                            tableInfo, physicalPath, compactedRow, bucketKey);
        } else {
            // ARROW format supports general internal row
            record = WriteRecord.forArrowAppend(tableInfo, physicalPath, row, bucketKey);
        }
        return send(record).thenApply(ignored -> APPEND_SUCCESS);
    }

    private CompactedRow encodeCompactedRow(InternalRow row) {
        if (row instanceof CompactedRow) {
            return (CompactedRow) row;
        }

        compactedRowEncoder.startNewRow();
        for (int i = 0; i < fieldCount; i++) {
            compactedRowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return compactedRowEncoder.finishRow();
    }

    private IndexedRow encodeIndexedRow(InternalRow row) {
        if (row instanceof IndexedRow) {
            return (IndexedRow) row;
        }

        indexedRowEncoder.startNewRow();
        for (int i = 0; i < fieldCount; i++) {
            indexedRowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return indexedRowEncoder.finishRow();
    }

    @Override
    public CompletableFuture<AppendColumnsResult> appendColumns(
            String columnGroup, TableBucket bucket, long sourceOffset, InternalRow enrichmentRow) {
        if (logFormat != LogFormat.ARROW) {
            throw new IllegalArgumentException(
                    "Column groups require ARROW log format, but table '"
                            + tablePath
                            + "' is "
                            + logFormat);
        }
        Schema schema = tableInfo.getSchema();
        Map<String, List<Integer>> groups = schema.getColumnGroups();
        List<Integer> indices = groups.get(columnGroup);
        if (indices == null) {
            throw new IllegalArgumentException(
                    "Unknown column group '" + columnGroup + "' on table " + tablePath);
        }
        if (enrichmentRow.getFieldCount() != indices.size()) {
            throw new IllegalArgumentException(
                    "Enrichment row has "
                            + enrichmentRow.getFieldCount()
                            + " fields, but column group '"
                            + columnGroup
                            + "' has "
                            + indices.size()
                            + " columns");
        }

        // Build the column group's RowType (typed view of just the group's columns).
        String[] groupColumnNames = new String[indices.size()];
        DataType[] groupTypes = new DataType[indices.size()];
        List<Schema.Column> allColumns = schema.getColumns();
        for (int i = 0; i < indices.size(); i++) {
            Schema.Column col = allColumns.get(indices.get(i));
            groupColumnNames[i] = col.getName();
            groupTypes[i] = col.getDataType();
        }
        RowType groupRowType = RowType.of(groupTypes, groupColumnNames);

        // Encode the enrichment row as an Arrow MemoryLogRecords. We allocate a fresh allocator +
        // pool per call: column-group writes are rare relative to base appends, and pooling
        // across calls would require plumbing a per-group pool through the writer. Phase C will
        // share allocators with the persistence layer.
        BufferAllocator allocator = BufferAllocatorUtil.createBufferAllocator();
        ArrowWriterPool pool = new ArrowWriterPool(allocator);
        BytesView recordsBytes;
        try {
            // Use table's schemaId for the wire batch header — pool keys schemas by tableId +
            // schemaId, so we offset by columnGroup to avoid colliding with the table's main
            // ArrowWriter (which carries the full table RowType).
            int poolSchemaId = tableInfo.getSchemaId() ^ columnGroup.hashCode();
            int bufferSize = 16 * 1024;
            ArrowWriter arrowWriter =
                    pool.getOrCreateWriter(
                            tableInfo.getTableId(),
                            poolSchemaId,
                            bufferSize,
                            groupRowType,
                            tableInfo.getTableConfig().getArrowCompressionInfo());
            List<MemorySegment> segs = new java.util.ArrayList<>();
            segs.add(MemorySegment.allocateHeapMemory(bufferSize));
            PreAllocatedPagedOutputView outputView = new PreAllocatedPagedOutputView(segs);
            try (MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            tableInfo.getSchemaId(), arrowWriter, outputView, true, null)) {
                builder.append(ChangeType.APPEND_ONLY, enrichmentRow);
                recordsBytes = builder.build();
            }
        } catch (Exception e) {
            try {
                pool.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            allocator.close();
            throw e instanceof RuntimeException
                    ? (RuntimeException) e
                    : new FlussRuntimeException("Failed to encode column-group enrichment row", e);
        }
        // Pool/allocator are kept open until the response future completes — the BytesView's
        // ByteBuf may reference Arrow buffers from the allocator.
        final BufferAllocator finalAllocator = allocator;
        final ArrowWriterPool finalPool = pool;

        MetadataUpdater metadataUpdater = writerClient.getMetadataUpdater();
        int leaderId = metadataUpdater.leaderFor(tablePath, bucket);
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leaderId);
        if (gateway == null) {
            try {
                finalPool.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            finalAllocator.close();
            throw new FlussRuntimeException(
                    "Cannot find tablet server gateway for leader id " + leaderId);
        }

        ProduceLogColumnsRequest request =
                new ProduceLogColumnsRequest()
                        .setTableId(bucket.getTableId())
                        .setColumnGroup(columnGroup);
        PbProduceLogColumnsReqForBucket bucketReq = request.addBucketsReq();
        bucketReq.setBucketId(bucket.getBucket());
        if (bucket.getPartitionId() != null) {
            bucketReq.setPartitionId(bucket.getPartitionId());
        }
        bucketReq.setRecordsBytesView(recordsBytes);
        bucketReq.addSourceOffset(sourceOffset);

        CompletableFuture<AppendColumnsResult> result =
                gateway.produceLogColumns(request)
                        .thenApply(
                                response -> {
                                    if (response.getBucketsRespsCount() == 0) {
                                        throw new FlussRuntimeException(
                                                "Empty produceLogColumns response for bucket "
                                                        + bucket);
                                    }
                                    PbProduceLogColumnsRespForBucket resp =
                                            response.getBucketsRespAt(0);
                                    if (resp.hasErrorCode() && resp.getErrorCode() != 0) {
                                        String errorMsg =
                                                resp.hasErrorMessage()
                                                        ? resp.getErrorMessage()
                                                        : null;
                                        throw new ApiError(
                                                        Errors.forCode((short) resp.getErrorCode()),
                                                        errorMsg)
                                                .exception();
                                    }
                                    return new AppendColumnsResult(resp.getEnrichmentWatermark());
                                });
        result.whenComplete(
                (ok, err) -> {
                    try {
                        finalPool.close();
                    } catch (Exception ignored) {
                        // best-effort cleanup
                    }
                    finalAllocator.close();
                });
        return result;
    }
}
