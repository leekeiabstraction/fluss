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

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.PreAllocatedPagedOutputView;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.record.BytesViewLogRecords;
import org.apache.fluss.record.FileLogInputStream;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordBatchFormat;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MultiBytesView;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.arrow.memory.BufferAllocatorUtil;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.AbstractIterator;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merge-on-read for column-group enrichment. Decodes raw base records and overlays per-row
 * enrichment values from {@link EnrichmentSegment}s, re-encoding the result as Arrow batches whose
 * schema matches the projected row type.
 *
 * <p>Used when a fetch projection includes columns from one or more enrichment groups. For
 * projections that touch only base columns, this merger is bypassed and the existing zero-copy
 * {@link org.apache.fluss.record.FileLogProjection} path is used instead.
 *
 * <p><b>Phase D scope:</b> assumes one row per enrichment batch (the current writer behavior in
 * {@code AppendWriterImpl.appendColumns}). Multi-row enrichment batches are rejected with an {@link
 * IllegalStateException} until row-index tracking inside enrichment batches is added.
 *
 * <p><b>Lifecycle:</b> one merger instance per fetch — construct, call {@link #merge}, then close.
 */
@NotThreadSafe
public final class EnrichmentMerger implements AutoCloseable {

    private final long tableId;
    private final int tableSchemaId;
    private final Schema tableSchema;
    private final RowType tableRowType;
    private final RowType projectedRowType;
    private final int[] projectedFields;
    private final ArrowCompressionInfo compressionInfo;
    private final BufferAllocator allocator;
    private final ArrowWriterPool writerPool;
    private final SchemaGetter schemaGetter;

    /**
     * Per projected position, the lookup info for an enrichment column, or {@code null} if the
     * position is a base column. Indexed by position in {@code projectedFields}.
     */
    private final EnrichmentColumnRef[] enrichmentRefs;

    /** Read context for raw base records (full table row type). */
    private final LogRecordReadContext baseReadContext;

    /** Field getters indexed by table column position; used to copy base columns into output. */
    private final FieldGetter[] tableFieldGetters;

    /**
     * Lazy decoder cache keyed by {@code (groupName, batchSchemaId)}. Each batch in an enrichment
     * segment carries its own schemaId in its header; the merger picks the matching decoder so
     * mixed-schema segments (the result of a schema evolution mid-stream) decode correctly.
     */
    private final Map<String, Map<Integer, GroupDecoder>> groupDecoders = new HashMap<>();

    /** Synthetic schemaId for the output ArrowWriter pool — keeps base/enrichment writers apart. */
    private final int outputPoolSchemaId;

    public EnrichmentMerger(
            Schema tableSchema,
            int tableSchemaId,
            long tableId,
            int[] projectedFields,
            ArrowCompressionInfo compressionInfo,
            SchemaGetter schemaGetter) {
        this.tableId = tableId;
        this.tableSchemaId = tableSchemaId;
        this.tableSchema = tableSchema;
        this.tableRowType = tableSchema.getRowType();
        this.projectedFields = projectedFields;
        this.projectedRowType = tableRowType.project(projectedFields);
        this.compressionInfo = compressionInfo;
        this.allocator = BufferAllocatorUtil.createBufferAllocator();
        this.writerPool = new ArrowWriterPool(allocator);
        this.outputPoolSchemaId = tableSchemaId ^ java.util.Arrays.hashCode(projectedFields);
        this.tableFieldGetters = InternalRow.createFieldGetters(tableRowType);
        this.schemaGetter = schemaGetter;
        this.baseReadContext =
                LogRecordReadContext.createArrowReadContext(
                        tableRowType, tableSchemaId, schemaGetter);
        this.enrichmentRefs = buildEnrichmentRefs(tableSchema, projectedFields);
    }

    /**
     * Whether the given projection touches at least one enrichment column. Used by the read path to
     * decide whether to invoke the merger.
     */
    public static boolean projectionTouchesEnrichment(Schema schema, int[] projectedFields) {
        if (projectedFields == null) {
            return false;
        }
        Map<String, List<Integer>> groups = schema.getColumnGroups();
        if (groups.isEmpty()) {
            return false;
        }
        java.util.Set<Integer> projected = new java.util.HashSet<>();
        for (int idx : projectedFields) {
            projected.add(idx);
        }
        for (List<Integer> groupCols : groups.values()) {
            for (int colIdx : groupCols) {
                if (projected.contains(colIdx)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Re-encode {@code baseRecords} with enrichment columns sourced from {@code
     * enrichmentSegments}, preserving the projected row order. Each input batch produces one output
     * batch whose baseLogOffset, magic, and schemaId match the input.
     */
    public LogRecords merge(
            LogRecords baseRecords, Map<String, EnrichmentSegment> enrichmentSegments)
            throws Exception {
        if (baseRecords == null || baseRecords.sizeInBytes() == 0) {
            return MemoryLogRecords.EMPTY;
        }
        MultiBytesView.Builder outputBuilder = MultiBytesView.builder();
        boolean wrote = false;
        for (LogRecordBatch batch : baseRecords.batches()) {
            BytesView batchBytes = mergeBatch(batch, enrichmentSegments);
            if (batchBytes != null && batchBytes.getBytesLength() > 0) {
                outputBuilder.addBytes(batchBytes);
                wrote = true;
            }
        }
        if (!wrote) {
            return MemoryLogRecords.EMPTY;
        }
        return new BytesViewLogRecords(outputBuilder.build());
    }

    private BytesView mergeBatch(
            LogRecordBatch batch, Map<String, EnrichmentSegment> enrichmentSegments)
            throws Exception {
        // Output is at most as large as the input batch — base columns dominate, enrichment is
        // typically narrower; one heap segment of that size + overflow-to-heap in the paged view
        // gives us safe headroom.
        int initialPage = Math.max(16 * 1024, batch.sizeInBytes());
        ArrowWriter arrowWriter =
                writerPool.getOrCreateWriter(
                        tableId,
                        outputPoolSchemaId,
                        initialPage,
                        projectedRowType,
                        compressionInfo);
        List<MemorySegment> segs = new ArrayList<>();
        segs.add(MemorySegment.allocateHeapMemory(initialPage));
        PreAllocatedPagedOutputView outputView = new PreAllocatedPagedOutputView(segs);
        try (MemoryLogRecordsArrowBuilder builder =
                MemoryLogRecordsArrowBuilder.builder(
                        batch.baseLogOffset(),
                        normalizeMagic(batch.magic()),
                        tableSchemaId,
                        arrowWriter,
                        outputView,
                        true)) {
            int rowCount = 0;
            try (CloseableIterator<LogRecord> rowIter = batch.records(baseReadContext)) {
                while (rowIter.hasNext()) {
                    LogRecord baseRecord = rowIter.next();
                    long sourceOffset = baseRecord.logOffset();
                    InternalRow baseRow = baseRecord.getRow();
                    GenericRow projectedRow = new GenericRow(projectedFields.length);
                    for (int p = 0; p < projectedFields.length; p++) {
                        EnrichmentColumnRef ref = enrichmentRefs[p];
                        if (ref != null) {
                            EnrichmentSegment seg = enrichmentSegments.get(ref.groupName);
                            if (seg == null) {
                                throw new IllegalStateException(
                                        "Enrichment group '"
                                                + ref.groupName
                                                + "' is not registered for the leader; "
                                                + "EWM gate should have prevented this fetch from reaching the merger.");
                            }
                            projectedRow.setField(p, lookupEnrichment(seg, ref, sourceOffset));
                        } else {
                            int colIdx = projectedFields[p];
                            projectedRow.setField(
                                    p, tableFieldGetters[colIdx].getFieldOrNull(baseRow));
                        }
                    }
                    builder.append(baseRecord.getChangeType(), projectedRow);
                    rowCount++;
                }
                if (rowCount == 0) {
                    return null;
                }
            }
            return builder.build();
        }
    }

    private Object lookupEnrichment(
            EnrichmentSegment seg, EnrichmentColumnRef ref, long sourceOffset) throws IOException {
        OffsetPosition pos = seg.lookup(sourceOffset);
        if (pos == null) {
            // Reaching here means the EWM gate let an unenriched offset through, which would be
            // an invariant violation. Rather than crash the fetch, surface a null and let the
            // caller decide — Phase B's EWM cap is the trusted gate.
            return null;
        }
        AbstractIterator<FileLogInputStream.FileChannelLogRecordBatch> iter =
                seg.records().batchIterator(pos.getPosition(), seg.records().sizeInBytes());
        if (!iter.hasNext()) {
            return null;
        }
        FileLogInputStream.FileChannelLogRecordBatch batch = iter.next();
        if (batch.getRecordCount() != 1) {
            throw new IllegalStateException(
                    "Multi-row enrichment batches are not yet supported (Phase D); "
                            + "batch at position "
                            + pos.getPosition()
                            + " has "
                            + batch.getRecordCount()
                            + " rows.");
        }
        // E.7: dispatch on the batch's schemaId so a segment that spans a schema evolution
        // decodes correctly. The cache holds a decoder per (group, schemaId) pair.
        GroupDecoder decoder = getOrBuildDecoder(ref.groupName, batch.schemaId());
        return decoder.readColumnByName(batch, ref.columnName);
    }

    private GroupDecoder getOrBuildDecoder(String groupName, int batchSchemaId) {
        return groupDecoders
                .computeIfAbsent(groupName, k -> new HashMap<>())
                .computeIfAbsent(batchSchemaId, sid -> buildDecoder(groupName, sid));
    }

    private GroupDecoder buildDecoder(String groupName, int batchSchemaId) {
        Schema schemaForBatch =
                batchSchemaId == tableSchemaId
                        ? tableSchema
                        : schemaGetter.getSchema(batchSchemaId);
        Map<String, List<Integer>> batchGroups = schemaForBatch.getColumnGroups();
        List<Integer> batchGroupCols = batchGroups.get(groupName);
        if (batchGroupCols == null) {
            // Group existed at fetch time but not in the batch's schema. Should not happen on
            // the read path because the writer only stamps a schemaId that defines the group.
            // Fail loudly instead of silently nulling the column.
            throw new IllegalStateException(
                    "Enrichment batch schemaId "
                            + batchSchemaId
                            + " has no column group '"
                            + groupName
                            + "'.");
        }
        RowType batchGroupRowType = buildGroupRowType(schemaForBatch, batchGroupCols);
        return new GroupDecoder(batchGroupRowType, batchSchemaId);
    }

    private EnrichmentColumnRef[] buildEnrichmentRefs(Schema schema, int[] projection) {
        Map<String, List<Integer>> groups = schema.getColumnGroups();
        EnrichmentColumnRef[] refs = new EnrichmentColumnRef[projection.length];
        for (int p = 0; p < projection.length; p++) {
            int colIdx = projection[p];
            Schema.Column col = schema.getColumns().get(colIdx);
            String groupName = col.getColumnGroup().orElse(null);
            if (groupName == null) {
                continue;
            }
            List<Integer> groupCols = groups.get(groupName);
            if (groupCols.indexOf(colIdx) < 0) {
                throw new IllegalStateException(
                        "Column "
                                + colIdx
                                + " claims group '"
                                + groupName
                                + "' but is not in the group's column list "
                                + groupCols);
            }
            refs[p] = new EnrichmentColumnRef(groupName, col.getName());
        }
        return refs;
    }

    private static RowType buildGroupRowType(Schema schema, List<Integer> groupCols) {
        DataType[] types = new DataType[groupCols.size()];
        String[] names = new String[groupCols.size()];
        for (int i = 0; i < groupCols.size(); i++) {
            Schema.Column col = schema.getColumns().get(groupCols.get(i));
            types[i] = col.getDataType();
            names[i] = col.getName();
        }
        return RowType.of(types, names);
    }

    /**
     * Pick a magic value the {@link MemoryLogRecordsArrowBuilder} can serialize. The base log can
     * be V0/V1/V2; the builder only directly supports V0/V1 today, so V2 is normalized down to V1
     * (statistics-capable, the closest fit). For our re-encoded batches we don't carry statistics
     * regardless.
     */
    private static byte normalizeMagic(byte inputMagic) {
        if (inputMagic >= LogRecordBatchFormat.LOG_MAGIC_VALUE_V1) {
            return LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
        }
        return LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
    }

    @Override
    public void close() {
        for (Map<Integer, GroupDecoder> perSchema : groupDecoders.values()) {
            for (GroupDecoder d : perSchema.values()) {
                d.close();
            }
        }
        groupDecoders.clear();
        baseReadContext.close();
        try {
            writerPool.close();
        } catch (Exception ignored) {
            // best-effort
        }
        allocator.close();
    }

    /** Per-projection-position lookup for an enrichment column. */
    private static final class EnrichmentColumnRef {
        final String groupName;
        final String columnName;

        EnrichmentColumnRef(String groupName, String columnName) {
            this.groupName = groupName;
            this.columnName = columnName;
        }
    }

    /**
     * Decodes Arrow batches stored in an {@link EnrichmentSegment} for one specific {@code (group,
     * batchSchemaId)} pair. Built lazily by {@link #getOrBuildDecoder} when a batch with that
     * schemaId is first encountered. Looks up columns by name so that an evolution which reorders
     * columns within the group is handled transparently.
     */
    private static final class GroupDecoder implements AutoCloseable {

        private final RowType groupRowType;
        private final FieldGetter[] groupFieldGetters;
        private final LogRecordReadContext readContext;

        GroupDecoder(RowType groupRowType, int batchSchemaId) {
            this.groupRowType = groupRowType;
            this.groupFieldGetters = InternalRow.createFieldGetters(groupRowType);
            // schemaGetter is null because the decoder is constructed for one specific schemaId
            // and only ever applied to batches with that schemaId — the dispatch in the parent
            // already routes to the right decoder.
            this.readContext =
                    LogRecordReadContext.createArrowReadContext(groupRowType, batchSchemaId, null);
        }

        Object readColumnByName(
                FileLogInputStream.FileChannelLogRecordBatch batch, String columnName)
                throws IOException {
            int idx = groupRowType.getFieldNames().indexOf(columnName);
            if (idx < 0) {
                // Column was added in a later schema; this batch's schema doesn't have it.
                // Surfacing null is the right behaviour — the row predates the column.
                return null;
            }
            try (CloseableIterator<LogRecord> rowIter = batch.records(readContext)) {
                if (!rowIter.hasNext()) {
                    return null;
                }
                LogRecord rec = rowIter.next();
                return groupFieldGetters[idx].getFieldOrNull(rec.getRow());
            }
        }

        @Override
        public void close() {
            readContext.close();
        }
    }
}
