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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.RowType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A scanner that co-reads from a parent log table and an overlay log table, merging records by
 * offset alignment.
 *
 * <p>The overlay table must have a {@code parent_offset} column (BIGINT) as its first field,
 * followed by enrichment columns. The merged output contains all parent columns followed by the
 * overlay enrichment columns (excluding the {@code parent_offset} column).
 *
 * <p>This class is NOT thread-safe.
 *
 * @since 0.6
 */
@PublicEvolving
public class MergedLogScanner implements AutoCloseable {

    private final LogScanner parentScanner;
    private final LogScanner overlayScanner;
    private final RowType parentRowType;
    private final RowType overlayRowType;
    private final RowType mergedRowType;
    private final MergeMode mergeMode;

    private final FieldGetter[] parentFieldGetters;
    private final FieldGetter[] overlayFieldGetters;

    /** Overlay records waiting to be matched, keyed by parent offset. */
    private final Map<Long, ScanRecord> overlayBuffer;

    /** Parent records waiting for overlay match (WAIT mode only), keyed by parent offset. */
    private final Map<Long, ScanRecord> parentBuffer;

    /**
     * Creates a merged log scanner.
     *
     * @param parentScanner scanner for the parent (base) log table
     * @param overlayScanner scanner for the overlay (enrichment) log table
     * @param parentRowType row type of the parent table
     * @param overlayRowType row type of the overlay table (field 0 must be parent_offset BIGINT)
     * @param mergeMode how to handle overlay lag
     */
    public MergedLogScanner(
            LogScanner parentScanner,
            LogScanner overlayScanner,
            RowType parentRowType,
            RowType overlayRowType,
            MergeMode mergeMode) {
        this.parentScanner = parentScanner;
        this.overlayScanner = overlayScanner;
        this.parentRowType = parentRowType;
        this.overlayRowType = overlayRowType;
        this.mergeMode = mergeMode;

        // Build merged RowType: all parent fields + overlay fields except parent_offset (index 0)
        List<DataField> mergedFields = new ArrayList<>();
        mergedFields.addAll(parentRowType.getFields());
        List<DataField> overlayFields = overlayRowType.getFields();
        for (int i = 1; i < overlayFields.size(); i++) {
            mergedFields.add(overlayFields.get(i));
        }
        this.mergedRowType = new RowType(mergedFields);

        this.parentFieldGetters = InternalRow.createFieldGetters(parentRowType);

        // Create getters for overlay enrichment fields (skip parent_offset at index 0)
        int enrichmentFieldCount = overlayRowType.getFieldCount() - 1;
        this.overlayFieldGetters = new FieldGetter[enrichmentFieldCount];
        for (int i = 0; i < enrichmentFieldCount; i++) {
            this.overlayFieldGetters[i] =
                    InternalRow.createFieldGetter(overlayRowType.getTypeAt(i + 1), i + 1);
        }

        this.overlayBuffer = new HashMap<>();
        this.parentBuffer = new HashMap<>();
    }

    /** Returns the merged row type (parent columns + overlay enrichment columns). */
    public RowType getMergedRowType() {
        return mergedRowType;
    }

    /**
     * Polls both scanners and returns merged records.
     *
     * @param timeout the maximum time to block waiting for records
     * @return a list of merged scan records
     */
    public List<ScanRecord> poll(Duration timeout) {
        // 1. Poll overlay scanner and buffer records keyed by parent_offset
        ScanRecords overlayRecords = overlayScanner.poll(Duration.ofMillis(100));
        for (ScanRecord rec : overlayRecords) {
            long parentOffset = rec.getRow().getLong(0);
            overlayBuffer.put(parentOffset, rec);
        }

        // 2. Poll parent scanner
        ScanRecords parentRecords = parentScanner.poll(timeout);

        // 3. Merge parent records with overlay buffer
        List<ScanRecord> merged = new ArrayList<>();
        for (ScanRecord parentRec : parentRecords) {
            long parentOffset = parentRec.logOffset();
            ScanRecord overlayRec = overlayBuffer.remove(parentOffset);

            if (overlayRec != null) {
                merged.add(mergeRows(parentRec, overlayRec));
            } else if (mergeMode == MergeMode.BEST_EFFORT) {
                merged.add(mergeWithNulls(parentRec));
            } else {
                // WAIT mode: buffer the parent record for later matching
                parentBuffer.put(parentOffset, parentRec);
            }
        }

        // 4. In WAIT mode, try to match buffered parent records with newly arrived overlay
        if (mergeMode == MergeMode.WAIT && !parentBuffer.isEmpty()) {
            Iterator<Map.Entry<Long, ScanRecord>> it = parentBuffer.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, ScanRecord> entry = it.next();
                ScanRecord overlayRec = overlayBuffer.remove(entry.getKey());
                if (overlayRec != null) {
                    merged.add(mergeRows(entry.getValue(), overlayRec));
                    it.remove();
                }
            }
        }

        return merged;
    }

    private ScanRecord mergeRows(ScanRecord parentRec, ScanRecord overlayRec) {
        int parentFieldCount = parentRowType.getFieldCount();
        int enrichmentFieldCount = overlayFieldGetters.length;
        GenericRow merged = new GenericRow(parentFieldCount + enrichmentFieldCount);

        InternalRow parentRow = parentRec.getRow();
        InternalRow overlayRow = overlayRec.getRow();

        for (int i = 0; i < parentFieldCount; i++) {
            merged.setField(i, parentFieldGetters[i].getFieldOrNull(parentRow));
        }
        for (int i = 0; i < enrichmentFieldCount; i++) {
            merged.setField(
                    parentFieldCount + i, overlayFieldGetters[i].getFieldOrNull(overlayRow));
        }

        return new ScanRecord(
                parentRec.logOffset(), parentRec.timestamp(), parentRec.getChangeType(), merged);
    }

    private ScanRecord mergeWithNulls(ScanRecord parentRec) {
        int parentFieldCount = parentRowType.getFieldCount();
        int enrichmentFieldCount = overlayFieldGetters.length;
        GenericRow merged = new GenericRow(parentFieldCount + enrichmentFieldCount);

        InternalRow parentRow = parentRec.getRow();
        for (int i = 0; i < parentFieldCount; i++) {
            merged.setField(i, parentFieldGetters[i].getFieldOrNull(parentRow));
        }
        // enrichment fields remain null

        return new ScanRecord(
                parentRec.logOffset(), parentRec.timestamp(), parentRec.getChangeType(), merged);
    }

    @Override
    public void close() throws Exception {
        parentScanner.close();
        overlayScanner.close();
    }
}
