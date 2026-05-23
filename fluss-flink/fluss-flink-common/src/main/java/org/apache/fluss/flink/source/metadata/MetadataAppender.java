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

package org.apache.fluss.flink.source.metadata;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.List;

/**
 * Splices bucket/offset metadata values into the produced row when the user has declared {@code
 * bucket} / {@code offset} as Flink METADATA columns.
 *
 * <p>The plan is computed once at planning time in {@code FlinkTableSource.applyProjection} and
 * shipped to the reader. Each emitted row is built by {@link #splice(RowData, int, long)} using the
 * per-record offset and the per-split bucket.
 */
public class MetadataAppender implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String BUCKET_KEY = "bucket";
    public static final String OFFSET_KEY = "offset";

    /** Sentinel values for the output plan. Physical row indices are non-negative. */
    private static final int BUCKET_SLOT = -1;

    private static final int OFFSET_SLOT = -2;

    /**
     * For each position in the produced row: a non-negative value is an index into the physical row
     * delivered by the deserializer; {@link #BUCKET_SLOT} means "write bucket"; {@link
     * #OFFSET_SLOT} means "write offset".
     */
    private final int[] outputPlan;

    public MetadataAppender(int[] outputPlan) {
        this.outputPlan = outputPlan;
    }

    public int outputArity() {
        return outputPlan.length;
    }

    public RowData splice(RowData physicalRow, int bucket, long offset) {
        GenericRowData out = new GenericRowData(outputPlan.length);
        out.setRowKind(physicalRow.getRowKind());
        GenericRowData phys = (GenericRowData) physicalRow;
        for (int i = 0; i < outputPlan.length; i++) {
            int slot = outputPlan[i];
            if (slot >= 0) {
                out.setField(i, phys.getField(slot));
            } else if (slot == BUCKET_SLOT) {
                out.setField(i, (long) bucket);
            } else {
                out.setField(i, offset);
            }
        }
        return out;
    }

    /**
     * Build a plan and the corresponding physical projection.
     *
     * <p>Flink's contract: {@code SupportsProjectionPushDown.applyProjection} carries only
     * <em>physical</em> positions — metadata columns are appended at the end of the source's
     * produced row, in {@code appliedMetadataKeys} order (Flink rearranges to the user's SELECT
     * order via a downstream Calc).
     *
     * @param projectedFields the physical projection from Flink, or {@code null} for "all physical
     *     columns".
     * @param physicalArity number of physical (non-metadata) columns on the Flink-side schema.
     * @param appliedMetadataKeys the metadata keys applied by Flink, in declared order.
     * @return a (MetadataAppender, physicalProjection) pair. {@code physicalProjection} is {@code
     *     projectedFields} unchanged.
     */
    public static Result plan(
            int[] projectedFields, int physicalArity, List<String> appliedMetadataKeys) {
        int physicalCount = (projectedFields == null) ? physicalArity : projectedFields.length;
        int totalArity = physicalCount + appliedMetadataKeys.size();
        int[] outputPlan = new int[totalArity];
        for (int i = 0; i < physicalCount; i++) {
            outputPlan[i] = i;
        }
        for (int j = 0; j < appliedMetadataKeys.size(); j++) {
            outputPlan[physicalCount + j] = sentinelFor(appliedMetadataKeys.get(j));
        }
        return new Result(new MetadataAppender(outputPlan), projectedFields);
    }

    private static int sentinelFor(String key) {
        if (BUCKET_KEY.equals(key)) {
            return BUCKET_SLOT;
        } else if (OFFSET_KEY.equals(key)) {
            return OFFSET_SLOT;
        } else {
            throw new IllegalArgumentException("Unknown metadata key: " + key);
        }
    }

    /** Result of {@link #plan(int[], int, List)}. */
    public static final class Result {
        public final MetadataAppender appender;
        public final int[] physicalProjection;

        Result(MetadataAppender appender, int[] physicalProjection) {
            this.appender = appender;
            this.physicalProjection = physicalProjection;
        }
    }
}
