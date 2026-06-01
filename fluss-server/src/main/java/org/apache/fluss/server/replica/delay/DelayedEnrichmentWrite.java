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

package org.apache.fluss.server.replica.delay;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.utils.types.Tuple2;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Delayed operation that completes when the per-(bucket, group) Committed Enrichment Watermark
 * (CEW) reaches the required offset, mirroring {@link DelayedWrite} for the enrichment path. Used
 * by {@code ProduceLogColumns} with {@code acks=-1} so the response only fires once the ISR has
 * acknowledged the enrichment write.
 */
public class DelayedEnrichmentWrite extends DelayedOperation {

    private final String columnGroup;
    private final Map<TableBucket, BucketState> bucketStateMap;
    private final ReplicaManager replicaManager;
    private final Consumer<Map<TableBucket, BucketResult>> callback;

    public DelayedEnrichmentWrite(
            long delayMs,
            String columnGroup,
            Map<TableBucket, BucketState> bucketStateMap,
            ReplicaManager replicaManager,
            Consumer<Map<TableBucket, BucketResult>> callback) {
        super(delayMs);
        this.columnGroup = columnGroup;
        this.bucketStateMap = bucketStateMap;
        this.replicaManager = replicaManager;
        this.callback = callback;
        updateStatus();
    }

    private void updateStatus() {
        bucketStateMap.forEach(
                (tb, state) -> {
                    if (state.localError == null) {
                        // Local write succeeded; we need to wait for CEW to catch up.
                        state.acksPending = true;
                        state.delayedError = Errors.REQUEST_TIME_OUT;
                    } else {
                        // Local write already failed; the response carries the error as-is.
                        state.acksPending = false;
                    }
                });
    }

    @Override
    public boolean tryComplete() {
        boolean allSatisfied = true;
        for (Map.Entry<TableBucket, BucketState> entry : bucketStateMap.entrySet()) {
            TableBucket tb = entry.getKey();
            BucketState state = entry.getValue();
            if (state.acksPending) {
                Tuple2<Boolean, Errors> result;
                try {
                    Replica replica = replicaManager.getReplicaOrException(tb);
                    result = replica.checkEnoughReplicasReachEwm(columnGroup, state.requiredEwm);
                } catch (Exception e) {
                    result = Tuple2.of(false, Errors.forException(e));
                }
                Errors err = result.f1;
                if (err != Errors.NONE || result.f0) {
                    state.acksPending = false;
                    state.delayedError = err;
                }
                if (state.acksPending) {
                    allSatisfied = false;
                }
            }
        }
        return allSatisfied && forceComplete();
    }

    @Override
    public void onExpiration() {
        // Bucket-level REQUEST_TIME_OUT is already set in updateStatus; nothing extra here.
    }

    @Override
    public void onComplete() {
        Map<TableBucket, BucketResult> results = new HashMap<>();
        bucketStateMap.forEach(
                (tb, state) -> {
                    if (state.localError != null) {
                        results.put(
                                tb, BucketResult.failure(state.localError, state.localErrorMsg));
                    } else if (state.delayedError != null && state.delayedError != Errors.NONE) {
                        results.put(tb, BucketResult.failure(state.delayedError, null));
                    } else {
                        results.put(tb, BucketResult.success(state.requiredEwm));
                    }
                });
        callback.accept(results);
    }

    /** Per-bucket mutable state tracked while the delayed operation is pending. */
    public static final class BucketState {
        private final long requiredEwm;
        private final @Nullable Errors localError;
        private final @Nullable String localErrorMsg;
        private volatile boolean acksPending;
        private volatile @Nullable Errors delayedError;

        public static BucketState localSuccess(long requiredEwm) {
            return new BucketState(requiredEwm, null, null);
        }

        public static BucketState localFailure(Errors error, @Nullable String message) {
            return new BucketState(0L, error, message);
        }

        private BucketState(
                long requiredEwm, @Nullable Errors localError, @Nullable String localErrorMsg) {
            this.requiredEwm = requiredEwm;
            this.localError = localError;
            this.localErrorMsg = localErrorMsg;
        }

        public long getRequiredEwm() {
            return requiredEwm;
        }

        public @Nullable Errors getLocalError() {
            return localError;
        }

        public @Nullable String getLocalErrorMsg() {
            return localErrorMsg;
        }
    }

    /** Per-bucket final result delivered to the response callback. */
    public static final class BucketResult {
        private final @Nullable Errors error;
        private final @Nullable String message;
        private final long ewm;

        public static BucketResult success(long ewm) {
            return new BucketResult(null, null, ewm);
        }

        public static BucketResult failure(Errors error, @Nullable String message) {
            return new BucketResult(error, message, 0L);
        }

        private BucketResult(@Nullable Errors error, @Nullable String message, long ewm) {
            this.error = error;
            this.message = message;
            this.ewm = ewm;
        }

        public @Nullable Errors getError() {
            return error;
        }

        public @Nullable String getMessage() {
            return message;
        }

        public long getEwm() {
            return ewm;
        }
    }
}
