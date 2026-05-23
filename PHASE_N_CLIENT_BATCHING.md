# Phase N — Client-side enrichment-write batching (Approach B)

**Status:** Design.
**Authors:** option02-lateMaterialized branch.
**Depends on:** Phase B+ (PRODUCE_LOG_COLUMNS RPC), Phase C (Arrow encoding),
Phase E (replication / EWM), Phase L+M (Flink connector — primary
consumer).

## 1. Context

Today `AppendWriter.appendColumns(group, bucket, sourceOffset,
enrichmentRow)` is single-row: each call constructs a fresh
`BufferAllocator` + `ArrowWriterPool`, encodes one row, builds one
`ProduceLogColumnsRequest`, and awaits the response. The wire protocol
is multi-row capable (one Arrow `MemoryLogRecords` with parallel
`source_offsets`, and one request can fan out across many buckets that
share a leader), but the client never exercises that.

For high-throughput enrichment writes — Flink streaming jobs at
1k+ rows/sec/partition, bulk backfills via the Java client — the
per-row RPC overhead is meaningful: allocator churn, network
round-trips, server-side request handling per row.

Phase N closes this gap by introducing a client-side accumulator
parallel to the existing 981-line `RecordAccumulator` for base
appends. Single-row calls accumulate transparently into multi-row
RPCs grouped by `(table, column_group, leader-node)`.

## 2. Goals & non-goals

### Goals

1. `AppendWriter.appendColumns(...)` remains a single-row API — no
   caller changes required.
2. Multi-row RPCs are produced automatically when the call rate
   exceeds the linger window or the in-flight batch grows past the
   configured size.
3. Throughput on a typical Flink-sink workload (1k+ rows/sec) is
   bounded by the network and server, not by per-row RPC overhead.
4. Correctness: per-row futures complete exactly when the
   server-acknowledged batch containing that row succeeds (or fails
   with that row's error).
5. No proto / server changes — the wire shape from Phase B+ supports
   this directly.

### Non-goals

- **Cross-group batching in a single RPC.** The proto carries one
  `column_group` per request; batching across groups would require a
  wire change. In practice groups have different Arrow schemas and
  rarely share write traffic.
- **Per-row idempotence / producer-id semantics.** Enrichment writes
  are addressed by `(bucket, source_offset, group)`; duplicates are
  server-rejected as "out of EWM order." No new dedup needed at the
  client.
- **Reordering across rows.** The server requires `source_offsets`
  inside one request to be strictly monotonic and contiguous from the
  current EWM. The accumulator preserves insertion order and lets the
  server validate; sort-on-batch would mask user errors.
- **Per-record retry policy.** Re-use whatever Phase B+'s
  single-row path does today (or doesn't). Batching neither helps
  nor hurts the retry story; treat as out of scope for N.
- **Sharing the existing `RecordAccumulator` directly.** Base-append
  accumulator is intertwined with `IdempotenceManager`,
  `WriteBatch` interface, partition/bucket distribution logic, and a
  `BufferPool` sized to base-append workloads. Forking is faster
  than retrofitting.

## 3. Architecture

### 3.1 Component overview

```
AppendWriterImpl.appendColumns(group, bucket, offset, row)
         |
         v
   WriterClient.appendColumns(...)
         |
         v
   EnrichmentAccumulator.append(key, offset, row, future)   ← per-row enqueue
         |                                                     returns CompletableFuture
         |    accumulates by EnrichmentBatchKey(table_id, group, table_bucket)
         |
   (linger or size threshold reached, or flush()/close())
         |
         v
   EnrichmentSender thread                                   ← drains per-leader-node
         |
         v
   TabletServerGateway.produceLogColumns(request)             ← one RPC, N rows, M buckets
         |
         v
   futures completed on response                              ← per-row results fan out
```

### 3.2 EnrichmentBatchKey

```java
final class EnrichmentBatchKey {
    final long tableId;
    final String columnGroup;
    final TableBucket bucket;       // includes partition_id
    // equals/hashCode on all three
}
```

A batch is the unit of accumulation. One batch holds many
`(source_offset, InternalRow, CompletableFuture)` tuples for one key.

### 3.3 EnrichmentWriteBatch

```java
final class EnrichmentWriteBatch {
    final EnrichmentBatchKey key;
    final List<EnrichmentEntry> entries;       // append-only
    final long createdMs;                       // for linger
    int sizeInBytes;                            // running estimate

    // built on first append, reused across appends to the same batch
    transient MemoryLogRecordsArrowBuilder arrowBuilder;
    transient ArrowWriter arrowWriter;
    transient PreAllocatedPagedOutputView outputView;

    boolean isFull(int batchSize);
    boolean isLingerExpired(long nowMs, int lingerMs);
    BytesView build();                          // close + return encoded bytes
    long[] sourceOffsets();
    void completeWithSuccess(long ewm);         // fans out to per-row futures
    void completeWithError(Throwable t);
}
```

Each entry carries:

```java
record EnrichmentEntry(long sourceOffset, InternalRow row,
                       CompletableFuture<AppendColumnsResult> future) {}
```

(In Java 8 syntax: a small static class with the three fields and
no methods.)

The batch's per-row futures are completed by `completeWithSuccess` —
all of them get the same `AppendColumnsResult.enrichmentWatermark`
(the server's post-put EWM after applying the whole batch). This
matches the semantics of the single-row API today, which always
returns the post-put EWM.

### 3.4 EnrichmentAccumulator

```java
@Internal
final class EnrichmentAccumulator {
    final int batchSize;        // bytes; default 64 KiB
    final int lingerMs;         // default 20 ms

    // per-key queue of batches: the head is currently appending; once it's
    // full or linger-expired, it moves to incomplete and a new head is
    // created on the next append.
    final ConcurrentMap<EnrichmentBatchKey, Deque<EnrichmentWriteBatch>> batchesByKey;
    final IncompleteBatches incomplete;
    final ArrowWriterPool arrowWriterPool;          // shared across all keys
    final BufferAllocator bufferAllocator;          // shared

    CompletableFuture<AppendColumnsResult> append(
            EnrichmentBatchKey key, long offset, InternalRow row);

    // sender-side methods
    Set<Integer> readyNodes(MetadataUpdater meta, long nowMs);
    Map<Integer, List<EnrichmentWriteBatch>> drain(
            MetadataUpdater meta, Set<Integer> nodes, int maxSize, long nowMs);

    CompletableFuture<Void> flushAll();
    void close();
}
```

Behavior:

- `append`: get/create the head batch for `key`; if the head can't
  accept the row (full or sealed by a prior drain), create a new head.
  Encode the row into the batch's Arrow builder. Register the entry.
  Return its future.
- `readyNodes`: walk `batchesByKey`; for each key, determine its
  leader via `MetadataUpdater.leaderFor(table, bucket)` (the
  accumulator's metadata view). A node is "ready" if any of its keys
  has a full or linger-expired batch (or `flushAll` was called).
- `drain(node, ...)`: for each key whose leader is `node`, pull
  ready batches off the queue and return them grouped by node. Caller
  is the sender thread.

### 3.5 EnrichmentSender (per-WriterClient daemon thread)

```java
final class EnrichmentSender implements Runnable {
    public void run() {
        while (!closed) {
            long nowMs = clock.milliseconds();
            Set<Integer> nodes = accumulator.readyNodes(meta, nowMs);
            if (nodes.isEmpty()) {
                accumulator.waitForReadyOrTimeout(lingerMs);
                continue;
            }
            Map<Integer, List<EnrichmentWriteBatch>> drained =
                    accumulator.drain(meta, nodes, maxRequestSize, nowMs);
            for (Map.Entry<Integer, List<EnrichmentWriteBatch>> e : drained.entrySet()) {
                sendOneRequestPerLeader(e.getKey(), e.getValue());
            }
        }
    }

    private void sendOneRequestPerLeader(int nodeId, List<EnrichmentWriteBatch> batches) {
        // Group batches by (table, group) — proto carries one column_group per request.
        // Within a (table, group) group, all batches' buckets fit into one ProduceLogColumnsRequest.
        ...
        gateway.produceLogColumns(request).whenComplete((resp, err) -> {
            if (err != null) {
                for (var b : batches) b.completeWithError(err);
            } else {
                // Map response back to batches by bucket; complete each.
                ...
            }
        });
    }
}
```

One thread per `WriterClient` (singleton via `Connection` is fine
because all writers in a connection share). Reuses
`TabletServerGateway` infrastructure for cluster discovery.

### 3.6 WriterClient integration

`WriterClient` already owns the base `RecordAccumulator` and Sender.
Phase N adds:

```java
class WriterClient {
    // existing
    final RecordAccumulator accumulator;
    final Sender sender;

    // new
    final EnrichmentAccumulator enrichmentAccumulator;
    final EnrichmentSender enrichmentSender;

    CompletableFuture<AppendColumnsResult> appendColumns(
            EnrichmentBatchKey key, long offset, InternalRow row);
}
```

`AppendWriterImpl.appendColumns` becomes a 3-line delegate to
`writerClient.appendColumns(...)`. All the Arrow-encoding /
allocator-management code at lines 187–230 is deleted.

### 3.7 Memory + backpressure

The base writer uses `LazyMemorySegmentPool` to bound write buffer
memory. For enrichment writes, the volume is bounded by
`batches × batchSize ≈ in-flight memory`. Defaults: 64 KiB batches
× O(100) in-flight = 6.4 MiB per writer. Acceptable without a
dedicated pool.

If pressure becomes an issue later, we can add a
`maxInFlightBytes` knob that blocks `append` until in-flight memory
falls below the threshold — same pattern as `RecordAccumulator`'s
`BufferPool.allocate()`.

## 4. Decisions

| # | Decision | Why |
|---|---|---|
| N.1 | Fork from `RecordAccumulator`, don't retrofit | Base-append accumulator's idempotence + partition/bucket distribution logic doesn't apply; cleaner to write a focused ~250-line class |
| N.2 | One batch per `(table, group, bucket)` | Matches the proto's per-bucket entry shape; one bucket per batch keeps the source_offsets array contiguous |
| N.3 | Per-row futures share the batch's post-put EWM | Same semantics as single-row API today |
| N.4 | Preserve insertion order in a batch; let server validate offset ordering | "Don't auto-sort" — surfaces caller errors loudly instead of masking them |
| N.5 | Per-request scope: one `(table, column_group)`; can pack multiple buckets if they share a leader | Already wire-supported; matches base RecordAccumulator's per-leader drain |
| N.6 | Shared `ArrowWriterPool` + `BufferAllocator` across all enrichment keys for a `WriterClient` | One pool, many keys — same as base accumulator. Pool key: `tableId ^ group.hashCode()` (mirrors the existing `AppendWriterImpl.appendColumns` formula) |
| N.7 | Defaults: `batchSize = 64 KiB`, `lingerMs = 20` | Comparable to base-append defaults (16 KiB / 0 ms today, with linger=0 disabled by default). Enrichment writes are higher-latency-tolerant — favor batching |
| N.8 | Configurable via `client.enrichment-writer.batch-size` and `client.enrichment-writer.linger-ms` | Parallel naming to existing `CLIENT_WRITER_BATCH_SIZE` etc. |
| N.9 | One `EnrichmentSender` thread per `WriterClient` | Mirrors the base `Sender` thread. Don't share the thread with base appends — they have different ready-policies |

## 5. Phasing

```
N.1  ADR (this doc)                                  ← current
N.2  EnrichmentWriteBatch + EnrichmentAccumulator    TBD
     (skeleton, no networking, unit tests)
     - append/drain/ready/flushAll/close
     - Unit tests for batch full / linger / drain
N.3  EnrichmentSender + WriterClient integration     ⚠ → N.3a (see below)
     - EnrichmentSender daemon thread implemented:
       leader resolution, drain → request → response
       fan-out, sealed-batch future completion.
     - WriterClient.newEnrichmentAccumulator factory
       wires the shared MemorySegmentPool,
       ArrowWriterPool, BufferAllocator, and
       DynamicWriteBatchSizeEstimator into a new
       accumulator scoped per (table, group).
     - RecordAccumulator exposes package-private
       getters for these shared resources.
N.3a Pivot to concatenated single-row batches       ✓ DONE (2026-05-23, superseded by N.3b)
     First fix: ship N single-row Arrow batches per
     RPC, server walks records.batches() and indexes
     each one's distinct position. Wire/disk footprint
     ~50%-3x larger than necessary for narrow rows;
     Arrow compression effectively disabled per batch.
     testEnrichmentSinkViaSql passed (21.2s).
N.3b True multi-row decode in the merger             ✓ DONE (2026-05-23)
     Replaces N.3a's wire shape with one multi-row
     Arrow batch per RPC. Storage layer reverts to
     one Arrow batch per call; index gains
     intra-batch row addressing.
     - EnrichmentSegment.append: per-ROW dense
       indexing where rows in the same Arrow batch
       share the batch's file position. Handles
       BOTH wire formats (N single-row batches OR
       1 N-row batch) uniformly.
     - EnrichmentSegment.lookupBatch(sourceOffset)
       returns (position, intraIndex). intraIndex is
       recovered by walking backward in OffsetIndex
       while position stays equal. O(intraIndex)
       mmap reads — proportional to the row's
       position within its batch.
     - EnrichmentMerger.lookupEnrichment uses the
       intraIndex; the "Multi-row enrichment batches
       are not yet supported (Phase D)" guard is
       gone. GroupDecoder.readColumnByName accepts
       a rowIndex parameter and advances the batch
       iterator to that row.
     - EnrichmentSegment.range rounds slices to
       whole-batch boundaries (defensive — followers
       always start at EWM which is batch-aligned).
     - EnrichmentWriteBatch reverts to a single
       Arrow builder accumulating N rows; seal
       produces ONE multi-row batch.
     - Outcome: wire size minimized (1 header per
       batch, not per row), Arrow compression
       effective again, encoding CPU lower (1
       writer borrow per batch, not per row).
     - testEnrichmentSinkViaSql passes (23.2s);
       new LogTabletTest case asserts per-row
       lookup correctness on a single multi-row
       batch (5 rows, shared file position,
       distinct intra-indices).
N.4  Configuration: reuse base-writer settings       ✓ DONE (2026-05-23)
     Decision: no new enrichment-specific keys. The
     accumulator already shares the underlying
     MemorySegmentPool, ArrowWriterPool, and
     DynamicWriteBatchSizeEstimator with base
     appends, so it shares the tuning too.
     - WriterClient.newEnrichmentAccumulator reads
       client.writer.batch-size,
       client.writer.batch-timeout, and
       client.writer.buffer-page-size from its
       Configuration.
     - The 16 KB / 5 ms / 16 KB hardcoded constants
       in AppendWriterImpl are gone; defaults now
       come from base-writer settings (2 MB / 100 ms
       / 64 KB).
     - Tradeoff: 100 ms linger is generous for
       enrichment workloads where latency matters
       (CEW gates downstream materialization). If
       this becomes painful, add an override later.
N.5  Batching efficiency tests                       ✓ DONE (2026-05-23)
     - manySmallRowsMergeIntoFewBatches: 50 small
       rows merge into ≤ 2 sealed batches. The
       central claim of Phase N — many appendColumns
       calls produce FAR fewer RPCs than rows —
       is asserted directly.
     - sizeBasedSealStartsNewBatch: 1000 rows of
       larger payload force the head batch to seal
       at the size limit and a new batch to start.
       Verifies the size-based seal path with
       contiguous source-offset assignment.
     - Latent bug found and fixed:
       EnrichmentWriteBatch.tryAppend was only
       checking the cumulative-size threshold; it
       missed the Arrow writer's INTERNAL write
       limit (~95% of bufferSize). With production
       defaults (batch-size=2 MB, buffer-page-size=
       128 KB), the writer would fill at ~121 KB
       and the next writeRow would throw. Fix is to
       also check builder.isFull() — matches what
       ArrowLogWriteBatch does on the base path.
     - Skipped: a separate JMH benchmark — the
       e2e ITCase plus these unit tests are the
       regression guard; JMH would add maintenance
       cost without proportional ROI.
```

N.2 + N.3 are the bulk of the work and are tightly coupled. N.4 is
trivial. N.5 wraps up.

## 6. Open questions

### 6.1 Ordering across batches for the same key

If batches A and B both target `(table, group, bucket)`, with A
created first carrying offsets [10..20] and B created after with
[21..30], can the sender send B before A?

The server rejects non-contiguous offsets, so out-of-order delivery
would fail. The accumulator's per-key Deque is FIFO and the sender
drains in order, so this naturally preserves order — but only if
batches are sent serially per key. If the sender fan-outs requests
to one leader in parallel, a network reorder could land B before A.

Mitigation: `EnrichmentSender` sends one request per leader at a
time (or per `(leader, key)` pair). Same as base writer's
in-flight-requests-per-bucket=1 semantics for non-idempotent
producers. Simple and correct.

### 6.2 Flush semantics for Flink sink integration

Today's `EnrichmentSinkWriter.flush(endOfInput)` calls
`appendWriter.flush()` (the base-append flush). After Phase N,
`appendWriter.flush()` should also drain pending enrichment batches.
Two options:

- **A**: extend `AppendWriter.flush()` to drain both base + enrichment.
  Single method, but mixes concerns.
- **B**: add `AppendWriter.flushEnrichment()` and call both from the
  sink writer.

Lean **A** — `flush()` is the natural choke point and conceptually
should drain *all* pending writes.

### 6.3 Backpressure boundary

If a Flink job overshoots enrichment write rate, the accumulator
fills up faster than the sender drains. Without a memory bound,
this OOMs. Options:

- **Soft bound**: track in-flight bytes; block `append` past the
  threshold (same as `RecordAccumulator.BufferPool`).
- **Hard bound**: drop or fail above the threshold.
- **No bound** (initial implementation): document that Flink's
  natural backpressure (await of the returned future) is the only
  guard.

Phase N starts with no-bound (simpler). Phase N follow-up adds a soft
bound if needed.

### 6.4 Per-row vs. per-batch failure

If the server fails an offset mid-batch (e.g., offset 15 doesn't
match the EWM=14), should the *whole batch* fail or only that one
row?

The proto `PbProduceLogColumnsRespForBucket` carries one error_code
per bucket, not per row. So the server already imposes per-bucket
all-or-nothing. The accumulator must complete all per-row futures
in a batch with the same outcome.

Acceptable — matches the proto's resolution.

### 6.5 Should the sender be a CompletableFuture-driven loop or a thread?

Base `Sender` is a thread. For consistency and ease of reasoning,
Phase N's `EnrichmentSender` is also a thread. CompletableFuture
chaining would need a scheduler anyway.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Ordering violation if leader changes mid-flight | N.6 §6.1 — one in-flight request per `(leader, key)` |
| Memory growth under unbounded backpressure | N.6 §6.3 — start with no bound; document. Add soft bound if reported |
| Per-row futures completed on the sender thread cause downstream stalls | Sender completes futures via `CompletableFuture.completeAsync(...)` with a small worker pool — but that adds threads. Start simple: complete on sender thread; if profiling shows it's a bottleneck, switch to async |
| Behavior change for existing test code | Single-row append tests stay valid; the accumulator preserves single-row semantics when load is low. Existing ITCases that don't measure RPC count should keep passing |
| Linger introduces latency for low-rate workloads | Linger default = 20 ms is short enough not to be perceived; existing single-row callers (one-shot ITCases) won't notice. Configurable to 0 for latency-sensitive use |
| Network reorder of consecutive requests on the same leader (rare) | N.6 §6.1 — one in-flight request per `(leader, key)` prevents this from translating into out-of-order server-side application |

## 8. Test strategy

### N.2 — unit tests on EnrichmentAccumulator

- `appendThenDrainProducesOneBatch`: 5 appends to one key, drain →
  exactly 1 batch with 5 entries.
- `batchFullTriggersNewHead`: append until full; 6th append goes to
  a new batch.
- `lingerTriggersReady`: append, wait `lingerMs`, `readyNodes`
  returns the node.
- `flushAllForcesReady`: regardless of linger / size, `flushAll`
  marks everything ready.
- `crossKeyIsolation`: appends to two different keys don't share a
  batch.
- `completeWithErrorFansOutToFutures`: simulate a server error;
  every per-row future completes exceptionally.

### N.3 — integration tests

- `enrichmentBatchesReduceRpcCount`: send 100 single-row
  `appendColumns` calls in quick succession; assert that the count
  of `PRODUCE_LOG_COLUMNS` RPCs received at the server is ≪ 100
  (target: < 10 with default linger). Implementation: use a
  counted gateway wrapper or a server-side request counter via
  `TabletService` metrics.
- `existingSingleRowITCasesStillPass`: run all existing
  `ColumnGroupEWMITCase` / `PaimonTieringITCase` /
  `FlinkTableSourceITCase` tests; verify no regression. Most should
  pass without changes because batching is transparent.
- `flinkSinkAtCheckpointDrains`: a Flink sink writes N rows between
  checkpoints; on the checkpoint barrier, all rows are flushed.
  Assertion: post-checkpoint CEW matches N.

### N.5 — performance smoke (optional)

- `enrichmentThroughputUnderBatching`: write 10K rows from a single
  thread via `appendColumns`; measure wall-clock. Compare with a
  no-batch baseline branch (one-RPC-per-row). Expect ≥ 5x
  improvement.

## 9. Implementation sketch (N.2 starting point)

```java
// fluss-client/.../write/EnrichmentBatchKey.java
public final class EnrichmentBatchKey {
    private final long tableId;
    private final String columnGroup;
    private final TableBucket bucket;
    // equals, hashCode, getters
}

// fluss-client/.../write/EnrichmentWriteBatch.java
public final class EnrichmentWriteBatch {
    private final EnrichmentBatchKey key;
    private final int schemaId;
    private final RowType groupRowType;
    private final ArrowWriter writer;
    private final MemoryLogRecordsArrowBuilder builder;
    private final List<EnrichmentEntry> entries = new ArrayList<>();
    private final long createdMs;

    public synchronized boolean tryAppend(long offset, InternalRow row,
                                          CompletableFuture<AppendColumnsResult> future,
                                          int sizeLimit) {
        if (closed) return false;
        if (builder.estimatedSizeInBytes() >= sizeLimit && !entries.isEmpty()) {
            return false;
        }
        builder.append(ChangeType.APPEND_ONLY, row);
        entries.add(new EnrichmentEntry(offset, future));
        return true;
    }

    public synchronized BuiltBatch sealAndBuild() {
        if (closed) throw new IllegalStateException("already built");
        closed = true;
        return new BuiltBatch(key, builder.build(),
                              entries.stream().mapToLong(e -> e.sourceOffset).toArray(),
                              entries.stream().map(e -> e.future).collect(toList()));
    }
}
```

`EnrichmentAccumulator.append` ↔ `EnrichmentWriteBatch.tryAppend`
returns false → caller creates a new batch and retries. Same
pattern as `RecordAccumulator`.

## 10. Out of scope (deferred)

- Cross-group batching (proto would change).
- Per-row partial-failure handling (proto would change).
- Memory bound on the accumulator (Phase N follow-up).
- Async future completion off the sender thread (Phase N follow-up
  if profiling shows it's needed).
- Per-table tuning of linger / batch-size (use connection-level
  defaults for now).
