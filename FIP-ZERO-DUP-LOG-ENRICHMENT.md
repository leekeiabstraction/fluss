# FIP: Zero-Duplication Log Enrichment via Append Columns

| Field    | Value |
|----------|-------|
| Status   | Proposed |
| Author   | Keith Lee |
| Created  | 2026-05-23 |
| Target   | TBD |
| Discussion thread | TBD |
| JIRA / GitHub issue | TBD |

---

## Abstract

This FIP proposes **column groups** for Apache Fluss log tables: a subset of a
table's columns can be declared "enrichment-only" and written *later* by
appending columns at existing source offsets. A new RPC, `ProduceLogColumns`,
carries the enrichment writes; the server stores each group in its own
segment file alongside the base log and maintains a per-bucket per-group
**Enrichment Watermark (EWM)** that tracks progress. Reads that project
enrichment columns are server-side-gated at `min(HWM, EWM_g for all groups
in projection)`, so consumers never see partial rows; pure-base reads keep
today's `HWM` ceiling unchanged. Replication, tiering, and lake
materialisation share the same gate, with a time-bounded escape valve to
prevent local disk overflow if enrichment falls behind. A client-side
accumulator batches enrichment writes so the wire cost is amortised across
many rows.

The mechanism eliminates the storage and I/O duplication that today's "raw
table → enrichment job → enriched table" pattern incurs, while preserving
Fluss's existing log-table guarantees (offset monotonicity, replication,
tiering, exactly-once writes) for the enrichment path.

---

## Motivation

A common Fluss enrichment pipeline today looks like:

```
edge device ──▶ log table A ──▶ Flink job ──▶ log table B ──▶ consumer
                                (enrichment)
```

Log table B's row is `A.* ⊕ enrichment_columns`. If A has 50 columns and the
enrichment adds 3, the 50 base columns are stored twice — once in A, once in
B. At scale this is real operational and infrastructure cost — 2× storage,
2× write I/O, 2× fetch I/O for any downstream that consumes B — plus the
additional architectural complexity of running and maintaining a separate
enrichment pipeline.

Workarounds outside Fluss either reintroduce duplication elsewhere or push
correctness concerns onto consumers:

- **[Overlay tables](https://hudi.apache.org/docs/table_types/)** — a side
  table keyed by the base offset, holding only the enrichment columns;
  readers merge the two at query time (the pattern Apache Hudi formalises
  as Merge-on-Read). No base-column duplication, but the system has no
  notion of "is the overlay caught up?" — the consumer must handle
  missing or stale rows.
- **[Application-side joins](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/#lookup-join)**
  — enrichment lives in a separate system (KV store, lookup table) and
  every consumer fetches per row; Flink's *lookup join* is the canonical
  form. Moves the storage cost out of the log but pushes the "is
  enrichment current?" question onto every consumer.
- **[Materialised views](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/materialized-table/overview/)**
  — express the join in SQL; the engine recomputes and stores the result
  as a new table (Flink's *Materialized Table* is the streaming-era
  instance, with declarative freshness). Mechanically this is still
  "log table B" wrapped in a SQL definition — base columns are
  physically duplicated, and the view lags real-time writes by the
  configured freshness.

We want a first-class primitive that lets the **same row** gain columns over
time, with the system enforcing "completeness" at the read boundary.

### Goals

1. **Zero storage duplication.** Base and enrichment columns share offsets;
   each is stored exactly once.
2. **Strict read consistency.** A consumer that projects an enrichment column
   never sees `NULL` or a partial row for that column unless the column is
   genuinely null in the data.
3. **Composability.** Multiple independent enrichment jobs can fill different
   column groups on the same base table; each advances its own EWM.
4. **Tiering & lake parity.** Remote segments and lake tables contain the
   enriched data once enrichment catches up.
5. **Operational safety.** Stalled enrichment must not silently corrupt
   downstream views or fill local disk forever.

### Non-goals

- KV / primary-key tables. Column groups are log-only by design.
- "Best-effort" or buffered enrichment modes that surface partial rows. This
  FIP commits to the strict-only consumer model — see *Rejected
  alternatives*.
- Cross-table joins or general projection materialisation; this is
  same-table column extension only.

---

## Public interfaces

### 1. SQL DDL — declare column groups

Column groups are declared via table-level `column-groups.<groupName>`
properties in the `WITH` clause; each property's value is a comma-separated
list of columns that belong to that group. Enrichment columns are typically
trailing nullable columns; the base writer leaves them NULL until the
enrichment job fills them.

```sql
CREATE TABLE device_logs (
  dt          STRING,
  device_id   STRING,
  ip          STRING,
  payload     STRING,
  -- Enrichment columns, written later via separate Flink jobs
  geo_region           STRING,
  risk_score           DOUBLE,
  risk_classification  STRING,
  -- Virtual metadata columns, surfaced by Fluss for use in enrichment SELECTs.
  _partition  STRING METADATA FROM 'partition' VIRTUAL,
  _bucket     BIGINT METADATA FROM 'bucket'    VIRTUAL,
  _offset     BIGINT METADATA FROM 'offset'    VIRTUAL
) PARTITIONED BY (dt) WITH (
  'bucket.num'                     = '16',
  'bucket.key'                     = 'device_id',
  'table.auto-partition.enabled'   = 'true',
  'table.auto-partition.time-unit' = 'DAY',
  'column-groups.enriched_geo'     = 'geo_region',
  'column-groups.enriched_risk'    = 'risk_score, risk_classification'
);
```

Multiple groups can coexist on one table; each group advances independently.
Partitioning is supported under one structural rule: **partition-key
columns must remain in the default group** — they must be set at base-row
insertion, while enrichment columns are populated later via separate
`appendColumns` writes. Including a partition-key column in any
`column-groups.<g>` value is rejected at create time.

The `column-groups.<g>` keys are internalized into the schema at create
time, validated, and re-emitted by `SHOW CREATE TABLE` for round-trip
fidelity.

Base-row writes are ordinary `INSERT` statements that target every
column; enrichment columns are explicitly `NULL` at this point and get
filled later by Section 2's write-only sink:

```sql
INSERT INTO device_logs VALUES
  ('20260525', 'dev-001', '10.0.0.1', 'login',  NULL, NULL, NULL),
  ('20260525', 'dev-002', '10.0.0.2', 'logout', NULL, NULL, NULL),
  ('20260525', 'dev-003', '10.0.0.3', 'login',  NULL, NULL, NULL);
```

The base log's `HWM` advances as usual. Each column group's `EWM` stays
at 0 until its enrichment job begins writing; until then a query that
projects an enrichment column returns no rows because reads are gated
at `min(HWM, EWM_g for groups in projection)`.

### 2. SQL DDL — write-only enrichment-target table

To write enrichment via Flink SQL, users define a *write-only* table whose
row shape mirrors the base table's addressing — `(BIGINT src_bucket,
BIGINT src_offset, <group columns...>)` for unpartitioned bases, or
`(STRING src_partition, BIGINT src_bucket, BIGINT src_offset, ...)` for
partitioned bases (as below) — and points at the base table. The source
SELECT reads the base row's `_partition` / `_bucket` / `_offset` from the
virtual METADATA columns declared on the base table in Section 1:

```sql
CREATE TABLE device_logs_geo_sink (
  src_partition  STRING,
  src_bucket     BIGINT,
  src_offset     BIGINT,
  geo_region     STRING
) WITH (
  'connector'         = 'fluss',
  'enrichment.target' = 'mydb.device_logs',
  'enrichment.group'  = 'enriched_geo'
);

INSERT INTO device_logs_geo_sink
SELECT _partition, _bucket, _offset, geo_lookup(ip)
FROM device_logs
WHERE geo_region IS NULL;
```

Selecting from `device_logs_geo_sink` is rejected at plan time — the table
is a sink only. Schema and field types are validated against the base
table's column-group definition when the job starts; mismatch fails fast
with a `ValidationException`.

### 3. Java client API — `AppendWriter.appendColumns`

From `fluss-client/src/main/java/org/apache/fluss/client/table/writer/AppendWriter.java`:

```java
@PublicEvolving
public interface AppendWriter extends TableWriter {

    /** Append a record into a Log Table. */
    CompletableFuture<AppendResult> append(InternalRow record);

    /**
     * Write enrichment columns for the given column group at an existing source offset on a
     * specific bucket. The enrichment row must contain only the columns of the named column
     * group, in the order they appear in the table schema.
     *
     * <p>After the put succeeds, the per-bucket enrichment watermark advances to the highest
     * contiguous offset that has been filled (starting from 0).
     */
    CompletableFuture<AppendColumnsResult> appendColumns(
            String columnGroup,
            TableBucket  bucket,
            long         sourceOffset,
            InternalRow  enrichmentRow);
}
```

Client batching, leader resolution, retries, and back-pressure are
transparent to the caller (see *Design § Client-side batching* below).

### 4. RPC — `ProduceLogColumns`

A new RPC carries multi-bucket, multi-row enrichment writes. Defined in
`fluss-rpc/src/main/proto/FlussApi.proto`:

```proto
message ProduceLogColumnsRequest {
  required int64  table_id     = 1;
  required string column_group = 2;
  repeated PbProduceLogColumnsReqForBucket buckets_req = 3;
}

message PbProduceLogColumnsReqForBucket {
  optional int64 partition_id   = 1;
  required int32 bucket_id      = 2;
  // MemoryLogRecords (ARROW format) containing the enrichment rows for this column
  // group, one row per source_offset entry. The Arrow batch's RowType is the column
  // group's projected RowType (group columns in their schema-defined order).
  required bytes records        = 3;
  // Base-log offsets being enriched, parallel to the rows in `records`.
  // Strictly monotonic, contiguous from the current EWM.
  repeated int64 source_offsets = 4 [packed = true];
}

message PbProduceLogColumnsRespForBucket {
  optional int64 partition_id          = 1;
  required int32 bucket_id             = 2;
  optional int32 error_code            = 3;
  optional string error_message        = 4;
  // Enrichment watermark for the column group on this bucket after the put.
  optional int64 enrichment_watermark  = 5;
}
```

`FetchLogRequest` / `FetchLogResponse` are unchanged on the wire — the
existing `projected_fields` is enough for the server to decide the read
gate. (We *do* surface per-group EWMs on the response for observability;
the field is additive and optional.)

### 5. On-disk layout

Each `(bucket, column-group)` gets its own segment file alongside the base
log:

```
{dataDir}/{tablePath}/{bucketId}/
  00000000000000000000.log                       <-- base log
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000000000000.col.enriched_geo.log      <-- column group 'enriched_geo'
  00000000000000000000.col.enriched_geo.index
  00000000000000000000.col.enriched_risk.log     <-- column group 'enriched_risk'
  00000000000000000000.col.enriched_risk.index
```

The per-group `.col.<group>.log` is a `FileLogRecords` of Arrow batches; the
matching `.index` is a standard sparse `OffsetIndex` (8-byte entries).

### 6. Schema metadata API

`Schema.Column` gains an optional `columnGroup` field (existing API,
nullable). Convenience accessors on `Schema`:

```java
public final class Schema {
    /** Returns groupName -> ordered list of column indices in that group. */
    public Map<String, List<Integer>> getColumnGroups();

    public Set<String>            getColumnGroupNames();
    public List<Integer>          getDefaultGroupColumnIndices();
}
```

---

## Design

### Storage layer

`EnrichmentSegment` is the per-(bucket, group) storage primitive. Append
walks the incoming Arrow batches and indexes each row's source offset at
its batch's file position. Multiple rows in one Arrow batch share the
batch's position; the merger recovers the intra-batch row index at read
time. From
`fluss-server/src/main/java/org/apache/fluss/server/log/EnrichmentSegment.java`:

```java
int append(MemoryLogRecords records, long[] sourceOffsets) throws IOException {
    int basePosition = fileLogRecords.sizeInBytes();
    int written      = fileLogRecords.append(records);
    if (written == 0 && records.sizeInBytes() > 0) {
        throw new IOException("FileLogRecords.append wrote 0 bytes ...");
    }
    int rowIdx        = 0;
    int relativeOffset = 0;
    for (LogRecordBatch batch : records.batches()) {
        int batchPosition = basePosition + relativeOffset;
        int recordCount   = batch.getRecordCount();
        for (int i = 0; i < recordCount; i++) {
            offsetIndex.append(sourceOffsets[rowIdx], batchPosition);
            rowIdx++;
        }
        relativeOffset += batch.sizeInBytes();
    }
    // ... validate counts, return basePosition
}

/**
 * Look up the file position AND intra-batch row index of the given source offset.
 * The intra-batch index is recovered by walking backward in OffsetIndex while
 * `position` stays equal — rows in the same Arrow batch all share the batch's
 * file position.
 */
BatchSlot lookupBatch(long sourceOffset) {
    if (offsetIndex.entries() == 0) return null;
    OffsetPosition pos = offsetIndex.lookup(sourceOffset);
    if (pos.getOffset() != sourceOffset) return null;
    int slot       = Math.toIntExact(sourceOffset);
    int position   = pos.getPosition();
    int intraIndex = 0;
    while (slot > 0) {
        OffsetPosition prev = offsetIndex.entry(slot - 1);
        if (prev.getPosition() != position) break;
        intraIndex++;
        slot--;
    }
    return new BatchSlot(position, intraIndex);
}
```

The accumulator on the client packs N rows into one multi-row Arrow batch
per RPC — see *§ Client-side batching*. The server-side append code works
identically for single-row and multi-row wire payloads.

### Read path — merge-on-read

`EnrichmentMerger` re-encodes a base Arrow batch with enrichment columns
sourced from the per-group segments. Each requested column-group cell is
extracted using `lookupBatch`'s `intraIndex`. From
`fluss-server/src/main/java/org/apache/fluss/server/log/EnrichmentMerger.java`:

```java
private Object lookupEnrichment(
        EnrichmentSegment seg, EnrichmentColumnRef ref, long sourceOffset)
        throws IOException {
    EnrichmentSegment.BatchSlot slot = seg.lookupBatch(sourceOffset);
    if (slot == null) {
        // The EWM gate ensures we never reach here for offsets the merger asked for.
        return null;
    }
    AbstractIterator<FileLogInputStream.FileChannelLogRecordBatch> iter =
            seg.records().batchIterator(slot.position, seg.records().sizeInBytes());
    if (!iter.hasNext()) return null;
    FileLogInputStream.FileChannelLogRecordBatch batch = iter.next();
    if (slot.intraIndex >= batch.getRecordCount()) {
        throw new IllegalStateException("Intra-batch index " + slot.intraIndex + " ...");
    }
    GroupDecoder decoder = getOrBuildDecoder(ref.groupName, batch.schemaId());
    return decoder.readColumnByName(batch, ref.columnName, slot.intraIndex);
}
```

The merger is invoked when the fetch's projection touches at least one
enrichment column. Pure-base fetches skip it entirely.

### EWM gate

`LogTablet` tracks per-group EWM. `appendColumnsAsLeader` enforces strict
contiguous-from-EWM writes:

```java
long expected = Math.max(0L, currentEwm);
for (int i = 0; i < sourceOffsets.length; i++) {
    long actual = sourceOffsets[i];
    if (actual != expected) {
        throw new IllegalArgumentException(
            "Out-of-order column-group write for bucket %s group '%s': "
          + "expected source_offset %d (next slot after EWM=%d) but got %d at index %d");
    }
    // (also: actual < localLogStart → reject; actual >= localLogEnd → reject)
    expected++;
}
// ... persist, advance EWM, attempt CEW advance
```

Read-side: the server computes the *projected* EWM bound as
`min(EWM_g for each g touched by the projection)` and clamps the fetch
ceiling at `min(HWM, projected_EWM)`. The implementation lives in the same
`Replica.fetchRecords` path that already enforces HWM.

### Replication and durability

Followers replicate enrichment via a parallel fetch stream:
`EnrichmentSegment.range(fromInclusive, toExclusive, maxBytes)` returns
zero-copy slices rounded to whole-batch boundaries. The leader tracks a
follower's per-group "enrichment cursor" alongside the existing log fetch
position.

The *Committed Enrichment Watermark (CEW)* is the per-group analogue of the
high watermark: `CEW_g = min(EWM_g across ISR ∪ {leader})`. Reads are gated
at the **CEW**, not the local EWM, so a survivor surviving leader failover
never reveals enrichment that wasn't durably replicated. From
`fluss-server/.../replica/Replica.java#maybeAdvanceCEW`:

```java
private void maybeAdvanceCEW(String groupName) {
    if (isUnderMinIsr()) return;
    long leaderEwm = logTablet.getEnrichmentWatermark(groupName);
    long newCew    = leaderEwm;
    for (FollowerReplica r : followerReplicasMap.values()) {
        if (!isrState.maximalIsr().contains(r.getFollowerId())) continue;
        Long followerEwm = followerEwmByGroup.get(groupName).get(r.getFollowerId());
        if (followerEwm == null) return;        // ISR member hasn't reported yet
        if (followerEwm < newCew) newCew = followerEwm;
    }
    logTablet.updateCommittedEnrichmentWatermark(groupName, newCew);
}
```

### Tiering and lake materialisation

Segment tier-eligibility extends the existing HWM gate to require all
column groups filled:

```
tierEligible(segment) =
    segment.isRolled()
 && for all groups g declared on the table: CEW_g >= segment.lastOffset()
```

A configurable escape valve (`log.tiering.enrichment-wait-timeout`, default
30 min) ships a *base-only* upload after the timeout to prevent local disk
filling forever; the manifest entry is marked `INCOMPLETE`, and a later
backfill writes a companion enrichment file. Background compaction merges
the companion back into a single file. Lake materialisation reuses the
same gate; partition-level Iceberg/Paimon files appear once their
contained offsets are CEW-covered.

### Client-side batching (Phase N)

`appendColumns` calls do not directly issue RPCs. They feed a per-(table,
group, bucket) `EnrichmentAccumulator`, modelled on Fluss's existing
`RecordAccumulator` for base appends. A daemon sender thread drains ready
batches and dispatches `ProduceLogColumns` RPCs. The accumulator shares
the writer's `MemorySegmentPool`, `ArrowWriterPool`, and dynamic
batch-size estimator with base appends, and inherits the writer's
`client.writer.batch-size` / `client.writer.batch-timeout` /
`client.writer.buffer-page-size` settings. From
`fluss-client/src/main/java/org/apache/fluss/client/write/EnrichmentRouter.java`:

```java
public synchronized EnrichmentAccumulator getOrCreateAccumulator(
        String columnGroup, EnrichmentAccumulator.BatchEncoderInfo encoderInfo) {
    if (closed) throw new IllegalStateException(...);
    EnrichmentAccumulator existing = accumulatorsByGroup.get(columnGroup);
    if (existing != null) return existing;
    EnrichmentAccumulator fresh =
            writerClient.newEnrichmentAccumulator(tablePath, encoderInfo);
    accumulatorsByGroup.put(columnGroup, fresh);
    ensureSenderStarted();   // start the daemon sender on first use
    return fresh;
}
```

### Flink SQL integration

`EnrichmentTableSink` is a write-only `DynamicTableSink` activated by the
`enrichment.target` / `enrichment.group` table properties. At plan time it
resolves the target schema, validates that the sink row matches
`(src_bucket BIGINT, src_offset BIGINT, <group cols>)` (or with a leading
`src_partition STRING` for partitioned tables), and at runtime the sink
calls `appendWriter.appendColumns(...)` per input row — which feeds the
accumulator above.

```java
public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    ResolvedTarget resolved = resolveTarget();      // plan-time schema check
    EnrichmentSink sink = new EnrichmentSink(
            targetTablePath, flussConfig, resolved.tableId, groupName,
            resolved.enrichmentValueRowType, resolved.partitioned);
    return (providerContext, dataStream) ->
            dataStream.sinkTo(sink)
                      .name("EnrichmentSink(" + targetTablePath + "/" + groupName + ")");
}
```

---

## Compatibility, deprecation, migration

- **Wire-compatible.** `ProduceLogColumns` is a new RPC; existing tables and
  clients are unaffected. `FetchLogResponse` gains optional EWM fields —
  unset on old servers, ignored by old clients.
- **Schema-compatible.** `column_group` is a new optional field on
  `Schema.Column` and round-trips through JSON serde. Existing tables have
  zero columns in any group and behave identically.
- **No migration tooling needed.** A table only uses column groups if the
  user declares them in the DDL.
- **Tiering & lake catalogs.** New tables with column groups can be created
  in existing clusters; old tables continue to work.
- **Java client semver.** `AppendWriter.appendColumns` is added as
  `@PublicEvolving`. Its absence on older clients means they simply cannot
  perform enrichment writes.

---

## Test plan

Implemented as part of the POC; concrete suites that ship as regression
guards:

| Layer | Tests |
|-------|-------|
| Schema | JSON round-trip, builder validation, group lookup helpers |
| Storage | `EnrichmentSegment` per-row index + multi-row batch decode (`LogTabletTest`) |
| EWM contract | Out-of-order rejection, EWM advance, CEW alignment under ISR change (`LogTabletTest`, `ReplicaTest`) |
| Replication | `FollowerEnrichmentReplicationITCase` covers leader → follower enrichment fetch + apply, recovery |
| Read gate | Fetch with mixed projection clamps at `min(HWM, EWM_g)`; pure-base reads unaffected |
| Tiering | Per-group tier-eligibility + escape valve + backfill |
| Flink SQL | DDL parse, `enrichment.target` / `enrichment.group` validation, write-only enforcement |
| Flink runtime | `testEnrichmentSinkViaSql` end-to-end: insert via SQL, read back enriched rows |
| Client batching | 50-row → 1-batch merge, 1000-row → multiple-batch split, contiguous source offsets across batches |
| Latent fix | Arrow writer `isFull()` is consulted in `tryAppend` (matches base writer) |

The complete ITCase + unit-test count from the POC is in the order of
~50–60 dedicated tests across `fluss-server` and `fluss-client`, plus the
Flink connector ITCases.

---

## Rejected alternatives

These were explored in the original design doc (see `README.md`,
*Options Explored*); the FIP commits to *Option 2*. Brief summaries:

- **Option 1 — Column groups, no EWM.** Writes columns at existing
  offsets, but lacks a system-level notion of "complete." Consumers must
  reason about NULLs as either "no value" or "pending"; tiering can't
  decide what to upload. Strictly weaker than Option 2.

- **Option 3 — Segment-level column append.** Add columns only at segment
  granularity. Simpler implementation, but loses row-level enrichment —
  unsuitable for the row-by-row enrichment pattern that motivated the
  proposal.

- **Option 4 — Overlay table with offset reference.** Keep enrichment in a
  separate table that references base offsets; consumer-side join. No
  storage duplication of base columns, but reintroduces offset duplication
  and pushes correctness onto consumers; same operational complexity as
  today's two-table pipeline.

- **Buffered / "best-effort" consumer modes.** Allowing reads past EWM
  with NULL placeholders for unfilled enrichment. Rejected because it
  silently weakens the consumer contract (a partial row looks like a
  fully-null one) and forces every consumer to re-implement the
  "is this NULL meaningful?" decision. The strict EWM gate keeps the
  contract simple — *if a column is in the projection, the value is the
  value that was written.*

- **Enrichment-specific batching configs.** Adding
  `client.writer.enrichment.*` keys parallel to the base writer. Rejected
  because the accumulator shares the *same* memory pool / Arrow encoder
  pool / dynamic estimator with base appends; tuning twice was pure
  surface area. A separate override can be added later if a real
  latency/throughput conflict surfaces.

- **Per-column inline `COLUMN GROUP` DDL clause.** Considered as a more
  ergonomic alternative to the table-level `column-groups.<g>` property
  (e.g. `geo_region STRING COLUMN GROUP 'enriched_geo'`), but would
  require extending Flink's Calcite parser — a meaningfully larger,
  Fluss-external change. Possible future ergonomic work once the
  property form is stable.

---

## References

- `README.md` — original POC design context (Parts 1–3): problem,
  alternatives, and Option 2 design.
- `PHASE_E_REPLICATION.md` — leader/follower enrichment replication, EWM
  durability, CEW.
- `PHASE_F_LAKE_TIERING.md` — tier-eligibility gate and escape valve.
- `PHASE_H_SCHEMA_EVOLUTION.md` — interaction with schema evolution.
- `PHASE_I_FLINK_CONNECTOR.md` — Flink source-side projection and EWM.
- `PHASE_K_FLINK_DDL_COLUMN_GROUPS.md` — DDL extension for column groups.
- `PHASE_L_FLINK_ENRICHMENT_WRITES.md` — `enrichment.target` /
  `enrichment.group` write-only sink.
- `PHASE_M_PARTITIONED_COLUMN_GROUPS.md` — partitioned base tables.
- `PHASE_N_CLIENT_BATCHING.md` — `EnrichmentAccumulator` /
  `EnrichmentSender` / `EnrichmentRouter`, the multi-row Arrow decode
  pivot, and the shared-config decision.
