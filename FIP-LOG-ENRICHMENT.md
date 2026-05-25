# FIP: Log Enrichment via Append Columns

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
today's **High Watermark (HWM)** ceiling unchanged. Replication, tiering, and lake
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

With this proposal, the enrichment job writes enrichment columns back to
the **same** log table A via `appendColumns`; there is no log table B:

```
edge device ──▶ log table A ──▶ consumer
                  │     ▲
       scan base  │     │  appendColumns
                  ▼     │
                Flink job
                (enrichment)
```

Base columns are stored once and enrichment columns are stored once.
Consumers project whatever they need against the single table:
projections that touch only base columns advance up to `HWM` as today;
projections that touch enrichment column group `g` are clamped at
`min(HWM, EWM_g)`, so partial rows are never observed and the system —
not the consumer — owns the "is enrichment caught up?" decision.

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

### Non-goals

- KV / primary-key tables. Column groups are log-only by design.
- "Best-effort" or buffered enrichment modes that surface partial rows. This
  FIP commits to the strict-only consumer model — see *Rejected
  alternatives*.
- Cross-table joins or general projection materialisation; this is
  same-table column extension only.
- Materialised table integration can be a follow-up FIP once this
  feature reaches maturity.

---

## Public interfaces

### 1. Java client API

Three surfaces: `Schema.Builder` declares the table shape with named
column groups, `AppendWriter.appendColumns` writes enrichment at
existing source offsets, and `LogScanner` reads merged rows under the
server-side EWM gate.

**Defining a table.** `Schema.Column` gains an optional `columnGroup`
field; `Schema.Builder.columnGroup(name, block)` scopes the columns
added inside the lambda to a named group. From
`fluss-common/src/main/java/org/apache/fluss/metadata/Schema.java`:

```java
public final class Schema {
    public static Builder newBuilder();

    /** Returns groupName -> ordered list of column indices in that group. */
    public Map<String, List<Integer>> getColumnGroups();
    public Set<String> getColumnGroupNames();
    public int[]       getDefaultGroupColumnIndices();

    public static final class Builder {
        public Builder column(String name, DataType dataType);

        /** Every column added inside {@code block} joins {@code groupName}. */
        public Builder columnGroup(String groupName, Consumer<Builder> block);

        // ... primaryKey(), withComment(), build() ...
    }
}
```

**Writing enrichment.** A single-row, leader-routed RPC. After the put
succeeds, the per-bucket EWM advances to the highest contiguous offset
filled (starting from 0). From
`fluss-client/.../client/table/writer/AppendWriter.java`:

```java
@PublicEvolving
public interface AppendWriter extends TableWriter {

    CompletableFuture<AppendResult> append(InternalRow record);

    /** Write enrichment columns for the given group at an existing source offset. */
    CompletableFuture<AppendColumnsResult> appendColumns(
            String columnGroup, TableBucket bucket, long sourceOffset, InternalRow enrichmentRow);
}
```

**Reading.** The standard `Table.newScan()` → `LogScanner` works
unchanged; scans whose projection touches column group `g` are clamped
server-side at `min(HWM, EWM_g)` and the merge happens inside
`EnrichmentMerger` (see *Design § Read path*).

**End-to-end example.** Java analogue of Section 3's SQL pattern
`INSERT INTO sink SELECT _bucket, _offset, geo_lookup(ip) FROM device_logs`:
declare the table, write base rows, scan, derive an enrichment from
each row's `ip`, and call `appendColumns` at the scanned offset.
Exercised by `ColumnGroupEWMITCase#testEnrichFromScannedBaseColumn`; a
multi-group variant lives in `testAppendColumnsTwoGroupsExample`.

```java
// 1. Define the table.
Schema schema = Schema.newBuilder()
        .column("device_id", DataTypes.STRING())
        .column("ip",        DataTypes.STRING())
        .column("payload",   DataTypes.STRING())
        .columnGroup("enriched_geo", g ->
                g.column("geo_region", DataTypes.STRING()))
        .build();

TablePath tablePath = TablePath.of("mydb", "device_logs");

try (Connection conn = ConnectionFactory.createConnection(flussConfig)) {
    // 2. Create it.
    try (Admin admin = conn.getAdmin()) {
        admin.createTable(
                        tablePath,
                        TableDescriptor.builder().schema(schema).distributedBy(1).build(),
                        /* ignoreIfExists */ false)
                .get();
    }

    try (Table table = conn.getTable(tablePath)) {
        AppendWriter writer = table.newAppend().createWriter();
        TableBucket bucket  = new TableBucket(table.getTableInfo().getTableId(), 0);

        // 3. Base writes — enrichment column NULL.
        for (int i = 0; i < 4; i++) {
            writer.append(GenericRow.of(
                            BinaryString.fromString("dev-" + i),
                            BinaryString.fromString("10.0.0." + i),
                            BinaryString.fromString("login"),
                            /* geo_region */ null))
                    .get();
        }

        // 4. Scan base rows, derive enrichment from each row's ip, call
        //    appendColumns at the scanned offset (SQL Section 3 in Java).
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(/* bucketId */ 0);
            int delivered = 0;
            while (delivered < 4) {
                for (ScanRecord r : scanner.poll(Duration.ofSeconds(1))) {
                    String ip = r.getRow().getString(1).toString();
                    writer.appendColumns(
                                    "enriched_geo",
                                    bucket,
                                    r.logOffset(),
                                    GenericRow.of(BinaryString.fromString("GEO:" + ip)))
                            .get();
                    delivered++;
                }
            }
        }

        // 5. Read back. Projecting geo_region (idx 3) clamps reads at EWM_geo
        //    and splices in the values written above via merge-on-read.
        int[] projection = new int[] {0, 1, 3};   // device_id, ip, geo_region
        try (LogScanner enriched =
                table.newScan().project(projection).createLogScanner()) {
            enriched.subscribeFromBeginning(/* bucketId */ 0);
            for (ScanRecord r : enriched.poll(Duration.ofSeconds(1))) {
                // r.getRow().getString(2).equals("GEO:" + r.getRow().getString(1));
            }
        }
    }
}
```

### 2. SQL DDL — declare column groups

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
filled later by Section 3's write-only sink:

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

### 3. SQL DDL — write-only enrichment-target table

To write enrichment via Flink SQL, users define a *write-only* table whose
row shape mirrors the base table's addressing — `(BIGINT src_bucket,
BIGINT src_offset, <group columns...>)` for unpartitioned bases, or
`(STRING src_partition, BIGINT src_bucket, BIGINT src_offset, ...)` for
partitioned bases (as below) — and points at the base table. The source
SELECT reads the base row's `_partition` / `_bucket` / `_offset` from the
virtual METADATA columns declared on the base table in Section 2:

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
FROM device_logs;
```

**Caveat — the SELECT must reference a base column.** A literal-only
enrichment expression (e.g. replacing `geo_lookup(ip)` with the constant
`'London'`) silently emits zero rows: with no base column in the
projection, the connector falls back to a full-column fetch, and the
server clamps that at `min(HWM, EWM_g) = 0` via the EWM gate. Any
base-column reference in the expression — `ip` inside `geo_lookup(ip)`
above — is enough to push down a narrow projection that bypasses the
gate.

Selecting from `device_logs_geo_sink` is rejected at plan time — the table
is a sink only. Schema and field types are validated against the base
table's column-group definition when the job starts; mismatch fails fast
with a `ValidationException`.

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

### 5. Error codes

Four new entries in `org.apache.fluss.rpc.protocol.Errors` cover the
enrichment-path failures; each maps to a typed `ApiException` subclass
in `fluss-common/.../exception/`. All four are **non-retriable** — they
indicate either a stale client view or a permanent condition, never a
transient RPC failure.

| Code | Name | Exception | Thrown when |
|------|------|-----------|-------------|
| 66 | `INVALID_COLUMN_GROUP_OFFSET` | `InvalidColumnGroupOffsetException` | `appendColumns` `source_offset` doesn't equal `EWM + 1` for the bucket (gap or already-filled) |
| 67 | `COLUMN_GROUP_SOURCE_OFFSET_TRUNCATED` | `ColumnGroupSourceOffsetTruncatedException` | `appendColumns` `source_offset` is below the bucket's local log start; the base segment is no longer locally present (e.g. already tiered to remote, or chopped by a head-truncation). With the tier-eligibility gate active, this should not arise in normal operation — it's a defensive check |
| 68 | `UNKNOWN_COLUMN_GROUP` | `UnknownColumnGroupException` | `appendColumns` or fetch projection names a group not declared on the table |
| 69 | `INVALID_COLUMN_GROUP_CONFIG` | `InvalidColumnGroupConfigException` | `CREATE TABLE` validation: partition-key column declared inside a `column-groups.<g>` property, group references an unknown column, duplicated group membership, group paired with primary key |

**Client handling.** The three runtime-path codes have distinct
recovery shapes:

- **`INVALID_COLUMN_GROUP_OFFSET`** is the only one where the client
  can auto-recover. The server populates
  `PbProduceLogColumnsRespForBucket.enrichment_watermark` with the
  actual EWM on this error path, so `EnrichmentAccumulator` refreshes
  its local EWM cache for `(bucket, group)` and drops in-flight
  batches whose first offset is now stale. It does **not** auto-retry
  the same RPC — a permanent gap (base offset that was never written)
  would otherwise loop.
- **`COLUMN_GROUP_SOURCE_OFFSET_TRUNCATED`** is terminal for that row.
  `EnrichmentAccumulator` fails the corresponding
  `CompletableFuture<AppendColumnsResult>` and continues. The Flink
  enrichment sink maps this to a `enrichment.skipped.truncated`
  metric increment rather than a job failure, so an enrichment job
  re-encountering an offset that's no longer locally available (e.g.
  after a tier-eligibility-gate bypass or head truncation) doesn't
  crash the job.
- **`UNKNOWN_COLUMN_GROUP`** is detected client-side by `AppendWriterImpl`
  (synchronous throw before the RPC) when the schema cache shows no
  such group, and again server-side by `Replica.appendColumnsAsLeader`
  if the client view is stale. The client refreshes `TableInfo` once;
  if the group is still absent, the original message is surfaced to
  the caller unchanged.

`INVALID_COLUMN_GROUP_CONFIG` is a `CREATE TABLE` validation failure;
the table is never created and the client surfaces the descriptive
error message verbatim.

---

## Design

### Storage layer

**On-disk layout.** Each `(bucket, column-group)` gets its own segment
file alongside the base log:

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

The per-group `.col.<group>.log` is a `FileLogRecords` of Arrow batches;
the matching `.index` is a standard sparse `OffsetIndex` (8-byte entries).

`EnrichmentSegment` is the per-(bucket, group) storage primitive. Append
walks the incoming Arrow batches and indexes each row's source offset at
its batch's file position. Multiple rows in one Arrow batch share the
batch's position; the merger recovers the intra-batch row index at read
time. From
`fluss-server/src/main/java/org/apache/fluss/server/log/EnrichmentSegment.java`:

```java
final class EnrichmentSegment implements Closeable {
    private final FileLogRecords fileLogRecords;
    private final OffsetIndex offsetIndex;
    // ... ctor / open / close omitted

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
public final class EnrichmentMerger implements AutoCloseable {
    // ... fields / ctor / close omitted

    /** Re-encode baseRecords with enrichment columns sourced from per-group segments. */
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
        return wrote ? new BytesViewLogRecords(outputBuilder.build()) : MemoryLogRecords.EMPTY;
    }

    private BytesView mergeBatch(
            LogRecordBatch batch, Map<String, EnrichmentSegment> enrichmentSegments)
            throws Exception {
        // ... allocate Arrow writer + paged output sized for this batch
        try (MemoryLogRecordsArrowBuilder builder = /* ... */ null) {
            int rowCount = 0;
            try (CloseableIterator<LogRecord> rowIter = batch.records(baseReadContext)) {
                while (rowIter.hasNext()) {
                    LogRecord baseRecord = rowIter.next();
                    long sourceOffset    = baseRecord.logOffset();
                    InternalRow baseRow  = baseRecord.getRow();
                    GenericRow projected = new GenericRow(projectedFields.length);
                    for (int p = 0; p < projectedFields.length; p++) {
                        EnrichmentColumnRef ref = enrichmentRefs[p];
                        if (ref != null) {
                            EnrichmentSegment seg = enrichmentSegments.get(ref.groupName);
                            projected.setField(p, lookupEnrichment(seg, ref, sourceOffset));
                        } else {
                            int colIdx = projectedFields[p];
                            projected.setField(
                                    p, tableFieldGetters[colIdx].getFieldOrNull(baseRow));
                        }
                    }
                    builder.append(baseRecord.getChangeType(), projected);
                    rowCount++;
                }
            }
            return rowCount == 0 ? null : builder.build();
        }
    }

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
}
```

The merger is invoked when the fetch's projection touches at least one
enrichment column. Pure-base fetches skip it entirely.

### Replication and durability

Two existing server classes pick up the new column-group surface:

- **`LogTablet`** gains per-group state — one `EnrichmentSegment` per
  declared group plus per-group local-EWM and CEW maps — and the methods
  that operate on it (leader/follower append, follower fetch, EWM/CEW
  accessors).
- **`Replica`** owns the enforcement layer: strict-from-EWM ordering on
  writes, CEW advancement once writes are durable on the ISR, and the
  read-side fetch gate (fetches whose projection touches column group
  `g` are clamped at `min(HWM, CEW_g)` for each touched `g`).

```java
public final class LogTablet {
    // ... existing log / HWM state omitted

    public void appendColumnsAsLeader(
            String groupName, MemoryLogRecords records, long[] sourceOffsets)
            throws IOException;

    public void appendColumnsAsFollower(
            String groupName, MemoryLogRecords records, long[] sourceOffsets)
            throws IOException;

    public EnrichmentReadResult readEnrichmentForFollower(
            String groupName, long fromInclusive, long toExclusive, int maxBytes)
            throws IOException;

    public long getEnrichmentWatermark(String groupName);
    public long getCommittedEnrichmentWatermark(String groupName);
    public void updateCommittedEnrichmentWatermark(String groupName, long newCew);
}

public final class Replica {
    // ... existing replica state omitted

    /** Validate strict-from-EWM ordering, then persist via LogTablet. */
    public long appendColumnsAsLeader(
            String columnGroup, MemoryLogRecords records, long[] sourceOffsets);

    /** CEW_g = min(EWM_g across ISR ∪ {leader}); called when a follower acks. */
    private void maybeAdvanceCEW(String groupName);
}
```

Followers replicate enrichment via a parallel fetch stream:
`LogTablet.readEnrichmentForFollower` returns zero-copy slices rounded to
whole-batch boundaries. The leader tracks each follower's per-group
"enrichment cursor" alongside the existing log fetch position.

The *Committed Enrichment Watermark (CEW)* is the per-group analogue of the
high watermark: `CEW_g = min(EWM_g across ISR ∪ {leader})`. Reads are gated
at the **CEW**, not the local EWM, so a survivor of leader failover never
reveals enrichment that wasn't durably replicated. From
`Replica#maybeAdvanceCEW`:

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

For a plain log table, `lakeLogEndOffset ≤ HWM`; for a column-group
table, `lakeLogEndOffset ≤ CEW ≤ HWM`. A column-group table is
structurally less timely in the lake than a plain log table by the
CEW-to-HWM gap — the same staleness clients experience inside Fluss,
just acknowledged on the tiering path.

Key design choices:

- **Server-side merge, not client-side.** The tiering job sets a full
  projection at scanner-construction time and reads through the
  existing fetch path, which routes through `EnrichmentMerger` on the
  `Replica`. The tiering job stays ignorant of enrichment segment
  layout and CEW gating — no separate logic to keep in sync.
- **Tiering uses CEW, not local EWM.** Lake commits should survive a
  clean Fluss leader failover; tiering at local EWM could leave the
  lake disagreeing with the new leader's view (phantom rows in the
  lake).
- **No new RPC.** Fetch already supports projection; the tiering split
  generator caps `stoppingOffset` at CEW for column-group tables.
- **LakeWriter implementations unchanged.** Iceberg / Paimon / Lance
  writers receive `LogRecord` instances and never inspect provenance —
  merged rows look identical to plain log rows.

### Client-side batching

`appendColumns` calls do not directly issue RPCs. They feed a per-(table,
group, bucket) `EnrichmentAccumulator`, modelled on Fluss's existing
`RecordAccumulator` for base appends. A daemon sender thread drains ready
batches and dispatches `ProduceLogColumns` RPCs. The accumulator shares
the writer's `MemorySegmentPool`, `ArrowWriterPool`, and dynamic
batch-size estimator with base appends, and inherits the writer's
`client.writer.batch-size` / `client.writer.batch-timeout` /
`client.writer.buffer-page-size` settings.

```java
public final class EnrichmentAccumulator {
    // ... fields / ctor / pool + estimator deps omitted

    /** Enqueue a row for {@code key}; future completes when the server acks the batch. */
    public CompletableFuture<AppendColumnsResult> append(
            EnrichmentBatchKey key, long sourceOffset, InternalRow row, long nowMs);

    /** True if some batch is full, has lingered past timeout, or flushAll() was called. */
    public boolean hasReady(long nowMs);

    /** Seal and return all ready batches, grouped per (table, group, bucket) key. */
    public Map<EnrichmentBatchKey, List<EnrichmentWriteBatch.SealedBatch>> drainReady(long nowMs)
            throws Exception;

    /** Mark all in-flight batches as ready on the next drain. */
    public void flushAll();

    public void close();
}
```

### Flink SQL integration

Three new classes and three modifications cover the Flink-SQL surface:

**New:**

- **`EnrichmentTableSink`** — the write-only `DynamicTableSink`
  activated by `enrichment.target` / `enrichment.group`. Plan-time
  validation of the sink row shape against the base table's
  column-group definition; SELECT against the table is rejected.
- **`EnrichmentSinkWriter`** (with **`EnrichmentSink`** as the thin
  `Sink<RowData>` shell) — the Sink V2 runtime that unpacks
  `(src_partition, src_bucket, src_offset, <group cols>)` from each
  incoming `RowData`, resolves partition name → partition ID via a
  cached `Admin.listPartitionInfos` lookup, and calls
  `appendWriter.appendColumns(...)` per row — which feeds the
  `EnrichmentAccumulator` above.
- **`MetadataAppender`** — splices per-row `bucket` / `offset` /
  `partition` values into the produced `RowData` when those METADATA
  columns are declared on the base table. Load-bearing for the
  Section 3 pattern of `SELECT _partition, _bucket, _offset, ...
  FROM base`.

**Modified:**

- **`FlinkTableFactory`** — recognizes `enrichment.target` and
  dispatches to `EnrichmentTableSink`; rejects SELECT on
  enrichment-target tables at plan time.
- **`FlinkConversions`** — `parseColumnGroups` translates
  `column-groups.<g>` DDL options into `Schema.Builder.columnGroup`
  calls; the read path re-synthesizes them so `SHOW CREATE TABLE`
  round-trips losslessly.
- **`FlinkTableSource`** — `listReadableMetadata()` advertises
  `bucket`, `offset`, `partition`; the per-row appender is the new
  `MetadataAppender`.

Key signatures, by class:

```java
// EnrichmentTableSink: plan-time validation + runtime sink wiring.
public class EnrichmentTableSink implements DynamicTableSink {
    public EnrichmentTableSink(
            TablePath targetTablePath, String groupName, Configuration flussConfig, /* ... */);

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        ResolvedTarget resolved = resolveTarget();   // plan-time schema check
        EnrichmentSink sink = new EnrichmentSink(
                targetTablePath, flussConfig, resolved.tableId, groupName,
                resolved.enrichmentValueRowType, resolved.partitioned);
        return (providerContext, dataStream) ->
                dataStream.sinkTo(sink)
                          .name("EnrichmentSink(" + targetTablePath + "/" + groupName + ")");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode);  // insert-only
}

// EnrichmentSink: thin Sink<RowData> shell that creates the per-task writer.
public class EnrichmentSink extends SinkAdapter<RowData> {
    public EnrichmentSink(
            TablePath tablePath, Configuration flussConfig, long tableId,
            String groupName, RowType enrichmentValueRowType, boolean partitioned);
}

// EnrichmentSinkWriter: per-task runtime that drives appendColumns.
public class EnrichmentSinkWriter implements SinkWriter<RowData> {
    /** Unpack (src_partition, src_bucket, src_offset, vals); call AppendWriter.appendColumns. */
    @Override
    public void write(RowData row, Context context) throws IOException;

    @Override
    public void flush(boolean endOfInput) throws IOException;

    @Override
    public void close() throws Exception;
}

// MetadataAppender: source-side splicer for bucket / offset / partition METADATA columns.
public class MetadataAppender implements Serializable {
    public static final String BUCKET_KEY    = "bucket";
    public static final String OFFSET_KEY    = "offset";
    public static final String PARTITION_KEY = "partition";

    /** Build an appender plan from the user's METADATA-column declarations. */
    public static Result plan(/* ... */);

    /** Per-row splice into the appender's metadata slots. */
    public RowData splice(
            RowData physicalRow, int bucket, long offset, @Nullable String partitionName);
}

// FlinkTableFactory: dispatch on `enrichment.target`.
public class FlinkTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    /** Returns an EnrichmentTableSink when `enrichment.target` is set; otherwise the
     *  standard FlinkTableSink. SELECT on an enrichment-target table is rejected. */
    @Override
    public DynamicTableSink createDynamicTableSink(Context context);
}

// FlinkConversions: DDL <-> schema column-group translation.
public final class FlinkConversions {
    /** Parse `column-groups.<g> = col1, col2, ...` DDL options into a column → group map. */
    private static Map<String, String> parseColumnGroups(
            Configuration flinkTableConf,
            Set<String> physicalColumnNames,
            boolean hasPrimaryKey,
            List<String> partitionKeys);
    // Read path (toFlinkTable) re-synthesizes the same options from the Schema's
    // column-group metadata so SHOW CREATE TABLE round-trips losslessly.
}

// FlinkTableSource: advertise bucket / offset / partition as METADATA-readable columns.
public class FlinkTableSource implements ScanTableSource, SupportsReadingMetadata /* ... */ {
    @Override
    public Map<String, DataType> listReadableMetadata();   // bucket, offset, partition

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType);
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
