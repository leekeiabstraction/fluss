# Phase F — Lake tiering for enrichment columns

**Status:** F.1 → F.6 implemented and verified end-to-end against
Paimon, Iceberg, and Lance. F.6 is this doc; see §5 for landed commits.
**Authors:** option02-lateMaterialized branch.
**Depends on:** Phase E (durable enrichment replication; CEW-bound read gate).

## 1. Context

Phase E made enrichment durable across ISR. Reads inside Fluss now return
fully-materialized rows up to CEW (committed enrichment watermark). What
they don't yet do is flow to the lake.

Today's tiering job (`fluss-flink-tiering/TieringSplitReader`) reads from
Fluss via `table.newScan().createLogScanner()` with **no projection**. For
a column-group table that means the server returns raw base-log records —
which physically don't carry the enrichment columns, since enrichment lives
in separate `EnrichmentSegment` files. The lake therefore receives rows
whose enrichment columns are missing or null. The data exists; the
materialization step is just absent in the tiering path.

## 2. Goals & non-goals

### Goals

1. A column-group table tiered to Iceberg/Paimon/Lance contains the
   **full materialized row** (base columns + every column-group column).
2. Tiering offset tracking respects CEW: the tiered range never claims
   coverage past what's durable across ISR. A reader of the lake sees the
   same set of rows a Fluss client would see at the same point in time
   (modulo tiering lag).
3. Re-uses the Phase D `EnrichmentMerger` so we don't duplicate the
   base-plus-enrichment splice logic. Server-side merge, lake-writer-side
   ignorance.
4. No regression for tables without column groups — the tiering pipeline
   for plain log tables stays byte-identical.

### Non-goals

- Storing enrichment in the lake as a separate physical structure
  (e.g. dedicated enrichment file per group). The lake gets a single
  flat row-major file per snapshot, same shape as today.
- Lake-side query gating. Once data is in the lake, it's the lake's
  durability and the lake's reader that govern visibility. Fluss only
  controls what gets in.
- Schema evolution at the lake layer for column-group changes — gated
  on the same `alterTable`-targets-column-groups work that blocks the
  E.7 schema-evolution ITCase.
- KV / primary-key tables. Phase D and Phase E both scoped to log tables;
  Phase F follows.

## 3. Architecture

### 3.1 Path comparison

```
TODAY (column-group table, plain log table identical):
  TieringSplitReader.newScan()  →  LogScanner (no projection)
       ↓
  Server.fetch  →  raw base-log records
       ↓
  LakeWriter.write(LogRecord)  →  Parquet/Avro file in lake
       ↓
  Result: column-group columns missing from lake.

PHASE F (column-group tables only):
  TieringSplitReader detects column groups in table schema
       ↓
  newScan().project(<all columns>)  →  LogScanner with full-row projection
       ↓
  Server.fetch  →  EnrichmentMerger applies, returns merged rows up to CEW
       ↓
  LakeWriter.write(LogRecord)  →  Parquet/Avro file (full materialization)
       ↓
  Result: lake has the same row image a Fluss client sees.
```

### 3.2 Why all-column projection (not no-projection)

The merger gate at `Replica.computeEffectiveEnrichmentCap` only fires
when `projectionTouchesEnrichment(...)` is true. Without a projection,
`projectedFields == null` short-circuits to `Long.MAX_VALUE` (no cap)
and the server returns base records untouched. With a full-column
projection that covers every group column, the merger runs and returns
rows bounded by CEW.

So: explicit projection is the trigger. The tiering reader must opt in
on a per-table basis once it sees `schema.getColumnGroups().isEmpty() ==
false`.

### 3.3 Tiering offset semantics

`Replica.getLakeLogEndOffset()` and `Replica.getLogHighWatermark()`
already disagree by tiering lag for plain log tables. With Phase F:

- For a plain log table: `lakeLogEndOffset ≤ HW` (unchanged).
- For a column-group table: `lakeLogEndOffset ≤ CEW ≤ HW`.

This means a column-group table is structurally less timely in the lake
than a plain log table by the difference between CEW and HW. That's the
same staleness clients experience inside Fluss — Phase F is not adding
latency, just acknowledging it.

## 4. Decisions

### 4.1 Server-side merge, not client-side

The merger lives on the Replica's read path. We could instead splice
enrichment in the tiering job by issuing two scans (base + each group)
and joining client-side. Rejected:

- Doubles the network traffic for column-group tables.
- Requires the tiering job to know the on-disk layout of enrichment
  segments — leaks server internals.
- Re-implements `EnrichmentMerger` semantics (CEW gate, lookup by
  source-offset, schema-version dispatch from E.7).

Server-side merge keeps the tiering job ignorant of column-group
mechanics.

### 4.2 Tiering uses CEW, not local EWM

Same reasoning as the E.4 read gate: data the lake commits should
survive a clean Fluss leader failover. If we tier at local EWM, a new
leader's snapshot of the lake might disagree with what's durable —
phantom rows in the lake. Tiering lags by the CEW→EWM gap for
column-group tables; this is intentional.

### 4.3 No new RPC

The fetch path already supports projection. The tiering job sets a
projection covering every column at scanner-construction time. No proto
changes, no new RPC verb.

### 4.4 LakeWriter implementations are unchanged

Iceberg / Paimon / Lance writers receive `LogRecord` instances and don't
inspect the row's provenance. Fully-materialized rows look identical to
plain log rows to them.

## 5. Phasing

```
F.1  Design ADR (this doc)              ─┐  ✓ 9711826b, 58f866bd, c7eb94fe
F.2  TieringSplitReader: detect column   │  ✓ fbc8df7a
     groups, project all columns         │
F.3  Split generator: cap stoppingOffset │  Critical path
     at CEW for column-group tables      │  ✓ 78ea5797
F.4  ITCase per lake format (Paimon →   ─┘  ✓ 827f7383 (Paimon), 5f1d9aef (Iceberg+Lance)
     Iceberg → Lance)
F.5  Snapshot reads — verify lake reads      ✓ b23893a8
     align with Fluss-client reads at
     same wall-clock time
F.6  Documentation: lake column-group         ← this update
     visibility semantics
```

**Critical path:** F.2 + F.3 must land together (see §5.1). F.5/F.6
anytime after F.4.

### 5.1 F.2 + F.3 are coupled

F.2 alone produces a hang. The tiering job's completion loop is:

```java
while (lastRecord.logOffset() < stoppingOffset - 1) {
    fetchResult = tieringSplitReader.fetch();
    ...
}
```

Today's `stoppingOffset` is the bucket's high watermark, returned by
`bucketOffsetsRetriever.latestOffsets(...)`. With F.2 the LogScanner
applies a full-column projection on column-group tables, which trips
the EnrichmentMerger's CEW cap. If `CEW < HW`, the server stops
returning records once `lastRecord.logOffset() == CEW - 1`, but the
loop is waiting for `HW - 1`. Spin forever.

**F.3 closes the gap** by setting the split's `stoppingOffset` to
`min(HW, CEW_min)` for column-group tables, where `CEW_min` is the
minimum CEW across all groups on the bucket. The tiering job then
covers `[start, CEW_min)` per pass; whatever HW reaches past CEW_min
is picked up on the next tiering cycle once enrichment catches up.

The CEW lookup can either:
- live server-side (extend the listOffsets RPC response with a
  tier-safe upper bound), or
- live client-side in the generator (a new admin call `getMinCew(tb,
  groupNames)`).

**Tentative resolution:** server-side. Listing offsets already returns
HW; adding a sibling field "tier-safe end offset" keeps the round-trip
count flat and means client code stays minimal.

**Implemented:** server-side. `PbListOffsetsRespForBucket` gained an
optional `tier_safe_end_offset` field; server emits it for column-group
tables on `LATEST_OFFSET` queries (`ReplicaManager.computeTierSafeEndOffset`);
the client (`FlussAdmin.sendListOffsetsRequest`) caps the returned offset
when the field is present.

### 5.2 Cross-module install gotcha (build-time)

`TieringSplitReader` lives in `fluss-flink-common`, but the per-Flink-version
modules — `fluss-flink-1.18` / `fluss-flink-1.19` / `fluss-flink-1.20` /
`fluss-flink-2.2` — bundle a *shaded copy* of it. Lake-format ITCases (e.g.
`fluss-lake-paimon`) depend on one of those per-version jars in test scope,
not on `fluss-flink-common` directly.

Consequence: after editing `TieringSplitReader`, running `./mvnw install -pl
fluss-flink/fluss-flink-common -am` is **not** sufficient — surefire for
the lake ITCases will still load the stale shaded copy. You must also
install the per-version module the test depends on:

```
./mvnw install -DskipTests -pl fluss-flink/fluss-flink-common,fluss-flink/fluss-flink-1.20
```

Symptom when this is missed: server-side merge-on-read works in unit tests
but the F.4 lake ITCase shows enrichment columns as null in the lake,
because the stale `TieringSplitReader` never sets the projection that
triggers the merger. To verify which jar contains the live class:

```
unzip -p ~/.m2/repository/org/apache/fluss/fluss-flink-1.20/.../fluss-flink-1.20-*.jar \
  org/apache/fluss/flink/tiering/source/TieringSplitReader.class \
  | javap -v - | grep <some-recently-added-string>
```

## 6. Open questions

### 6.1 Empty tier when leader is fresh

A table created and immediately enabled for tiering, with no enrichment
written, has `EWM = 0` for every group → `CEW = 0` → tiering covers no
offsets. The tiering job's progress would stall until the first
column-group write on every group. Acceptable, or do we tier base-only
rows with null group columns until enrichment arrives?

**Tentative resolution:** stall is correct. Producing null-filled rows
in the lake that later get fixed up means a lake reader sees a row
twice with different content — violates lake idempotency. Wait for the
write.

### 6.2 Multi-group tiering pace

A table with three groups A/B/C tiers at `min(CEW_A, CEW_B, CEW_C)`. A
single slow group pins the whole bucket's tiering progress.

**Tentative resolution:** matches the read gate's behavior. If this
becomes operationally painful, the per-group tiering plan would
require splitting the lake table by group, which we explicitly rejected
in §2 non-goals.

### 6.3 Schema evolution interaction

What does the lake's schema look like when Fluss `alterTable` adds a
column to a group (once that's supported)? The lake table's column set
has to grow. Iceberg supports schema evolution natively; Paimon mostly;
Lance more constrained.

**Tentative resolution:** defer alongside the E.7 schema-evolution
ITCase. Until `alterTable` can target column groups, this is moot.

### 6.4 Failure isolation between tiering job and replication

If Phase E's CEW advance is held back (slow follower, ISR shrinking),
tiering also stalls. Should tiering observe a per-job timeout and
escalate, or just lag indefinitely?

**Tentative resolution:** lag indefinitely; surface via metrics
(deferred E.7). Tiering catching up after replication recovers is the
correct steady-state behavior.

### 6.5 Per-group tiering opt-out (deferred)

Some column groups carry data that doesn't belong in the lake — e.g. a
short-lived enrichment used only for in-Fluss queries, or PII-bearing
columns subject to deletion SLAs that the lake table can't honour
cheaply. Phase F as currently scoped tiers every group; opting one out
would require a per-table or per-group config.

**Tentative shape (not implemented):** a table-level property
`table.datalake.excluded-column-groups` (comma-separated group names).
TieringSplitReader projects all columns *except* the columns belonging
to excluded groups, the merger naturally skips those groups, and the
lake table's schema is built from `tableSchema − excluded-group-columns`.
Tiering offset advances at `min(CEW_g)` over the *included* groups
only — excluding a slow group would unstick tiering progress for the
remaining ones.

**Status:** deferred — not on the F.1→F.6 critical path. Revisit when
a concrete need surfaces; the design above is a sketch, not a
commitment.

## 7. Risks

| Risk | Mitigation |
|------|------------|
| Per-fetch merge cost on tiering path inflates CPU on tablet servers | Equal-slice budget already bounds enrichment payload (PHASE_E §6.1); tiering scans inherit the same shape. |
| Lake schema mismatch after alterTable on a column group | Deferred — gated on alterTable supporting it. |
| Lake reader sees CEW-stale data while base log races ahead | Documented (§3.3, §6.1); same trade-off the read gate makes. |
| Existing tiering ITCases regress for plain log tables | F.2 must opt-in only for `getColumnGroups().nonEmpty()`. |

## 8. Test strategy

- **F.2 unit:** TieringSplitReader correctly switches to full-column
  projection for column-group tables and stays no-projection otherwise.
  *Implemented inline in F.4 ITCases — `computeProjectionForTiering(...)`
  is exercised on every column-group ITCase run.*
- **F.3 unit:** the tiering offset tracking advance is bounded by CEW.
  *Covered by `assertReplicaStatus(bucket, 5)` in each F.4 ITCase, with
  HW = 10 — proves the cap held.*
- **F.4 ITCases:** one per lake format. Implemented as
  `testTieringForColumnGroupLogTable` in:
  - `PaimonTieringITCase` (commit `827f7383`)
  - `IcebergTieringITCase` (commit `5f1d9aef`)
  - `LanceTieringITCase` (commit `5f1d9aef`)

  The implemented pattern advances CEW partway (write 10 base, enrich
  5 → CEW=5) so each test simultaneously verifies F.2 (merger fires,
  rows are fully materialized) and F.3 (lake stops at CEW=5 even with
  HW=10), then enriches the rest and confirms catch-up to HW=10. Each
  format uses its native reader: Paimon's `getPaimonRowCloseableIterator`,
  Iceberg's `Record` API, Lance's Arrow `VectorSchemaRoot` (TSV
  comparison was infeasible because of FP precision in `0.5 + i*0.1`).

- **F.5 ITCase:** `testTieringReadAlignmentForColumnGroupTable` in
  `PaimonTieringITCase` (commit `b23893a8`). Reads the same column-group
  table both ways at the same wall-clock moment — once via Paimon, once
  via Fluss `LogScanner` with full projection — and asserts row-equality
  on the 4 user columns. Catches inconsistencies between the lake-side
  and live-Fluss read paths that neither F.4 nor `ColumnGroupEWMITCase`
  surfaces alone.

## 9. Lake column-group visibility semantics

This section is the user-facing contract for what a lake reader sees
when querying a column-group log table tiered by Fluss. It is normative
for downstream tools (Spark / Trino / lake-native readers).

### 9.1 What is in the lake

For a **column-group log table** (one or more column groups declared on
the schema):

- The lake contains exactly the rows in offset range
  `[lake_log_start_offset, lake_log_end_offset)` where
  `lake_log_end_offset = min(HW, min(CEW_g)) ≤ HW`
  evaluated at lake-commit time.
- Every row in that range has all enrichment columns fully populated —
  there are no rows with `geo_region = NULL` for offsets where Fluss
  knows a value. If a column is null in the lake for an offset, the
  enrichment for that offset was never written to Fluss.

For a **plain log table** (no column groups): the lake contains
`[lake_log_start_offset, lake_log_end_offset)` with
`lake_log_end_offset ≤ HW`. Behavior is byte-identical to pre-Phase F.

### 9.2 Lag relative to live Fluss

A column-group lake table is structurally less timely than the live
Fluss table by `HW - CEW + tiering_lag`:

- `HW - CEW`: base records exist in Fluss past the latest enrichment
  watermark, but cannot be tiered until enrichment catches up.
- `tiering_lag`: the difference between when CEW advances and when the
  next tiering snapshot commits, governed by
  `TABLE_DATALAKE_FRESHNESS` (default 1s).

A query against the lake at wall-clock time T returns the same set of
rows that a Fluss `LogScanner` with full projection would return at
some `T' ≤ T`. There is **no scenario** in which the lake leads Fluss.

### 9.3 Operational implications

- **Sparse enrichment is sparse in the lake.** If the application keeps
  base writes hot but only enriches periodically, the lake will lag
  proportionally. Tiering will catch up on each enrichment burst.
- **Ingestion stalls don't corrupt the lake.** If a follower lags and
  CEW stops advancing, tiering stops. No partial / null rows leak into
  the lake. Recovery resumes tiering automatically.
- **Lake schema is the full Fluss schema.** Every base column and every
  column-group column appears in the lake-side schema with its
  declared Fluss type, plus the standard system columns
  (`__bucket`, `__offset`, and on Paimon `__timestamp`).

### 9.4 What is *not* covered by these semantics

- Schema evolution on a column group (deferred — see §6.3).
- Per-group lake opt-out (deferred — see §6.5). Today, every group's
  columns appear in the lake.
- Primary-key / KV tables — Phase F is log-tables-only.
