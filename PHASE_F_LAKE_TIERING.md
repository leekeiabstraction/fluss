# Phase F — Lake tiering for enrichment columns

**Status:** ADR — F.1 (this doc).
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
F.1  Design ADR (this doc)              ─┐
F.2  TieringSplitReader: detect column   │
     groups, project all columns         │
F.3  Tier-side offset tracking — track   │  Critical path
     against CEW, expose on metadata     │
F.4  ITCase per lake format (Paimon →   ─┘
     Iceberg → Lance)
F.5  Snapshot reads — verify lake reads
     align with Fluss-client reads at
     same wall-clock time
F.6  Documentation: lake column-group
     visibility semantics
```

**Critical path:** F.2 → F.3 → F.4. F.5/F.6 anytime after F.4.

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
- **F.3 unit:** the tiering offset tracking advance is bounded by CEW.
- **F.4 ITCases:** one per lake format. Pattern:
  1. Create column-group table, enable tiering.
  2. Write 10 base + 10 enrichment rows on leader.
  3. Wait for ISR ack (E.3c machinery) → CEW = 10.
  4. Wait for tiering job → lake snapshot has 10 rows with full column set.
  5. Read from lake via lake-native reader → assert row contents
     match Fluss-client reads.
- **F.5 ITCase:** time-aligned consistency check. Same data, two readers
  (Fluss client at HW, lake reader at lake snapshot). Difference is
  bounded by `HW - CEW + tiering_lag`.
