# Phase M — Partitioned column-group tables

**Status:** Design.
**Authors:** option02-lateMaterialized branch.
**Depends on:** Phases A–L (all prior column-group work), and is a lift of
the Phase A scope decision "column-group tables are not partitioned".

## 1. Context

PHASE_A scoped column-group tables to **non-partitioned** log tables and
every subsequent phase (B–L) was designed against that simpler addressing
model. The implementation has carried partition-awareness *at the data
structure level* throughout — `TableBucket` already has an optional
`partitionId`, `AppendWriterImpl.appendColumns` already serializes it on
the wire, `LogTablet` is keyed per-`(table, partition, bucket)` so EWM
state is already partition-scoped — but no Flink-facing or DDL surface
exposes the combination, no test ever exercises it, and a handful of
validation paths *expect* non-partitioned column-group tables and would
need to grow new branches.

Phase M opens this combination: log tables that are both **partitioned by
a base-group column** and have **one or more enrichment column groups**.

## 2. Goals & non-goals

### Goals

1. A Fluss log table can be declared with both `PARTITIONED BY (<base
   col>)` and `column-groups.<g>` and behaves correctly end-to-end:
   - Base appends land in the correct partition.
   - Enrichment writes target `(partition, bucket, offset)` and advance
     per-partition CEW.
   - Flink reads project bucket/offset *and partition* via METADATA
     columns, gated at the per-partition CEW.
2. The Flink SQL Option B write-only enrichment-target table supports
   partitioned base tables (row layout grows to carry the partition
   value).
3. Lake-tiered base + enrichment for partitioned column-group tables
   honors the same partition layout in Iceberg/Paimon/Lance.
4. Drop-partition cleans up associated enrichment state and segments.

### Non-goals

- **Partitioning by enrichment columns.** Structurally impossible —
  enrichment values are `NULL` at base-write time, so partition keys
  must be drawn from the default/base group. Captured in §4.1.
- **Per-partition column-group declarations.** Every partition shares
  the same set of column groups; you can't add an enrichment group to
  only one partition. Carried through from PHASE_H §2 non-goals.
- **Auto-partitioned column-group tables.** Auto-partition (Fluss's
  time-based partition rollover) interacts with EWM in ways that need
  their own design (partition lifecycle vs. CEW; do dropped partitions
  reclaim CEW state? when?). Defer.
- **KV/PK column-group tables.** Permanent non-goal regardless of
  partitioning (see `project_kv_pk_scope` memory).

## 3. Architecture

### 3.1 Addressing model — what is and isn't already partition-aware

Already partition-aware (no changes needed):

- `TableBucket(tableId, @Nullable Long partitionId, int bucket)` —
  `fluss-common/.../metadata/TableBucket.java`.
- Wire protocol: `PbTableBucket` carries optional `partition_id`;
  `AppendWriterImpl.appendColumns` already sets it from `bucket
  .getPartitionId()` when non-null
  (`fluss-client/.../writer/AppendWriterImpl.java:252-253`).
- Server-side replica routing: `Replica` is keyed by `TableBucket`, so
  partitioned tables already route to the correct replica per partition.
- EWM state: `LogTablet.committedEnrichmentWatermarks` is per-LogTablet
  (a `LogTablet` is per-`(table, partition, bucket)`), so CEW is
  implicitly per-partition-bucket — no change needed.
- On-disk enrichment segments (Phase C): one segment directory per
  `LogTablet`, hence per-partition-bucket.

Needs work:

- **Validation**: no current code rejects "partition key in a
  non-default group" — needs an explicit check in
  `TableDescriptor` / `FlinkConversions.parseColumnGroups`.
- **Flink source METADATA**: `MetadataAppender` exposes `bucket` and
  `offset` only; needs a `partition` (STRING) key for partitioned
  tables.
- **Flink sink (Phase L)**: `EnrichmentTableSink`'s row schema assumes
  `(BIGINT bucket, BIGINT offset, <vals>)` — needs to grow a leading
  `partition_value` (STRING) for partitioned targets, or accept the
  partition via a dedicated property.
- **Lake tiering**: audit `fluss-lake-*/tiering/` for partition-key flow
  on the enrichment path.

### 3.2 The structural invariant: partition keys ⊂ default group

A partition key column must be writable at base-row-insertion time.
Enrichment-group columns are `NULL` at base-write time and filled
later via `appendColumns`. Therefore:

> **Invariant M.1**: every partition key column belongs to the
> default/base group; no partition key column may be a member of any
> named column group.

This is enforced in three places:

1. **`Schema.Builder.columnGroup(...)`** (or its caller): reject if
   the column being tagged is already a partition key.
   - In practice this is enforced one layer up because `Schema` itself
     doesn't know about partition keys — `TableDescriptor` does. So the
     check lives in `TableDescriptor`'s construction.

2. **`TableDescriptor`**: validate at table-construction time that the
   intersection of `partitionKeys` and any non-default group's columns
   is empty.

3. **`FlinkConversions.parseColumnGroups`** (catalog-time, in the Flink
   factory): same check, lifts the error to `ValidationException` at
   `CREATE TABLE`.

ALTER TABLE paths (Phase H):
- `addColumn(... groupName)` must reject if the column is/will be a
  partition key.
- A future "add partition key" alter (if/when one exists) must reject
  if any current partition key is in a non-default group.

### 3.3 Flink source — `partition` METADATA column

Phase L.2 added `bucket` and `offset` METADATA. Phase M adds:

```sql
CREATE TABLE cg_partitioned (
    dt         DATE,
    device_id  INT,
    payload    STRING,
    geo_region STRING,
    risk_score DOUBLE,
    _partition STRING METADATA FROM 'partition' VIRTUAL,
    _bucket    BIGINT METADATA FROM 'bucket'    VIRTUAL,
    _offset    BIGINT METADATA FROM 'offset'    VIRTUAL
) PARTITIONED BY (dt)
  WITH ('column-groups.enriched' = 'geo_region, risk_score');
```

Implementation:

- `FlinkTableSource.listReadableMetadata()` adds `"partition" → STRING
  NOT NULL` to the existing map.
- `MetadataAppender` grows a `PARTITION_SLOT` sentinel and a partition
  field in `splice(...)` — sourced from `SourceSplitState.getTableBucket()
  .getPartitionId()` and resolved to the partition *name* (STRING) via
  a lookup against the table's partition catalog at the reader level.

The partition value is exposed as a STRING (the user-visible name, e.g.
`'2025-12-31'`), not the internal numeric `partitionId`. This matches
how Fluss's existing `$partition` virtual column works (open question
6.1).

### 3.4 Flink sink — partition value in the write-only row

The Phase L Option B row layout for the write-only enrichment table is
`(BIGINT src_bucket, BIGINT src_offset, <group cols…>)`. For a
partitioned target, two design choices:

**Choice A — partition is a leading column in the sink row.**

```sql
CREATE TABLE cg_partitioned_writes (
    src_partition STRING,           -- NEW for partitioned targets
    src_bucket    BIGINT,
    src_offset    BIGINT,
    geo_region    STRING,
    risk_score    DOUBLE
) WITH (
    'enrichment.target' = 'cg_partitioned',
    'enrichment.group'  = 'enriched'
);
```

Sink-side validation: leading three columns are `(STRING, BIGINT,
BIGINT)`. The writer resolves the partition *name* → internal
`partitionId` per row via a cache against the target's partition
catalog.

**Choice B — partition specified via a `enrichment.target-partition`
property.**

Static partition known at create time. Doesn't work for streaming
backfill across partitions in a single job. Rejected.

Lean **Choice A** — the row layout grows by one column for partitioned
targets, sink validates the layout difference at plan time. For
non-partitioned targets, the existing 2-leading-col layout is
unchanged.

The sink writer (`EnrichmentSinkWriter`) extends to:
- Resolve partition name → `partitionId` per row (cache via `Admin`).
- Call `AppendWriter.appendColumns(group, new TableBucket(tableId,
  partitionId, bucket), offset, valuesRow)`.

### 3.5 Lake tiering — partition mapping

Audit each lake-format adapter:

- **Paimon**: Paimon has its own partition concept. Phase F's tier
  output already aligns Fluss partition → Paimon partition for base
  data. Verify enrichment writes produced by the tiering pipeline land
  in the same partition.
- **Iceberg**: Iceberg partition spec mapping. Same audit.
- **Lance**: Lance partition support is more limited; may not need
  changes if the existing flow already projects partition columns into
  the Arrow output.

The base-side mapping is already in place from Phase F.1–F.3. Phase M
verifies the enrichment-write path (F.4) honors it for partitioned
tables. If the existing F.4 path doesn't carry partition through, this
is a small wiring fix.

### 3.6 Drop-partition lifecycle

When `ALTER TABLE cg_partitioned DROP PARTITION (dt = '2025-01-01')`
runs:

1. Coordinator marks the partition as dropped.
2. Per-partition `LogTablet`s are closed; their EWM state is released.
3. On-disk enrichment segments under the dropped partition's directory
   are deleted as part of the existing log-segment cleanup (no new
   cleanup path needed — Phase C's segments live alongside the log
   files).
4. Lake tiering: depends on lake format's handling of dropped
   partitions; out of scope for Phase M itself.

Open question: should there be a window where dropped-partition
enrichment writes are gracefully rejected vs. silently dropped?
Probably reject with `PartitionNotExistException`, consistent with
base-row writes.

## 4. Decisions

| # | Decision | Why |
|---|---|---|
| M.1 | Partition keys ⊂ default group, enforced at table-construction time and at the Flink catalog layer | Structural — enrichment values are NULL at base-write time and can't determine partition |
| M.2 | Flink METADATA exposes `partition` as STRING (not BIGINT partitionId) | Matches `$partition` convention; user-friendly; avoids tying users to internal numeric IDs |
| M.3 | Write-only sink row layout grows by one leading STRING column for partitioned targets | Composable with normal SQL; sink validates layout per target's partitioning |
| M.4 | Per-row partition-name → partitionId resolution at the sink, with caching | Streaming jobs may write to many partitions; static-partition restrictions are too constraining |
| M.5 | Drop-partition releases EWM state + segments via existing log-cleanup path | No new lifecycle hook needed; reuse |
| M.6 | Auto-partition + column groups is out of scope (separate phase) | Auto-partition rollover and CEW interaction is its own design problem |
| M.7 | Validation is layered: TableDescriptor (catalog-agnostic) + FlinkConversions (lifts to ValidationException at CREATE TABLE) | Same pattern as Phase K's column-group validation |

## 5. Phasing

```
M.1  ADR (this doc)                                  ← current
M.2  TableDescriptor + Schema validation             TBD
     - Reject partition key in non-default group.
     - Unit tests on TableDescriptor.
M.3  Flink catalog validation                        TBD
     - parseColumnGroups + applyReadableMetadata
       expand to handle partitioned column-group
       tables.
     - Negative-case test: declare partition key in
       a column group → ValidationException.
M.4  Flink source `partition` METADATA               TBD
     - MetadataAppender.PARTITION_SLOT.
     - FlinkTableSource.listReadableMetadata adds
       partition: STRING.
     - Reader-side partition-name resolution.
     - ITCase: partitioned base read with metadata
       projection.
M.5  Flink sink: partition column in write-only row  TBD
     - EnrichmentTableSink schema validation for
       partitioned targets.
     - EnrichmentSinkWriter partition-name → ID cache.
     - ITCase: end-to-end SQL across two partitions.
M.6  Schema evolution interaction                    ✓ no-op given current Fluss
     - The M.1 invariant is preserved transitively by
       the existing "column already exists" rejection
       in SchemaUpdate.addColumn — partition keys are
       fixed at table-create time and cannot be
       promoted from a non-partition column via any
       TableChange, so a new column with a groupName
       can never become a partition key. Test
       cannotAddColumnWithSameNameAsExistingColumn
       in SchemaUpdateTest documents this.
     - If a future "add partition key" alter is
       added, it must enforce M.1 at that point.
M.7  Lake tiering audit                              TBD
     - Verify F.4-style enrichment-write paths in
       Paimon/Iceberg/Lance carry partition through.
     - ITCase: tier a partitioned column-group table
       to Paimon and read back via Spark.
M.8  Drop-partition cleanup verification             TBD
     - Reuses existing log-cleanup; confirm EWM state
       is released and segments are removed.
     - ITCase: drop a partition, attempt enrichment
       write → PartitionNotExistException.
M.9  Docs                                            TBD
     - Recipe in writes.md/ddl.md/reads.md updates.
```

M.2 + M.3 are tightly coupled and small (~50 lines + a couple of
negative tests). M.4 + M.5 are the bulk of the work — they touch the
Flink source/sink runtime paths and need ITCases. M.6–M.8 are audits
+ verification tests.

## 6. Open questions

### 6.1 Partition value vs. partitionId in METADATA

The Flink source exposes `partition` as a STRING (the user-visible
name, e.g. `'2025-12-31'`). But internally everything addresses by
numeric `partitionId`. Three options:

- **A**: source emits the STRING name (requires a name lookup per
  record — cached against partition-info cache that's already in the
  scanner).
- **B**: source emits the numeric `partitionId` as a BIGINT (matches
  internal addressing exactly; user would compose with `$partition`
  if they need the name).
- **C**: expose both (`partition` STRING + `partition_id` BIGINT).

Lean **A** — matches `$partition` convention and user-facing layer.
The sink resolves STRING → `partitionId` on the write side; reads emit
the STRING.

### 6.2 Auto-partition interaction

Auto-partition (Fluss's time-rollover feature) creates new partitions
over time. When a partition is auto-created:

- Existing column-group declarations apply to it (no per-partition
  configuration).
- CEW starts at 0 for each `(new-partition, bucket)` pair.

When a partition is auto-dropped:

- EWM state is released (same as manual drop).
- In-flight enrichment writes to the dropped partition fail loudly.

The interaction is straightforward in principle but needs an ITCase
that exercises the rollover-during-enrichment scenario. Defer to a
separate phase (M.10?) unless the core M.2–M.5 work surfaces issues.

### 6.3 Sink schema disambiguation: partitioned vs. non-partitioned

If the user writes:
```sql
CREATE TABLE x_writes (a STRING, b BIGINT, c BIGINT, d STRING) WITH (
  'enrichment.target' = 'cg_partitioned',  -- IS partitioned
  'enrichment.group' = 'enriched'
);
```

…the sink expects 4 cols: `(partition STRING, bucket BIGINT, offset
BIGINT, val STRING)`. But the same DDL against a non-partitioned target
expects 3 cols: `(bucket BIGINT, offset BIGINT, val STRING, ???)`.

The sink-build-time validation in `EnrichmentTableSink.resolveTarget`
fetches the target's `TableInfo` (and thus partitioning) — it knows
which layout to expect. The error message should clearly say "target
is partitioned; sink row must start with `(partition STRING, bucket
BIGINT, offset BIGINT, …)`" so users can fix the DDL.

### 6.4 Should `partition` METADATA be auto-included on partitioned tables?

Currently METADATA is opt-in (user declares it in CREATE TABLE).
For partitioned tables, users may *expect* to see partition values.
Either:

- **Opt-in (current)**: user declares `_partition STRING METADATA …`
  if they want it. Consistent with how Fluss exposes `$partition` as
  a virtual column (open question; need to verify how `$partition` is
  declared today).
- **Auto-include**: when reading a partitioned Fluss table, the
  partition column is always projected. But this is already the case
  because partition columns are part of the schema (`dt DATE`); the
  METADATA col is purely for *enrichment-write* use.

The METADATA col stays opt-in. The user already sees the partition
value as the partition column itself (`dt`); the METADATA `partition`
is just for the enrichment write recipe where they need the partition
*name* to project into the write-only sink. Open: is `dt` and the
METADATA `partition` the same? §6.1 says partition is exposed as
STRING name; `dt` is DATE. They're different representations of the
same logical value. Awkward but not broken.

### 6.5 Sink performance: per-row partition resolution

`EnrichmentSinkWriter` resolves `partition_name → partitionId` per row.
For a streaming job that writes to one partition at a time, the cache
hit rate is 100%. For a job that writes across many partitions
(backfill, time-shifted enrichment), the cache may have to query the
catalog repeatedly.

Mitigation: cache invalidation only on `PartitionNotExistException`;
otherwise cache forever (partition IDs don't change).

### 6.6 Drop-partition during in-flight enrichment writes

If a partition is dropped while an enrichment write is in flight:

- The append RPC arrives at the server with a stale `partitionId`.
- Server should respond with `PartitionNotExistException`.
- Client may or may not retry; for an enrichment write, the partition
  is gone, so retrying is futile. The sink writer should treat this
  as a fatal error and surface it.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Existing Phase B-D code has latent assumptions about non-partitioned tables that surface only when both features combine | M.2–M.5 ITCases drive end-to-end coverage; surface latent bugs early |
| Partition-resolution cache stale on partition rename (if Fluss supports rename) | Confirm if rename is supported; if so, add cache invalidation on `PartitionRenamedException`-like event |
| Lake-tier path doesn't carry partition for enrichment writes | M.7 audit + ITCase per lake format |
| User confuses internal `partitionId` (BIGINT) and partition name (STRING) in the sink schema | §6.3 — error messages explicitly state the expected types/order |
| Drop-partition leaves orphaned enrichment segments under a since-deleted directory | Reuse existing log-segment cleanup; M.8 verification ITCase confirms |
| `_partition STRING METADATA FROM 'partition'` collides with user-declared columns named `_partition` | Standard METADATA shadowing — Flink rejects duplicate column names |

## 8. Test strategy

### M.2/M.3 — validation negatives

- TableDescriptor unit test: partition key in named group → throws.
- TableDescriptor unit test: partition key in default group + column
  group on another column → OK.
- FlinkConversions parseColumnGroups: catalog-time rejection with
  ValidationException, message names the offending column.

### M.4 — source METADATA

In `FlinkTableSourceITCase` (extends to partitioned tables):

- `testPartitionedColumnGroupReadWithMetadata`:
  - Create partitioned table with column group.
  - Add partitions (dt='2025-01-01', dt='2025-01-02').
  - Write base rows across both partitions.
  - SELECT `_partition, _bucket, _offset, device_id` — assert each row
    has the correct partition name.

### M.5 — sink end-to-end

- `testEnrichmentSinkPartitioned`:
  - Same setup as M.4.
  - Declare write-only enrichment table with leading partition col.
  - INSERT INTO via streaming SQL.
  - Wait for CEW on both partitions to advance.
  - SELECT enrichment values across both partitions.

### M.7 — lake tiering

- Per-format ITCase (Paimon first):
  - Partitioned column-group table → tier → read back via Spark with
    partition predicate (`WHERE dt = '2025-01-01'`).
  - Assert enrichment values are present and partition pruning works.

### M.8 — drop-partition

- Drop a partition with enrichment values present.
- Confirm LogTablet's EWM state is released.
- Attempt an enrichment write at the dropped partition's offset:
  expect `PartitionNotExistException`.

## 9. Implementation sketch (M.2 specifically)

Concrete starting point: add the partition-key/group exclusivity check
to `TableDescriptor`:

```java
// fluss-common/.../metadata/TableDescriptor.java

// After existing partition-key validation (line ~104):
Map<String, List<Integer>> groups = schema.getColumnGroups();
if (!groups.isEmpty() && !partitionKeys.isEmpty()) {
    Set<String> groupedColumnNames = groups.values().stream()
            .flatMap(List::stream)
            .map(idx -> schema.getColumns().get(idx).getName())
            .collect(Collectors.toSet());
    List<String> illegal = partitionKeys.stream()
            .filter(groupedColumnNames::contains)
            .collect(Collectors.toList());
    if (!illegal.isEmpty()) {
        throw new IllegalArgumentException(
                "Partition keys must belong to the default column group "
                + "(no enrichment values can determine the partition). "
                + "Offending columns: " + illegal);
    }
}
```

The same predicate moves into `FlinkConversions.parseColumnGroups` so
the catalog raises `ValidationException` at `CREATE TABLE` time rather
than letting it surface as `IllegalArgumentException` from the
TableDescriptor builder.

## 10. Out of scope (carried over)

- Auto-partition + column groups (§6.2).
- KV/PK column-group tables (permanent non-goal).
- Per-partition column-group configuration.
- Schema evolution beyond add-column (PHASE_H follow-up).
