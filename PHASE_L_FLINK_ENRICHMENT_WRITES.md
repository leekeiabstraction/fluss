# Phase L — Flink SQL enrichment writes

**Status:** Design — options enumerated, recommendation pending review.
**Authors:** option02-lateMaterialized branch.
**Depends on:** Phase H (column-group schema mutation API), Phase I
(Flink connector read path), Phase K (Flink DDL for column groups).

## 1. Context

Phases I and K closed the read path and DDL surface for column-group
tables in Flink. Today a user can:

- `CREATE TABLE` a column-group table via Flink SQL (Phase K).
- `SELECT` from it via Flink SQL, with merge-at-CEW semantics (Phase I).
- Append base rows via Flink SQL `INSERT INTO` (no enrichment columns
  — they fall to default-group, which is just the base columns).

What is still Java-only today:

- **Writing enrichment rows.** The Java client exposes
  `AppendWriter.appendColumns(bucket, offset, groupName, values)` (Phase
  C). There is no Flink SQL equivalent. Users who want SQL end-to-end
  must drop to a DataStream sink with a custom function that calls the
  Java writer — surprising and gating SQL-only adoption.

Phase L closes this gap by adding a SQL path for enrichment writes.
The hard question is *how to spell it in SQL* — column-group writes
are fundamentally different from row-append writes (they target an
existing row by `(bucket, offset)`, fill a specific group's columns,
and leave base columns untouched). None of Flink's stock DML
statements map onto this cleanly.

## 2. Goals & non-goals

### Goals

1. A SQL spelling for enrichment writes that round-trips through
   `INSERT INTO` (or equivalent) and survives Flink's planner.
2. Standard Flink SQL — no parser extensions, no custom catalog
   surfaces beyond table properties.
3. Composes with normal Flink streaming pipelines: a job can `SELECT`
   from a Fluss column-group table (base side), join with a lookup
   source, and `INSERT` the enrichment row back, all in SQL.
4. Failure modes (wrong group, wrong arity, missing
   `(bucket, offset)`) surface as planner-time `ValidationException`
   or write-time `IllegalArgumentException` with clear messages.

### Non-goals

- **`INSERT INTO ... VALUES` for one-off enrichment writes.** Useful
  but secondary to streaming pipelines. The selected option must
  *support* it, but optimization isn't a goal.
- **Auto-derived `(bucket, offset)` from joins.** The user is
  responsible for projecting the base row's bucket/offset into the
  enrichment stream. Magic auto-correlation is out of scope.
- **Upserts on enrichment groups.** Phase A's scope: log-only,
  no PK. An enrichment "write" is append-only at the target
  `(bucket, offset)`. Overwrites on the same `(bucket, offset, group)`
  are validated against deduplication semantics, not silently merged.
- **CDC mirroring.** Streaming enrichment from a CDC source where
  primary-key updates need to flow as `UPDATE` events is its own
  design — would force Phase A's log-only scope open.

## 3. Architecture — SQL options surveyed

The core asymmetry: a row-shape mismatch. The Fluss column-group table
has N columns (some base, some enrichment), but an enrichment write
carries only the enrichment-group columns plus `(bucket, offset)`
for addressing. SQL's standard `INSERT` doesn't have a clean way to
say "write only these columns and target row X."

Four options canvassed below.

### 3.1 Option A — Magic metadata columns in `INSERT INTO`

Add reserved metadata columns to the Fluss table's Flink-side schema:
`__source_bucket`, `__source_offset`, `__column_group`. Users
`INSERT INTO` the table specifying these columns alongside the
enrichment values. The sink-side `OutputFormat` dispatches on
`__column_group`: empty/null → base append; non-null → enrichment
write to that group.

```sql
INSERT INTO cg_table
    (__source_bucket, __source_offset, __column_group,
     geo_region, risk_score)
VALUES
    (0, 42, 'enriched', 'US-WEST', 0.87),
    (0, 43, 'enriched', 'EU-CENTRAL', 0.12);
```

- **Pros:** Single Flink table object; standard `INSERT INTO`; one
  catalog entry; works for both base and enrichment writes.
- **Cons:** Per-row dispatch is surprising — a `SELECT *` returns
  rows without `__column_group` set, but writes need it. The
  control-plane columns pollute the user-visible schema. Mixing
  base appends and enrichment writes in one statement is legal but
  confusing. Reading the metadata columns back via `SELECT` requires
  defining what they return for already-merged rows (null? the
  group that authored each column?).

### 3.2 Option B — Separate write-only enrichment table (recommended)

Declare a second Flink-side table whose sole purpose is enrichment
writes. Its schema is `(bucket, offset, <enrichment-group columns>)`.
Its connector properties point at the target column-group table and
specify which group it writes to.

```sql
-- Base reader (Phase K, with metadata columns exposed)
CREATE TABLE cg_base (
    device_id   INT,
    payload     STRING,
    geo_region  STRING,
    risk_score  DOUBLE,
    _bucket     BIGINT METADATA FROM 'bucket'   VIRTUAL,
    _offset     BIGINT METADATA FROM 'offset'   VIRTUAL
) WITH (
    'connector' = 'fluss',
    'column-groups.enriched' = 'geo_region, risk_score'
);

-- Write-only enrichment target (Phase L)
CREATE TABLE cg_enriched_writes (
    src_bucket  BIGINT,
    src_offset  BIGINT,
    geo_region  STRING,
    risk_score  DOUBLE
) WITH (
    'connector'         = 'fluss',
    'enrichment.target' = 'default.cg_table',
    'enrichment.group'  = 'enriched'
);

-- End-to-end SQL job
INSERT INTO cg_enriched_writes
SELECT
    b._bucket,
    b._offset,
    lookup_geo(b.payload)    AS geo_region,
    score_risk(b.payload)    AS risk_score
FROM cg_base AS b
WHERE b.geo_region IS NULL;  -- only enrich rows that need it
```

- **Pros:** Standard SQL throughout; clean separation between read and
  write semantics; one catalog entry per group is symmetric with the
  group declaration itself; composes with windowing, joins, lookup
  sources without special casing. The write-only table is a *view*
  in the SQL sense — it has no `SELECT` semantics (or its `SELECT`
  errors loudly).
- **Cons:** Two table objects per group (the base table + one
  write-only sibling per group). Marginal catalog bloat. The
  write-only "view" pattern is unusual — users may try to `SELECT`
  from it and be surprised. Mitigation: error message explicitly
  says "this table is write-only; read from `cg_table` instead."

### 3.3 Option C — Statement-level query hint

Stick with one table, but tag the `INSERT INTO` with a Flink SQL hint
specifying the enrichment group.

```sql
INSERT INTO cg_table /*+ OPTIONS('enrichment.group' = 'enriched') */
    (bucket, offset, geo_region, risk_score)
VALUES
    (0, 42, 'US-WEST', 0.87);
```

- **Pros:** Single table; no synthetic metadata columns visible on the
  read path; hint scope is exactly one statement.
- **Cons:** Hints are *advisory* in Flink — they survive most rewrites
  but the contract is weaker than schema. Multi-INSERT statements
  with different groups (`INSERT INTO ... SELECT FROM ... UNION ALL
  ...`) become awkward. The `bucket, offset` columns still need to
  be addressable somehow (either as magic columns or as metadata —
  back to Option A's problem in miniature). Hint-based control flow
  is a code smell.

### 3.4 Option D — `MERGE INTO`

Use SQL's `MERGE` to express "for each row in the enrichment stream,
find the target row in `cg_table` and update its enrichment columns."

```sql
MERGE INTO cg_table AS t
USING enrichment_stream AS s
ON  t._bucket = s.src_bucket
AND t._offset = s.src_offset
WHEN MATCHED THEN
    UPDATE SET t.geo_region = s.geo_region, t.risk_score = s.risk_score;
```

- **Pros:** SQL-native expression of "address row X, write columns Y";
  the most semantically truthful statement form.
- **Cons:** Flink's `MERGE INTO` support is version-dependent
  (1.18: not supported; 1.20: limited; 2.x: planned). The `ON` clause
  pretends there's a join when really `(bucket, offset)` is an
  addressing primitive, not a join key — the planner may try to build
  a hash join. Streaming `MERGE` semantics are still being worked out
  upstream. Tying Phase L to upstream `MERGE` maturity blocks shipping.

## 4. Decision: Option B (separate write-only enrichment table)

The selected approach for the first cut. Rationale:

1. **Pure standard SQL.** No hints, no parser changes, no magic
   columns leaking onto the read path. Every existing Flink SQL
   tool (planner, IDE autocomplete, lineage tracking) works without
   modification.
2. **Symmetric with the data model.** A column group already feels
   like a "second table glued onto the base table" (Phase A's
   conceptual framing). Surfacing it as a sibling Flink table is
   honest about the underlying structure.
3. **Composes cleanly.** The user reads from the base table (with
   `bucket`/`offset` exposed as metadata columns — a Phase L sub-task)
   and writes to the enrichment-target table. Any SQL between those
   two — joins, lookups, windowing — works untouched.
4. **No upstream Flink dependencies.** Options C and D both
   require features that aren't reliable across Flink 1.18–2.x.
   Option B uses only `CREATE TABLE` + `INSERT INTO`, both stable
   since Flink 1.0.

The cost — two catalog entries per group — is acceptable. Users
already write `CREATE TABLE cg_table ... 'column-groups.<g>' = '...'`
to declare a group; adding `CREATE TABLE cg_<g>_writes ...
'enrichment.target' = 'cg_table'` is a parallel pattern.

### 4.1 Where the connector dispatches

The Fluss Flink connector's `DynamicTableSink` factory branches on
`enrichment.target` presence:

- **Absent** → standard base-table sink (today's path).
- **Present** → enrichment sink. The factory validates:
  - `enrichment.target` resolves to an existing column-group table.
  - `enrichment.group` is declared on that target table.
  - The Flink schema's columns (minus `bucket`/`offset`) exactly
    match the target group's column list, in order, by type.
  - Adds a `SinkFunction` that calls `AppendWriter.appendColumns(...)`
    on each row.

### 4.2 Metadata column exposure on the base side

The base reader (Phase K) does not currently expose `bucket` and
`offset` as Flink metadata columns. Phase L adds that:

```java
// FlinkConversions.toFlinkTable: METADATA columns
b.column("_bucket", BIGINT).withComment("Source bucket (METADATA)");
b.column("_offset", BIGINT).withComment("Source offset (METADATA)");
```

Reads via `SELECT _bucket, _offset, * FROM cg_base` then carry the
addressing primitives downstream into the enrichment pipeline.
Without the user declaring those `METADATA VIRTUAL` columns, the
two-table pattern doesn't compose end-to-end in SQL.

### 4.3 Read on the write-only table

A `SELECT * FROM cg_enriched_writes` returns a `ValidationException`
at plan time. The connector factory rejects `createDynamicTableSource`
when `enrichment.target` is set:

> Table `cg_enriched_writes` is a write-only enrichment target for
> column group `enriched` on table `default.cg_table`. To read the
> enriched data, query `default.cg_table` directly.

### 4.4 Schema evolution interaction

When the target column-group table evolves (Phase H), the write-only
sibling's schema is *not* auto-updated — the user must `ALTER TABLE
cg_enriched_writes ADD COLUMN ...` themselves to add the new
enrichment column. Open question §6.3 below.

## 5. Phasing

```
L.1  ADR (this doc)                                  ← current
L.2  Metadata column exposure (bucket/offset)        TBD
     in FlinkConversions.toFlinkTable; passthrough
     in projection planner; ITCase
L.3  Write-only sink: factory branch on              TBD
     enrichment.target; validation;
     EnrichmentTableSinkFunction wrapping
     AppendWriter.appendColumns; ITCase end-to-end
L.4  Negative cases: unknown target, unknown         TBD
     group, schema mismatch, SELECT on
     write-only table → ValidationException
L.5  Docs: user-facing recipe for the two-table      TBD
     pattern; SQL examples; how to handle
     schema evolution on the writes-side table
```

L.2 must land before L.3 — the sink validation needs `bucket`/`offset`
on the source side to produce a useful end-to-end example.

## 6. Open questions

### 6.1 One write-only table per group, or one per (target, group)?

Two ways to spell the same thing:

**(a)** One write-only table per group:
```sql
CREATE TABLE cg_enriched_writes ... 'enrichment.target' = 'cg_table',
    'enrichment.group' = 'enriched';
```

**(b)** One write-only table covers all groups on a target,
selected by a runtime column:
```sql
CREATE TABLE cg_writes (..., __group STRING NOT NULL, ...) ...
    'enrichment.target' = 'cg_table';
```

(a) is what the §4 design assumes — simpler, more standard, lets each
group's schema be statically validated. (b) is more compact for
multi-group targets but reintroduces per-row dispatch (Option A's
weakness). Lean (a).

### 6.2 `bucket` and `offset` as columns vs. metadata

Declaring them as `METADATA FROM 'bucket' VIRTUAL` (Flink's stock
metadata column syntax) is idiomatic but requires the connector to
implement `SupportsReadingMetadata`. Declaring them as ordinary
columns is simpler but lies about their nature (they're not stored
fields; they're row coordinates). Lean `METADATA`.

### 6.3 Schema-evolution sync

Today (Phase H), `ALTER TABLE cg_table ADD COLUMN ... TO GROUP
enriched` adds a column to the enrichment group. The write-only
sibling table `cg_enriched_writes` doesn't auto-pick-up the new
column — the user must manually alter both.

Options:
- **Manual** (current): user runs two `ALTER TABLE`s. Simple, but
  surprising and error-prone.
- **Auto-mirror**: catalog watches for changes to `enrichment.target`
  schema and emits a synthetic `ALTER` on the write-only sibling.
  Magic; would need a refresh hook.
- **Dynamic schema**: write-only table's schema is *derived* at plan
  time from the target. Simplest UX (single ALTER suffices) but
  requires the sink factory to fetch the target's current schema and
  refuse if the SQL projects columns that don't match.

Lean *dynamic schema* — costs one extra metadata lookup at plan time,
buys the user a single point of truth.

### 6.4 Idempotency on writes-side

What happens if the same `(bucket, offset, group)` enrichment row is
written twice (e.g. a Flink restart replays from checkpoint)? Today
the Java writer accepts duplicate appends — second copy lands in the
log. The merger on read side dedups by taking the latest. Acceptable
for streaming pipelines that are inherently at-least-once before
checkpoint commit. Exactly-once requires the sink to be a 2PC sink,
which the base path already implements (sketch in
`FlinkSinkFunction`). The enrichment sink should reuse the same
2PC infrastructure. Not a Phase L decision — inherit Phase I's
sink semantics.

### 6.5 Streaming SQL recipe for the "enrich on the fly" use case

The end-to-end pattern is:

```sql
INSERT INTO cg_enriched_writes
SELECT
    b._bucket, b._offset,
    enrich_geo(b.payload), score_risk(b.payload)
FROM cg_base AS b
WHERE b.geo_region IS NULL;  -- backfill rows missing enrichment
```

But what if the user wants to *always* compute enrichment, not just
backfill? `WHERE b.geo_region IS NULL` only fires on rows that haven't
been enriched yet — for a fresh column group, that's all rows. Fine.
For a column group that's been partly enriched, it's only the gap.

Open question: do we provide a SQL convenience for this
("enrich-all" vs "enrich-missing") or leave it to the user? Lean
leave-to-user — it's a one-line WHERE clause.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Two-table pattern confuses users — they expect to read and write through the same table name. | Docs (L.5) lead with the recipe. The write-only table errors loudly on `SELECT`. The naming convention `cg_<group>_writes` makes the role obvious. |
| Metadata column exposure (`bucket`, `offset`) on the base table breaks existing Flink reads. | Metadata columns are opt-in in Flink — the user must declare them in `CREATE TABLE`. Existing tables without those declarations behave identically. |
| Schema evolution drift: target table evolves but write-only sibling doesn't, leading to plan-time errors that look like SQL errors. | §6.3 — lean on dynamic schema lookup at plan time so the sibling tracks the target automatically. Error message points the user at the right `ALTER`. |
| Sink performance: a separate `AppendWriter` per Flink subtask might fragment per-bucket batching. | Connector configures the underlying `AppendWriter` to batch by `(target, group, bucket)` — same as today's base-path sink. Phase L L.3 verifies via a perf microbench. |
| Naming collision: `enrichment.target` / `enrichment.group` keys collide with future Fluss option names. | No current Fluss option is prefixed `enrichment.`; reserve the prefix. Mirrors Phase K's `column-groups.` decision. |
| User writes wrong arity / wrong types into enrichment table; failure surfaces deep in the sink. | Factory-level schema validation (§4.1) fails at plan time with a clear `ValidationException`, not at runtime. |

## 8. Test strategy

- **L.2 ITCase** — `FlinkTableSourceITCase`:
  - `CREATE TABLE cg_base ... _bucket METADATA FROM 'bucket' VIRTUAL,
     _offset METADATA FROM 'offset' VIRTUAL`.
  - Write 10 base rows via Java client; `SELECT _bucket, _offset, *
    FROM cg_base` returns the rows with addressing primitives.
  - Verify projection pushdown still works when metadata columns
    are *not* requested.

- **L.3 ITCase** — `FlinkSinkITCase` (new test):
  - Create target column-group table.
  - Create write-only sibling with `enrichment.target` +
    `enrichment.group`.
  - Run `INSERT INTO ... SELECT ... FROM cg_base` job; verify
    enrichment rows arrive on the target by `SELECT`ing it back via
    SQL — assert merge-at-CEW yields the expected rows.
  - `INSERT INTO ... VALUES (...)` one-shot enrichment writes.

- **L.4 negative cases**:
  - `enrichment.target` points at a non-existent table →
    `ValidationException` at `CREATE TABLE`.
  - `enrichment.group` points at a non-existent group on a real
    target → `ValidationException`.
  - Schema mismatch (wrong column count / wrong type) at write-only
    table create → `ValidationException`.
  - `SELECT * FROM cg_enriched_writes` → `ValidationException` with
    "write-only" message.
  - Schema evolution sync (§6.3): if dynamic-schema is chosen, an
    `ALTER TABLE cg_table ... TO GROUP enriched` causes subsequent
    `INSERT INTO cg_enriched_writes ...` to validate against the new
    schema.

- **L.5 user docs**:
  - Recipe section in connector docs: "Writing enrichment rows from
    Flink SQL".
  - SQL examples cover backfill, streaming-enrich, batch-enrich.
