# Phase I — Flink connector for column-group tables

**Status:** ADR — I.1 (this doc).
**Authors:** option02-lateMaterialized branch.
**Depends on:** Phase E (CEW-bound read gate), Phase F (lake tiering),
Phase H (schema evolution).

## 1. Context

Phases A–H landed the column-group feature on the Java-client, server,
lake-tiering, and schema-evolution paths. The **user-facing Flink
connector** (`fluss-flink-common` + the per-Flink-version modules) was
touched by Phase F.2 for the *tiering* job — Phase F's `TieringSplitReader`
sets full-column projection on column-group tables so the server-side
merger fires.

That handled tiering. The **regular Flink SQL / DataStream read path**
— `FlinkSourceSplitReader`, used by user queries like
`SELECT geo_region FROM cg_table` — has not been verified against
column-group tables. The **Flink sink** path has no enrichment surface
at all: today's sink uses `AppendWriter.append(...)` and there is no
Flink-side equivalent of `appendColumns(group, bucket, sourceOffset, row)`.

This is a gap, but a narrow one. The server-side semantics (merger
firing on projection that touches enrichment, CEW gate, per-batch
schema dispatch from Phase H) are independent of which Flink-version
class makes the FETCH_LOG RPC. If projection pushdown reaches the
server intact, Flink reads work for free.

## 2. Goals & non-goals

### Goals

1. **Flink SQL reads** of a column-group table produce the same row set
   as a Java-client `LogScanner` with full projection: enrichment
   columns populated up to CEW, rows past CEW gated.
2. **Schema discovery**: the Flink-Fluss catalog exposes column-group
   columns as regular columns in Flink's `Table` schema. Group
   metadata stays on the server; Flink doesn't need to know.
3. **Schema evolution** during a running Flink streaming query
   behaves predictably — either the job stops with a recognizable
   schema-change error or it continues under its cached schema
   without emitting malformed rows. No `IndexOutOfBoundsException`,
   no silent data corruption.
4. **No regression** for plain log-table queries — the existing Flink
   SQL behavior on non-column-group tables is byte-identical.

### Non-goals

- **Flink-native enrichment writes.** Surfacing `appendColumns(...)` via
  Flink SQL or DataStream API would need (a) a way to attach a
  `source_offset` to each enrichment record in the stream, and (b)
  new DDL or operator semantics (`ENRICH TABLE` or similar). No
  natural fit in Flink's `INSERT INTO` model. Defer until a concrete
  workload demands it; until then, Java client is the canonical
  enrichment write surface.
- **`CREATE TABLE` DDL extension** for declaring column groups from
  Flink SQL. Expected pattern: `admin.createTable(...)` via Java API to
  declare the schema; query via Flink SQL. If users push back, the
  catalog's `WITH (...)` parser would gain a
  `'column-groups.<name>' = 'col1,col2'` option (~30 lines).
- **Partitioned column-group tables.** Same scope decision as F & H.
- **KV/PK column-group tables.** Out of scope of the feature overall.

## 3. Architecture

### 3.1 What we expect to work for free

Three pieces of generic Flink/Fluss machinery should compose without
intervention:

- **Projection pushdown.** Flink's table planner pushes a
  `SupportsProjectionPushDown.applyProjection(int[] projected)` into the
  Fluss source. The Fluss source forwards that to
  `LogScanner.project(...).subscribe(...)` via `FlinkSourceSplitReader`.
  Phase E.4's server-side gate fires on
  `projectionTouchesEnrichment(...)`; Phase H's fix uses the latest
  schema. Same path used by direct Java-client tests like
  `ColumnGroupEWMITCase#testProjectionGatedVisibility` —
  if the Flink source forwards projection unmangled, it just works.
- **Schema discovery.** The Flink-Fluss catalog reads
  `tableInfo.getRowType()` to populate Flink's `Table` schema.
  Column-group columns are regular columns in `getRowType()`; they
  differ only in the `columnGroup` metadata on `Schema.Column`, which
  Flink doesn't read.
- **Per-batch schema dispatch.** `FileLogProjection` and the Phase H
  merger both consult `SchemaGetter` per batch. The Flink source
  reuses the same `SchemaGetter` via the Java-client `Table`. A
  Flink job that started under v1 and is still running when an alter
  bumps to v2 will decode v2 batches under v2 — without restarting,
  *for the columns its projection already covers*. To reference a
  brand-new column from a v2 alter, the job has to be restarted so
  the catalog re-reads the schema.

### 3.2 What might not work

Failure modes worth pinning down in I.2:

- **Flink's projection-pushdown drops or reorders columns** such that
  the projection reaching the server doesn't actually include the
  enrichment column indices the user's query referenced. Symptom:
  `SELECT geo_region FROM cg_table` returns the same number of rows
  as `SELECT device_id FROM cg_table` (HW, not CEW) with `geo_region`
  null — the merger never fires.
- **The Flink-Fluss catalog filters out columns that have a non-null
  `columnGroup`** — plausible if someone wrote the catalog assuming
  only base columns should be visible, treating groups as
  "server-internal." Symptom: Flink SQL can't reference enrichment
  columns at all (compile error: "no such column").
- **Mid-job alter crashes the Flink task** because the source operator
  caches the schema and chokes on a batch encoded under a newer
  schemaId. Symptom: `IndexOutOfBoundsException` or
  `ClassCastException` mid-stream.

### 3.3 Read semantics — user-facing contract

For a column-group table queried via Flink SQL:

| Query shape | Rows returned | Why |
|---|---|---|
| `SELECT base_col FROM cg_table` | up to HW | projection doesn't touch enrichment; server gate inactive |
| `SELECT enrichment_col FROM cg_table` | up to CEW | projection touches enrichment; server gates |
| `SELECT *  FROM cg_table` | up to CEW | `*` expands to include enrichment columns |
| `SELECT base_col, enrichment_col FROM cg_table` | up to CEW | enrichment col present in projection |

Identical to direct Java-client `LogScanner` semantics from Phase E.4.

### 3.4 Write semantics — user-facing contract

For a column-group table written via Flink SQL:

- `INSERT INTO cg_table VALUES (a, b, NULL, NULL)` writes a base row,
  leaving enrichment columns null. Standard `AppendWriter.append`.
- There is **no Flink SQL surface** for filling enrichment values
  post-base-write. The user must invoke
  `AppendWriter.appendColumns(group, bucket, sourceOffset, row)` from
  Java (or a custom Flink operator that wraps the Java client).
  Documented in I.5; the recommended pattern is a separate enrichment
  service / batch job that reads base rows + computes enrichment +
  writes via the Java client API.

## 4. Decisions

### 4.1 Java client remains the canonical enrichment write surface

Re-stating §2's non-goal: surfacing `appendColumns(...)` through Flink
SQL or DataStream API needs source-offset semantics that Flink streams
don't carry naturally. Defer until a workload demands it. Document the
Java-client pattern as the contract.

### 4.2 No catalog cache invalidation for mid-job alter

Flink jobs are typically short-lived and restartable. "Restart on
schema change" is the right policy and matches what Flink-Kafka does
for Schema Registry-managed topics. A mid-job alter that adds a new
column means existing queries continue under the old projection (no
data loss, just no access to the new column until a restart).

### 4.3 No new DDL in Phase I core

`CREATE TABLE` with explicit column groups is deferred. Today's path
is `admin.createTable(...)` via Java API; query via Flink SQL. If
needed later, the catalog gains a parsed `WITH ('column-groups.<g>' =
'col1,col2')` property — ~30 lines, no other code-path changes.

### 4.4 One Flink version is sufficient for the ITCase

Phase F.4 went `Paimon → Iceberg → Lance` for cross-format coverage,
which made sense because each lake format has its own `LakeWriter`
plugin with independent code. Phase I has no such variation — the
Flink source code is shared via the shaded per-version jars. Mirroring
Phase F.4's choice, I.2 runs against one Flink version (the same
`fluss-flink-1.20` the lake ITCases use). Other versions are covered
incidentally by their copies of the shared source.

## 5. Phasing

```
I.1  ADR (this doc)                                  ← landing now
I.2  ITCase: Flink SQL read of column-group table    (next)
     ─ asserts projected read produces merged rows up to CEW
     ─ asserts base-only projection returns rows up to HW
I.3  Whatever fixes I.2 surfaces                     (may be empty)
I.4  Schema-evolution ITCase under a running query   (after I.2)
     ─ alter mid-query; assert no crash / corruption
I.5  Documentation                                    (after I.2 + I.4)
     ─ section in this doc describing user-facing contract
     ─ code example for Java-API enrichment write pattern
```

Optimistic path: I.2 passes first run → I.3 empty → I.4 either passes
or finds a minor schema-cache wrinkle → I.5 pure docs.

Pessimistic path: I.2 surfaces projection-pushdown issues → I.3 takes
the bulk of the time.

## 6. Open questions

### 6.1 Does the existing Flink-Fluss catalog expose column-group columns?

If `FlussCatalog#getTable(...)` filters out columns with a non-null
`columnGroup`, Flink SQL can't reference enrichment columns at all.
This would be a load-bearing bug. First I.2 check.

### 6.2 Mid-job alter — what's "acceptable" exactly?

Three buckets:

- **Job stops with a recognizable schema-change error.** Best.
- **Job continues, emits rows under the cached schema, ignores new
  columns until restart.** Fine.
- **Job continues but emits malformed rows or crashes with type errors
  mid-stream.** Not acceptable; I.4 has to fix.

The schema-cache fix (if needed) is probably in the Flink-Fluss catalog
or `FlinkSourceSplitReader`, not in the server. The server is already
per-batch-schema-aware via Phase H.

### 6.3 Per-Flink-version coverage

Decision (§4.4): one version. If a regression appears in another
version later, the standard
`./mvnw install -pl fluss-flink/fluss-flink-X.Y` debug pattern from
Phase F's stale-shaded-jar gotcha applies.

### 6.4 Catalog vs source: where does projection pushdown live?

Unknown without reading the code. `FlinkSourceSplitReader` is the
likely site (it's the per-bucket reader), but the catalog's
`getScanRuntimeProvider` or similar might intercept projection first.
I.2 will surface this if there's a wiring issue.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Flink's column-pruning silently drops columns the user "didn't reference" but the merger needs (e.g. it might prune base columns when projecting only enrichment, expecting the source to handle it). | Server-side merger output is in the projected RowType. Same path tested under Java-client reads. Phase E.4 verifies the gate; I.2 verifies it under Flink. |
| `FlinkSourceSplitReader` and `TieringSplitReader` diverge — Phase F.2 set projection in tiering but the regular reader has its own path. | The two readers are intentionally different (tiering forces full projection; the regular reader honors the user's projection). The shared component is the underlying `LogScanner`, which Phase E.4 already gates correctly. |
| Mid-job alter introduces a schema-cache inconsistency between the catalog and the running source operator. | §4.2 sets the policy: rely on Flink's restart-on-checkpoint mechanism. I.4 confirms this works. |
| Adding "Flink-native enrichment writes" later requires DDL changes Phase I closed off. | Non-goal explicitly documented. Future ADR (Phase J?) can revisit without breaking I. |

## 8. Test strategy

- **I.2 ITCase** — `FlinkSqlColumnGroupITCase` (or extend an existing
  Flink SQL ITCase):
  1. `admin.createTable(...)` with one column group of two columns.
  2. Write 10 base + 5 enrichment via Java client (same setup as F.4).
  3. Retry until `CEW = 5`.
  4. Submit `SELECT device_id, geo_region FROM cg_table` via Flink SQL
     → assert 5 rows, `geo_region = "US-WEST-<i>"`.
  5. Submit `SELECT device_id FROM cg_table` via Flink SQL → assert
     10 rows (base-only projection, gated at HW).
  6. Submit `SELECT * FROM cg_table` → assert 5 rows with enrichment
     populated.

- **I.4 schema-evolution ITCase** — start a streaming `SELECT * FROM
  cg_table` Flink job; while running, `admin.alterTable(... addColumn
  ... TO GROUP enriched)`; write v2-enriched rows; assert one of:
  (a) job terminates with a recognizable error, (b) job continues
  and emits v1 rows correctly (new column visibly null in output).

- **I.5 — no tests, pure docs** — code example for "Flink writes base
  rows; separate Java-API service writes enrichment."
