# Phase I — Flink connector for column-group tables

**Status:** I.1 → I.5 landed and verified.
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
I.1  ADR (this doc)                                  ✓ cbb339da
I.2  ITCase: Flink SQL read of column-group table    ✓ landed alongside I.3
     ─ explicit projection of enrichment col is gated at CEW with merged values
     ─ base-only projection is gated at HW
     ─ SELECT * also gated at CEW (after the I.3 fix)
I.3  FlinkSourceSplitReader: force full projection   ✓ landed alongside I.2
     when none was applied and the table has column
     groups. Required for SELECT * to see merged
     enrichment values; see §5.2 below.
I.4  Schema-evolution ITCase under a running query   ✓ green first try
     ─ Flink streaming SELECT keeps emitting under
       cached projection after alterTable; v2 segments
       decode under v1-shaped projection via Phase H
       per-(group, batchSchemaId) dispatch.
I.5  User-facing contract + Java-API enrichment       ✓ §9 below
     pattern documentation
```

### 5.2 What I.2 surfaced and I.3 fixed

I.2 ran with three assertions: base-only projection (gated at HW),
explicit enrichment projection (gated at CEW with merged values), and
`SELECT *` (expected gated at CEW per §3.3). The first two passed on
first run. The third returned the first 5 rows with **null enrichment
columns** — the merger never fired.

Cause: Flink's planner doesn't push a projection when nothing is
pruned. `SELECT *` references every column, so the planner sends the
source no projection. `Replica.readRecords` then takes the
"no-projection" path (`enrichmentTouched = false`), returns raw
base-log records, and Flink decodes them with null enrichment columns.

Fix in `FlinkSourceSplitReader#forceFullProjectionForColumnGroups`:
when `projectedFields == null` and the table has at least one column
group, synthesize an identity projection `[0..n-1]`. The server then
takes the merge-on-read path and applies the CEW gate. Mirrors
Phase F.2's `TieringSplitReader.computeProjectionForTiering`. Plain
log tables are unaffected.

Side note: when the test asserted on the SELECT * output, Flink's
`Row#toString` formats `0.5 + 3 * 0.1` as `"0.8"`, not Java's
`"0.7999999999999999"`. The test expected `"0.8"`.

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

## 9. User-facing contract for Flink users

### 9.1 Reading column-group tables in Flink SQL

A column-group table is queryable as a regular Flink `Table`. Every
column — base and column-group — is visible in the schema and can be
referenced from SQL. There is no Flink-side concept of "column group";
the grouping is server-side metadata that controls when the merger
fires.

For a column-group table whose schema is
`(device_id INT, payload STRING, geo_region STRING TO GROUP enriched,
risk_score DOUBLE TO GROUP enriched)`:

| Flink SQL                                        | Rows returned | Why                                                                                  |
|---|---|---|
| `SELECT device_id, payload FROM cg`              | up to HW       | projection doesn't reference enrichment; server gate inactive                        |
| `SELECT device_id, geo_region FROM cg`           | up to CEW      | projection touches `geo_region` (enrichment); server fires merger, gates at CEW      |
| `SELECT * FROM cg`                               | up to CEW      | `*` covers enrichment columns; `FlinkSourceSplitReader` synthesizes full projection  |
| `SELECT risk_score FROM cg`                      | up to CEW      | same as above                                                                        |

The CEW gate is per-(bucket, group). With multiple groups, projection
touching *any* enrichment column gates at `min(CEW_g)` over the groups
the projection touches. This matches the Java-client semantics from
Phase E.4.

### 9.2 Writing to column-group tables from Flink

The Flink Fluss sink uses `AppendWriter.append(...)`. For a column-group
table, that means inserting **base rows with enrichment columns as
NULL**:

```sql
-- The schema is (device_id INT, payload STRING, geo_region STRING, risk_score DOUBLE)
INSERT INTO cg VALUES
    (1, 'p1', CAST(NULL AS STRING), CAST(NULL AS DOUBLE)),
    (2, 'p2', CAST(NULL AS STRING), CAST(NULL AS DOUBLE));
```

The enrichment columns can only be filled **later**, via the Java
client API's `AppendWriter.appendColumns(group, bucket, sourceOffset, row)`.
There is no Flink SQL surface for this today (PHASE_I §2 / §4.1).

#### Canonical enrichment pattern

A typical column-group workload runs **two jobs**:

1. **Base ingest** (Flink): `INSERT INTO cg SELECT ... FROM kafka_source`.
   Standard Flink Fluss sink. Writes base rows with enrichment columns
   null.
2. **Enrichment service** (Java): reads the base log via
   `LogScanner` (no projection — gets all rows, including unfilled
   enrichment), computes enrichment for each offset, writes via
   `appendColumns(...)`.

```java
// Sketch of the enrichment service.
try (Connection conn = ConnectionFactory.createConnection(flussConf);
     Table table = conn.getTable(TablePath.of("db", "cg"))) {
    AppendWriter writer = table.newAppend().createWriter();
    try (LogScanner scanner = table.newScan().createLogScanner()) {
        scanner.subscribeFromBeginning(0);  // bucket 0
        while (running) {
            ScanRecords records = scanner.poll(Duration.ofSeconds(1));
            for (ScanRecord r : records) {
                long offset = r.logOffset();
                InternalRow baseRow = r.getRow();
                // Compute enrichment values for this offset.
                GenericRow enrichmentRow = computeEnrichmentFor(baseRow);
                writer.appendColumns(
                        "enriched",
                        new TableBucket(tableId, 0),
                        offset,
                        enrichmentRow).get();
            }
        }
    }
}
```

The enrichment writes must arrive **strictly in source-offset order**
per group — `appendColumns` rejects out-of-order writes loudly
(`ColumnGroupEWMITCase#testAppendColumnsRejectsOutOfOrder`). A
single-threaded service per bucket is the simplest pattern.

### 9.3 Schema evolution while a Flink query is running

Phase I.4 verified that a streaming `SELECT` survives a mid-query
`alterTable` (adding a column to a group). The running query keeps
emitting rows under the **projection it computed at job-start**:

- Old projection indices remain valid in v2 (additions go to the end
  of the column list, so existing indices don't shift).
- The server-side merger uses
  `schemaGetter.getLatestSchemaInfo()` for the output row type and
  per-`(group, batchSchemaId)` decoders for inputs (Phase H §5.2 fix).
- v1 segments produce v1 enrichment values; v2 segments produce v2
  values. Both fit into the v1-shaped output (newly-added columns are
  outside the projection and not emitted).

To **see** the new column in a Flink query, the user has to restart
the job. Until restart, the running query continues with full row-set
correctness for the columns it already references — the new column is
simply invisible until reload.

### 9.4 Catalog: how column-group tables get created

Today's Flink Fluss catalog has no DDL syntax for declaring column
groups (`PHASE_I §4.3`). The expected pattern:

1. Create the table once via the **Java admin API** with the
   column-group schema:

   ```java
   Schema schema = Schema.newBuilder()
       .column("device_id", DataTypes.INT())
       .column("payload", DataTypes.STRING())
       .column("geo_region", DataTypes.STRING()).columnGroup("enriched")
       .column("risk_score", DataTypes.DOUBLE()).columnGroup("enriched")
       .build();
   TableDescriptor descriptor = TableDescriptor.builder().schema(schema)
       .distributedBy(1, "device_id")
       .build();
   admin.createTable(TablePath.of("db", "cg"), descriptor, false).get();
   ```

2. Query from Flink SQL as usual. The Flink catalog reads
   `tableInfo.getRowType()`; column groups are transparent to it.

Adding a `WITH ('column-groups.<g>' = 'col1,col2')` parser to the
catalog is a small follow-up (~30 lines) but deferred until a real
need surfaces.
