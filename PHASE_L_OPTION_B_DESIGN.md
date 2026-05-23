# Phase L Option B — Implementation design

**Status:** L.1–L.5 landed.
**Authors:** option02-lateMaterialized branch.
**Depends on:** [PHASE_L_FLINK_ENRICHMENT_WRITES.md](PHASE_L_FLINK_ENRICHMENT_WRITES.md)
(option survey + decision).

PHASE_L §4 chose Option B (separate write-only enrichment table).
This doc nails down the concrete implementation: where each piece
lives, what it does at plan time vs. runtime, and how the pieces
compose end-to-end.

## 1. End-to-end picture

A column-group table on the Fluss side surfaces as **two** Flink
tables:

```
+---------------------+        +-------------------------------+
| cg_base (READ)      |        | cg_enriched_writes (WRITE)    |
| - device_id   INT   |        | - src_bucket  BIGINT          |
| - payload     STRING|        | - src_offset  BIGINT          |
| - geo_region  STRING|        | - geo_region  STRING          |
| - risk_score  DOUBLE|        | - risk_score  DOUBLE          |
| - _bucket  (METADATA)        | WITH:                         |
| - _offset  (METADATA)        |   enrichment.target = cg_table|
| WITH:                        |   enrichment.group  = enriched|
|   column-groups.enriched=... +-------------------------------+
+---------------------+

         |                                      ^
         |    SELECT _bucket, _offset, payload   |  INSERT INTO ...
         +------- > Flink streaming job >--------+
```

Both tables share the same underlying Fluss table identifier (the
write-only table's `enrichment.target` resolves to the base table).
The read side is closed by Phases I + K; the write side is what
Phase L adds.

A single Flink SQL job:

```sql
INSERT INTO cg_enriched_writes
SELECT b._bucket,
       b._offset,
       lookup_geo(b.payload),
       score_risk(b.payload)
FROM   cg_base AS b
WHERE  b.geo_region IS NULL;
```

…drives the entire enrichment pipeline. No DataStream code, no
custom sink function, no procedural Java.

## 2. Components

### 2.1 Source side: `bucket` / `offset` as METADATA columns

**File:** `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/source/FlinkTableSource.java`
(line 107). Currently implements `ScanTableSource`,
`SupportsProjectionPushDown`, etc., but **not** `SupportsReadingMetadata`.

L.2 adds `SupportsReadingMetadata`:

```java
public class FlinkTableSource implements
        ScanTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsReadingMetadata,        // ← NEW
        ... {

    @Override
    public Map<String, DataType> listReadableMetadata() {
        Map<String, DataType> m = new LinkedHashMap<>();
        m.put("bucket", DataTypes.BIGINT().notNull());
        m.put("offset", DataTypes.BIGINT().notNull());
        return m;
    }

    @Override
    public void applyReadableMetadata(
            List<String> metadataKeys, DataType producedDataType) {
        this.producedMetadataKeys = metadataKeys;   // remembered for runtime
        this.producedDataType    = producedDataType;
    }
}
```

The `FlinkSourceSplitReader` (Phase I) already has access to each
record's `(bucket, offset)` — it reads from `LogScanner` results.
L.2 wires the metadata columns through the projection: the reader
appends `bucket` and `offset` to the emitted row when the
corresponding metadata keys are in `producedMetadataKeys`.

Metadata columns are *opt-in*: a Flink table with no `METADATA FROM
'bucket'` declaration in its DDL is byte-identical to today.

### 2.2 Sink-factory dispatch

**File:** `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/catalog/FlinkTableFactory.java`
(line 173, `createDynamicTableSink`).

The current factory unconditionally builds a `FlinkTableSink`
(line 198). L.3 adds a branch at the top of the method:

```java
public DynamicTableSink createDynamicTableSink(Context context) {
    FactoryUtil.TableFactoryHelper helper = ...;
    final ReadableConfig tableOptions = helper.getOptions();

    String enrichmentTarget = tableOptions.get(ENRICHMENT_TARGET);
    if (enrichmentTarget != null) {
        return buildEnrichmentSink(context, tableOptions);
    }

    // existing base-sink path unchanged
    ...
    return new FlinkTableSink(...);
}
```

`buildEnrichmentSink(...)`:

1. Parses `enrichment.target` → `TablePath` (`db.tableName`).
2. Resolves the target via `Admin.getTableInfo()` — fetches the
   current `SchemaInfo`.
3. Validates `enrichment.group` exists on the target's schema.
4. Extracts the target group's column list (name, type, order).
5. Compares against this sink table's columns (minus the leading
   two: `src_bucket BIGINT`, `src_offset BIGINT`). Mismatch →
   `ValidationException` at plan time.
6. Builds an `EnrichmentTableSink` carrying:
   - `tablePath` (target),
   - `groupName`,
   - the target's `tableId` + `schemaId` (snapshot at plan time),
   - the Fluss client `Configuration`.

The validation is *plan-time only*. The runtime sink does not
re-validate; it trusts the schema check passed.

### 2.3 Sink runtime

**New file:** `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/sink/EnrichmentTableSink.java`

Mirrors `FlinkTableSink`'s structure (Phase I sink). Differences:

- No partitioning / bucket-key concerns — the bucket comes from the
  *first column of each row*.
- No primary key path — column-group tables are log-only.
- The sink-writer's `write(RowData row)` does:

```java
long bucket   = row.getLong(0);
long offset   = row.getLong(1);
GenericRowData valueRow = projectRest(row, 2, schema.getFieldCount());
appendWriter.appendColumns(
        (int) bucket, offset, groupName, valueRow);
```

`AppendWriter` itself is Phase C's existing API; no Fluss-side changes.

**New file:** `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/sink/writer/EnrichmentSinkWriter.java`

Implements Flink's `SinkWriter<RowData>`:
- `open()` — establish `Connection`, get `Table`, get `AppendWriter`.
- `write(row)` — extract bucket/offset, call `appendColumns`.
- `flush(endOfInput)` — drain pending writes.
- `close()` — close writer, table, connection.

For 2PC / exactly-once semantics, the existing
`AppendSinkWriter`'s checkpoint-aware flushing is the reference;
`EnrichmentSinkWriter` reuses the same pattern (snapshot all
pending writes at checkpoint barrier, ack only after flush succeeds).

### 2.4 Catalog: persistence of `enrichment.*` properties

Unlike `column-groups.*` (which Phase K strips and re-synthesizes),
`enrichment.target` and `enrichment.group` are real, user-visible
properties on the write-only table:

- They are persisted to the underlying Fluss `TableDescriptor`'s
  custom-properties map (no special handling in `FlinkConversions`).
- `SHOW CREATE TABLE cg_enriched_writes` emits them back verbatim
  — they round-trip through the property dictionary unchanged.
- Validation that the target actually exists happens at *sink-build*
  time, not at *catalog create-table* time. A user can declare a
  write-only table whose target doesn't exist yet; the failure
  surfaces when they first try to insert.

(Why not validate at create-table time too? Because Flink's catalog
operations don't have access to the Fluss `Admin` in a clean way
through the standard `Catalog` interface. Sink-build-time
validation is sufficient — the user can't actually *use* a broken
write-only table.)

## 3. Property keys

```java
// fluss-flink/fluss-flink-common/.../catalog/FlinkConnectorOptions.java

public static final ConfigOption<String> ENRICHMENT_TARGET =
    ConfigOptions.key("enrichment.target")
        .stringType()
        .noDefaultValue()
        .withDescription(
            "Marks this table as a write-only enrichment target. "
            + "Value: fully-qualified Fluss table name "
            + "(e.g. `default.cg_table`).");

public static final ConfigOption<String> ENRICHMENT_GROUP =
    ConfigOptions.key("enrichment.group")
        .stringType()
        .noDefaultValue()
        .withDescription(
            "Required when enrichment.target is set. Name of the "
            + "column group on the target table to write to.");
```

**Validation rules:**

- `enrichment.target` present without `enrichment.group` →
  `ValidationException`: "enrichment.target requires enrichment.group".
- `enrichment.group` present without `enrichment.target` →
  `ValidationException`.
- Both present + sink table has primary key → `ValidationException`
  (column-group tables are log-only, so primary-key sink semantics
  don't apply).

## 4. Source-side `SELECT` on a write-only table

When `enrichment.target` is set, the table factory's
`createDynamicTableSource` rejects:

```java
public DynamicTableSource createDynamicTableSource(Context context) {
    String enrichmentTarget = ...;
    if (enrichmentTarget != null) {
        throw new ValidationException(
            "Table " + context.getObjectIdentifier()
            + " is a write-only enrichment target for column group '"
            + enrichmentGroup + "' on " + enrichmentTarget
            + ". To read enriched data, query "
            + enrichmentTarget + " directly.");
    }
    ...
}
```

The error message contains both the *what* (write-only) and the
*remedy* (query the target).

## 5. Schema validation (sink-build time)

The validator's input:
- `sinkSchema` — this table's resolved schema (from
  `context.getCatalogTable().getResolvedSchema()`).
- `targetSchema` — fetched at plan time via `Admin.getTableInfo()`.
- `groupName` — from `enrichment.group`.

Steps:

1. Find the target group's columns in `targetSchema.getColumnGroups()`.
   Missing → `"column group '<group>' not declared on '<target>'"`.

2. Strip the first two columns from `sinkSchema` and confirm they are:
   - `BIGINT NOT NULL` (or `BIGINT`) — both `src_bucket` and `src_offset`.
   - Names are free (user can call them `bucket`/`offset`/`_bucket`/
     anything) — only types + position matter.

3. Compare remaining columns to the target group's columns by:
   - Same count.
   - Same names, in same order.
   - Same logical types (using the same comparator as Phase H.5's
     schema-mutation API).

4. Mismatch → `ValidationException` with the diff.

### 5.1 Why name-equality, not type-only?

Equality on names trades flexibility for clarity. If the user writes:

```sql
CREATE TABLE cg_writes (src_bucket BIGINT, src_offset BIGINT,
                        region STRING,    -- ← user wrote "region"
                        risk   DOUBLE)    -- ← user wrote "risk"
WITH ('enrichment.target' = 'cg_table',
      'enrichment.group'  = 'enriched');
```

…against a target that has `geo_region` / `risk_score`, we *could*
match by position alone. But the error "rename `region` →
`geo_region`" is clearer than "type mismatch on column 3."
Lean strict by default.

## 6. Schema evolution interaction

When the target's column group evolves (Phase H,
`ALTER TABLE cg_table ADD COLUMN ... TO GROUP enriched`), the
write-only sibling does **not** auto-update. The user must:

```sql
ALTER TABLE cg_enriched_writes ADD COLUMN device_score DOUBLE;
```

…to mirror the new column on the writes side. Until they do,
INSERT statements fail validation at plan time with:

> Sink schema does not match target group 'enriched' on
> 'default.cg_table': target has 3 enrichment columns (geo_region,
> risk_score, device_score), sink has 2 (geo_region, risk_score).

(PHASE_L §6.3 raised auto-mirror / dynamic-schema as alternatives.
This design defers them — explicit ALTER on both sides is one extra
step but keeps the model simple. Revisit if user feedback shows
friction.)

## 7. Decisions (recap, with rationale)

| # | Decision | Why |
|---|---|---|
| 1 | One write-only table per `(target, group)` | Simpler than per-row dispatch; static schema check at plan time |
| 2 | Property keys: `enrichment.target` (qualified name) + `enrichment.group` | Mirrors `column-groups.<g>` namespace convention from Phase K |
| 3 | Sink-side schema: leading 2 cols `(BIGINT bucket, BIGINT offset)`, then group cols in order | Position-based addressing primitives + name-matched value cols |
| 4 | `bucket`/`offset` exposed as `METADATA` on the source side | Idiomatic Flink; opt-in (no impact on existing reads) |
| 5 | `SELECT` on write-only table → `ValidationException` | Clear write-only semantics; error points to the right table |
| 6 | Validation at sink-build, not catalog-create | Flink Catalog API doesn't have Admin access; sink-build catches all failures before runtime |
| 7 | Strict name+type+position equality in schema validation | Clearer error messages than position-only |
| 8 | Schema evolution: manual ALTER on both sides | Simpler than auto-mirror; revisit if users push back |
| 9 | Inherit Phase I 2PC checkpoint pattern | `EnrichmentSinkWriter` reuses `AppendSinkWriter`'s flush/commit cadence |

## 8. Phasing

```
L.1  ADR (PHASE_L survey) + this design doc          ✓ done (this commit)

L.2  Source-side METADATA columns                    TBD
     Files:
       fluss-flink/.../source/FlinkTableSource.java
       fluss-flink/.../source/reader/FlinkSourceSplitReader.java
     - Implement SupportsReadingMetadata
     - Add bucket/offset metadata keys
     - Wire through reader projection
     - ITCase: read with METADATA cols; assert pushdown
       still works when META cols absent

L.3  Sink-factory branch + EnrichmentTableSink       TBD
     Files (new):
       fluss-flink/.../sink/EnrichmentTableSink.java
       fluss-flink/.../sink/writer/EnrichmentSinkWriter.java
     Files (modified):
       fluss-flink/.../catalog/FlinkTableFactory.java
       fluss-flink/.../catalog/FlinkConnectorOptions.java
     - Add ENRICHMENT_TARGET / ENRICHMENT_GROUP options
     - Branch in createDynamicTableSink
     - Schema validation against target's current schema
     - Implement sink + writer
     - ITCase: end-to-end SQL enrich pipeline

L.4  Validation negatives                            TBD
     - target doesn't exist
     - group doesn't exist on target
     - schema mismatch (count / order / name / type)
     - PK declared on write-only table
     - SELECT on write-only table
     - enrichment.target without enrichment.group (and vice versa)

L.5  Docs                                            ✓ landed in this commit
     - Two-table recipe + end-to-end SQL example in
       website/docs/engine-flink/writes.md
     - Column-groups DDL section in ddl.md
     - CEW read semantics in reads.md
```

L.2 lands first (the sink ITCase needs metadata columns on the read
side to produce a useful end-to-end example).

L.3 + L.4 can land together (the negative cases are part of the
factory's validation logic, naturally tested alongside the happy path).

## 9. Test strategy

### L.2 — metadata column ITCase

In `FlinkTableSourceITCase`:

```java
@Test
public void testColumnGroupTableBucketOffsetMetadata() {
    // CREATE TABLE cg_base (..., _bucket BIGINT METADATA FROM 'bucket'
    //                            VIRTUAL,
    //                       _offset BIGINT METADATA FROM 'offset'
    //                            VIRTUAL)
    // WITH ('column-groups.enriched' = '...');
    // Write 10 base rows via Java client at known (bucket, offset).
    // SELECT _bucket, _offset, device_id FROM cg_base
    //   → assert 10 rows, each with the bucket/offset it was written at.
    // SELECT device_id FROM cg_base  (no metadata projected)
    //   → existing fast path; assert no regression.
}
```

### L.3 — end-to-end enrich pipeline ITCase

In a new `FlinkEnrichmentSinkITCase`:

```java
@Test
public void testEnrichViaSql() {
    // CREATE TABLE cg_base (...) WITH 'column-groups.enriched' = '...';
    //   - with _bucket, _offset METADATA VIRTUAL columns.
    // CREATE TABLE cg_enriched_writes (src_bucket BIGINT, src_offset BIGINT,
    //                                  geo_region STRING, risk_score DOUBLE)
    //   WITH 'enrichment.target' = 'default.cg_base',
    //        'enrichment.group'  = 'enriched';
    // Write 10 base rows via Java client.
    // Run streaming SQL job:
    //   INSERT INTO cg_enriched_writes
    //   SELECT _bucket, _offset, 'US-' || CAST(device_id AS STRING),
    //          CAST(device_id AS DOUBLE) / 10.0
    //   FROM cg_base;
    // Wait for advance of CEW.
    // SELECT * FROM cg_base → assert enrichment columns populated.
}
```

### L.4 — validation negatives

In `FlinkEnrichmentSinkITCase`:

| Test | Setup | Expect |
|---|---|---|
| `unknownTarget` | `enrichment.target = 'default.no_such_table'` | `ValidationException` at INSERT plan time |
| `unknownGroup` | `enrichment.target` ok, `enrichment.group = 'no_such_group'` | `ValidationException`, message names the missing group |
| `schemaMismatch_count` | Sink table has 1 enrichment col, target has 2 | `ValidationException`, message says "2 expected, 1 provided" |
| `schemaMismatch_name` | Sink col named `region`, target col `geo_region` | `ValidationException`, message names mismatched cols |
| `schemaMismatch_type` | Sink col `STRING`, target col `DOUBLE` | `ValidationException`, message names mismatched col + types |
| `pkOnSink` | Sink table declared with `PRIMARY KEY` | `ValidationException` at CREATE TABLE (or first INSERT) |
| `selectOnWriteOnly` | `SELECT * FROM cg_enriched_writes` | `ValidationException` with write-only message + remedy |
| `targetWithoutGroup` | Only `enrichment.target` set, no `enrichment.group` | `ValidationException` |
| `groupWithoutTarget` | Only `enrichment.group` set, no `enrichment.target` | `ValidationException` |

### L.5 — docs

User-facing connector docs: a "Writing enrichment rows from Flink SQL"
section with the end-to-end example from §1 and the schema-evolution
recipe.

## 10. Open questions

### 10.1 Cross-database `enrichment.target` values

The qualified name in `enrichment.target` may include a database
prefix (`mydb.cg_table`) or not (`cg_table`, defaulting to the
sink's own database). Standard Flink: use the sink table's database
as the default. Confirm in L.3.

### 10.2 `bucket` data type: `INT` vs. `BIGINT`

Bucket IDs in Fluss are `int`. Exposing them as `BIGINT METADATA`
is a small widening (lossless), matches `offset`'s natural type
(`long`), and avoids a per-row narrow cast in user SQL. Alternative:
expose `bucket` as `INT METADATA`. Either is correct. Lean `BIGINT`
for symmetry with `offset`.

### 10.3 Should the write-only table support `INSERT OVERWRITE`?

Probably not — overwrite semantics on append-only column-group writes
don't have a clear meaning. Reject loudly at sink-build time:
`"INSERT OVERWRITE is not supported on enrichment-target tables; "
"use INSERT INTO."`. Catch via Flink's `SupportsOverwrite` —
*don't* implement it, so the planner rejects `OVERWRITE` automatically.

### 10.4 Multiple INSERT statements per Flink job

If a single Flink job has two INSERTs targeting two different
enrichment tables (or even the same one twice), each builds its
own `EnrichmentSinkWriter`. They open independent `AppendWriter`s.
No coordination across them. Confirm via a multi-INSERT ITCase that
both sinks succeed independently.

## 11. Risks

| Risk | Mitigation |
|---|---|
| Sink validation reads target schema at plan time → planning is non-trivially I/O-bound | Cache the resolved target schema for the lifetime of the `DynamicTableSink` instance; only one fetch per plan |
| Schema cached at plan time goes stale before job starts | The job starts immediately after plan; stale window is sub-second. If the target schema *evolves* during job execution, Phase H+I's mid-query handling already covers reads; for writes, a follow-on stale-schema rejection at the server (Phase H.6) would surface as a clear error to the user |
| User declares wrong order of `src_bucket` / `src_offset` (swaps them) | Position-based validation catches it; the resulting `appendColumns(bucket=<offset>, offset=<bucket>)` either fails at the server (offset out of range) or silently writes garbage. To prevent the latter: enforce both leading cols *named* `*bucket*` and `*offset*` (regex match)? Or just require explicit names `src_bucket`/`src_offset`? Lean explicit names — decided in L.3 |
| `EnrichmentSinkWriter` connection leaks on Flink subtask failure | Inherit `AppendSinkWriter`'s `close()` discipline; ITCase covers subtask restart |
| Per-row `appendColumns` call → potentially per-row RPC | `AppendWriter` already batches internally (Phase C); `EnrichmentSinkWriter` only needs to call `flush()` at checkpoint barriers |
| Catalog property names `enrichment.*` collide with future Fluss reserved keys | No current Fluss option starts with `enrichment.`; reserve the prefix (mirrors `column-groups.` from Phase K) |
