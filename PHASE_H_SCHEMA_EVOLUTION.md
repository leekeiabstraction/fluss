# Phase H — Schema evolution for column groups

**Status:** ADR — H.1 (this doc).
**Authors:** option02-lateMaterialized branch.
**Depends on:** Phase B (column-group schema), Phase E (per-(group, batchSchemaId) decoder dispatch — commit `63e5a12f`).

## 1. Context

The decoder dispatch infrastructure for mixed-version enrichment segments
landed during Phase E (commit `63e5a12f` — `EnrichmentMerger` keyed by
`(groupName, batchSchemaId)`, name-based column lookup). It was built for
a future where `alterTable` mutates a column group's row type. That
future never arrived in Phase E or F: today's `SchemaUpdate.addColumn`
appends columns to the *base* (default) group only, and the other three
schema-change ops (`dropColumn`, `modifyColumn`, `renameColumn`) all
throw `SchemaChangeException("Not support … now.")`.

Consequence: there is no production path that produces mixed-shape
enrichment batches in a single segment, and the deferred E.7
schema-evolution ITCase has no way to drive the dispatch logic. Phase H
closes that gap with the smallest viable change.

## 2. Goals & non-goals

### Goals

1. A user can `ALTER TABLE … ADD COLUMN <c> <t>` and target an existing
   column group, bumping the table's schemaId and producing a v2 schema
   whose group has one more column.
2. Enrichment writes after the alter encode rows under v2; reads with
   full projection see both v1 and v2 segments correctly merged. Old
   rows have NULL for the new column; new rows have the value the
   producer wrote.
3. The deferred E.7 schema-evolution ITCase can land — it drives the
   v1 → v2 transition end-to-end and asserts dispatch.
4. No regression for plain `addColumn` (base column at end of table) —
   existing callers continue to work byte-identically.

### Non-goals

- **Drop / modify / rename of group columns.** All three currently throw
  `SchemaChangeException`. Lifting them is unrelated to dispatch
  correctness and not required by any deferred test. Defer.
- **Drop / rename of an entire group.** Same reasoning.
- **Partitioned column-group tables.** Phase A→F was log-tables-only on
  the bucketed (non-partitioned) path. Same scope here.
- **KV / primary-key tables.** Out of scope of the column-group feature
  to date.
- **Lake schema evolution.** When Fluss bumps v1 → v2, the Iceberg /
  Paimon / Lance schema needs to bump too. Iceberg supports it natively;
  Paimon mostly; Lance is constrained. Plausible follow-up; not in H.

## 3. Architecture

### 3.1 The single new operation

```
ALTER TABLE <t> ADD COLUMN <c> <type> [TO GROUP <groupName>]
```

When `groupName` is omitted, behavior is unchanged from today (column
joins the default base group at end of table). When `groupName` is
provided:

- If `groupName` already exists in the table's column-group set, the new
  column is appended to that group's column list.
- If `groupName` does not exist, a new column group is created with this
  column as its sole member.

In either case the new column is appended at the *end of the table's
column list* and the schemaId increments by one (existing alterTable
behavior).

### 3.2 Wire-format change

`PbAddColumn` proto gains one `optional string column_group = 5;`. Old
clients send no field; old servers ignore unknown fields (proto2). Pure
wire-compat extension.

### 3.3 Read path — what doesn't change

The Phase E `EnrichmentMerger` already keys decoders by
`(groupName, batchSchemaId)`. After H, a real production path produces
two segment shapes for a single group — a v1 segment and a v2 segment —
so the existing key actually distinguishes things at runtime.

For every projected output row at offset N:

1. Look up `(groupName, segment.schemaId)` decoder.
2. Decoder reads the columns *that exist in the segment's schema*,
   placed by name into the projected output's slots.
3. Slots in the projected output that the segment doesn't carry (e.g.
   v2's `risk_score` when reading a v1 segment) are left NULL.

This is standard schema-evolution semantics and the dispatch
infrastructure already implements it. H exists to drive it, not change
it.

### 3.4 Write path — what changes for the producer

`AppendWriter.appendColumns(group, bucket, sourceOffset, row)` already
accepts a row matching the *current* group RowType. After an alter to
v2, callers must supply v2-shaped rows (with the new column). Old
producers running against a mid-migration cluster either:

- Bump their schema knowledge before the alter, or
- Continue writing v1 rows — these will be rejected by the validator
  (row arity mismatch) once the schema bumps, which is loud-fail.
  The producer must redeploy with the v2 row shape.

This matches every other existing schema-evolution behavior in Fluss.

## 4. Decisions

### 4.1 `addColumn` overload, not a new factory

`TableChange.addColumn(name, type, comment, position)` gets a sibling
overload `addColumn(name, type, comment, position, groupName)` and the
existing `AddColumn` class gains a `@Nullable groupName` field. The
existing factory delegates with `groupName = null`. No new top-level
operation, no fluent builder.

Rationale: minimum API surface. A fluent `addColumn(...).inGroup("g")`
would require a new mutable returned-builder pattern that none of the
other `TableChange` factories use.

### 4.2 New group is created on first add, not declared separately

A user who wants a new group `"X"` adds a column to it — that's the
declaration. There is no separate `TableChange.createColumnGroup(...)`.

Rationale: a column group with zero columns is structurally meaningless.
Coupling group creation to first column add eliminates an empty-group
edge case and one operation.

### 4.3 SchemaUpdate.addColumn is the only validator that changes

`Schema.Builder.column(name, type).columnGroup(groupName)` already
exists. `SchemaUpdate` just needs to call it when `groupName != null`.
No changes to the schema persistence path or to ZK serialization —
column-group metadata is already part of `Schema`.

### 4.4 Position constraint stays at `last()`

Today's `SchemaUpdate.addColumn` only accepts `ColumnPosition.last()`.
H keeps that — the new column lands at the end of the table's column
list regardless of which group it's tagged to. `last()` in this
context means "last in the table"; the group is a tag, not a placement
constraint.

Rationale: any other position introduces field-id juggling for nested
types (see `Schema.Builder.fromColumns` field-id preservation logic).
Out of scope for H.

## 5. Phasing

```
H.1  ADR (this doc)                      ✓ this commit
H.2  TableChange.AddColumn.groupName +
     SchemaUpdate group-aware            (next)
H.3  Wire format: PbAddColumn.column_group
     + client/server util plumbing        (next)
H.4  Schema-evolution ITCase              (drives v1 → v2 dispatch end-to-end)
H.5  Negative tests                       (validator rejects ambiguous calls)
```

H.2 and H.3 are coupled (the proto change has to land with the Java
overload that fills it). H.4 is the deliverable that makes the
deferred E.7 dispatch ITCase real. H.5 is polish.

## 6. Open questions

### 6.1 Default-group ambiguity

A user does `addColumn(... groupName="default")` where `"default"`
collides with the conventional name for the base group. Today the
base/default group has no externalized name in `Schema.getColumnGroups()`
(its returned map only contains user-declared groups; the unnamed base
columns come from `Schema.getDefaultGroupColumnIndices()`).

**Tentative resolution:** if `groupName.equals("default")` (or any
reserved name we pick), reject with `InvalidAlterTableException`. Same
class as today's "Column already exists." rejection.

### 6.2 What if `groupName` is set but the table has no column groups yet

The first column with `groupName="X"` creates group `"X"`. Schema after
the alter has one user-declared group with one column. This is allowed
and matches §3.1.

### 6.3 Mixed-version segment flush boundary

The H.4 ITCase needs to provoke a *real* v1 segment on disk before the
alter — otherwise the v2 write subsumes everything and dispatch is
never exercised. Plausible mechanisms:

- Force a flush on the leader's enrichment segment between v1 writes
  and the alter. Most direct; needs an internal helper.
- Use enough v1 enrichment writes to roll the segment via the size
  threshold. Slow and config-fragile.
- Restart the leader after v1 writes, then alter, then continue with
  v2 writes. Heavy but realistic.

**Tentative resolution:** flush via the same path E uses internally
when EWM advances; expose a test-only helper if no public path exists.
Resolve in H.4.

### 6.4 Producer rejection on row arity mismatch

If a producer writes a v1-shaped row (one fewer column) after the
schema bumps to v2, the server's `appendColumns` validator should
reject it with a clear error.

**Tentative resolution:** the validator is the same one that handles
all `appendColumns` rows — it checks arity against the *current*
group RowType. If it currently throws something useful, no change.
If not, H.5 adds a clearer error message.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Producer continues writing v1 rows after server-side alter; rows are silently truncated to v1 columns. | The validator should reject on arity mismatch with a loud error, not truncate. H.5 verifies. |
| Old enrichment segments on disk reference a v1 schemaId that the schema cache no longer holds (schema GC). | Schema cache is keyed by schemaId and persists every version; existing behavior. Verified by reading `SchemaCache` to confirm versions are not GC'd while segments reference them. |
| `Schema.Builder.column(...).columnGroup(name)` is order-sensitive. A future `SchemaUpdate` change that batches multiple ops in one alterTable could break that ordering. | H.2 keeps additions strictly sequential; multi-op alterTable is out of scope. |
| Cross-module install gotcha (PHASE_F §5.2). Touching `fluss-rpc` proto requires regenerating across `fluss-rpc`, `fluss-server`, `fluss-client`, plus `fluss-flink-1.X`. | Documented; standard install incantation applies. |

## 8. Test strategy

- **H.2 unit:** `SchemaUpdateTest.addColumnToExistingGroup` — start with
  a 4-col schema (2 base + 2 in group "enriched"), apply
  `addColumn("risk_score_v2", DOUBLE, ..., last(), "enriched")`, assert
  v2 has 5 cols and group "enriched" now contains
  `[geo_region_idx, risk_score_idx, risk_score_v2_idx]`.
- **H.2 unit:** `SchemaUpdateTest.addColumnCreatingNewGroup` — start
  with a 2-col base-only schema, add one column with `groupName = "g"`,
  assert v2 has 3 cols and one user-declared group `"g"` with the new
  column.
- **H.3 wire-format roundtrip:** `ClientRpcMessageUtilsTest` /
  `ServerRpcMessageUtilsTest` — encode an `AddColumn` with `groupName`
  set, send through `toPbAddColumn` → `toAddColumns`, assert the
  groupName survives.
- **H.4 ITCase** (`SchemaEvolutionITCase` or extend
  `ColumnGroupEWMITCase`):
  1. Create table with group `"enriched" = {geo_region}`.
  2. Append 5 base rows + enrich offsets 0..4 under v1.
  3. Force a v1 segment flush.
  4. `admin.alterTable(... addColumn("risk_score", DOUBLE, ..., last(), "enriched"))`.
  5. Append 5 more base rows + enrich offsets 5..9 under v2.
  6. Read with full projection `[device_id, geo_region, risk_score]`.
  7. Assert offsets 0..4 have `risk_score = NULL`; offsets 5..9 have
     the values written.
- **H.5 negative:** producer-side arity mismatch → loud rejection.
  Reserved-name groupName → loud rejection.
