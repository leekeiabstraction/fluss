# Phase K — Flink DDL for column groups

**Status:** ADR — K.1 (this doc).
**Authors:** option02-lateMaterialized branch.
**Depends on:** Phase H (column-group schema mutation API), Phase I
(Flink connector read path).

## 1. Context

Phase I deferred `CREATE TABLE` DDL extensions in Flink for declaring
column groups (PHASE_I §4.3). The expected pattern was "create the
table via Java admin API; query via Flink SQL." That works but is
asymmetric — most Fluss tables are declared via Flink SQL DDL today.
Phase K closes this surface.

The Java-side mechanics already exist: `Schema.Builder.column(...)
.columnGroup(name)` tags columns to groups, and the catalog's
table-creation path already converts a Flink `CatalogTable` to a Fluss
`TableDescriptor`. Phase K adds (a) a parser for a new property,
(b) the apply logic when building the Fluss schema, and (c) the
inverse emit so `SHOW CREATE TABLE` round-trips.

## 2. Goals & non-goals

### Goals

1. Users can declare column groups in Flink `CREATE TABLE ... WITH`
   properties.
2. `SHOW CREATE TABLE` on a column-group table emits the equivalent
   properties so a Flink user can copy the DDL back out.
3. Malformed DDL is rejected loudly with `ValidationException` at
   create time, not at write/read time.
4. No regression for plain log tables — tables without
   `column-groups.<g>` properties build byte-identical descriptors.

### Non-goals

- **`ALTER TABLE ... ADD COLUMN ... TO GROUP <g>`** Flink DDL.
  Phase H's Java API (`TableChange.addColumn(..., groupName)`) is the
  surface today. A SQL syntax for it is a separate piece — needs
  parser extensions or a workaround via Flink's `ADD COLUMN <name>
  <type>` with WITH-clause overrides, which Flink doesn't natively
  support for column annotations.
- **Removing groups at create time.** Groups are declared by listing
  columns; an empty group is structurally meaningless. Same scope
  decision as Phase H §4.2.
- **Round-trip-perfect identifier escaping.** Comma-separated lists
  inside a property string don't tolerate column names containing
  commas. Acceptable for the column-group feature scope (no such
  identifiers exist in practice).
- **Partitioned column-group tables.** Carried-through scope decision
  from Phase A onwards.

## 3. Architecture

### 3.1 Property shape

```sql
CREATE TABLE cg_table (
    device_id  INT,
    payload    STRING,
    geo_region STRING,
    risk_score DOUBLE
) WITH (
    'connector' = 'fluss',
    'column-groups.enriched' = 'geo_region, risk_score'
);
```

- Key: `column-groups.<groupName>`.
- Value: comma-separated list of column names. Whitespace around
  commas is trimmed.
- Multiple groups → multiple properties
  (`column-groups.enriched`, `column-groups.fraud_scores`, ...).

### 3.2 Create-table conversion

In the catalog's table-creation code path (the converter from Flink
`CatalogTable` to Fluss `TableDescriptor`):

1. Iterate the table options, collect keys matching
   `^column-groups\.([^.]+)$`. Capture the group name from the suffix.
2. For each, split the value on `,`, trim each token. Empty result →
   reject.
3. Build `Map<columnName, groupName>` and verify:
   - Every referenced column exists in the schema.
   - No column appears in two groups.
   - `groupName` is non-empty (re-uses Phase H.5's reservation rule).
4. When the converter builds the Fluss `Schema`, after each
   `.column(name, type)` call, look up the column in the map and apply
   `.columnGroup(groupName)` if present.
5. Strip the `column-groups.*` keys from the user-visible properties
   stored on the descriptor (they're internalized into the schema).

### 3.3 Get-table conversion

In the catalog's table-reading code path
(`TableInfo` → Flink `CatalogTable`):

1. Read `tableInfo.getSchema().getColumnGroups()` (existing accessor —
   returns `Map<String, List<Integer>>`).
2. For each `(groupName, columnIndices)`, materialize the column-name
   list by indexing into the schema's columns.
3. Emit `column-groups.<groupName>` = `col1, col2, ...` as a synthetic
   property in the returned `CatalogTable`'s options.

Round-trip via `SHOW CREATE TABLE` then works for free — Flink prints
the options it sees on the `CatalogTable`.

## 4. Decisions

### 4.1 Property prefix: `column-groups.<g>` (plural)

Mirrors the existing `WITH (...)` style: hierarchical dot-separated
keys with hyphens. Plural matches the *singular value per key* pattern
(each `<g>` is one group; the prefix is over the set of groups, hence
plural). Consistent with conventions like `auto-partition.time-unit`.

### 4.2 No default-group property

Columns not mentioned in any `column-groups.<g>` are base/default
columns automatically. Re-uses the existing implicit-default-group
semantics. No `column-groups.default` reservation (consistent with
Phase H §6.1's deferral).

### 4.3 Strip the keys after applying

The `column-groups.*` properties are internalized into the schema
metadata once parsed. They should NOT appear in the
`tableDescriptor.getProperties()` map after conversion — otherwise
they'd be persisted twice (once in schema, once in properties).

Symmetric on the read path: `getTable` *re-synthesizes* them from the
schema's group metadata. There is no `column-groups.*` round-trip
through the property dictionary at all.

### 4.4 Rejection at create time

All validation runs during `createTable`, throwing
`ValidationException` with a clear message. Catching errors at
create time is much better DX than at write/read time (where the
failure would surface as "Unknown column group" from the AppendWriter
or as a mismatched schema from a query).

### 4.5 No `ALTER TABLE` syntax

Phase H's `TableChange.addColumn(... groupName)` is reachable via the
Java admin API. Surfacing it via Flink SQL is a meaningfully larger
piece (extending Flink's `ALTER TABLE ADD COLUMN` parser to accept a
`TO GROUP <g>` clause is not a Fluss-only change). Defer.

## 5. Phasing

```
K.1  ADR (this doc)
K.2  Catalog: parse + apply `column-groups.<g>` properties on
     createTable; emit them on getTable; validation suite.
K.3  ITCase: end-to-end Flink DDL → query → SHOW CREATE TABLE
     round-trip on a column-group table.
```

K.2 and K.3 are tightly coupled (the ITCase exercises the new
property). K.1 lands first; K.2 and K.3 may land together.

## 6. Open questions

### 6.1 Where does the conversion live?

Unknown without reading the code. Candidates:
`fluss-flink-common/.../catalog/FlinkCatalog.java` for the catalog
itself, or a dedicated converter (`FlinkTableFactory`?). K.2's first
step is to find the right site.

### 6.2 Property visibility in tests

Existing catalog tests likely round-trip properties. Verify that
stripping `column-groups.*` from the visible properties doesn't break
any assumption made by `FlinkCatalogITCase`.

### 6.3 Edge cases

- Column name with a leading/trailing space inside the comma list:
  trim aggressively, accept.
- Group name with a `.`: would collide with the prefix parser
  (`column-groups.a.b` is ambiguous). Reject group names containing
  `.`.
- Case-sensitivity of column names: Flink SQL identifiers default to
  case-insensitive (folded); Fluss matches the user's case. Match
  what the existing catalog does for column references — probably
  case-sensitive within stored schema. Verify in K.2.

### 6.4 Mixing with `PRIMARY KEY`

Column-group tables are log-only by Phase A scope. A `CREATE TABLE`
with both `column-groups.<g>` and `PRIMARY KEY (...)` should be
rejected loudly. Add the check in K.2 validation.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Stripping `column-groups.*` from properties confuses Flink's `CatalogTable` equality / change-detection (e.g. on `ALTER TABLE` no-op). | The schema's group metadata is the source of truth. `getTable`'s re-synthesis means the returned `CatalogTable` always agrees. K.3 round-trip test confirms. |
| Existing tests rely on full property pass-through. | Stripping is narrowly limited to the `column-groups.*` prefix. K.3 + the full `FlinkCatalogITCase` regression confirms no impact. |
| Property name `column-groups.<g>` conflicts with some other Fluss/Flink reserved key. | Existing property prefixes in Fluss are checked against `ConfigOptions` constants; no current key starts with `column-groups`. |
| User declares `column-groups.foo = ''` (empty value): get an empty group with no columns. | Validation rejects empty value (Phase H.5 already rejects empty group name). |

## 8. Test strategy

- **K.3 ITCase** in `FlinkTableSourceITCase`:
  - Create table via Flink DDL with `column-groups.enriched = '...'`.
  - Write 10 base + 5 enrichment via Java client (Phase I.2 pattern).
  - Query via Flink SQL with explicit enrichment projection — assert
    5 rows merged at CEW (verifies the column-group metadata
    propagated from DDL into the server-side schema).
  - `tEnv.executeSql("show create table cg_ddl")` — assert the
    output contains the original `column-groups.enriched` property.
- **K.3 negative cases**:
  - `column-groups.empty = ''` → ValidationException.
  - `column-groups. = 'a, b'` (empty group name) → ValidationException.
  - `column-groups.x = 'no_such_col'` → ValidationException.
  - `column-groups.x = 'a, a'` (duplicate column) → ValidationException.
  - `column-groups.x = 'a'` + `column-groups.y = 'a'` (column in two
    groups) → ValidationException.
  - `column-groups.bad.name = 'a'` (dot in group name) →
    ValidationException.
  - `CREATE TABLE ... PRIMARY KEY (a), column-groups.x = '...'`
    → ValidationException.
