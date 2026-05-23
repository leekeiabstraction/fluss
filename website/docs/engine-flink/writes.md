---
sidebar_label: Writes
title: Flink Writes
sidebar_position: 4
---

# Flink Writes

You can directly insert or update data into a Fluss table using the `INSERT INTO` statement.
Fluss primary key tables can accept all types of messages (`INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`), while Fluss log table can only accept `INSERT` type messages.


## INSERT INTO
`INSERT INTO` statements are used to write data to Fluss tables. 
They support both streaming and batch modes and are compatible with primary-key tables (for upserting data) as well as log tables (for appending data).

### Appending Data to the Log Table
#### Create a Log Table.
```sql title="Flink SQL"
CREATE TABLE log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
);
```

#### Insert Data into the Log Table.
```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
INSERT INTO log_table
SELECT * FROM source;
```


### Perform Data Upserts to the PrimaryKey Table.

#### Create a primary key table.
```sql title="Flink SQL"
CREATE TABLE pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
);
```

#### Updates All Columns
```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
INSERT INTO pk_table
SELECT * FROM source;
```


#### Partial Updates

```sql title="Flink SQL"
CREATE TEMPORARY TABLE source (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT
) WITH ('connector' = 'datagen');
```

```sql title="Flink SQL"
-- only partial-update the num_orders column
INSERT INTO pk_table (shop_id, user_id, num_orders)
SELECT shop_id, user_id, num_orders FROM source;
```

## Enrichment Writes (Column Groups)

Column groups are subsets of a log table's columns that can be **written separately** from
the base row. A typical use case: write a base "fact" row first (with the enrichment columns
left as `NULL`), then fill in the enrichment values later from a Flink streaming job that joins
against a lookup source or runs a UDF.

Column-group tables are **log-only** — they have no primary key. Enrichment values are addressed
by the source row's `(bucket, offset)`, not by a join key. Each `(bucket, offset, group)` accepts
exactly one write; subsequent reads see the base columns merged with their enrichment values once
the per-bucket *Committed Enrichment Watermark* (CEW) advances past the offset.

### The two-table pattern

A column-group table surfaces as **two Flink tables**:

1. A **base reader** — the underlying Fluss table. Query it normally; declare the source row's
   `bucket` and `offset` as `METADATA` columns to project them downstream.
2. A **write-only enrichment target** — a separate Flink table whose connector properties point
   at the base table and name the column group to fill. `INSERT INTO` it writes enrichment
   values; `SELECT` is rejected.

The pattern composes any Flink streaming pipeline (joins, lookup sources, UDFs) with no Fluss-
specific machinery.

### Step 1 — declare the base table

```sql title="Flink SQL"
CREATE TABLE cg_base (
  device_id  INT,
  payload    STRING,
  geo_region STRING,
  risk_score DOUBLE,
  -- expose the source row's bucket and offset as opt-in METADATA columns
  _bucket BIGINT METADATA FROM 'bucket' VIRTUAL,
  _offset BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
  -- declare the `enriched` column group containing geo_region and risk_score
  'column-groups.enriched' = 'geo_region, risk_score',
  'bucket.key' = 'device_id',
  'bucket.num' = '4'
);
```

The `METADATA FROM 'bucket'` / `METADATA FROM 'offset'` declarations are
**opt-in** — the columns are absent from the schema if you don't declare them, and existing
tables that don't need them stay byte-identical. You can name them anything (`_bucket`,
`b`, `src_bucket`…); only the metadata key (`'bucket'` / `'offset'`) is fixed.

### Step 2 — declare the write-only enrichment target

```sql title="Flink SQL"
CREATE TABLE cg_enriched_writes (
  src_bucket BIGINT,
  src_offset BIGINT,
  geo_region STRING,
  risk_score DOUBLE
) WITH (
  'enrichment.target' = 'cg_base',
  'enrichment.group'  = 'enriched'
);
```

Row schema is fixed: **two leading `BIGINT` columns** carry the addressing primitives (bucket
and offset, in that order), followed by the enrichment group's columns in declared order.

`enrichment.target` accepts either a bare table name (defaults to the current database) or a
fully-qualified `db.table` name. `enrichment.group` is required when `enrichment.target` is
present.

### Step 3 — run the streaming enrichment job

```sql title="Flink SQL"
INSERT INTO cg_enriched_writes
SELECT
  b._bucket,
  b._offset,
  lookup_geo(b.payload)    AS geo_region,
  score_risk(b.payload)    AS risk_score
FROM cg_base AS b
WHERE b.geo_region IS NULL;  -- only enrich rows that haven't been enriched yet
```

The job streams `cg_base`, projects each row's bucket/offset alongside computed enrichment
values, and writes them through the enrichment target. The base table's CEW advances per
bucket as enrichment values arrive in contiguous-offset order; queries against `cg_base` that
project enrichment columns are gated at the CEW, so partially-enriched ranges are visible
atomically.

### Schema evolution

Adding a column to an enrichment group is a two-step process:

```sql title="Flink SQL"
-- 1. Add the column to the base table (server-side schema mutation):
--    use the Java admin API's TableChange.addColumn(..., 'enriched') today.

-- 2. Mirror the new column on the write-only sibling:
ALTER TABLE cg_enriched_writes ADD device_score DOUBLE;
```

The write-only table's schema is fixed at create time — the catalog does **not** automatically
mirror changes from the base table. If you forget step 2, subsequent `INSERT INTO` statements
will fail at plan time with a clear error naming the missing column.

### Restrictions

- `SELECT * FROM cg_enriched_writes` — rejected at plan time. Query the base table instead.
- Declaring `PRIMARY KEY` on an enrichment-target table — rejected at `CREATE TABLE` time.
  Column-group tables are log-only by design.
- Column-group tables cannot be partitioned (current scope).
- The `enrichment.target` property must point at a Fluss table that has the named column group;
  the catalog validates this at the first `INSERT INTO` against the table.

## DELETE FROM

Fluss supports deleting data for primary-key tables in batch mode via `DELETE FROM` statement. Currently, only single data deletions based on the primary key are supported.

* the Primary Key Table
```sql title="Flink SQL"
-- DELETE statement requires batch mode
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
-- The condition must include all primary key equality conditions.
DELETE FROM pk_table WHERE shop_id = 10000 AND user_id = 123456;
```

## UPDATE
Fluss enables data updates for primary-key tables in batch mode using the `UPDATE` statement. Currently, only single-row updates based on the primary key are supported.

```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET execution.runtime-mode = batch;
```

```sql title="Flink SQL"
-- The condition must include all primary key equality conditions.
UPDATE pk_table SET total_amount = 2 WHERE shop_id = 10000 AND user_id = 123456;
```