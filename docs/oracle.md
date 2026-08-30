# Oracle: Schema, Bulk Loading, and Idempotency

This document covers the 5-table Oracle schema (`docker/oracle/init/*.sql`), why the `_invalid`
tables widen every data column to nullable `VARCHAR2`, the `INTERVAL` daily-partitioning scheme,
the `executemany()` bulk-insert mechanism (LOAD-01/LOAD-02), the checksum-based idempotency
round trip (LOAD-04), the customers⋈orders business report evidence, and the DB-level correlation
constraints (PK/index/trigger) backing that report. See `docs/csv-engine.md` for what produces the
rows this document describes loading, and `docs/configuration.md` for where table names come from
(`OracleTargetSpec`).

## The 5-Table Schema

```
customers_valid      customers_invalid      orders_valid      orders_invalid      ingestion_metadata
```

Two data tables per dataset (`<dataset>_valid` / `<dataset>_invalid`) plus one shared
`ingestion_metadata` table. All four data tables use `INTERVAL` daily partitioning on
`ingested_at`; `ingestion_metadata` does not (it's small, append-only, and never needs
partition-level maintenance).

### `<dataset>_valid`

Native, typed columns matching `config.json`'s `ColumnSpec` list exactly (`customer_id
VARCHAR2(64) NOT NULL`, `birth_date DATE`, `event_ts TIMESTAMP WITH TIME ZONE NOT NULL`, etc.) —
`docker/oracle/init/02_customers.sql`/`03_orders.sql`. A row only ever reaches this table after
passing `csv_processor.validate.check_row()`'s structural/type/nullability checks and
`normalize_row()`'s type conversion. `customers_valid.customer_id` and `orders_valid.order_id`
each carry a `PRIMARY KEY`; `orders_valid.customer_id` carries a supporting index and a
`BEFORE INSERT` FK-existence trigger — see "Correlation Constraints" below.

### `<dataset>_invalid`

Same base column set as `_valid`, but **every data column is widened to nullable `VARCHAR2`** at
its current declared size (`docker/oracle/init/04_widen_invalid_columns.sql`), plus five fixed
suffix columns:

```sql
error_code       VARCHAR2(64)   NOT NULL
error_message    VARCHAR2(4000) NOT NULL
source_file      VARCHAR2(255)  NOT NULL
row_number       NUMBER         NOT NULL
raw_line         VARCHAR2(4000)          -- the entire original CSV line, defense-in-depth
```

**Why widen to `VARCHAR2`:** ENGINE-06 requires invalid rows to carry their *original* field
values — a malformed date string (`"not-a-date"`) or an oversized numeric literal cannot literally
be inserted into a native `DATE`/`NUMBER(12,2)` column; the insert would just fail with a second,
unrelated error, destroying the very evidence the `_invalid` table exists to preserve. Widening to
nullable `VARCHAR2` (never silently coercing, never widening to a generic `VARCHAR2(4000)` "just
in case") means a genuinely malformed value is stored exactly as it appeared in the file, and a
value that's simply *too long* for its column's declared width is its own distinct, worth-flagging
error condition — not something this migration papers over.

### `ingestion_metadata`

```sql
CREATE TABLE ingestion_metadata (
  id             NUMBER GENERATED ALWAYS AS IDENTITY,
  dataset        VARCHAR2(64) NOT NULL,
  file_name      VARCHAR2(255) NOT NULL,
  checksum       VARCHAR2(64) NOT NULL,
  processed_at   TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  total_rows     NUMBER NOT NULL,
  valid_rows     NUMBER NOT NULL,
  invalid_rows   NUMBER NOT NULL,
  status         VARCHAR2(32) NOT NULL,
  CONSTRAINT uq_ingestion_metadata_dataset_checksum UNIQUE (dataset, checksum)
);
```

One row per successfully-processed file, keyed by `(dataset, checksum)` — this is both the
reporting/evidence table (`scripts/verify_evidence.sql` reads it directly, see below) and the
idempotency mechanism's source of truth.

## `INTERVAL` Daily Partitioning

Every data table declares:

```sql
PARTITION BY RANGE (ingested_at)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
( PARTITION p_initial VALUES LESS THAN (DATE '2020-01-01') );
```

Oracle **auto-creates** each new day's partition the first time a row with that day's
`ingested_at` is inserted — there is no manual partition-maintenance job to run, schedule, or
forget. `p_initial` is just a bootstrap partition covering everything before this project's own
epoch; it's never actually populated in practice (`ingested_at` defaults to `SYSDATE`, always
"now").

## Bulk Insert: `executemany()` (LOAD-01/LOAD-02)

`csv_processor.load.insert_rows()` is the **only** write path into any of the four data tables —
one `cursor.executemany()` call per chunk, using array binding:

```python
column_list = ", ".join(columns)
bind_list = ", ".join(f":{column}" for column in columns)
sql = f"INSERT INTO {table} ({column_list}) VALUES ({bind_list})"
cursor.executemany(sql, rows)
```

**Why `executemany()`, never a per-row loop:** Oracle has no `COPY`-equivalent bulk-load command
(unlike Postgres); `executemany()`'s array binding is the closest equivalent — it sends the whole
chunk's rows to the database in a single network round trip rather than one round trip per row.
`table`/every entry in `columns` is re-validated via `is_safe_identifier()` immediately before
building the SQL string (defense-in-depth, T-04-01) — Oracle has no bind-parameter mechanism for
identifiers, only values, so this allowlist check is the only defense available before a
config-sourced name is interpolated directly into SQL text.

**Contrast — `benchmark/naive_loader.py`:** this project also ships a *deliberately naive*
comparison implementation, `benchmark/naive_loader.py`, whose entire purpose is to demonstrate why
`executemany()` is the real path: it issues one `cursor.execute()` call per row, in a Python loop,
never `executemany()` at any chunk size. It exists **only** as a benchmark baseline (D-01/D-04) —
it is never imported by `csv_processor`, never runs against real ingestion traffic, and lives
outside `packages/csv-processor/` specifically so it can never be confused with the reusable
engine. The measured result: `executemany()` is ~182.85× faster than the naive per-row loop for
Oracle write throughput at ~100K rows (see `docs/benchmark.md` for the full comparison, including
why `chunk_size=1` `executemany()` would *not* have been a valid naive baseline — it still
batch-binds internally and would understate the real per-round-trip cost).

## Idempotency (LOAD-04)

`process()` checks for an existing `(dataset, checksum)` record **before** running
`process_chunks()` at all:

1. `load.find_existing_ingestion(cursor, dataset=..., checksum=...)` — a `SELECT` against
   `ingestion_metadata` keyed by `(dataset, checksum)`, **never** `file_name` (two
   differently-named, byte-identical files for the same dataset are treated as the *same*
   processed file — this is the app-level fast-path check).
2. A match short-circuits `process()` entirely: it returns the **original** recorded
   `ProcessingResult`, read back verbatim, without touching the file's rows a second time.
3. No match: `process_chunks()` runs, chunks stream into `insert_rows()`, then
   `load.record_ingestion()` inserts one new `ingestion_metadata` row.
4. The database's own `UNIQUE(dataset, checksum)` constraint is the **second, authoritative**
   layer beneath the app-level check above — if two concurrent `process()` calls for the same
   file both pass step 1 (race window), the loser's `record_ingestion()` insert raises
   `oracledb.IntegrityError` (`ORA-00001`). `process()` catches exactly that error code, rolls
   back its own uncommitted inserts, re-reads the winner's now-committed row via
   `find_existing_ingestion()`, and returns *that* result instead of raising or double-counting.

**Why this matters (LOAD-04's "reprocessing is a safe no-op" guarantee):** retrying an Airflow
task, or re-encountering the same file under a different name, never duplicates data in either
`_valid` or `_invalid` — the checksum is the only identity that matters, not the filename or the
task-retry count.

## Business Report Evidence (`scripts/verify_evidence.sql`)

The same, single business-report SQL text — `ingestion_metadata`'s latest run per dataset, joined
against a `customers_valid`⋈`orders_valid` JOIN on `customer_id`, grouped by
`(customers.country, TRUNC(orders.order_date, 'MM'))` (`country` stands in for "region" since no
literal `region` column exists in this schema, an explicit substitution, not a silent assumption)
— is mirrored verbatim across every path that materializes it, never re-authored independently:

- `scripts/verify_evidence.sql`, run ad hoc via `make verify-evidence`
- `scripts/regenerate_readme_summary.py`, which live-regenerates README's Executive Summary table
  after every merge to `master` (`.github/workflows/readme-summary.yml`)
- the `report_ready` Airflow DAG (`airflow/dags/report_ready.py`, see `docs/airflow-dag.md`), whose
  `build_report_task` runs it once a deferrable trigger confirms both `customers` and `orders` have
  ingested data for the current day's partition, and logs the result

`customers_valid⋈orders_valid` returns real, non-empty rows because `orders.customer_id` is a
Zipf-weighted sample drawn from the actual pool of `customer_id` values generated for
`customers_valid` in the same run — never independently random (see "Correlation constraints"
below). This is a **read-only reporting JOIN**, not the mechanism that enforces the relationship —
that enforcement lives at the DB level (see below).

## Correlation Constraints (`docker/oracle/init/05_correlation_constraints.sql`)

`customers_valid` carries a `PRIMARY KEY` on `customer_id`; `orders_valid` carries a `PRIMARY KEY`
on `order_id` plus a plain index on `customer_id` supporting the JOIN workload above. A
`BEFORE INSERT` trigger on `orders_valid` (`trg_orders_valid_customer_exists`) rejects — failing
the whole insert batch — any row whose `customer_id` does not already exist in `customers_valid`.
This is a DB-level safety net on top of the Python-side correlation `generator/generate_csv.py`
already guarantees, catching any future generator regression as a load failure instead of silent
bad data; it does not reintroduce the `csv_processor` validation engine's own referential-validator
exclusion (still out of scope per `PROJECT.md`, spec §28) — this trigger is a separate DDL layer,
not a new engine-side validator. `customers_invalid`/`orders_invalid` remain completely
unconstrained: no PK, index, or trigger of any kind applies to either.
