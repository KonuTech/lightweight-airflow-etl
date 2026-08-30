# Configuration: `config.json` Contract Shape

This document covers the `DatasetConfig` model tree every `configs/datasets/<name>.json` must
validate against, how `configs/defaults.json` merges under a dataset's own config, and the two
real, working dataset configs shipped in this repo (`customers`, `orders`) — referenced by their
actual field values, not invented placeholder JSON. See `docs/csv-engine.md` for what the engine
*does* with a validated config and `docs/architecture.md` for where config loading sits in the
overall request path.

## The `DatasetConfig` Model Tree

Defined in `packages/csv-processor/src/csv_processor/config/models.py`, validated once per run
via Pydantic v2 (never per row — construction cost, and Pydantic raises instead of collecting,
which fights the collect-and-continue invalid-row model this project uses). Every model in the
tree is `frozen=True, extra="forbid"` — an unrecognized/typo'd config key is a validation-time
error, never silently ignored, and a validated config can never be mutated after construction.

```
DatasetConfig
├── dataset: str                      -- e.g. "customers"
├── file_pattern: str                 -- glob, e.g. "customers_*.csv*"
├── csv: CsvDialectConfig             -- every field has a concrete default, see below
├── columns: list[ColumnSpec]         -- min length 1
├── oracle: OracleTargetSpec          -- table names ONLY, never connection details/credentials
└── processing: ProcessingConfig      -- chunk_size: int (> 0)
```

### `ColumnSpec` fields

One entry per CSV column:

| Field | Type | Meaning |
|---|---|---|
| `name` | `str` | Must match `^[A-Za-z_][A-Za-z0-9_]{0,127}$` (`is_safe_identifier()`) — Oracle has no bind-parameter mechanism for identifiers, so every column name is validated at config-load time AND again immediately before it's interpolated into an INSERT string (`load.insert_rows()`, defense-in-depth, T-04-01). |
| `type` | `"string" \| "integer" \| "decimal" \| "date" \| "timestamp" \| "boolean"` | The column's logical type. |
| `nullable` | `bool` | Can a *present* value be empty? |
| `required` | `bool` | Must the column be present in the file at all? Deliberately a separate boolean from `nullable` (D-09) — future-proofing, even though neither shipped dataset currently needs the distinction. |
| `format` | `str \| None` | A `strptime` format string — **required** for `type: "date"`/`"timestamp"`, rejected for every other type. |
| `precision` / `scale` | `int \| None` | **Required together** for `type: "decimal"` (`scale` cannot exceed `precision`), rejected for every other type. |

### `OracleTargetSpec`

```json
"oracle": { "valid_table": "customers_valid", "invalid_table": "customers_invalid" }
```

`valid_table` and `invalid_table` must differ and must each be a safe SQL identifier — no
connection string, host, or credential ever lives in `config.json` (D-12); those come from
`ORACLE_DSN`/`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD` environment variables instead
(`csv_processor.load`), keeping `config.json` safe to commit, log, or version.

### `CsvDialectConfig`

Every field always has a concrete value — unlike the reference repo's detect-or-override shape
(`str | None = None`), this project's dialect config never leaves a field ambiguous:

```json
{
  "delimiter": ",",
  "encoding": "utf-8",
  "quotechar": "\"",
  "header": true,
  "escapechar": null,
  "doublequote": true,
  "lineterminator": "\n",
  "decimal_separator": ".",
  "has_footer": false
}
```

`has_footer` defaults to `false` for every dataset shipped today — it's a per-dataset **opt-in**
for footer-row exclusion, never a heuristic that runs unconditionally (a 5-round gap-closure chain
in Phase 3 found unconditional footer heuristics silently drop a genuinely malformed last row when
no dataset-level signal distinguishes "real footer" from "corrupted data").

### `ProcessingConfig`

```json
"processing": { "chunk_size": 5000 }
```

`chunk_size` governs `process_chunks()`'s streaming batch size (ENGINE-07's bounded-memory
guarantee — one chunk in memory at a time) and, downstream, the array size of each
`cursor.executemany()` call in `load.insert_rows()`. Both shipped datasets use `chunk_size: 5000`.

## `defaults.json` Shallow-Merge

`csv_processor.config.loader.load_config(path, defaults_path=...)` merges a dataset's own JSON
document over `configs/defaults.json`:

```python
merged = {**defaults, **dataset}
```

This is a **shallow, top-level-only merge** — dataset keys win on any collision, and it is never
recursive/deep. `configs/defaults.json` in this repo carries only the `csv` block:

```json
{
  "csv": {
    "delimiter": ",", "encoding": "utf-8", "quotechar": "\"", "header": true,
    "escapechar": null, "doublequote": true, "lineterminator": "\n",
    "decimal_separator": ".", "has_footer": false
  }
}
```

Because the merge is shallow at the top level, a dataset config that declares its **own** `csv`
key entirely replaces `defaults.json`'s whole `csv` block — it does not merge field-by-field
inside `csv`. Neither `configs/datasets/customers.json` nor `configs/datasets/orders.json`
declares a `csv` key today, so both inherit `defaults.json`'s `csv` block unchanged; if a future
dataset needed one non-default CSV field (e.g. a `;` delimiter), its own `config.json` would need
to repeat the **entire** `csv` block with that one field changed, not just the one field it wants
to override. This is a deliberate simplicity trade-off (the reference repo's own
`dataplat/config/loader.py` comment flags the same shallow-vs-deep footgun) — a dataset with a
partial `csv` override is expected to copy the full block from `defaults.json` and edit it, not
rely on field-level inheritance.

A missing `defaults.json` is treated as `{}` (no shared keys contributed) rather than an error —
a dataset config may be fully self-contained. An empty/whitespace-only file (either one) parses as
`{}` too; a missing/malformed **dataset** file (not defaults) surfaces as `ConfigurationError`,
never a raw `json.JSONDecodeError`/`pydantic.ValidationError` escaping the loader (CONFIG-02's
"before any CSV processing begins" requirement).

## Real Example: `configs/datasets/customers.json`

```json
{
  "dataset": "customers",
  "file_pattern": "customers_*.csv*",
  "columns": [
    { "name": "customer_id", "type": "string", "nullable": false, "required": true },
    { "name": "name", "type": "string", "nullable": false, "required": true },
    { "name": "country", "type": "string", "nullable": false, "required": true },
    { "name": "birth_date", "type": "date", "nullable": true, "required": true, "format": "%Y-%m-%d" },
    { "name": "event_ts", "type": "timestamp", "nullable": false, "required": true, "format": "%Y-%m-%dT%H:%M:%S%z" },
    { "name": "signup_country", "type": "string", "nullable": true, "required": false }
  ],
  "oracle": { "valid_table": "customers_valid", "invalid_table": "customers_invalid" },
  "processing": { "chunk_size": 5000 }
}
```

Note `signup_country` is the one column with `required: false` in either shipped dataset — it may
be entirely absent from a given file's header, in which case `process_chunks()` backfills it as an
empty string per-row rather than raising `KeyError` (its `nullable: true` then governs that
backfilled empty value exactly as it would any other blank value).

## Real Example: `configs/datasets/orders.json`

```json
{
  "dataset": "orders",
  "file_pattern": "orders_*.csv*",
  "columns": [
    { "name": "order_id", "type": "string", "nullable": false, "required": true },
    { "name": "customer_id", "type": "string", "nullable": false, "required": true },
    { "name": "order_date", "type": "date", "nullable": true, "required": true, "format": "%Y-%m-%d" },
    { "name": "amount", "type": "decimal", "nullable": true, "required": true, "precision": 6, "scale": 2 }
  ],
  "oracle": { "valid_table": "orders_valid", "invalid_table": "orders_invalid" },
  "processing": { "chunk_size": 5000 }
}
```

`amount` is `orders.json`'s only `decimal` column, showing the `precision`/`scale` pair in
practice. `precision`/`scale` are generator-side metadata only — they bound the range
`generator/generate_csv.py`'s `format_decimal()` draws from, never enforced by
`csv_processor.validate`/`.normalize` at load time — so they need only fit within, not exactly
match, the wider `NUMBER(12,2)` column on the Oracle side (`docker/oracle/init/03_orders.sql`).

## Adding a New Dataset

Per `DAG-05`'s zero-branching design, adding a third dataset never touches `airflow/dags/`: write
a new `configs/datasets/<name>.json` (following either example above), add the matching Oracle
DDL (`<name>_valid`/`<name>_invalid` tables + a widened-`_invalid`-columns migration, following
`docker/oracle/init/02_customers.sql`+`04_widen_invalid_columns.sql`'s pattern), and add the new
name to the DAG's `dataset` `Param`'s `enum` list — no other code change required. See
`docs/development.md`'s "Adding a New Dataset" section for the full step-by-step.
