# Phase 4: Oracle Bulk Load, Idempotency & Engine Entrypoint - Research

**Researched:** 2026-08-29
**Domain:** `python-oracledb` bulk DML (`executemany()`), Oracle transaction/exception semantics, checksum-based idempotency, and a `process()` entrypoint that unifies status/exception translation
**Confidence:** HIGH (all `python-oracledb` semantics confirmed via Context7 official docs; all table shapes/config/engine contracts confirmed by reading the actual project files this session)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Re-run / idempotency behavior**
- **D-01:** [auto] Re-processing an already-recorded file (same filename + checksum + dataset)
  returns `ProcessingResult` with the **original recorded outcome** (status, total/valid/invalid
  counts read back from `ingestion_metadata`), not a distinct new status — the project's status
  enum is closed (`SUCCESS` / `SUCCESS_WITH_INVALID_ROWS` / `FILE_NOT_FOUND` / `INVALID_FILE` /
  `CONFIGURATION_ERROR` / `DATABASE_ERROR` / `PROCESSING_ERROR`, per ENGINE-08/REQUIREMENTS.md) —
  adding an 8th "already processed" member is out of scope for this phase.

**Per-file load atomicity**
- **D-02:** [auto] A file's load is **all-or-nothing**: valid-row inserts, invalid-row inserts, and
  the `ingestion_metadata` upsert commit together in one Oracle transaction per `process()` call.
  If the load fails partway, nothing commits and the file is NOT marked processed — a retry
  re-attempts the full load rather than resuming from a partial/chunk-level checkpoint.

**Oracle bulk-insert batch size**
- **D-03:** [auto] `executemany()`'s array size reuses each dataset's existing `chunk_size` config
  value (currently 5000 for both `customers` and `orders`) rather than introducing a second,
  Oracle-specific batch-size config key.

**Carried forward from Phase 1 (`01-CONTEXT.md`)**
- **D-04** [= Phase 1 D-01]: `<DATASET>_VALID`/`<DATASET>_INVALID` table shapes are already DDL'd;
  `_INVALID` carries original columns plus `ERROR_CODE`/`ERROR_MESSAGE`/`SOURCE_FILE`/`ROW_NUMBER`.
- **D-05** [= Phase 1 D-04]: `INGESTION_METADATA` already has a `UNIQUE(dataset, checksum)`
  DB-level constraint — this phase's application-level check (D-02) sits **in addition to**, not
  instead of, this constraint.
- **D-06** [= Phase 1 D-05]: `scripts/verify_environment.py`'s `oracledb`-based verification
  pattern is the precedent for this phase's own Oracle integration tests (TEST-02).

**Carried forward from Phase 3 (`03-CONTEXT.md`)**
- **D-07** [= Phase 3 D-11]: `process_chunks(file_path, config) -> Iterator[tuple[list[dict], list[dict]]]`
  is the exact generator this phase's loader consumes chunk-by-chunk.
- **D-08** [= Phase 3 D-23]: A whole-file structural reject surfaces as a plain
  `StructuralValidationError` (or subclass) from the engine — `process()` catches it and translates
  it into `ProcessingResult(status=INVALID_FILE)`. `csv_processor` stays Airflow-agnostic.

### Claude's Discretion

- Exact SQL/parameter-binding order for `executemany()` calls against `<DATASET>_VALID`/
  `<DATASET>_INVALID` — implementation detail once the table DDL (already fixed) is read.
- Exact sequencing of "compute checksum → check `ingestion_metadata` → short-circuit or load →
  upsert metadata" within one transaction — implementation detail, constrained by D-02's
  atomicity requirement.
- Which Oracle exception types map to `DATABASE_ERROR` vs `PROCESSING_ERROR` — flagged in
  `STATE.md`'s Blockers/Concerns as needing a research pass (`setinputsizes()` type-derivation,
  `batcherrors` semantics); resolved in this document (see Architecture Patterns / Code Examples).

### Deferred Ideas (OUT OF SCOPE)

None — discussion stayed within phase scope (single `--auto` pass, no new scope surfaced).
</user_constraints>

## Summary

This phase has unusually little genuine unknown left in it: the schema (Phase 1), the config
model (Phase 2), and the row-producing generator (Phase 3, `process_chunks()`) are all already
built and were read in full this session. What remained open — `setinputsizes()` type derivation,
`batcherrors` semantics, and which Oracle exception maps to which `ProcessingResult` status — is
now resolved directly against `python-oracledb`'s official docs (Context7 `/oracle/python-oracledb`).

The single most load-bearing finding: **`REQUIREMENTS.md`'s own v2 backlog (`REL-01`) explicitly
defers `batcherrors=True`/`getbatcherrors()` to a later release** ("add once the primary
validate-then-split path is proven and a real DB-side rejection is observed in practice"). This
directly resolves the open question in `04-CONTEXT.md`'s Claude's Discretion about batcherrors —
**Phase 4 must NOT implement a batcherrors defensive layer.** Plain `executemany()` (the default,
`batcherrors=False`) is correct for v1: any single-row DML failure raises immediately, which
combined with D-02's rollback-on-any-failure is exactly the intended all-or-nothing behavior.

The second load-bearing finding: `04-CONTEXT.md`'s own "Claude's Discretion" text already states
the intended sequence verbatim — **"compute checksum → check `ingestion_metadata` → short-circuit
or load → upsert metadata"** — this takes precedence over `ARCHITECTURE.md`'s data-flow *diagram*,
which (imprecisely) drew the checksum/upsert step *after* the per-chunk load loop. The prose in
`ARCHITECTURE.md`'s own "Key Data Flows" #3 ("checked/recorded against Oracle's `ingestion_metadata`
table **before any row is bulk-loaded**") agrees with `04-CONTEXT.md`, not with the diagram. Treat
the diagram as approximate; the check-first sequence is authoritative for this phase's design.

Third: the Phase 1 `04_widen_invalid_columns.sql` migration and Phase 3's `engine.py` already
independently converged on the same `raw_line` field — the invalid-row dict Phase 3 yields
literally includes a `"raw_line"` key (engine.py:86,113), and `CUSTOMERS_INVALID`/`ORDERS_INVALID`
literally have a `RAW_LINE VARCHAR2(4000)` column (04_widen_invalid_columns.sql:37,48). No adapter
or gap-filling is needed at the load boundary — this is a direct 1:1 key-to-column match, already
verified by reading both files this session.

**Primary recommendation:** One `oracledb.connect()` per `process()` call, `autocommit=False`
(the default — confirmed no override needed), checksum-check first via a `SELECT` on the same
connection, then per-chunk `cursor.executemany()` (no `batcherrors`) into `<DATASET>_VALID`/
`<DATASET>_INVALID` using the dict-of-column-name shape `process_chunks()` already yields verbatim,
then one `INSERT INTO ingestion_metadata` + one `connection.commit()` at the end. Catch
`oracledb.IntegrityError` **only** around the `ingestion_metadata` insert and check
`error_obj.full_code == "ORA-00001"` to distinguish "a concurrent run already recorded this exact
file" (safe no-op, re-query and return the winner's result) from a genuine `DATABASE_ERROR`.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Bulk-insert valid rows | Engine (`csv_processor.load`) | Database (Oracle native type binding: `NUMBER`↔`Decimal`, `DATE`/`TIMESTAMP WITH TIME ZONE`↔`datetime`) | Engine owns SQL/binding order; Oracle owns physical type coercion, per `python-oracledb`'s documented type map |
| Bulk-insert invalid rows | Engine (`csv_processor.load`) | Database (all `_INVALID` data columns are nullable `VARCHAR2` per Phase 1's widening migration) | Same split — engine never converts, DB just stores original strings |
| Idempotency check (checksum lookup) | Engine (`csv_processor.load`, app-level `SELECT` before load) | Database (`UNIQUE(dataset, checksum)` constraint, the race-safety backstop) | App-level check is the fast path; DB constraint is what makes it *correct* under concurrency, not just fast |
| Per-file transaction atomicity | Engine (`csv_processor.load`, explicit `commit()`/`rollback()`) | — | `python-oracledb` does not autocommit by default — this is a design *choice already forced* by the driver's default, not a decision the engine could get wrong by doing nothing |
| Status/exception classification | Engine (`csv_processor.engine.process()`) | — | Pure Python decision tree over caught exception types; no DB or Airflow involvement |
| Public entrypoint aggregation | Engine (`csv_processor.engine.process()`) | — | Assembles Phase 2 (config) + Phase 3 (`process_chunks`) + this phase (`load`) behind one function signature |

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| LOAD-01 | Valid rows bulk-inserted into `<DATASET>_VALID` via `executemany()`, no per-row INSERT | Confirmed `executemany()` accepts `list[dict]` directly with named binds matching dict keys (Context7, `batch_statement.md`); `process_chunks()`'s valid-row dict keys already match `config.columns` names 1:1 |
| LOAD-02 | Invalid rows bulk-inserted into `<DATASET>_INVALID` with error metadata, same bulk mechanism | Confirmed invalid-row dict shape (`errors.py`/`engine.py`, read this session) matches `_INVALID` table's post-migration column set 1:1, including `raw_line` |
| LOAD-03 | Each processed file recorded in `ingestion_metadata` (file_name, checksum, dataset, timestamp, counts, status) | `01_ingestion_metadata.sql` DDL read verbatim this session — exact column list and `VARCHAR2(64)` checksum width confirmed |
| LOAD-04 | Re-processing an already-recorded file is a safe no-op | Idempotency sequence resolved: app-level `SELECT` first + DB `UNIQUE` constraint as race backstop; `oracledb.IntegrityError`/`ORA-00001` is the exact signal to catch |
| ENGINE-08 | `process()` returns `ProcessingResult` with correct status among the 7-member closed enum | Exception-to-status mapping table below, grounded in `python-oracledb`'s documented exception hierarchy |
| TEST-02 | Integration tests exercise a real Oracle container, verify actual resulting rows | Oracle container confirmed running and healthy this session (`gvenzl/oracle-free:23.26.2-faststart`, `localhost:1521/FREEPDB1`); `scripts/verify_environment.py`'s `verify_tables()`/`verify_columns()` are reusable, already designed for this per their own docstrings |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `oracledb` | 4.0.2 (pinned, confirmed current via `pip index versions oracledb` this session — latest on PyPI) | Bulk DML (`executemany()`), transaction control, exception classification | Already installed and approved via Phase 1's package-legitimacy checkpoint; Oracle's own actively-maintained driver, thin mode (no Oracle Client install) |

No new packages are introduced by this phase — `oracledb` was installed in Phase 1. This phase is
the first to actually exercise its bulk-DML/transaction API surface, which is why the deep-dive
below exists.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| App-level `SELECT`-then-`INSERT` idempotency check + DB `UNIQUE` backstop | DB-level `MERGE ... WHEN NOT MATCHED` for `ingestion_metadata` | A `MERGE` would need a `WHEN MATCHED` branch too, but D-01 never *updates* an existing record — it only ever inserts once or short-circuits. A plain `INSERT` (caught on conflict) is simpler and matches D-01's actual semantics; `MERGE` would be over-engineering for a table that's genuinely insert-only. |
| Plain `executemany()` (batcherrors=False) | `executemany(..., batcherrors=True)` + `getbatcherrors()` | Explicitly deferred to v2 by `REQUIREMENTS.md`'s `REL-01`. Do not implement in this phase. |

**Installation:** N/A — `oracledb==4.0.2` already present in `pyproject.toml`/`uv.lock`.

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | Verdict | Disposition |
|---------|----------|-----|-----------|-------------|---------|-------------|
| oracledb | PyPI | 4.0.2 published 2026-07-14 [VERIFIED: pypi registry] | unknown (package-legitimacy seam returned `null` for weekly downloads — a PyPI stats API gap, not a legitimacy signal) | `github.com/oracle/python-oracledb` [VERIFIED: package-legitimacy check] | SUS (reason: `unknown-downloads` only) | **Already approved** — Phase 1 Plan 01-01's package-legitimacy checkpoint covers this exact pinned version; no new install action needed this phase. The `SUS` signal here is a downloads-API gap, not a provenance concern — the repo is Oracle's own official GitHub org, and the version matches PyPI's own latest listing. |

**Packages removed due to [SLOP] verdict:** none.
**Packages flagged as suspicious [SUS]:** `oracledb` — already installed/approved, no new checkpoint needed; documented here only because this phase is the first consumer of its bulk-DML surface.

## Architecture Patterns

### System Architecture Diagram

```
process(file_path, config)
   │
   ├─ 1. Re-validate config (cheap, DatasetConfig already a Pydantic model)
   │       └─ raises ConfigurationError ────────────────► status=CONFIGURATION_ERROR
   │
   ├─ 2. Resolve file_path, confirm it exists
   │       └─ missing ───────────────────────────────────► status=FILE_NOT_FOUND
   │
   ├─ 3. oracledb.connect(user, password, dsn)   [ONE connection for the whole call]
   │       │
   │       ├─ 4. checksum = sha256_file(file_path)         (own local helper -- see Pitfall 5)
   │       │
   │       ├─ 5. SELECT total_rows, valid_rows, invalid_rows, status
   │       │       FROM ingestion_metadata
   │       │      WHERE dataset=:dataset AND checksum=:checksum
   │       │       │
   │       │       ├─ FOUND  ──► build ProcessingResult from the recorded row (D-01)
   │       │       │             ──────────────────────────────────────────────► return, no writes
   │       │       │
   │       │       └─ NOT FOUND ──► continue to step 6
   │       │
   │       ├─ 6. for (valid_rows, invalid_rows) in process_chunks(file_path, config):
   │       │       │  (raises StructuralValidationError on a whole-file reject)
   │       │       ├─ cursor.executemany(INSERT ... valid_table ..., valid_rows)
   │       │       └─ cursor.executemany(INSERT ... invalid_table ..., invalid_rows)
   │       │           (no batcherrors -- a row-level Oracle error here raises immediately)
   │       │
   │       ├─ 7. cursor.execute(INSERT INTO ingestion_metadata ..., {...})
   │       │       │
   │       │       ├─ oracledb.IntegrityError, full_code == "ORA-00001"
   │       │       │     ──► rollback() [undoes step 6's uncommitted inserts]
   │       │       │     ──► re-SELECT the now-committed winner's row, return its ProcessingResult
   │       │       │
   │       │       └─ success ──► continue
   │       │
   │       ├─ 8. connection.commit()   [ALL of step 6 + step 7 commit together, D-02]
   │       │
   │       └─ on StructuralValidationError from step 6:
   │             rollback() ─────────────────────────────► status=INVALID_FILE
   │       └─ on any other oracledb.Error from steps 5-8:
   │             rollback() ─────────────────────────────► status=DATABASE_ERROR
   │       └─ on any other unexpected Exception:
   │             rollback() (best-effort) ────────────────► status=PROCESSING_ERROR
   │
   └─ 9. return ProcessingResult(status, total, valid, invalid, duration, ...)
```

### Recommended Project Structure

Matches `ARCHITECTURE.md`'s already-proposed layout exactly — this phase adds `load.py`,
`models.py`, and fleshes out `engine.py`'s `process()`:

```
packages/csv-processor/src/csv_processor/
├── load.py       # NEW: connect(), checksum, idempotency check, executemany() calls, commit/rollback
├── models.py     # NEW: Status enum, ProcessingResult (Pydantic or plain dataclass)
└── engine.py     # EXTEND: add process(file_path, config) -> ProcessingResult
                  #   (process_chunks() already exists, untouched by this phase)
```

### Pattern 1: One connection, one transaction, per `process()` call

**What:** `oracledb.connect(user=..., password=..., dsn=...)` opens exactly once at the top of
`process()`'s Oracle-touching path, reused for the idempotency `SELECT`, every chunk's
`executemany()` calls, and the final `ingestion_metadata` `INSERT` — never reopened per chunk.
`autocommit` is left at its documented default (`False`) [CITED: `python-oracledb` `txn_management.md`
— "By default, python-oracledb does not automatically commit these changes; you must explicitly
use `Connection.commit()` or `Connection.rollback()`"]. This is not a configuration the engine
opts into — it is the driver's out-of-the-box behavior, so a loader that never calls `.commit()`
at the end of a successful file will silently persist **nothing** on connection close: "Uncommitted
transactions are automatically rolled back when a connection is closed or goes out of scope"
[CITED: same source].

**When to use:** Always, for this phase — matches `ARCHITECTURE.md`'s explicit "one connection per
`process()` call" design and D-02's all-or-nothing requirement.

**Example:**
```python
# Source: python-oracledb txn_management.md (Context7 /oracle/python-oracledb) + this project's
# own scripts/verify_environment.py connection pattern (read this session).
import oracledb

with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN) as connection:
    with connection.cursor() as cursor:
        ...  # SELECT idempotency check, executemany() calls, INSERT ingestion_metadata
    connection.commit()  # explicit -- autocommit defaults to False
```

### Pattern 2: `executemany()` binds a `list[dict]` directly against named binds

**What:** `process_chunks()` already yields `valid_rows`/`invalid_rows` as `list[dict[str, object]]`
with keys equal to the target table's column names. `python-oracledb`'s `executemany()` accepts this
shape natively when the SQL uses named binds (`:col_name`) matching the dict keys — no conversion to
tuples needed [CITED: `python-oracledb` `batch_statement.md`, "Predefining memory areas with named
bind variables" — the exact example binds `list[dict]` against `:pid`/`:pdesc}`].

**Example (grounded in the real DDL read this session — `02_customers.sql`, `04_widen_invalid_columns.sql`):**
```python
# customers_valid columns (02_customers.sql:12-18), ingested_at excluded (DEFAULT SYSDATE, D-08 below)
VALID_INSERT_SQL = """
    INSERT INTO customers_valid
        (customer_id, name, country, birth_date, event_ts, signup_country)
    VALUES
        (:customer_id, :name, :country, :birth_date, :event_ts, :signup_country)
"""
cursor.executemany(VALID_INSERT_SQL, valid_rows)  # valid_rows: list[dict], keys == column names

# customers_invalid: all data columns now nullable VARCHAR2 (04_widen_invalid_columns.sql:28-37),
# plus error_code/error_message/source_file/row_number/raw_line -- exact keys engine.py's
# invalid_rows dicts already carry (engine.py:79-88, 106-114).
INVALID_INSERT_SQL = """
    INSERT INTO customers_invalid
        (customer_id, name, country, birth_date, event_ts, signup_country,
         error_code, error_message, source_file, row_number, raw_line)
    VALUES
        (:customer_id, :name, :country, :birth_date, :event_ts, :signup_country,
         :error_code, :error_message, :source_file, :row_number, :raw_line)
"""
cursor.executemany(INVALID_INSERT_SQL, invalid_rows)
```
Build these SQL strings from `config.columns` (names) + the fixed 5-column invalid-row suffix,
rather than hand-typing per dataset — `orders_valid`/`orders_invalid` follow the identical pattern
against `03_orders.sql`'s columns (`order_id, customer_id, order_date, amount`).

### Pattern 3: `setinputsizes()` for wide/variable-length `_INVALID` columns

**What:** `python-oracledb` "defers type determination until a non-null value is found" per column
across an `executemany()` batch; if an entire column is `None` for the whole batch it defaults to a
1-character string, and the driver dynamically reallocates buffers as it encounters longer values —
"this can cause performance overhead due to memory reallocation and data copying" [CITED:
`python-oracledb` `batch_statement.md`, "Predefining Memory Areas"]. The `_INVALID` tables' widened
columns are all fixed-size `VARCHAR2` (e.g. `error_message VARCHAR2(4000)`, `raw_line VARCHAR2(4000)`,
`name VARCHAR2(255)` — sizes read verbatim from `02_customers.sql`/`04_widen_invalid_columns.sql`
this session), so their max sizes are already known statically from the DDL.

**When to use:** Before each `_INVALID` `executemany()` call, at 5,000-row array size (D-03), to
avoid buffer-resize overhead across the batch — not required for correctness, but a documented
performance lever directly relevant to this project's own benchmark goal (`TEST-04`, Phase 6).

**Example:**
```python
# Source: python-oracledb batch_statement.md (Context7) -- named-bind form.
cursor.setinputsizes(
    customer_id=64, name=255, country=64, birth_date=64, event_ts=64, signup_country=64,
    error_code=64, error_message=4000, source_file=255, raw_line=4000,
    # row_number is NUMBER, not sized via setinputsizes
)
cursor.executemany(INVALID_INSERT_SQL, invalid_rows)
```
Sizes above are read verbatim from `04_widen_invalid_columns.sql` (customer_id/country/event_ts/
signup_country → `VARCHAR2(64)`, name → `VARCHAR2(255)`, error_message/raw_line → `VARCHAR2(4000)`)
and `01_ingestion_metadata.sql`/`02_customers.sql` (`error_code VARCHAR2(64)`, `source_file
VARCHAR2(255)`).

### Pattern 4: Idempotency race resolved via `oracledb.IntegrityError.full_code`

**What:** The app-level check-then-insert is not atomic across two concurrent `process()` calls for
the same file — the DB's `UNIQUE(dataset, checksum)` constraint (`01_ingestion_metadata.sql:21`,
already read this session: `CONSTRAINT uq_ingestion_metadata_dataset_checksum UNIQUE (dataset,
checksum)`) is the actual arbiter. `python-oracledb` raises `oracledb.IntegrityError` on a unique
constraint violation, and the error object carries a `.full_code` attribute (`"ORA-00001"`) and
`.code` (numeric `1`) [CITED: `python-oracledb` `exception_handling.md`].

**When to use:** Catch `oracledb.IntegrityError` **only** around the `ingestion_metadata` INSERT
(step 7 in the diagram above) — never around the `_VALID`/`_INVALID` `executemany()` calls, which
have no unique constraints to violate and whose errors should always map to `DATABASE_ERROR`.

**Example:**
```python
# Source: python-oracledb exception_handling.md (Context7 /oracle/python-oracledb)
try:
    cursor.execute(METADATA_INSERT_SQL, metadata_row)
    connection.commit()
except oracledb.IntegrityError as exc:
    error_obj, = exc.args
    if error_obj.full_code == "ORA-00001":
        connection.rollback()  # undoes this call's own VALID/INVALID inserts
        return _reread_recorded_result(cursor, dataset, checksum)  # D-01: return the winner's result
    connection.rollback()
    raise  # any other integrity error is a genuine DATABASE_ERROR, let process() classify it
```

### Anti-Patterns to Avoid

- **Using `batcherrors=True` in this phase:** `REQUIREMENTS.md`'s `REL-01` explicitly defers this
  to v2. Beyond being out of scope, it also changes the failure semantics in a way that fights D-02:
  "a transaction is started but not committed, even if autocommit is enabled" [CITED:
  `python-oracledb` `batch_statement.md`] — meaning the app itself would have to inspect
  `getbatcherrors()` and decide what to do with partially-inserted rows, adding exactly the
  complexity `REL-01`'s own rationale says isn't earned yet.
- **Reopening a connection per chunk:** Breaks D-02's atomicity — a mid-file failure on chunk 3 of 5
  would leave chunks 1-2 already committed under a per-chunk-connection design. One connection for
  the whole `process()` call is what makes "nothing commits on partial failure" achievable at all.
- **Including `ingested_at` in the INSERT column list:** Both `_VALID` and `_INVALID` tables declare
  `ingested_at DATE DEFAULT SYSDATE NOT NULL` (`02_customers.sql:18`, `03_orders.sql:16`) — binding
  an explicit value here is unnecessary and risks fighting the `INTERVAL` daily-partitioning design
  (D-03/Phase 1), which depends on `SYSDATE` being the actual insert time, not a re-derived value.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Row-by-row Oracle inserts | A `for row in rows: cursor.execute(...)` loop | `cursor.executemany()` array binding | This is the entire point of `LOAD-01`/`LOAD-02` and the project's own `TEST-04` benchmark — a per-row loop is the exact anti-pattern the benchmark exists to demonstrate is worse |
| Cross-process duplicate-load locking | A file lock, advisory lock, or Redis-based mutex around the idempotency check | The DB's own `UNIQUE(dataset, checksum)` constraint + `IntegrityError` catch | Oracle already serializes concurrent inserts against the same unique key — a hand-rolled lock adds a second source of truth that can itself race with the DB constraint |
| File checksum computation | A new, from-scratch chunked-read SHA-256 function | Mirror the existing `tools/corpus/digests.py::sha256_file()` pattern (5-line chunked `hashlib.sha256` read) — **but do not import it directly**, see Pitfall 5 | The exact algorithm already exists and is tested in this repo; only the import boundary needs respecting |
| `getbatcherrors()`-based defensive row routing | A custom "retry this row into `_INVALID` if Oracle rejects it" fallback | Nothing, this phase — deferred to `REL-01` (v2) | Explicitly out of scope per `REQUIREMENTS.md`; building it now is scope creep against the project's own backlog |

**Key insight:** Nearly everything this phase would be tempted to hand-roll (batch insert, dedup
locking, batch-level partial-failure routing) already has either a driver-native mechanism
(`executemany`, `IntegrityError`) or an explicit "not yet" from the project's own requirements
document. The actual engineering work is wiring the existing pieces together correctly, not
inventing new mechanisms.

## Common Pitfalls

### Pitfall 1: Forgetting the explicit `commit()` (silent no-op persistence)
**What goes wrong:** A `process()` call appears to succeed (no exception, correct `ProcessingResult`
returned) but zero rows are actually visible in Oracle afterward.
**Why it happens:** `autocommit` defaults to `False`; closing a connection without committing
silently rolls back everything [CITED: `python-oracledb` `txn_management.md`].
**How to avoid:** Exactly one `connection.commit()` call, on the success path only, after both the
row inserts and the `ingestion_metadata` insert have succeeded. Never call `commit()` speculatively
mid-loop.
**Warning signs:** Integration tests (`TEST-02`) that pass without ever asserting a `SELECT COUNT(*)`
against the real table would not catch this — the acceptance criteria must query Oracle directly,
not just check `ProcessingResult.status`.

### Pitfall 2: Checking idempotency after loading instead of before
**What goes wrong:** Wasted detect/parse/validate/load work on every re-encounter of an
already-processed file, and — worse — a window where rows could be double-inserted if the
`ingestion_metadata` check happens only at the very end.
**Why it happens:** `ARCHITECTURE.md`'s own data-flow *diagram* draws the checksum/upsert step after
the per-chunk load loop, which is inconsistent with its own prose ("Key Data Flows" #3) and with
`04-CONTEXT.md`'s explicit discretion-note sequence.
**How to avoid:** Compute the checksum and run the `SELECT` against `ingestion_metadata` **first**,
before calling `process_chunks()` at all. A found record short-circuits before any file parsing
happens.
**Warning signs:** A test that re-processes the same file twice and asserts row counts didn't double
— but doesn't also assert the second call skipped calling `process_chunks()` (e.g. via a spy/mock)
— would pass even with the wrong ordering, since both orderings avoid duplicate rows. Add an
explicit "second call never touches the file's rows" assertion.

### Pitfall 3: Treating every `oracledb.IntegrityError` as an idempotency race
**What goes wrong:** A genuine data problem (e.g. a `NOT NULL` violation that somehow slipped past
Phase 3's validation, or an unrelated constraint) gets silently swallowed and misreported as "already
processed."
**Why it happens:** Catching a bare `except oracledb.IntegrityError` without checking `full_code`
against `"ORA-00001"` specifically.
**How to avoid:** Check `error_obj.full_code == "ORA-00001"` (and consider also checking the
constraint name substring `"UQ_INGESTION_METADATA_DATASET_CHECKSUM"` in `error_obj.message` for
defense in depth) before treating it as the idempotency case. Any other `IntegrityError` re-raises
to the generic `DATABASE_ERROR` path.
**Warning signs:** A test with a deliberately malformed `ingestion_metadata` insert (wrong dataset
name length, etc.) that gets misreported as a successful no-op instead of `DATABASE_ERROR`.

### Pitfall 4: `setinputsizes()` mismatched against a chunk containing longer values than expected
**What goes wrong:** `ORA-01461`/`ORA-06502` (or similar buffer-related errors) if `setinputsizes()`
declares a size smaller than an actual value in the batch — e.g., an oversized `error_message` that
exceeds the sized hint.
**Why it happens:** `setinputsizes()` sizes must be read from the **real DDL column widths**, not
guessed — every size in Pattern 3 above was copied verbatim from the migration SQL, not estimated.
**How to avoid:** Source every `setinputsizes()` value from the DDL file, ideally via a small
constants module or by querying `ALL_TAB_COLUMNS` at startup (mirroring `verify_environment.py`'s
own `verify_columns()` pattern) rather than hand-typing magic numbers that can drift from the DDL.
**Warning signs:** A DDL migration changes a column's width but `load.py`'s `setinputsizes()` values
aren't updated in the same change — a silent skew with no compile-time signal.

### Pitfall 5: Cross-importing `tools.corpus.digests.sha256_file` into `csv_processor.load`
**What goes wrong:** `csv_processor` would gain a runtime dependency on `tools/`, a dev/test-only
namespace package that is **not** part of what `docker/airflow/Dockerfile` installs into the Airflow
worker container (per `pyproject.toml`'s own comment: the Airflow container installs `csv_processor`
via `pip install --no-deps`, separately from local `uv` workspace tooling). A production DAG run
would `ImportError` on `tools.corpus.digests`.
**Why it happens:** The exact chunked-SHA-256 pattern already exists and is tested at
`tools/corpus/digests.py::sha256_file()`, making it tempting to import directly.
**How to avoid:** Reimplement the same ~5-line chunked-read pattern locally inside `csv_processor`
(e.g. `csv_processor.load._sha256_file()` or a new small `csv_processor.checksum` module) rather
than importing across the `tools/` boundary. This is a 5-line function, not worth a shared-package
extraction for this project's scope.
**Warning signs:** `grep -r "from tools" packages/csv-processor/` returning any hit at all.

### Pitfall 6: Reading credentials as hardcoded literals instead of environment variables
**What goes wrong:** `scripts/verify_environment.py` (the D-06 "precedent" this phase is told to
follow) actually hardcodes `ORACLE_USER = "admin"` / `ORACLE_PASSWORD = "admin"` as Python module
constants — **not** `os.environ.get(...)` reads — despite `INFRA-03` requiring credentials be
"managed consistently... via `.env`/docker-compose environment variables — not scattered inline or
hardcoded differently across configs." [VERIFIED: scripts/verify_environment.py:34-35 — `ORACLE_DSN
= "localhost:1521/FREEPDB1"`, `ORACLE_USER = "admin"`, `ORACLE_PASSWORD = "admin"`]
**Why it happens:** `verify_environment.py` was written as a standalone dev-verification script
where hardcoding matched the single documented dev credential pair; it was never required to read
from env vars, only to use the *same* values as the env vars.
**How to avoid:** This is a planner decision point, not a settled fact — `04-CONTEXT.md` frames
`verify_environment.py`'s pattern as precedent for *connection setup*, not necessarily for hardcoding.
Recommend `load.py` read `ORACLE_USER`/`ORACLE_PASSWORD`/`ORACLE_DSN` via `os.environ.get(name,
"admin")`-style fallbacks (env-var-first, dev-default-fallback) so the Airflow worker container's
own env vars are actually honored in a way `verify_environment.py`'s copy-paste-hardcoded constants
never needed to be. Flag this explicitly to the planner rather than assuming — it's the one place
this phase's "reuse the precedent" instruction and the project's own `INFRA-03` requirement are in
mild tension.
**Warning signs:** `load.py` importing `ORACLE_USER`/`ORACLE_PASSWORD` as literal string constants
copy-pasted from `verify_environment.py`, rather than reading them from the process environment.

## Runtime State Inventory

Not applicable — this is new-code (greenfield loader/entrypoint) work, not a rename/refactor/
migration phase. Skipped per the trigger condition.

## Code Examples

### `models.py` — Status enum and `ProcessingResult` (ENGINE-08)

```python
# No official-docs source for this -- ENGINE-08's exact 7-member enum is copied verbatim
# from REQUIREMENTS.md (already read this session), not invented here.
from __future__ import annotations

import datetime as dt
from enum import Enum

from pydantic import BaseModel


class Status(str, Enum):
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_INVALID_ROWS = "SUCCESS_WITH_INVALID_ROWS"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    INVALID_FILE = "INVALID_FILE"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    PROCESSING_ERROR = "PROCESSING_ERROR"


class ProcessingResult(BaseModel):
    """Crosses the DAG<->engine boundary as a plain dict via .model_dump(mode="json")
    (ARCHITECTURE.md Pattern 3) -- never per-row detail.
    """

    status: Status
    dataset: str
    file_name: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    duration_seconds: float
    checksum: str | None = None  # None on FILE_NOT_FOUND/CONFIGURATION_ERROR (never computed)
```

### Exception-to-status mapping (ENGINE-08's decision table)

| Caught exception | Status | Where it's raised (verified this session) |
|-------------------|--------|---------------------------------------------|
| `ConfigurationError` | `CONFIGURATION_ERROR` | `csv_processor.config.errors` (Phase 2) |
| `FileNotFoundError` (or path check before `process_chunks()`) | `FILE_NOT_FOUND` | `process()`'s own file-existence check |
| `StructuralValidationError` | `INVALID_FILE` | `csv_processor.errors` (Phase 3, D-08/D-23) |
| `oracledb.IntegrityError` with `full_code == "ORA-00001"` on the metadata insert | *(not an error status — resolves to the pre-existing recorded status, D-01)* | This phase's `load.py` |
| Any other `oracledb.Error` (`DatabaseError`, `OperationalError`, `IntegrityError` with a different code, DPY-* driver/connection errors) | `DATABASE_ERROR` | This phase's `load.py` |
| Any other unexpected `Exception` | `PROCESSING_ERROR` | Catch-all in `process()` |
| (no invalid rows across the whole file) | `SUCCESS` | `process()`, derived from `invalid_rows == 0` |
| (at least one invalid row, load itself succeeded) | `SUCCESS_WITH_INVALID_ROWS` | `process()`, derived from `invalid_rows > 0` |

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| `cx_Oracle` | `python-oracledb` | cx_Oracle's last release (8.3, 2022) predates this project; `python-oracledb` is its direct, actively-maintained successor with a "thin" mode requiring no separate Oracle Client install | Already resolved by an earlier phase (STACK note in `CLAUDE.md`) — no action needed, just confirming the choice is still current at the pinned version |

**Deprecated/outdated:** None directly relevant beyond the cx_Oracle → python-oracledb transition,
already settled before this phase began.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `oracledb.IntegrityError.message` contains the literal constraint name `UQ_INGESTION_METADATA_DATASET_CHECKSUM` in a form substring-matchable (not just `full_code`) | Pitfall 3 | Low — `full_code == "ORA-00001"` alone is sufficient for correctness in this project (only one unique constraint exists on `ingestion_metadata`); the constraint-name check is optional defense-in-depth, not load-bearing |
| A2 | `os.environ.get(...)`-style env-var reads (rather than `verify_environment.py`'s hardcoded literals) is the intended interpretation of D-06/INFRA-03 for `load.py` | Pitfall 6 | Medium — if the planner instead deliberately chooses to mirror `verify_environment.py`'s hardcoded constants exactly (since both scripts target the identical single-dev-credential setup), this is a legitimate alternate reading; flagged as a discretion point, not asserted as the only correct approach |

## Open Questions

1. **Should the idempotency `SELECT` use `FOR UPDATE` to reduce (not eliminate) the race window?**
   - What we know: The DB `UNIQUE` constraint is the correctness backstop regardless of whether the
     `SELECT` locks.
   - What's unclear: Whether adding `FOR UPDATE` (and the resulting lock-wait behavior for genuinely
     concurrent same-file runs) is worth the added complexity at this project's scale (LocalExecutor,
     two datasets, unlikely to see real concurrent duplicate submissions).
   - Recommendation: Skip `FOR UPDATE` for v1 — the `IntegrityError` catch already makes the outcome
     *correct* under a race, just not *lock-free-fast*. Revisit only if a real race is observed.

2. **Exact `ingestion_metadata.status` value stored for a file with invalid rows** — `SUCCESS` vs
   `SUCCESS_WITH_INVALID_ROWS` as the stored string.
   - What we know: The column is `VARCHAR2(32) NOT NULL` (`01_ingestion_metadata.sql:19`), wide
     enough for either enum member's string value.
   - What's unclear: Nothing structurally — this is a planner-level detail (store the `Status` enum's
     `.value` verbatim), not a research gap.
   - Recommendation: Store `Status.value` directly; no separate mapping needed.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Oracle Database Free container | All of LOAD-01..04, TEST-02 | ✓ (confirmed running + healthy this session) | `gvenzl/oracle-free:23.26.2-faststart` | — |
| `oracledb` Python driver | Same | ✓ (installed, pinned) | 4.0.2 | — |
| `uv` | Running tests/scripts | ✓ | 0.12.3 | — |
| Python | Runtime | ✓ | 3.12.3 | — |

**Missing dependencies with no fallback:** none.
**Missing dependencies with fallback:** none — full environment already up and verified this session
(`docker compose ps` showed all services, including `oracle`, `Up ... (healthy)`).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 (pinned, `pyproject.toml` dev group) |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths = ["tests"]`, `pythonpath = ["."]`) |
| Quick run command | `uv run pytest tests/unit/ -x` |
| Full suite command | `uv run pytest tests/ -x` (requires `make up` first — Oracle must be running for the new `tests/integration/` suite) |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| LOAD-01 | Valid rows land in `<DATASET>_VALID` via `executemany()` | integration | `uv run pytest tests/integration/test_load_oracle.py::test_valid_rows_bulk_inserted -x` | ❌ Wave 0 |
| LOAD-02 | Invalid rows land in `<DATASET>_INVALID` with error metadata | integration | `uv run pytest tests/integration/test_load_oracle.py::test_invalid_rows_bulk_inserted -x` | ❌ Wave 0 |
| LOAD-03 | `ingestion_metadata` row recorded with correct counts/status | integration | `uv run pytest tests/integration/test_load_oracle.py::test_ingestion_metadata_recorded -x` | ❌ Wave 0 |
| LOAD-04 | Re-processing an already-recorded file is a safe no-op | integration | `uv run pytest tests/integration/test_load_oracle.py::test_reprocess_is_idempotent -x` | ❌ Wave 0 |
| ENGINE-08 (non-DB status paths) | `FILE_NOT_FOUND`/`INVALID_FILE`/`CONFIGURATION_ERROR` returned correctly | unit | `uv run pytest tests/unit/test_engine_process.py -x` | ❌ Wave 0 |
| ENGINE-08 (DB-dependent status paths) | `SUCCESS`/`SUCCESS_WITH_INVALID_ROWS`/`DATABASE_ERROR` returned correctly | integration | `uv run pytest tests/integration/test_engine_process_oracle.py -x` | ❌ Wave 0 |
| TEST-02 | Integration suite itself exercises a real Oracle container, not mocks | integration | `uv run pytest tests/integration/ -x` (requires `make up`) | ❌ Wave 0 (suite doesn't exist yet) |

### Sampling Rate
- **Per task commit:** `uv run pytest tests/unit/ -x`
- **Per wave merge:** `uv run pytest tests/ -x` (with `make up` run beforehand)
- **Phase gate:** Full suite green (unit + integration, real Oracle) before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/integration/__init__.py` + `tests/integration/conftest.py` — Oracle connection fixture
  (reuse `scripts/verify_environment.py`'s `ORACLE_DSN`/`ORACLE_USER`/`ORACLE_PASSWORD` pattern or
  env-var equivalent per Pitfall 6) and a per-test table-truncation/cleanup fixture (the `_VALID`/
  `_INVALID` tables are `INTERVAL`-partitioned but still plain `DELETE`/`TRUNCATE`-able for test
  isolation)
- [ ] `tests/integration/test_load_oracle.py` — covers LOAD-01/02/03/04
- [ ] `tests/unit/test_engine_process.py` — covers ENGINE-08's pure-Python status paths with a
  stubbed/mocked `load` module (no real Oracle needed for `CONFIGURATION_ERROR`/`FILE_NOT_FOUND`/
  `INVALID_FILE`)
- [ ] `tests/integration/test_engine_process_oracle.py` — covers ENGINE-08's DB-dependent status
  paths end-to-end against the real Oracle container
- [ ] `Makefile` — a `verify-phase4` (or similarly named) target combining unit + integration,
  matching the `verify-phase2`/`verify-phase3` precedent already in the Makefile
- [ ] No `pytest` marker currently distinguishes `tests/unit/` from `tests/integration/` (directory
  separation only) — fine for now since `testpaths = ["tests"]` picks up both, but Phase 6's CI-01
  will need to explicitly scope to `tests/unit/` unless CI also stands up Oracle (`CI-02` is
  deferred to v2, so CI should NOT attempt to run `tests/integration/`)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-------------------|
| V2 Authentication | No | No end-user auth surface in this phase — Oracle connection uses the project's single dev credential pair (INFRA-03), not a user-facing auth flow |
| V3 Session Management | No | Not applicable — batch loader, no sessions |
| V4 Access Control | No | Single Oracle schema (`ADMIN`), single credential pair; no per-user access differentiation in this project's scope |
| V5 Input Validation | Yes | Row-level validation already done by Phase 3 (`validate.py`/`normalize.py`) before rows reach `load.py`. **New finding this phase:** `OracleTargetSpec.valid_table`/`invalid_table` (config/models.py, read this session) are plain `str` fields with **no identifier-format constraint** — since table/column names cannot be bound as SQL parameters (only values can), the INSERT SQL string is built by interpolating these config-sourced names directly. Recommend validating `valid_table`/`invalid_table` (and any config-driven column name used to build a dynamic INSERT) against a strict identifier regex (e.g. `^[A-Za-z_][A-Za-z0-9_]*$`) before use in `load.py`, as defense-in-depth against a malformed/malicious `config.json` — even though `config.json` is developer-authored in this project's threat model, not runtime attacker input |
| V6 Cryptography | Yes (narrow) | `hashlib.sha256` for the file-checksum idempotency key is appropriate — this is a content-addressing/collision-resistance use, not a secrets-protection use, so SHA-256 (not a password hash) is the correct tool. No new crypto library needed. |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|----------------------|
| SQL injection via config-driven table name interpolation | Tampering | Validate `oracle.valid_table`/`oracle.invalid_table` (and any column name) against a strict identifier allowlist regex before string-formatting into an INSERT statement — bind parameters cover *values*, not identifiers, so this check is the only defense available |
| Silent non-persistence from a missing `commit()` | Tampering / Integrity | Exactly one `commit()` call, on the success path only, verified by an integration test that queries Oracle directly (not just `ProcessingResult.status`) — see Pitfall 1 |
| Credentials hardcoded/duplicated across scripts | Information Disclosure | Prefer `os.environ.get(...)`-sourced credentials in `load.py` over copy-pasting `verify_environment.py`'s hardcoded literals — see Pitfall 6 |

## Sources

### Primary (HIGH confidence)
- `/oracle/python-oracledb` (Context7) — `batch_statement.md` (executemany with list-of-dicts,
  setinputsizes named binds, batcherrors/getbatcherrors semantics, type-deferral behavior),
  `txn_management.md` (autocommit default, commit/rollback semantics), `exception_handling.md`
  (`IntegrityError`, `.code`/`.full_code`/`.message` attributes), `appendix_a.md` (Oracle↔Python
  type mapping: `NUMBER`↔`Decimal`, `DATE`/`TIMESTAMP`↔`datetime`), `connection_handling.md` /
  `release_notes.md` (DPY-* driver-level error codes)
- `docker/oracle/init/01_ingestion_metadata.sql`, `02_customers.sql`, `03_orders.sql`,
  `04_widen_invalid_columns.sql` — read in full this session; every column name/type/constraint
  quoted in this document is verbatim from these files
- `scripts/verify_environment.py` — read in full this session; connection pattern and the hardcoded
  credential-literal finding (Pitfall 6) both come directly from this file
- `packages/csv-processor/src/csv_processor/engine.py`, `errors.py`, `normalize.py`,
  `config/models.py` — read in full this session; `process_chunks()`'s exact yield shape, the
  invalid-row dict's `raw_line` key, the six typed-value Python types, and `DatasetConfig`'s field
  shapes are all confirmed from these files, not assumed
- `configs/datasets/customers.json`, `orders.json` — read in full this session
- `.planning/REQUIREMENTS.md` — `REL-01`'s v2-deferral of `batcherrors` is the single most
  load-bearing citation in this document
- `.planning/research/ARCHITECTURE.md` — pre-settled design, read in full; the diagram/prose
  inconsistency noted in Pitfall 2 was found by reading it closely, not assumed

### Secondary (MEDIUM confidence)
- `pip index versions oracledb` (run this session) — confirms 4.0.2 is PyPI's current latest,
  matching the already-pinned version

### Tertiary (LOW confidence)
- None used for load-bearing claims in this document.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new packages, existing pinned version confirmed still current
- Architecture: HIGH — every table/config/engine-contract claim grounded in files read this session, not training memory
- Pitfalls: HIGH — all six pitfalls trace to either an official-docs citation or a verbatim file read this session

**Research date:** 2026-08-29
**Valid until:** 2026-09-28 (30 days — stable driver API, unlikely to change; re-verify `oracledb`
version currency if this phase's planning/execution slips past that window)
