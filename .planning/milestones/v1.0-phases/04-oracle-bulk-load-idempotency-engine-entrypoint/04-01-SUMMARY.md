---
phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
plan: 01
subsystem: database
tags: [oracledb, executemany, pydantic, idempotency, sql-injection]

# Dependency graph
requires:
  - phase: 01-environment-oracle-foundation
    provides: "Oracle DDL (customers_valid/invalid, orders_valid/invalid, ingestion_metadata with UNIQUE(dataset, checksum)), env-var-first admin/admin credential pattern"
  - phase: 02-config-contract-csv-generator
    provides: "DatasetConfig/ColumnSpec/OracleTargetSpec Pydantic v2 model tree"
  - phase: 03-csv-processing-engine
    provides: "process_chunks() generator yielding (valid_rows, invalid_rows) per chunk"
provides:
  - "csv_processor.load: sha256_file, get_connection, insert_rows, find_existing_ingestion, record_ingestion"
  - "csv_processor.models: Status (7-member closed enum), ProcessingResult"
  - "csv_processor.config.models.is_safe_identifier() SQL-identifier allowlist, enforced on ColumnSpec.name and OracleTargetSpec.valid_table/invalid_table"
  - "tests/integration/ real-Oracle test suite (conftest.py fixtures + test_load_oracle.py, 9 tests)"
affects: ["04-02 (process() entrypoint)", "05 (Airflow DAG wiring)"]

# Actuals (#2632)
actuals:
  tokens: 7800
  tasks: 2
  commits: 3

# Tech tracking
tech-stack:
  added: ["oracledb==4.0.2 (csv-processor package dependency)"]
  patterns:
    - "One connection per process()-style call; load.py never commits/rollbacks itself, caller owns the transaction boundary"
    - "SQL-identifier allowlist regex enforced at two layers: Pydantic model_validator (config-load time) + defense-in-depth re-check in insert_rows"
    - "Insert-only ingestion_metadata; idempotency race resolved via oracledb.IntegrityError.full_code == 'ORA-00001'"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/models.py
    - packages/csv-processor/src/csv_processor/load.py
    - tests/integration/__init__.py
    - tests/integration/conftest.py
    - tests/integration/test_load_oracle.py
  modified:
    - packages/csv-processor/src/csv_processor/config/models.py
    - packages/csv-processor/pyproject.toml
    - tests/unit/test_config_models.py

key-decisions:
  - "load.py reads ORACLE_APP_USER/ORACLE_APP_USER_PASSWORD/ORACLE_DSN via os.environ.get(..., 'admin')-style fallbacks -- this project's real docker-compose.yml env var names, never verify_environment.py's hardcoded literals (04-RESEARCH.md Pitfall 6)"
  - "is_safe_identifier() defined once in config/models.py and imported by both the Pydantic validators and load.insert_rows, rather than duplicating the regex"
  - "No TYPE_CHECKING-guarded lazy oracledb import in load.py (unlike verify_environment.py) -- this module's entire purpose is Oracle I/O, so a lazy import buys nothing"

patterns-established:
  - "TDD RED/GREEN split for Task 1 (tracer): test-only commit demonstrating real failures (ValidationError not raised, ImportError on missing load module), followed by a feat commit implementing the surface"
  - "Test-only commit for Task 2 per the plan's own carve-out -- no production change needed since the empty-list guard already existed"

requirements-completed: [LOAD-01, LOAD-02, LOAD-03, LOAD-04, TEST-02]

coverage:
  - id: D1
    description: "Valid rows bulk-inserted into customers_valid via cursor.executemany(), confirmed by direct SELECT COUNT(*)"
    requirement: "LOAD-01"
    verification:
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_valid_rows_bulk_inserted"
        status: pass
    human_judgment: false
  - id: D2
    description: "Invalid rows bulk-inserted into customers_invalid with error_code/error_message/source_file/row_number/raw_line, confirmed by direct SELECT"
    requirement: "LOAD-02"
    verification:
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_invalid_rows_bulk_inserted"
        status: pass
    human_judgment: false
  - id: D3
    description: "Exactly one ingestion_metadata row per processed file with correct counts/status, including a zero-row (header-only) file"
    requirement: "LOAD-03"
    verification:
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_ingestion_metadata_recorded"
        status: pass
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_zero_row_file_still_records_metadata_row"
        status: pass
    human_judgment: false
  - id: D4
    description: "Re-processing an already-recorded file is a safe no-op: app-level (dataset, checksum)-only lookup returns the original outcome without re-inserting, and a genuine DB-level race raises oracledb.IntegrityError (ORA-00001)"
    requirement: "LOAD-04"
    verification:
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_reprocess_is_idempotent"
        status: pass
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_duplicate_checksum_raises_integrity_error"
        status: pass
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_renamed_file_same_checksum_is_treated_as_same_file"
        status: pass
    human_judgment: false
  - id: D5
    description: "insert_rows() empty-batch guard and oversized-value DATABASE_ERROR proof"
    verification:
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_insert_rows_skips_executemany_for_empty_list"
        status: pass
      - kind: integration
        ref: "tests/integration/test_load_oracle.py::test_oversized_value_raises_database_error"
        status: pass
    human_judgment: false
  - id: D6
    description: "SQL-injection-via-identifier gap closed: ColumnSpec.name and OracleTargetSpec.valid_table/invalid_table rejected at config-load time when not a safe identifier"
    requirement: "TEST-02"
    verification:
      - kind: unit
        ref: "tests/unit/test_config_models.py::TestSqlIdentifierAllowlist::test_column_name_rejects_unsafe_identifier"
        status: pass
      - kind: unit
        ref: "tests/unit/test_config_models.py::TestSqlIdentifierAllowlist::test_oracle_target_spec_rejects_unsafe_table_name"
        status: pass
    human_judgment: false

duration: 15min
completed: 2026-08-29
status: complete
---

# Phase 4 Plan 1: Oracle Bulk Load Primitives Summary

**`csv_processor.load` bulk-inserts valid/invalid rows via `cursor.executemany()`, records one `ingestion_metadata` row per file with a `(dataset, checksum)`-only idempotency guard backed by Oracle's `UNIQUE` constraint, and closes a real SQL-injection-via-identifier gap in Phase 2's `config/models.py` -- all proven against the real running Oracle container, no mocks.**

## Performance

- **Duration:** ~15 min
- **Completed:** 2026-08-29
- **Tasks:** 2
- **Files modified:** 8 (5 created, 3 modified)

## Accomplishments
- `csv_processor.load`: `sha256_file`, `get_connection`, `insert_rows`, `find_existing_ingestion`, `record_ingestion` -- the full Oracle bulk-load/idempotency primitive set, env-var-first credentials (`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD`)
- `csv_processor.models`: `Status` (7-member closed enum) + `ProcessingResult`, ready for Plan 04-02's `process()` entrypoint to consume
- `csv_processor.config.models.is_safe_identifier()`: SQL-identifier allowlist regex enforced on `ColumnSpec.name` and `OracleTargetSpec.valid_table`/`invalid_table` at config-load time, plus a defense-in-depth re-check inside `load.insert_rows` immediately before building the INSERT string
- `tests/integration/`: a real-Oracle integration suite (9 tests) proving LOAD-01 through LOAD-04, including the idempotency race (`oracledb.IntegrityError`/`ORA-00001`), the empty-batch guard, and the oversized-value `DATABASE_ERROR` path

## Task Commits

Each task was committed atomically (TDD RED/GREEN split for the tracer task):

1. **Task 1 (tracer): models.py, config identifier-allowlist, and load.py's core primitives**
   - `f113970` (test, RED) - failing identifier-rejection tests + integration suite that fails to collect (no `load.py` yet)
   - `ba196d7` (feat, GREEN) - `models.py`, `load.py`, `config/models.py` validators, `oracledb==4.0.2` added to `packages/csv-processor/pyproject.toml`
2. **Task 2 (auto): idempotency mechanism, empty-batch guard, and oversized-value proof**
   - `833a78d` (test) - 6 new integration tests; no production change needed (empty-list guard already existed from Task 1)

_Tracer feedback gate: re-ran Task 1's `<verify>` end-to-end after committing (both `uv run pytest tests/unit/test_config_models.py -k identifier -x` and `uv run pytest tests/integration/test_load_oracle.py -x`) -- both green, proceeded directly to Task 2 (auto mode active, `_auto_chain_active: true`)._

## Files Created/Modified
- `packages/csv-processor/src/csv_processor/models.py` - `Status` enum + `ProcessingResult`
- `packages/csv-processor/src/csv_processor/load.py` - Oracle bulk-load/idempotency primitives
- `packages/csv-processor/src/csv_processor/config/models.py` - `is_safe_identifier()` + two new `model_validator`s
- `packages/csv-processor/pyproject.toml` - `oracledb==4.0.2` dependency
- `tests/integration/__init__.py` - new package
- `tests/integration/conftest.py` - `oracle_cursor` fixture + autouse `clean_customers_tables` cleanup
- `tests/integration/test_load_oracle.py` - 9 tests covering LOAD-01/02/03/04
- `tests/unit/test_config_models.py` - 2 new identifier-allowlist tests

## Decisions Made
- Credentials read via `os.environ.get("ORACLE_APP_USER", "admin")`-style fallbacks in `load.py` rather than mirroring `verify_environment.py`'s hardcoded literals, per 04-RESEARCH.md's Pitfall 6/Assumption A2 resolution -- test fixtures (`conftest.py`) may still rely on the same dev-default fallback since they never hardcode credentials themselves.
- `is_safe_identifier()` lives once in `csv_processor.config.models` and is imported directly by `load.py` (no re-export via `config/__init__.py` needed, since only `load.py` and the config models themselves use it).
- No `TYPE_CHECKING`-guarded lazy `oracledb` import in `load.py` (unlike `verify_environment.py`'s WR-03 convention) -- this module's entire purpose is Oracle I/O, so eager top-level `import oracledb` is correct here.

## Deviations from Plan

None - plan executed exactly as written. Both tasks' `<behavior>`/`<action>` blocks were implemented verbatim; no Rule 1-4 auto-fixes were needed.

## Issues Encountered

None. The Oracle container was already up and healthy (confirmed via `docker compose ps` before this plan started), so the tracer task's `<precondition>` was met without any setup action.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `csv_processor.load` and `csv_processor.models` are fully proven against real Oracle and ready for Plan 04-02's `process(file_path, config) -> ProcessingResult` entrypoint to assemble alongside Phase 3's `process_chunks()`.
- The SQL-injection-via-identifier gap RESEARCH.md flagged in Phase 2's already-existing config code is now closed at both the config-validation layer and the load-time defense-in-depth layer.
- No blockers for Plan 04-02: the exception-to-status mapping table in 04-RESEARCH.md (`ConfigurationError` -> `CONFIGURATION_ERROR`, `FileNotFoundError` -> `FILE_NOT_FOUND`, `StructuralValidationError` -> `INVALID_FILE`, `oracledb.IntegrityError`/`ORA-00001` -> re-read winner's result, any other `oracledb.Error` -> `DATABASE_ERROR`, anything else -> `PROCESSING_ERROR`) is ready to implement directly against this plan's primitives.

---
*Phase: 04-oracle-bulk-load-idempotency-engine-entrypoint*
*Completed: 2026-08-29*

## Self-Check: PASSED

All 8 created/modified files confirmed present on disk; all 3 task commit hashes (`f113970`, `ba196d7`, `833a78d`) confirmed in `git log`.
