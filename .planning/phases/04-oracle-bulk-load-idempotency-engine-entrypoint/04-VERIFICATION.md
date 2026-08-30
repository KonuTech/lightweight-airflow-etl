---
phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
verified: 2026-08-29T00:00:00Z
status: passed
score: 11/11 must-haves verified
behavior_unverified: 0
overrides_applied: 0
---

# Phase 4: Oracle Bulk Load, Idempotency & Engine Entrypoint Verification Report

**Phase Goal:** Validated rows from the engine land in Oracle via true bulk operations,
re-processing an already-recorded file is a safe no-op, and the engine's public entrypoint
returns a complete, correctly-classified result.
**Verified:** 2026-08-29
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Processing a fixture file bulk-inserts valid rows into `<DATASET>_VALID` and invalid rows (with error metadata) into `<DATASET>_INVALID` via `executemany()`, verified against real Oracle | ✓ VERIFIED | `load.insert_rows()` builds `INSERT ... VALUES (:col...)` and calls `cursor.executemany(sql, rows)` (load.py:136-139); proven by `tests/integration/test_load_oracle.py::test_valid_rows_bulk_inserted`/`test_invalid_rows_bulk_inserted` and `tests/integration/test_engine_process_oracle.py::test_process_success_status_end_to_end`/`test_process_success_with_invalid_rows`, all asserting via direct `SELECT COUNT(*)` against the real running Oracle container (`docker compose ps oracle` confirmed `Up ... (healthy)`) |
| 2 | Every processed file produces exactly one `ingestion_metadata` row with file_name, checksum, dataset, timestamp, total/valid/invalid counts | ✓ VERIFIED | `load.record_ingestion()` inserts all 7 non-generated columns; `processed_at` has an Oracle-side `DEFAULT SYSTIMESTAMP` (`docker/oracle/init/01_ingestion_metadata.sql`). Proven for a normal file (`test_ingestion_metadata_recorded`), a zero-row file (`test_zero_row_file_still_records_metadata_row`), and via `process()` itself (`test_process_reprocess_returns_identical_result` asserts `metadata_count == 1` both before and after a second call) |
| 3 | Re-processing the same file (same filename + checksum + dataset) does not duplicate rows in either target table | ✓ VERIFIED | App-level fast path: `find_existing_ingestion()` keyed on `(dataset, checksum)` only (never `file_name`) — proven by `test_reprocess_is_idempotent` and `test_renamed_file_same_checksum_is_treated_as_same_file`. DB-level backstop: `record_ingestion()` twice for the same `(dataset, checksum)` raises `oracledb.IntegrityError` with `full_code == "ORA-00001"` — proven by `test_duplicate_checksum_raises_integrity_error`. Full-stack proof at the `process()` level: `test_process_reprocess_returns_identical_result` calls `process()` twice on the byte-identical file and asserts row counts in `customers_valid`/`customers_invalid`/`ingestion_metadata` are unchanged between calls |
| 4 | `process(file_path, config)` returns a `ProcessingResult` with the correct status for each scenario (all 7 closed `Status` values) | ✓ VERIFIED | All 7 reachable and correctly classified: `SUCCESS`/`SUCCESS_WITH_INVALID_ROWS`/`DATABASE_ERROR`/reprocess round trip proven against real Oracle (`tests/integration/test_engine_process_oracle.py`, 4 tests); `FILE_NOT_FOUND`/`CONFIGURATION_ERROR`/`INVALID_FILE`/`PROCESSING_ERROR` proven mocked and fast (`tests/unit/test_engine_process.py`, 4 tests) — plus the gap-closure regression test `test_connection_failure_returns_database_error` proving a real `oracledb.Error` from `load.get_connection()` now yields `DATABASE_ERROR` instead of an unhandled `AttributeError` (CR-01 closed) |

### PLAN-Level Must-Haves (additional, beyond ROADMAP SCs)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 5 | `insert_rows()` returns immediately without calling `executemany()` for an empty row list | ✓ VERIFIED | `load.py:125-126` (`if not rows: return`); proven by `test_insert_rows_skips_executemany_for_empty_list` (row count unchanged, no exception) |
| 6 | An oversized value (exceeding declared VARCHAR2 width) raises `oracledb.DatabaseError` during `executemany()` rather than silent truncation | ✓ VERIFIED | `test_oversized_value_raises_database_error` (load-primitive level) and `test_process_oversized_value_returns_database_error` (full `process()` level, also proving rollback — 0 rows left in `customers_invalid`) |
| 7 | A `config.json` declaring an unsafe SQL identifier (table/column name) is rejected by Pydantic validation at config-load time, before any SQL is built | ✓ VERIFIED | `is_safe_identifier()` regex allowlist (`config/models.py:33-44`), enforced via `ColumnSpec._check_name_is_safe_sql_identifier` and `OracleTargetSpec._check_table_names_are_safe_sql_identifiers`; proven by `tests/unit/test_config_models.py::test_column_name_rejects_unsafe_identifier`/`test_oracle_target_spec_rejects_unsafe_table_name` (both raise `pydantic.ValidationError`) |
| 8 | The identifier allowlist is re-checked defense-in-depth inside `load.insert_rows` immediately before building the INSERT string | ✓ VERIFIED | `load.py:128-134` calls `is_safe_identifier()` on `table` and every entry in `columns`, raising `ValueError` before any string interpolation |
| 9 | `ingestion_metadata` is insert-only — no `UPDATE` path exists | ✓ VERIFIED | `grep -n "UPDATE ingestion_metadata" packages/csv-processor/src/csv_processor/load.py` returns no matches; `record_ingestion()`'s only statement is a plain `INSERT` |
| 10 | CR-01 gap-closure: `connection.rollback()` is only called when `connection is not None` in both `except StructuralValidationError:` and `except oracledb.Error:` branches | ✓ VERIFIED | `engine.py:294-300` — both branches now read `if connection is not None:\n    connection.rollback()`; confirmed via direct grep and via `test_connection_failure_returns_database_error` (patches `load.get_connection` with `side_effect=oracledb.OperationalError(...)`, asserts `Status.DATABASE_ERROR`, not a crash) |
| 11 | `make verify-phase4` (unit + real-Oracle integration) is green with no regressions after the gap-closure fix | ✓ VERIFIED | Ran independently in this verification session: `uv run pytest tests/unit/ -q` → 199 passed; `uv run pytest tests/integration/ -q` → 13 passed (Oracle container confirmed `Up ... (healthy)` beforehand) |

**Score:** 11/11 truths verified (0 present-but-behavior-unverified)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `packages/csv-processor/src/csv_processor/models.py` | `Status` (7-member closed enum) + `ProcessingResult` | ✓ VERIFIED | Exact 7 members verbatim; frozen, `extra="forbid"` BaseModel with all documented fields |
| `packages/csv-processor/src/csv_processor/load.py` | `sha256_file`, `get_connection`, `insert_rows`, `find_existing_ingestion`, `record_ingestion`, `INVALID_ROW_SUFFIX_COLUMNS` | ✓ VERIFIED | All present with exact signatures per plan; env-var-first credentials (`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD`/`ORACLE_DSN`), never `ORACLE_USER`/`ORACLE_PASSWORD` |
| `packages/csv-processor/src/csv_processor/config/models.py` | `is_safe_identifier()` + 2 new validators | ✓ VERIFIED | Regex `^[A-Za-z_][A-Za-z0-9_]{0,127}$`; validators on `ColumnSpec`/`OracleTargetSpec` |
| `packages/csv-processor/src/csv_processor/engine.py` | `process()` entrypoint, CR-01 fix applied | ✓ VERIFIED | Full sequence implemented per 04-RESEARCH.md; `# type: ignore` count is 0 (both suppressions removed as documented byproduct) |
| `tests/integration/conftest.py` | `oracle_cursor` + autouse `clean_customers_tables` fixtures | ✓ VERIFIED | Both present, correctly scoped |
| `tests/integration/test_load_oracle.py` | 9 tests covering LOAD-01..04 | ✓ VERIFIED | All 9 present, all pass, all query real Oracle directly |
| `tests/integration/test_engine_process_oracle.py` | 4 tests covering DB-dependent status paths | ✓ VERIFIED | All 4 present, all pass |
| `tests/unit/test_engine_process.py` | 5 tests (4 non-DB paths + CR-01 regression) | ✓ VERIFIED | All 5 present, all pass, zero real Oracle connections attempted for the mocked tests |
| `Makefile` (`verify-phase4` target) | Combined unit + integration gate | ✓ VERIFIED | `.PHONY` entry + target present; ran independently, both suites green |
| `packages/csv-processor/pyproject.toml` | `oracledb==4.0.2` pinned dependency | ✓ VERIFIED | Confirmed present |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `process_chunks()`'s `(valid_rows, invalid_rows)` | `load.insert_rows()` | Exact dict-key-to-bind-name match | ✓ WIRED | `engine.py:231-244` passes chunk rows directly to `insert_rows`, no adapter |
| `config.oracle.valid_table`/`invalid_table` + column names | `load._build_insert_sql`'s identifier validation | Checked BEFORE string interpolation | ✓ WIRED | `load.py:128-134` |
| `load.record_ingestion()`'s INSERT | Oracle's `UNIQUE(dataset, checksum)` constraint | `oracledb.IntegrityError(full_code='ORA-00001')` | ✓ WIRED | Proven by `test_duplicate_checksum_raises_integrity_error` and `process()`'s own race-handling branch (`engine.py:258-281`) |
| `csv_processor.config.errors.ConfigurationError`/`pydantic.ValidationError` | `process()`'s re-validation catch | `Status.CONFIGURATION_ERROR` | ✓ WIRED | `engine.py:199-202`, proven by `test_invalid_config_returns_configuration_error` |
| `StructuralValidationError` (from `process_chunks()`) | `process()`'s sole catcher | `Status.INVALID_FILE` | ✓ WIRED | `engine.py:294-297`, proven by `test_structurally_broken_file_returns_invalid_file` |
| `load.find_existing_ingestion()` FOUND branch | D-01 short-circuit | Return before `process_chunks()` is called | ✓ WIRED | `engine.py:215-226` |
| `load.get_connection()`'s real failure (`oracledb.OperationalError`) | `process()`'s `except oracledb.Error:` | `Status.DATABASE_ERROR`, never `AttributeError` | ✓ WIRED (gap-closure) | Proven by `test_connection_failure_returns_database_error` |

### Behavioral Spot-Checks / Full-Suite Run

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Full unit suite green | `uv run pytest tests/unit/ -q` | 199 passed | ✓ PASS |
| Full real-Oracle integration suite green | `uv run pytest tests/integration/ -q` | 13 passed | ✓ PASS |
| CR-01 regression test passes in isolation | `uv run pytest tests/unit/test_engine_process.py::test_connection_failure_returns_database_error -x -q` | 1 passed | ✓ PASS |
| `type: ignore` fully removed from `engine.py` | `grep -c "type: ignore" engine.py` | `0` | ✓ PASS |
| No `UPDATE` against `ingestion_metadata` | `grep -n "UPDATE ingestion_metadata" load.py` | no matches | ✓ PASS |
| Both rollback branches guarded | `grep -A1 "except StructuralValidationError:\|except oracledb.Error:" engine.py` | both show `if connection is not None:` | ✓ PASS |

Oracle container confirmed running and healthy (`docker compose ps oracle` → `Up 12 hours (healthy)`) before running the integration suite — the suite was NOT skipped.

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|--------------|--------|----------|
| LOAD-01 | 04-01 | Valid rows bulk-inserted via `executemany()` | ✓ SATISFIED | `test_valid_rows_bulk_inserted`, `test_process_success_status_end_to_end` |
| LOAD-02 | 04-01 | Invalid rows bulk-inserted with error metadata | ✓ SATISFIED | `test_invalid_rows_bulk_inserted`, `test_process_success_with_invalid_rows` |
| LOAD-03 | 04-01 | One `ingestion_metadata` row per file, correct counts | ✓ SATISFIED | `test_ingestion_metadata_recorded`, `test_zero_row_file_still_records_metadata_row` |
| LOAD-04 | 04-01 | Re-processing is a safe no-op | ✓ SATISFIED | `test_reprocess_is_idempotent`, `test_duplicate_checksum_raises_integrity_error`, `test_process_reprocess_returns_identical_result` |
| ENGINE-08 | 04-02, 04-03 | `process()` returns correct structured status for all 7 cases | ✓ SATISFIED | All 8 status-path tests across unit+integration; CR-01 gap-closure regression test |
| TEST-02 | 04-01, 04-02 | Integration tests exercise real Oracle, not mocked | ✓ SATISFIED | 13 tests in `tests/integration/`, all confirmed running against the real container in this verification session |

No orphaned requirements — REQUIREMENTS.md's traceability table maps exactly these 6 IDs to Phase 4, and all 6 appear in the union of the 3 plans' `requirements:` frontmatter.

### Anti-Patterns Found

None. Scanned all created/modified files (`models.py`, `load.py`, `config/models.py`, `engine.py`, `conftest.py`, `test_load_oracle.py`, `test_engine_process_oracle.py`, `test_engine_process.py`, `test_config_models.py`, `Makefile`) for `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER`/"not yet implemented" — zero matches.

### Open Items From 04-REVIEW.md (Assessed, Not Fixed — By Design)

04-REVIEW.md's CR-01 (BLOCKER) was fixed by 04-03. The following WARNING/INFO items remain open, deliberately scoped out of 04-03 per its own "Out of scope" section. Assessed against Phase 4's stated goal/success criteria (not against some other bar):

| ID | Item | Blocking for Phase 4's goal? | Reasoning |
|----|------|------------------------------|-----------|
| WR-02 | `ColumnSpec` doesn't forbid a stray `format` field on non-date/timestamp columns | No | This is a Phase 2 config-model completeness gap (config authoring ergonomics), not a bulk-load/idempotency/entrypoint-status defect. None of Phase 4's 4 success criteria depend on this validation being stricter. |
| WR-03 | Oracle credential fallback to `admin`/`admin` is silent (no warning log) | No | INFRA-03 (Phase 1) explicitly establishes `admin`/`admin` as the single documented dev credential pair used consistently everywhere — this fallback is the *intended* behavior, not a regression; the only gap is observability (a missing log line) if the env var is ever misconfigured, which doesn't affect any Phase 4 success criterion. |
| WR-04 | `process()`'s cursor is never explicitly `.close()`d | No | `connection.close()` in the `finally` block already invalidates the cursor implicitly (confirmed no resource leak across the life of one `process()` call); doesn't affect correctness of bulk-load, idempotency, or status-classification, which is what Phase 4's success criteria test. |
| IN-01 | Broader `# type: ignore` suppression pattern elsewhere in the codebase | No | Out of this phase's file set entirely (04-03 removed the two instances directly implicated in CR-01); no bearing on Phase 4's goal. |

None of these represent a silent regression of a required truth — all are genuinely deferred, non-blocking robustness/completeness items, consistent with 04-03-PLAN.md's explicit scoping.

### Human Verification Required

None. All must-haves were verifiable programmatically against the real running Oracle container.

### Gaps Summary

No gaps found. All 4 ROADMAP success criteria and all 7 additional plan-level must-haves are verified with real, substantive evidence — actual `executemany()` calls, actual `SELECT` queries against a live Oracle Database Free container, actual `IntegrityError`/`DatabaseError` exceptions triggered and caught. The full unit (199 tests) and integration (13 tests) suites were re-run independently in this verification session (not merely trusted from SUMMARY.md) and both passed. The 04-03 gap-closure plan's fix for CR-01/WR-01 was confirmed directly in `engine.py`'s source (both `connection.rollback()` call sites now guarded on `connection is not None`) and via its dedicated regression test. The 04-REVIEW.md items left open (WR-02/WR-03/WR-04/IN-01) are correctly non-blocking for this phase's stated goal.

---

*Verified: 2026-08-29*
*Verifier: Claude (gsd-verifier)*
