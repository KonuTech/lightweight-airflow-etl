---
phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
plan: 02
subsystem: database
tags: [oracledb, executemany, pydantic, idempotency, engine-entrypoint, bounded-memory]

# Dependency graph
requires:
  - phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
    plan: "04-01"
    provides: "csv_processor.load (sha256_file, get_connection, insert_rows, find_existing_ingestion, record_ingestion) and csv_processor.models (Status, ProcessingResult)"
  - phase: 03-csv-processing-engine
    provides: "process_chunks() generator yielding (valid_rows, invalid_rows) per chunk"
provides:
  - "csv_processor.engine.process(file_path, config) -> ProcessingResult -- the phase's public entrypoint (ENGINE-08)"
  - "make verify-phase4 -- the phase's own combined unit + real-Oracle integration gate"
affects: ["05 (Airflow DAG wiring -- process() is the exact function the DAG will call)"]

# Actuals (#2632)
actuals:
  tokens: 7044
  tasks: 3
  commits: 4

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "One Oracle connection per process() call, opened at the top of the Oracle-touching path and closed in a finally block regardless of which status path returns (D-02)"
    - "Checksum computed BEFORE the Oracle connection opens -- pure file I/O, populated on every failure path from INVALID_FILE onward"
    - "D-01 idempotency short-circuit: a found ingestion_metadata record short-circuits BEFORE process_chunks() is ever called, returning the original recorded outcome verbatim"
    - "Nested try/except around record_ingestion() specifically for oracledb.IntegrityError/ORA-00001 (the concurrent-winner race), inside the outer StructuralValidationError -> oracledb.Error -> bare Exception catch chain"

key-files:
  created:
    - tests/unit/test_engine_process.py
  modified:
    - packages/csv-processor/src/csv_processor/engine.py
    - tests/integration/test_engine_process_oracle.py
    - tests/unit/test_engine_chunks.py
    - Makefile

key-decisions:
  - "process() and its oracledb/csv_processor.load imports stay at engine.py's module level (not lazy/function-local) so patch(\"csv_processor.engine.load.get_connection\") remains patchable as a plain module attribute for tests/unit/test_engine_process.py's mocking -- a function-local lazy import was tried and rejected specifically because it breaks that patch target"
  - "tests/unit/test_engine_chunks.py's RLIMIT_AS bounded-memory cap raised from 100 MiB to 128 MiB (134,217,728 bytes) to absorb process()'s new module-level oracledb import, re-verified empirically (3/3 reliable runs for the streaming variant, buffering negative control still reliably fails under the same cap)"
  - "_build_result() is the single ProcessingResult construction site every process() return path shares, keeping duration_seconds/zero-count/checksum defaults consistent across all 7 status paths"

requirements-completed: [ENGINE-08, TEST-02]

coverage:
  - id: D1
    description: "process() returns SUCCESS for an all-valid file, proven end-to-end against real Oracle with a direct SELECT COUNT(*) against customers_valid"
    requirement: "ENGINE-08"
    verification:
      - kind: integration
        ref: "tests/integration/test_engine_process_oracle.py::test_process_success_status_end_to_end"
        status: pass
    human_judgment: false
  - id: D2
    description: "process() returns SUCCESS_WITH_INVALID_ROWS for a mixed valid/invalid file with correct counts, proven via direct SELECT against both customers_valid and customers_invalid"
    requirement: "ENGINE-08"
    verification:
      - kind: integration
        ref: "tests/integration/test_engine_process_oracle.py::test_process_success_with_invalid_rows"
        status: pass
    human_judgment: false
  - id: D3
    description: "process() returns FILE_NOT_FOUND for a missing file without ever attempting an Oracle connection"
    requirement: "ENGINE-08"
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_process.py::test_missing_file_returns_file_not_found"
        status: pass
    human_judgment: false
  - id: D4
    description: "process() returns CONFIGURATION_ERROR when config re-validation fails, before any file I/O or Oracle connection"
    requirement: "ENGINE-08"
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_process.py::test_invalid_config_returns_configuration_error"
        status: pass
    human_judgment: false
  - id: D5
    description: "process() returns INVALID_FILE for a genuinely structurally-broken CSV (missing required column), with checksum populated and zero counts"
    requirement: "ENGINE-08"
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_process.py::test_structurally_broken_file_returns_invalid_file"
        status: pass
    human_judgment: false
  - id: D6
    description: "process() returns PROCESSING_ERROR for an unexpected non-oracledb exception, distinct from the oracledb.Error-specific DATABASE_ERROR path"
    requirement: "ENGINE-08"
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_process.py::test_unexpected_exception_returns_processing_error"
        status: pass
    human_judgment: false
  - id: D7
    description: "process() returns DATABASE_ERROR (never raises) for an oversized value that fails the Oracle INSERT, with the failed chunk's rows never left committed (rollback proven via SELECT COUNT(*))"
    requirement: "ENGINE-08"
    verification:
      - kind: integration
        ref: "tests/integration/test_engine_process_oracle.py::test_process_oversized_value_returns_database_error"
        status: pass
    human_judgment: false
  - id: D8
    description: "D-01: re-processing the identical file returns the same status/counts/checksum both times, with no row duplication in either target table and exactly one ingestion_metadata row"
    requirement: "ENGINE-08"
    verification:
      - kind: integration
        ref: "tests/integration/test_engine_process_oracle.py::test_process_reprocess_returns_identical_result"
        status: pass
    human_judgment: false
  - id: D9
    description: "make verify-phase4 runs tests/unit/ then tests/integration/, both green, as this phase's own combined local gate"
    requirement: "TEST-02"
    verification:
      - kind: integration
        ref: "make verify-phase4"
        status: pass
    human_judgment: false

duration: 25min
completed: 2026-08-29
status: complete
---

# Phase 4 Plan 2: Oracle Bulk Load, Idempotency & Engine Entrypoint Summary

**`csv_processor.engine.process(file_path, config) -> ProcessingResult` assembles config re-validation, the D-01 idempotency short-circuit, `process_chunks()`, and Plan 04-01's Oracle bulk-load primitives behind one entrypoint, proving every one of the 7 closed `Status` values reachable -- SUCCESS/SUCCESS_WITH_INVALID_ROWS/DATABASE_ERROR/the reprocess round trip against the real running Oracle container, FILE_NOT_FOUND/CONFIGURATION_ERROR/INVALID_FILE/PROCESSING_ERROR mocked and fast.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-08-29
- **Tasks:** 3
- **Files modified:** 5 (1 created, 4 modified)

## Accomplishments
- `csv_processor.engine.process()`: the phase's public entrypoint (ENGINE-08) -- owns the full detect->parse->validate->normalize->chunk->load sequence and all status/exception translation, exactly the function Phase 5's Airflow DAG will call
- All 7 closed `Status` values proven reachable with correct semantics: 4 DB-dependent paths against the real Oracle container (no mocks), 4 non-DB paths mocked and fast (under 5 seconds, zero real Oracle connection attempts)
- D-01's re-run contract and D-02's all-or-nothing atomicity both proven at the full `process()` public-API level (not just `load.py`'s primitive level, which 04-01 already covered)
- `make verify-phase4`: this phase's own combined local gate, running the full unit suite then the real-Oracle integration suite
- Closed a genuine cross-file regression (Rule 1): re-tuned `tests/unit/test_engine_chunks.py`'s RLIMIT_AS bounded-memory cap from 100 MiB to 128 MiB after `process()`'s module-level `oracledb`/`csv_processor.load` imports pushed `process_chunks()`'s own import-time memory budget over the old cap

## Task Commits

Each task was committed atomically (TDD RED/GREEN split for the tracer task):

1. **Task 1 (tracer): process() happy path -- SUCCESS status, wired end-to-end against real Oracle**
   - `ff438bc` (test, RED) - `tests/integration/test_engine_process_oracle.py` fails to even collect (`ImportError: cannot import name 'process'`)
   - `02c8823` (feat, GREEN) - `process()` added to `packages/csv-processor/src/csv_processor/engine.py`; SUCCESS status proven against the real Oracle container
2. **Task 2 (auto): non-DB status paths -- CONFIGURATION_ERROR, FILE_NOT_FOUND, INVALID_FILE, PROCESSING_ERROR (mocked, no real Oracle)**
   - `adaa47c` (test) - `tests/unit/test_engine_process.py`; no production change needed, Task 1's `process()` already implements all four paths correctly (mirrors 04-01's own precedent for a test-only commit)
3. **Task 3 (auto): DB-dependent status paths + make verify-phase4**
   - `83375c6` (test) - extended `tests/integration/test_engine_process_oracle.py` with `SUCCESS_WITH_INVALID_ROWS`/`DATABASE_ERROR`/reprocess tests, added `verify-phase4` to the `Makefile`, and re-tuned `tests/unit/test_engine_chunks.py`'s RLIMIT_AS cap (Rule 1 deviation, see below)

_Tracer feedback gate: re-ran Task 1's `<verify>` end-to-end after committing (`uv run pytest tests/integration/test_engine_process_oracle.py -x`) -- green, proceeded directly to Task 2 (auto mode active, `_auto_chain_active: true`)._

## Files Created/Modified
- `packages/csv-processor/src/csv_processor/engine.py` - `process()` entrypoint added alongside Phase 3's existing `process_chunks()`
- `tests/unit/test_engine_process.py` - 4 mocked tests covering the non-DB status paths
- `tests/integration/test_engine_process_oracle.py` - 4 real-Oracle tests covering the DB-dependent status paths and the D-01 reprocess round trip
- `tests/unit/test_engine_chunks.py` - RLIMIT_AS cap raised from 100 MiB to 128 MiB (Rule 1 deviation)
- `Makefile` - `verify-phase4` target added

## Decisions Made
- `process()` and its `oracledb`/`csv_processor.load` imports stay at `engine.py`'s module level (not lazy/function-local) so `patch("csv_processor.engine.load.get_connection")` remains patchable as a plain module attribute for `tests/unit/test_engine_process.py`'s mocking -- a function-local lazy import was tried first specifically to avoid the RLIMIT_AS regression below, then rejected because it silently broke that patch target (`csv_processor.engine.load` would not exist as a module attribute until `process()` had already been called once).
- `_build_result()` is the single `ProcessingResult` construction site every `process()` return path shares, so `duration_seconds`/zero-count/`checksum` defaults stay consistent across all 7 status paths rather than being re-typed at each return site.
- The D-01 short-circuit and the `oracledb.IntegrityError`/`ORA-00001` concurrent-winner race both return via the same construction path (`_build_result` with the recorded row's fields), keeping the "read back the original outcome verbatim" contract enforced in exactly one place.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `tests/unit/test_engine_chunks.py`'s RLIMIT_AS bounded-memory test broke under `process()`'s new module-level Oracle imports**
- **Found during:** Task 3's `make verify-phase4` run
- **Issue:** `test_process_chunks_streaming_survives_the_rlimit_as_cap` spawns a subprocess that sets `RLIMIT_AS` to a tightly empirically-tuned 100 MiB cap, then does `from csv_processor.engine import process_chunks`. Because Task 1 added `process()` to the SAME `engine.py` file with `import oracledb` and `from csv_processor import load` at module scope (needed so `csv_processor.engine.load.get_connection` stays patchable for Task 2's mocking), importing `process_chunks` alone now transitively pulls in the Oracle driver too. This left less address-space headroom for the test's real ~60 MiB file-streaming read, surfacing as a `MemoryError` inside `source.py`'s row iterator (not at import time).
- **Fix:** Re-tuned the cap empirically via a standalone probe script: 100/110/120 MiB all reliably die with a `MemoryError` during the streaming read; 125 MiB is flaky (1 failure in 3 runs); 128 MiB (134,217,728 bytes) reliably succeeds (3/3 runs) while the buffering negative control still reliably fails under the SAME 128 MiB cap, keeping the streaming-vs-buffering contrast meaningful.
- **Files modified:** `tests/unit/test_engine_chunks.py`
- **Commit:** `83375c6`

## Issues Encountered

None beyond the RLIMIT_AS regression documented above. The Oracle container was already up and healthy (confirmed via `docker compose ps oracle` before this plan started), so Task 1's tracer `<precondition>` was met without any setup action.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- `csv_processor.engine.process(file_path, config) -> ProcessingResult` is complete and independently proven correct against both mocked and real-Oracle scenarios -- exactly the function Phase 5's Airflow DAG will call, per ARCHITECTURE.md's own build order.
- `make verify-phase4` is the phase's own combined local gate (unit + real-Oracle integration), ready to be wired into CI in Phase 6 (unit suite only, per 04-RESEARCH.md's own note that CI should not attempt to run `tests/integration/` since standing up Oracle in CI is deferred).
- No blockers for Phase 5: `ProcessingResult.model_dump(mode="json")` is ready to cross the DAG<->engine boundary as a plain dict (ARCHITECTURE.md Pattern 3), carrying only aggregate counts/status/checksum/duration, never credentials or row-level detail (T-04-05, verified by `ProcessingResult`'s frozen/`extra="forbid"` fixed field list).

---
*Phase: 04-oracle-bulk-load-idempotency-engine-entrypoint*
*Completed: 2026-08-29*

## Self-Check: PASSED

All 5 created/modified files confirmed present on disk; all 4 task commit hashes (`ff438bc`, `02c8823`, `adaa47c`, `83375c6`) confirmed in `git log`.
