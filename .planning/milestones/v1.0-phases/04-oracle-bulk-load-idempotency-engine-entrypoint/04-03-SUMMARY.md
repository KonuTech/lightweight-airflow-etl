---
phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
plan: 03
subsystem: database
tags: [oracledb, exception-handling, gap-closure, engine-entrypoint]

# Dependency graph
requires:
  - phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
    plan: "04-02"
    provides: "csv_processor.engine.process() -- the function this plan's gap-closure fix corrects"
provides:
  - "csv_processor.engine.process()'s except StructuralValidationError:/except oracledb.Error: branches guard connection.rollback() on connection is not None -- CR-01/WR-01 (04-REVIEW.md) closed"
  - "tests/unit/test_engine_process.py::test_connection_failure_returns_database_error -- regression coverage for a real oracledb.Error raised by load.get_connection() itself (WR-05 closed)"
affects: ["05 (Airflow DAG wiring -- process() is the exact function the DAG will call, now provably crash-free on Oracle connection failure)"]

# Actuals (#2632)
actuals:
  tokens: 612
  tasks: 3
  commits: 2

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Both StructuralValidationError and oracledb.Error except branches now mirror the already-correct except Exception: branch's `if connection is not None: connection.rollback()` guard -- three consistent guard sites instead of two unguarded plus one guarded"

key-files:
  created: []
  modified:
    - packages/csv-processor/src/csv_processor/engine.py
    - tests/unit/test_engine_process.py

key-decisions:
  - "Task 3 (make verify-phase4 regression check) is verification-only per the plan's own files_modified spec -- no files created/modified, no commit needed beyond Task 1's test and Task 2's fix"

requirements-completed: [ENGINE-08]

coverage:
  - id: D1
    description: "A real oracledb.Error (oracledb.OperationalError) raised by load.get_connection() results in process() returning ProcessingResult(status=Status.DATABASE_ERROR, total_rows=0, valid_rows=0, invalid_rows=0), never an unhandled AttributeError -- CR-01 closed"
    requirement: "ENGINE-08"
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_process.py::test_connection_failure_returns_database_error"
        status: pass
    human_judgment: false
  - id: D2
    description: "connection.rollback() is called only when connection is not None in both the except StructuralValidationError: and except oracledb.Error: branches -- WR-01's fragile-by-construction guard gap closed, mirroring the already-correct except Exception: pattern"
    requirement: "ENGINE-08"
    verification:
      - kind: unit
        ref: "grep -A1 -n \"except StructuralValidationError:|except oracledb.Error:\" packages/csv-processor/src/csv_processor/engine.py"
        status: pass
    human_judgment: false
  - id: D3
    description: "Full tests/unit/ suite (199 tests) and real-Oracle tests/integration/ suite (13 tests) both remain green after the fix -- no regression to any of the 7 status paths Plan 04-01/04-02 established"
    requirement: "ENGINE-08"
    verification:
      - kind: integration
        ref: "make verify-phase4"
        status: pass
    human_judgment: false

duration: 2min
completed: 2026-08-29
status: complete
---

# Phase 4 Plan 3: Guard connection.rollback() Against a None Connection Summary

**Fixed CR-01/WR-01 (04-REVIEW.md): both `connection.rollback()` call sites in `process()`'s `except StructuralValidationError:`/`except oracledb.Error:` branches now guard on `connection is not None`, closing a real, empirically-verified crash where a bare Oracle connection failure (`oracledb.OperationalError`) raised an unhandled `AttributeError` instead of returning `Status.DATABASE_ERROR`.**

## Performance

- **Duration:** ~2 min
- **Started:** 2026-08-29T18:11:24Z
- **Completed:** 2026-08-29T18:12:40Z
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments
- `csv_processor.engine.process()`'s documented "never raises" contract (ENGINE-08) is now provably held for the single most ordinary Oracle failure mode: the connection itself cannot be opened (Oracle down, bad credentials, network unreachable)
- CR-01 (BLOCKER) closed: `except oracledb.Error:` now guards `connection.rollback()` on `connection is not None`, mirroring the already-correct `except Exception:` pattern three lines below it
- WR-01 (WARNING) closed alongside it: `except StructuralValidationError:` gets the identical guard for defense-in-depth consistency, even though it was not independently reachable as a crash today
- WR-05 (WARNING, coverage gap) closed: a new regression test proves a real `oracledb.Error` from `load.get_connection()` now yields `DATABASE_ERROR` instead of crashing, mirroring the RED proof documented empirically in 04-REVIEW.md
- IN-01's two specific `# type: ignore[union-attr]` instances on these exact lines removed as a direct byproduct of the guard (the type checker no longer needs suppressing); IN-01's broader pattern elsewhere in the codebase remains untouched, as planned
- Full regression check green: 199/199 unit tests, 13/13 real-Oracle integration tests (`make verify-phase4`), zero collateral damage to Plan 04-01/04-02's already-proven 7-status-path coverage

## Task Commits

Each task was committed atomically (TDD RED/GREEN split):

1. **Task 1: Regression test proving CR-01's crash is real (RED)** - `e2c0be5` (test)
2. **Task 2: Guard connection.rollback() against a None connection (GREEN)** - `14f0b03` (fix)
3. **Task 3: Full unit + real-Oracle integration regression check** - verification-only, no commit (no files created/modified per plan's `files_modified: N/A` spec)

## Files Created/Modified
- `tests/unit/test_engine_process.py` - added `test_connection_failure_returns_database_error`, patching `csv_processor.engine.load.get_connection` with `side_effect=oracledb.OperationalError(...)` and asserting `Status.DATABASE_ERROR` (not a crash)
- `packages/csv-processor/src/csv_processor/engine.py` - both `except StructuralValidationError:` and `except oracledb.Error:` branches now read `if connection is not None: connection.rollback()` instead of an unguarded `connection.rollback()  # type: ignore[union-attr]`

## Decisions Made
- Task 3 ran as a pure verification gate (`make verify-phase4`) with no code changes and no separate commit, exactly matching the plan's own `files: N/A -- verification-only task` spec for that task.
- Confirmed empirically (not assumed) both before and after the fix: the RED run reproduced the exact `AttributeError: 'NoneType' object has no attribute 'rollback'` traceback documented in 04-REVIEW.md's CR-01 finding, and the GREEN run confirmed `grep -c "type: ignore"` on `engine.py` returns `0`.

## Deviations from Plan

None - plan executed exactly as written. No Rule 1/2/3/4 auto-fixes were needed; the fix matched 04-REVIEW.md's prescribed patch verbatim.

## Issues Encountered

None. The Oracle container was already up and healthy (`docker compose ps oracle` showed `Up ... (healthy)`) before Task 3's precondition check ran, so no `make up` was needed.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- CR-01 (BLOCKER) and WR-01/WR-05 (WARNING) are closed; `process()`'s "never raises" contract is now correct for every reachable failure mode this phase's test suite exercises, including the one that previously crashed silently.
- WR-02 (stray `format` field on non-date columns), WR-03 (silent credential fallback), WR-04 (cursor not explicitly closed), and IN-01's broader `# type: ignore`-suppression pattern elsewhere in the codebase remain open and undisturbed, as scoped -- tracked in `04-REVIEW.md` for a future pass if prioritized.
- Phase 5's Airflow DAG can call `process()` with full confidence that an Oracle outage at connection time surfaces as `Status.DATABASE_ERROR`, not an unhandled exception that would crash the DAG task.

---
*Phase: 04-oracle-bulk-load-idempotency-engine-entrypoint*
*Completed: 2026-08-29*

## Self-Check: PASSED

All 2 created/modified files confirmed present on disk; both task commit hashes (`e2c0be5`, `14f0b03`) confirmed in `git log`.
