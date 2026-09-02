---
phase: 10-oraclepartitionreadytrigger-robustness-fix
plan: 01
subsystem: infra
tags: [airflow, oracledb, deferrable-trigger, retry-backoff, asyncio]

# Dependency graph
requires:
  - phase: 07 (v1.0)
    provides: "OraclePartitionReadyTrigger/ReportReadySensor, the custom deferrable BaseTrigger polling ingestion_metadata"
provides:
  - "Bounded retry/backoff on transient oracledb.OperationalError inside OraclePartitionReadyTrigger.run()"
  - "Non-transient oracledb.Error subclasses (e.g. ProgrammingError) still propagate immediately, uncaught"
  - "connection.close() failures inside finally never mask the original propagating exception"
  - "verify-phase10 Makefile target (full unit suite gate)"
affects: [report_ready-dag, deferrable-triggers]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Bounded exponential backoff (base_delay * 2**(n-1), capped at poke_interval) via asyncio.sleep, never time.sleep -- mirrors scripts/verify_environment.py's retry convention"
    - "Narrow except clause (oracledb.OperationalError only) for retry eligibility; every other oracledb.Error subclass propagates immediately"
    - "Nested try/except around a finally-block cleanup call (connection.close()) so a secondary failure during cleanup never masks the primary exception"

key-files:
  created: []
  modified:
    - airflow/dags/_common/oracle_partition_trigger.py
    - tests/unit/test_oracle_partition_trigger.py
    - Makefile

key-decisions:
  - "connect_async() moved inside the outer try block so a connection failure is no longer unhandled (D-01)"
  - "Only oracledb.OperationalError is retried; retry cap is count-based, max 10 consecutive failures (D-02/D-03)"
  - "11th consecutive OperationalError re-raises the original exception uncaught, no custom failure TriggerEvent (D-04)"
  - "connection.close() failures inside finally are caught, logged at debug with exc_info=True, never re-raised (D-06)"

patterns-established:
  - "Per-phase Makefile gate (verify-phaseN) that mirrors the shape of the prior phase's gate unless new infrastructure justifies a different shape -- verify-phase10 follows verify-phase2's plain full-unit-suite shape since this phase touches no live DAG structure"

requirements-completed: [ROBUST-01]

# Metrics
duration: 3min
completed: 2026-09-02
---

# Phase 10 Plan 01: OraclePartitionReadyTrigger Robustness Fix Summary

**Bounded 10-retry exponential backoff on transient `oracledb.OperationalError` inside `OraclePartitionReadyTrigger.run()`'s Oracle polling loop, with non-transient errors still propagating immediately and a guarded `connection.close()` that never masks the original exception.**

## Performance

- **Duration:** 3 min
- **Started:** 2026-09-02T06:18:59Z
- **Completed:** 2026-09-02T06:21:20Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Rewrote `OraclePartitionReadyTrigger.run()` so `connect_async()` sits inside a `try` block covering the full poll cycle, with a bounded (10 consecutive failures) exponential-backoff retry on `oracledb.OperationalError` only
- Any other `oracledb.Error` subclass (e.g. `ProgrammingError` from a bad query or dropped/renamed table) still propagates immediately on first occurrence, no retry attempted
- `connection.close()` inside `finally` is now guarded by its own nested `try/except oracledb.Error`, logged at debug, never masking the original exception that was already propagating
- Added 4 new unit tests covering all 4 D-08 scenarios (retry-then-succeed, exhausted-retry re-raise, non-transient immediate propagation, close-failure non-masking) — file now has 8 passing tests (4 pre-existing + 4 new)
- Added `verify-phase10` Makefile target running the full unit suite, matching `verify-phase2`'s shape

## Task Commits

Each task was committed atomically:

1. **Task 1: Rewrite run() with bounded retry/backoff (D-01 through D-07)** - `43fc0d7` (fix)
2. **Task 2: D-08 test coverage + verify-phase10 Makefile target** - `fd3abe6` (test)

_Note: Task 2 is labeled `tdd="true"` in the plan but was implemented as tests-added-after-implementation (implementation and coverage were designed together per the plan's exact target-code specification in `<interfaces>`), not a strict RED→GREEN cycle — the plan's own two-task structure (Task 1 implements, Task 2 tests) does not fit the plan-level TDD gate model. See Deviations below._

## Files Created/Modified
- `airflow/dags/_common/oracle_partition_trigger.py` - Rewrote `run()` with bounded retry/backoff; added `_MAX_TRANSIENT_RETRIES = 10` and `_RETRY_BASE_DELAY_SECONDS = 1.0` module constants
- `tests/unit/test_oracle_partition_trigger.py` - Added `import oracledb`/`import pytest`, plus 4 new test functions covering the D-08 scenarios
- `Makefile` - Added `verify-phase10` target (`.PHONY` line + target body), following `verify-phase2`'s plain full-suite shape

## Decisions Made
- Followed the plan's `<interfaces>` target `run()` implementation verbatim — no deviation on the retry/backoff shape, exception scoping, or logging format strings
- Ran `ruff format` on the new test file after adding the 4 new tests — one line exceeded the project's line-length/formatting convention (an `AsyncMock(side_effect=[...] * 11)` call); auto-reformatted, zero behavior change

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug/Formatting] Reformatted new test code to satisfy `ruff format --check`**
- **Found during:** Task 2 (after adding the 4 new D-08 tests)
- **Issue:** One new `patch(...)` call in `test_run_reraises_after_exhausting_transient_retries` spanned an extra line beyond `ruff format`'s canonical formatting
- **Fix:** Ran `uv run ruff format tests/unit/test_oracle_partition_trigger.py`; re-verified `ruff format --check` passes and all 8 tests in the file still pass
- **Files modified:** `tests/unit/test_oracle_partition_trigger.py`
- **Verification:** `uv run ruff format --check tests/unit/test_oracle_partition_trigger.py` passes; `uv run pytest tests/unit/test_oracle_partition_trigger.py -x -v` still 8 passed
- **Committed in:** `fd3abe6` (Task 2 commit — formatting was applied before staging/committing, not a separate commit)

---

**Total deviations:** 1 auto-fixed (Rule 1, cosmetic formatting only)
**Impact on plan:** No scope creep. All plan-specified behavior, exception scoping, and test coverage implemented exactly as written in `<interfaces>`/`<action>`.

### Documentation discrepancy (not a deviation, noted for accuracy)

The plan's acceptance criteria and `<done>` text describe the pre-existing test file as having "3 passed" / "3 pre-existing tests," expecting "7 passed (3 pre-existing + 4 new)" after Task 2. The actual pre-existing file (read at execution start) contains 4 tests (`test_serialize_returns_the_expected_classpath_and_poke_interval`, `test_run_does_not_yield_when_only_one_dataset_is_present`, `test_run_yields_exactly_one_trigger_event_once_both_datasets_present`, `test_poll_query_uses_real_wall_clock_date_never_logical_date_or_data_interval`), so the post-Task-2 total is correctly 8 (4 pre-existing + 4 new), not 7. All grep-based acceptance criteria (test names, constant names, Makefile target) passed exactly as specified; only the plan's numeric test-count narration was off by one. No functional impact — flagging for plan-authoring accuracy only.

## Issues Encountered

None — plan executed cleanly against the live repo state; the current buggy `run()` and existing test scaffolding matched the plan's `<interfaces>` block verification exactly.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Phase 10 is a single-plan phase (10-01 was the only plan). `OraclePartitionReadyTrigger.run()` now implements D-01 through D-07: bounded exponential-backoff retry on transient `OperationalError`, immediate propagation of any other `oracledb.Error`, and a guarded `connection.close()` that never masks the original exception. All 4 of ROADMAP.md Phase 10's success criteria are covered by a named, passing unit test. `make verify-phase10` exists and passes. No blockers for phase completion / milestone transition.

## Self-Check: PASSED

- FOUND: airflow/dags/_common/oracle_partition_trigger.py
- FOUND: tests/unit/test_oracle_partition_trigger.py
- FOUND: Makefile
- FOUND commit: 43fc0d7
- FOUND commit: fd3abe6

---
*Phase: 10-oraclepartitionreadytrigger-robustness-fix*
*Completed: 2026-09-02*
