---
phase: 09-hourly-orchestrator-dag-csv-generate-schedule
plan: 01
subsystem: infra
tags: [airflow, taskflow, python, testing, pytest]

# Dependency graph
requires:
  - phase: 05-airflow-taskflow-dag
    provides: "_common/paths.py and _common/reporting.py's zero-Airflow-import module
      convention and sys.path-bootstrap test pattern, mirrored exactly here"
provides:
  - "derive_seed(logical_date) -> int -- per-hour, retry-reproducible seed derivation (D-04)"
  - "format_cascade_summary(dataset_results) -> str -- cascade summary log line with
    report_ready=OK heartbeat token (SCHED-07, D-12/D-13/D-14)"
  - "retention_sweep(base_dir, dataset, cutoff) -> (deleted, skipped) -- never-raising
    30-day CSV/CSV.GZ retention logic (SCHED-10, D-16/D-18)"
affects: [09-02-csv-generate-schedule-dag]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Pure, zero-Airflow-import helper module under airflow/dags/_common/, unit-tested via
      the tests/unit/dags/ sys.path.insert(str(_REPO_ROOT / 'airflow' / 'dags')) bootstrap"
    - "Never-raising filesystem helper: every per-entry operation wrapped in its own narrow
      try/except (ValueError for date parsing, OSError for unlink), never a bare except"

key-files:
  created:
    - airflow/dags/_common/generate_schedule_helpers.py
    - tests/unit/dags/test_generate_schedule_helpers.py
  modified: []

key-decisions:
  - "derive_seed/format_cascade_summary/retention_sweep implemented as three independent
    functions in one module (not three files), mirroring the plan's interface-first design
    for Plan 09-02's DAG to import directly"

patterns-established:
  - "Pattern: retention-style filesystem sweeps wrap each per-file operation (date parsing,
    unlink) in its own try/except and return (succeeded, skipped-with-reason) tuples instead
    of raising, so one bad entry never aborts the whole sweep"

requirements-completed: [SCHED-02, SCHED-07, SCHED-10]

# Metrics
duration: 12min
completed: 2026-09-01
---

# Phase 9 Plan 1: Generate-Schedule Pure Helpers Summary

**Three zero-Airflow-import helper functions (`derive_seed`, `format_cascade_summary`,
`retention_sweep`) backing Plan 09-02's `csv_generate_schedule` DAG, built TDD (RED/GREEN
per task) with full unit coverage.**

## Performance

- **Duration:** 12 min
- **Started:** 2026-09-01T19:37:02Z
- **Completed:** 2026-09-01T19:49:00Z
- **Tasks:** 2 completed
- **Files modified:** 2

## Accomplishments
- `derive_seed(logical_date)` derives a retry-reproducible `YYYYMMDDHH` int seed from a
  DagRun's `logical_date` (D-04)
- `format_cascade_summary(dataset_results)` formats both datasets' total/valid/invalid row
  counts plus a fixed `report_ready=OK` heartbeat token in `format_summary_log()`'s established
  single-f-string-return shape (SCHED-07, D-12/D-13/D-14), handling a `None` dataset result as
  `NO_DATA` instead of raising
- `retention_sweep(base_dir, dataset, cutoff)` deletes only CSV/CSV.GZ files whose embedded
  date is older than `cutoff`, and never raises for unparseable filenames or unlink failures
  (SCHED-10, D-16/D-18) — proven live for both an `IsADirectoryError` on unlink and a
  malformed date token

## Task Commits

Each task was committed atomically (TDD RED -> GREEN per task):

1. **Task 1: derive_seed() and format_cascade_summary()**
   - `a1ba3df` test(09-01): add failing test for derive_seed and format_cascade_summary
   - `5feb5fe` feat(09-01): implement derive_seed and format_cascade_summary
2. **Task 2: retention_sweep()**
   - `6e0e031` test(09-01): add failing test for retention_sweep
   - `b965756` feat(09-01): implement retention_sweep

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified
- `airflow/dags/_common/generate_schedule_helpers.py` - `derive_seed()`,
  `format_cascade_summary()`, `retention_sweep()`; zero Airflow imports
- `tests/unit/dags/test_generate_schedule_helpers.py` - 7 tests covering all three helpers,
  including the exact test names 09-VALIDATION.md's Per-Task Verification Map requires
  (`test_seed_varies_by_hour`, `test_summary_format`, `test_retention_deletes_old_files`,
  `test_retention_never_raises`)

## Decisions Made
- All three helpers live in one module (`generate_schedule_helpers.py`), not split across
  files, since the plan's own interface spec names a single module and Plan 09-02 imports all
  three from it together — no separate rationale needed beyond following the plan as written.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed own test fixture date direction in `test_retention_deletes_old_files`**
- **Found during:** Task 2 (retention_sweep implementation, first GREEN run)
- **Issue:** The "recent" fixture file's date was computed as `cutoff - timedelta(days=5)`,
  which is actually *older* than `cutoff` (further in the past), not newer — the test asserted
  it should survive the sweep but the fixture data made it eligible for deletion, causing the
  correct implementation to fail the test.
- **Fix:** Changed to `cutoff + timedelta(days=5)` so the "recent" fixture date sits after the
  retention cutoff, correctly simulating a file within the retention window.
- **Files modified:** tests/unit/dags/test_generate_schedule_helpers.py
- **Verification:** `uv run pytest tests/unit/dags/test_generate_schedule_helpers.py -x` — all
  7 tests pass after the fix.
- **Committed in:** b965756 (Task 2 GREEN commit)

---

**Total deviations:** 1 auto-fixed (1 bug, in the executor's own test authoring)
**Impact on plan:** No scope creep — a test-fixture-only fix caught immediately during the
RED->GREEN cycle, before the implementation commit landed.

## Issues Encountered
None beyond the deviation above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Plan 09-02 can `from _common.generate_schedule_helpers import derive_seed,
  format_cascade_summary, retention_sweep` with no further changes needed to this module.
- Full `tests/unit/dags/` suite (18 tests across 4 files) passes with zero regressions.
- `ruff check` and `mypy` both pass clean on the new module.

---
*Phase: 09-hourly-orchestrator-dag-csv-generate-schedule*
*Completed: 2026-09-01*

## Self-Check: PASSED

- FOUND: airflow/dags/_common/generate_schedule_helpers.py
- FOUND: tests/unit/dags/test_generate_schedule_helpers.py
- FOUND: a1ba3df (test: derive_seed/format_cascade_summary RED)
- FOUND: 5feb5fe (feat: derive_seed/format_cascade_summary GREEN)
- FOUND: 6e0e031 (test: retention_sweep RED)
- FOUND: b965756 (feat: retention_sweep GREEN)
