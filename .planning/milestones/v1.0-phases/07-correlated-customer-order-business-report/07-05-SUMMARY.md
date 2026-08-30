---
phase: 07-correlated-customer-order-business-report
plan: 05
subsystem: data-generation
tags: [python, airflow, oracle, python-oracledb, staging-rename, e2e, pytest, partitioning]

# Dependency graph
requires:
  - phase: 07-02
    provides: "write_staged()/staging_path() -- the one staging+atomic-rename write path this plan adopts into regenerate_readme_summary.py and the new live e2e test"
  - phase: 07-04
    provides: "PK/index/BEFORE INSERT trigger DDL on customers_valid/orders_valid -- this plan's live e2e test proves the trigger tolerates legitimate correlated backdated inserts, and its ordering requirement (customer_id must already be committed) drove this plan's own Rule 1 fix"
provides:
  - "scripts/regenerate_readme_summary.py: main() calls generate_correlated_datasets() exactly once, before the per-dataset trigger/wait loop; _run_ingestion() accepts a pre-generated GeneratedCsv/DatasetConfig and writes via write_staged()"
  - "tests/e2e/test_correlated_report_e2e.py: second test proving D-24/D-25 (live staging+rename against the real csv_ingest DAG) and D-12 (multi-day backdated-partition report aggregation)"
affects: [07-06-plan, future regenerate_readme_summary.py callers, any future live e2e test that triggers customers and orders together]

# Actuals (#2632)
actuals:
  tokens: 3469
  tasks: 2
  commits: 2

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Full-completion sequencing between two DB-trigger-dependent DAG runs (wait_for_dag_run_result for the upstream dataset before triggering the downstream one), layered on top of the existing deferred-wake-before-write ordering -- necessary once a DB-level FK-existence trigger (Plan 07-04) makes commit order matter, not just file-write order"

key-files:
  created: []
  modified:
    - scripts/regenerate_readme_summary.py
    - tests/e2e/test_correlated_report_e2e.py

key-decisions:
  - "_run_ingestion()'s signature changed to accept a pre-generated GeneratedCsv + DatasetConfig instead of generating rows internally -- generation is coupled (one generate_correlated_datasets() call in main()), triggering/waiting stays decoupled and per-dataset (D-23)"
  - "The new live e2e test waits for the customers DAG run to fully COMPLETE before triggering orders, deviating from the plan's literal trigger-both-then-wait-both ordering -- Plan 07-04's trg_orders_valid_customer_exists trigger requires customer_id already committed in customers_valid, and the literal ordering raced and failed empirically with DATABASE_ERROR before this fix. Both datasets' wait_for_file-deferred-before-write ordering (D-24/D-25's actual subject) is unaffected."

patterns-established: []

requirements-completed: [TEST-06, INFRA-04]

coverage:
  - id: D1
    description: "regenerate_readme_summary.py generates both correlated CSVs via the one shared generate_correlated_datasets() function, called exactly once per main() invocation, and writes each via the shared write_staged() staging+rename helper"
    requirement: "INFRA-04"
    verification:
      - kind: other
        ref: "uv run python -c \"import ast; ast.parse(open('scripts/regenerate_readme_summary.py').read())\" && grep -c generate_correlated_datasets scripts/regenerate_readme_summary.py (returns 2: the call site + a docstring/comment mention)"
        status: pass
      - kind: other
        ref: "grep -n generate_rows|write_csv|write_staged scripts/regenerate_readme_summary.py -- only write_staged appears, no generate_rows/write_csv call remains"
        status: pass
    human_judgment: false
  - id: D2
    description: "A second live e2e test proves the staging+atomic-rename mechanism against the real, already-proven csv_ingest DAG for both customers and orders (D-24/D-25), never mocked"
    requirement: "TEST-06"
    verification:
      - kind: e2e
        ref: "tests/e2e/test_correlated_report_e2e.py#test_correlated_ingestion_via_live_dag_trigger_reports_across_backdated_partitions"
        status: pass
    human_judgment: false
  - id: D3
    description: "The same live e2e test backdates 2 orders_valid rows across multiple partition days (via direct load.insert_rows() with an extended ingested_at column) and confirms the business report aggregates correctly across the partition boundary"
    requirement: "TEST-06"
    verification:
      - kind: e2e
        ref: "tests/e2e/test_correlated_report_e2e.py#test_correlated_ingestion_via_live_dag_trigger_reports_across_backdated_partitions (asserts len(distinct_months) > 1 over _BUSINESS_REPORT_SQL's results)"
        status: pass
    human_judgment: false

duration: ~25min
completed: 2026-08-30
status: complete
---

# Phase 7 Plan 5: Live Staging+Rename + Multi-Day Backdated-Partition Report Proof Summary

**`scripts/regenerate_readme_summary.py` adopts the one shared `generate_correlated_datasets()` + `write_staged()` write path, and a new live e2e test proves that mechanism against the real `csv_ingest` DAG plus D-12's multi-day backdated-partition report aggregation -- with a Rule 1 fix sequencing customers' full DAG completion before orders to respect Plan 07-04's FK-existence trigger.**

## Performance

- **Duration:** ~25 min
- **Tasks:** 2
- **Files modified:** 2 (0 created, 2 modified)

## Accomplishments
- `scripts/regenerate_readme_summary.py`'s `main()` now calls `generate_correlated_datasets()` exactly once, before the per-dataset trigger/wait loop, sharing one run-unique seed across both datasets
- `_run_ingestion()` no longer generates rows internally -- it accepts a pre-generated `GeneratedCsv`/`DatasetConfig` and writes via `write_staged()` (staging+atomic-rename) instead of the bare `write_csv()`
- New `tests/e2e/test_correlated_report_e2e.py` test proves, against the real running Oracle+Airflow stack: (1) `wait_for_file` reaches `"deferred"` for both `customers` and `orders` before either staged file is renamed into its watched directory, (2) both DAG runs succeed, (3) a direct `load.insert_rows()` call backdating 2 `orders_valid` rows across multiple partition days succeeds against a real, pool-matched `customer_id` (proving Plan 07-04's trigger tolerates legitimate correlated backdated inserts), and (4) the business report query returns rows spanning more than one distinct `order_month` bucket
- Discovered and fixed (Rule 1) a real race condition: triggering orders before customers' DAG run fully completes fails with `DATABASE_ERROR`, because Plan 07-04's `trg_orders_valid_customer_exists` BEFORE INSERT trigger requires `customer_id` rows already committed in `customers_valid`

## Task Commits

Each task was committed atomically:

1. **Task 1: Adopt generate_correlated_datasets() + staging/rename in regenerate_readme_summary.py** - `fd7300f` (feat)
2. **Task 2: Live proof — staging+rename against the real Airflow stack, backdated multi-day partitions** - `cc1d5df` (feat)

## Files Created/Modified
- `scripts/regenerate_readme_summary.py` - `main()` builds both dataset configs and calls `generate_correlated_datasets()` once with a shared run-unique seed; `_run_ingestion(dataset, generated, config)` signature changed to consume pre-generated data and write via `write_staged()`
- `tests/e2e/test_correlated_report_e2e.py` - New `dag_polling` module load, `_clear_dataset_fixtures()` helper (extends `test_csv_ingest_e2e.py`'s pattern to `.staging/` subdirs), and the new `test_correlated_ingestion_via_live_dag_trigger_reports_across_backdated_partitions` test

## Decisions Made
- `_run_ingestion()`'s parameters are `(dataset, generated, config)` rather than deriving `config` from `dataset` internally a second time -- `main()` already loaded both configs for the shared `generate_correlated_datasets()` call, so passing them through avoids a redundant `load_config()` call per dataset
- The new e2e test sequences customers' DAG run to full completion (`wait_for_dag_run_result`) before triggering orders, rather than triggering both and waiting for both at the end as the plan's action text literally described -- confirmed empirically that the literal ordering races Plan 07-04's FK-existence trigger and fails; the deferred-wake-before-write ordering that D-24/D-25 actually test is preserved for both datasets, only the DAG-completion boundary moved

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed a race condition between the customers and orders DAG runs**
- **Found during:** Task 2, first `pytest` run of the new test
- **Issue:** The plan's literal action text describes: trigger customers -> wait deferred -> write; trigger orders -> wait deferred -> write; THEN wait for both DAG runs to complete. Following this literally, orders' `process_csv_task` ran and attempted its first `insert_rows()` into `orders_valid` before customers' `process_csv_task` had committed its rows into `customers_valid` -- Plan 07-04's `trg_orders_valid_customer_exists` BEFORE INSERT trigger then rejected every orders row (no matching `customer_id` existed yet), and `process()`'s generic `except oracledb.Error:` branch swallowed the underlying `ORA-20001` and returned `DATABASE_ERROR` with `total_rows=0`. Confirmed via `docker compose exec oracle sqlplus`, querying `ingestion_metadata`: customers' `SUCCESS_WITH_INVALID_ROWS` row was recorded at `10:18:12`, four seconds AFTER orders' `process_csv_task` had already failed at `10:18:08` (from `airflow-scheduler` logs).
- **Fix:** Moved `dag_polling.wait_for_dag_run_result(...)` for customers to run immediately after writing the customers CSV, BEFORE triggering orders at all. Orders' trigger/wait-deferred/write/wait-complete sequence then runs entirely after customers' rows are committed. Both datasets' own deferred-wake-before-write assertion (`wait_for_file` reaches `"deferred"` before the staged rename) is unaffected -- only the point at which the two datasets' DAG-completion waits are sequenced relative to each other changed.
- **Files modified:** `tests/e2e/test_correlated_report_e2e.py`
- **Verification:** Re-ran `uv run pytest tests/e2e/test_correlated_report_e2e.py -x -v` -- both tests pass (`2 passed in 23.83s`). Also ran the full `tests/unit` (224 passed) and `tests/e2e tests/integration` (21 passed) suites to confirm no regression.
- **Committed in:** `cc1d5df` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** The fix is required for the live proof to be reliable at all -- without it the test is flaky-to-always-failing depending on Oracle/Airflow scheduling timing. No scope creep; the fix only reorders two existing `wait_for_dag_run_result` calls already present in the plan's own design.

## Issues Encountered
None beyond the deviation above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `scripts/regenerate_readme_summary.py` and the live e2e suite now share the exact same correlated-generation + staging+rename production path (D-21/D-23/D-24/D-25) -- no second independent implementation exists anywhere in this project
- D-12's multi-day backdated-partition report aggregation is proven live, against the real Airflow+Oracle stack
- No blockers for 07-06

---
*Phase: 07-correlated-customer-order-business-report*
*Completed: 2026-08-30*

## Self-Check: PASSED

- FOUND: scripts/regenerate_readme_summary.py
- FOUND: tests/e2e/test_correlated_report_e2e.py
- FOUND commit: fd7300f
- FOUND commit: cc1d5df
