---
phase: 06-end-to-end-verification-benchmark-ci-docs
plan: 01
subsystem: testing
tags: [pytest, oracledb, airflow-rest-api, urllib, e2e, tdd]

# Dependency graph
requires:
  - phase: 05-airflow-dag-wiring-deferrable-file-wait
    provides: "Live-verified csv_ingest DAG (deferred wait_for_file sensor, process_csv_task) and the proven scripts/trigger_dag.sh auth/trigger flow this plan automates"
  - phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
    provides: "csv_processor.load.get_connection()/oracle_user()/oracle_password()/oracle_dsn() reused verbatim by the e2e conftest's oracle_cursor fixture"
provides:
  - "scripts/dag_polling.py: reusable Airflow REST trigger/poll helpers (get_jwt_token, trigger_dag, poll_task_instance_state, wait_for_task_state, wait_for_dag_run_result)"
  - "tests/e2e/: real, passing end-to-end proof of TEST-03/D-08 (HTTP trigger -> deferred wake -> process_csv -> real Oracle rows), asserted via oracledb SELECT, never DagRun.state alone"
  - "tests/unit/test_dag_polling.py: fast, no-live-stack unit coverage for the polling helpers"
affects: ["06-02 (benchmark)", "06-04 (evidence/executive-summary regeneration script, documented reuser of scripts/dag_polling.py)", "06-03 (CI, runs both suites)"]

# Actuals (#2632)
actuals:
  tokens: 4822
  tasks: 2
  commits: 2

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Reusable stdlib-only (urllib.request/json) Airflow REST polling module, subprocess-wrapping the already-proven scripts/trigger_dag.sh auth flow instead of re-deriving it in Python"
    - "e2e test proves the deferred-wake ordering by asserting the poll-to-deferred call returns BEFORE the fixture file is written to disk, then closes the loop with a real oracledb SELECT against customers_valid/customers_invalid (D-08) rather than trusting DagRun.state/result[\"status\"] alone"
    - "urllib.request.urlopen mocked at the module-attribute level (scripts.dag_polling.urllib.request.urlopen) for fast, no-network unit coverage of the same polling helpers the e2e test exercises live"

key-files:
  created:
    - scripts/dag_polling.py
    - tests/e2e/__init__.py
    - tests/e2e/conftest.py
    - tests/e2e/test_csv_ingest_e2e.py
    - tests/unit/test_dag_polling.py
  modified: []

key-decisions:
  - "Unit tests mock urllib.request.urlopen directly (not poll_task_instance_state), matching the plan's action text -- proves the real HTTP-shaped request/response contract, not just the polling loop's control flow in isolation"
  - "wait_for_dag_run_result's unit test reproduces docs/airflow-dag.md's literal documented ndjson response shape (heartbeat line + final results line) rather than a simplified synthetic body, so the test doubles as a regression guard against that endpoint's documented quirk"

patterns-established:
  - "Fast unit-test companion file for any live-stack-dependent integration/e2e module: mock the stdlib HTTP call at its lowest boundary (urlopen), never mock the module's own higher-level functions, to keep the test honest about the wire contract"

requirements-completed: [TEST-03]

coverage:
  - id: D1
    description: "HTTP trigger to real, unmocked Airflow REST API genuinely wakes a deferred wait_for_file sensor (observed state == \"deferred\" BEFORE the fixture file exists on disk)"
    requirement: "TEST-03"
    verification:
      - kind: e2e
        ref: "tests/e2e/test_csv_ingest_e2e.py#test_wait_for_file_defers_before_file_exists_then_lands_in_oracle"
        status: pass
    human_judgment: false
  - id: D2
    description: "Correct/incorrect CSV rows land in real Oracle customers_valid/customers_invalid tables, proven via a live oracledb SELECT, never DagRun.state/result[\"status\"] alone (D-08)"
    requirement: "TEST-03"
    verification:
      - kind: e2e
        ref: "tests/e2e/test_csv_ingest_e2e.py#test_wait_for_file_defers_before_file_exists_then_lands_in_oracle"
        status: pass
    human_judgment: false
  - id: D3
    description: "scripts/dag_polling.py's polling helpers (wait_for_task_state genuine-poll behavior, TimeoutError-with-last-observed-state, wait_for_dag_run_result's results[result_task_id] extraction) have fast, no-live-stack unit coverage"
    requirement: "TEST-03"
    verification:
      - kind: unit
        ref: "tests/unit/test_dag_polling.py#test_wait_for_task_state_polls_until_target_state_reached"
        status: pass
      - kind: unit
        ref: "tests/unit/test_dag_polling.py#test_wait_for_task_state_raises_timeout_error_with_last_observed_state"
        status: pass
      - kind: unit
        ref: "tests/unit/test_dag_polling.py#test_wait_for_dag_run_result_extracts_results_for_task_id"
        status: pass
    human_judgment: false

duration: 39min
completed: 2026-08-30
status: complete
---

# Phase 6 Plan 1: End-to-End Trigger-to-Oracle Proof Summary

**Automated pytest e2e proof that an HTTP trigger genuinely wakes a deferred `wait_for_file` sensor before the fixture file exists, then flows real rows into Oracle's `customers_valid`/`customers_invalid` tables (TEST-03/D-08), plus a stdlib-only `scripts/dag_polling.py` REST polling module and its fast unit-test companion.**

## Performance

- **Duration:** 39 min (plan creation 23:52:30 to final task commit 00:31:15)
- **Started:** 2026-08-29T23:52:30+02:00
- **Completed:** 2026-08-30T00:31:15+02:00
- **Tasks:** 2
- **Files modified:** 5 (4 created by Task 1, 1 created by Task 2)

## Accomplishments

- `scripts/dag_polling.py`: reusable, stdlib-only (`urllib.request`/`json`) Airflow REST helpers — `get_jwt_token`, `trigger_dag` (subprocess-wraps the already-proven `scripts/trigger_dag.sh`), `poll_task_instance_state`, `wait_for_task_state` (bounded poll loop, raises `TimeoutError` naming the last-observed state), `wait_for_dag_run_result` (parses the `wait` endpoint's ndjson response per `docs/airflow-dag.md`'s documented API note)
- `tests/e2e/test_csv_ingest_e2e.py`: real, passing end-to-end test — triggers `csv_ingest` for `customers` with the fixture file confirmed absent, polls to a genuine `deferred` state BEFORE writing the file (Pitfall 4 ordering, textually verified), then asserts real `customers_valid`/`customers_invalid` row counts via a live `oracledb` `SELECT`, never `DagRun.state`/`result["status"]` alone
- `tests/unit/test_dag_polling.py`: fast (0.17s), no-live-stack unit coverage for the three Behavior-required scenarios (genuine multi-call polling, bounded-timeout `TimeoutError`, `wait` endpoint result extraction), mocking `urllib.request.urlopen` at the wire boundary
- Full plan verification (`uv run pytest tests/unit/test_dag_polling.py tests/e2e/test_csv_ingest_e2e.py -x`) passes against the live `make up` stack: 4 passed in 19.32s
- Full unit suite (`uv run pytest tests/unit/ -x`) still passes: 214 passed

## Task Commits

1. **Task 1: E2E proof — HTTP trigger -> deferred wake -> process_csv -> real Oracle rows (customers)** - `f00b694` (feat) — tracer, verified live twice against the running stack, checkpoint approved by the human before Task 2 began
2. **Task 2: Fast unit coverage for the polling helpers (no live stack required)** - `225742b` (test)

**Plan metadata:** (this commit, follows)

## Files Created/Modified

- `scripts/dag_polling.py` - Reusable Airflow REST trigger/poll helpers, reused verbatim (not re-derived) by future evidence-regeneration tooling (Plan 04)
- `tests/e2e/__init__.py` - Package marker for the new e2e test package
- `tests/e2e/conftest.py` - `airflow_stack_reachable` health-poll fixture, `oracle_cursor` fixture, autouse `clean_customers_tables` fixture (mirrors `tests/integration/conftest.py`)
- `tests/e2e/test_csv_ingest_e2e.py` - The one real e2e scenario proving TEST-03/D-08
- `tests/unit/test_dag_polling.py` - Fast, no-network unit tests for `scripts/dag_polling.py`'s polling helpers

## Decisions Made

- Unit tests mock `urllib.request.urlopen` directly (matching the plan's own action text) rather than mocking `poll_task_instance_state`/`wait_for_dag_run_result` at a higher level — keeps the test honest about the real HTTP request/response wire contract those functions build on.
- `wait_for_dag_run_result`'s unit test reproduces `docs/airflow-dag.md`'s literal documented `wait` endpoint response shape (heartbeat line + final results line) instead of a simplified synthetic body, doubling as a regression guard for that endpoint's documented quirk.
- Task 1's tracer feedback gate: after committing Task 1, the e2e test was re-verified live against the running stack (confirmed a second time, 1 passed in 19.19s) before Task 2 (expansion) began, per this plan's `type="tracer"` execution contract.

## Deviations from Plan

### Auto-fixed Issues (recorded by the prior Task 1 executor, carried forward here for completeness)

**1. [Rule 2 - Missing Critical] Added `_clear_existing_customers_fixtures()` to `tests/e2e/conftest.py`**
- **Found during:** Task 1
- **Issue:** A leftover `customers_*.csv*` fixture file from a prior run would make the file-glob match immediately on trigger, never deferring — breaking the exact "deferred BEFORE file exists" proof this plan requires.
- **Fix:** Added a fixture that clears stale `customers_*.csv*` files from the bind-mounted `./data/customers/` directory before triggering.
- **Files modified:** `tests/e2e/conftest.py`
- **Verification:** Confirmed by two independent live runs against the running stack with no collision, plus this plan's own re-run during Task 2's verification pass.
- **Committed in:** `f00b694` (Task 1 commit)

No deviations occurred during Task 2 — the module executed exactly as the plan's Behavior/action text specified.

---

**Total deviations:** 1 auto-fixed (1 missing critical, Task 1)
**Impact on plan:** Necessary for correctness of the deferred-wake proof; no scope creep.

## Issues Encountered

None. `ruff`/`mypy` are not yet installed in this environment (their setup is D-14's scope, a later plan in this phase) — lint/type-check was not run against the new files, consistent with the plan's own scope boundary.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- `scripts/dag_polling.py` is ready for direct reuse by Plan 04's evidence-regeneration script (already flagged as a planned reuser in this module's own docstring and this plan's `key_links`).
- The e2e suite is a real, permanent TEST-03 regression test — Plan 03 (CI) can wire `uv run pytest tests/e2e/ -x` into the CI Oracle+e2e job (D-06/D-07) directly.
- TEST-04's benchmark (Plan 02) remains unresolved by this plan and must never substitute this plan's small e2e fixture for the ~100K-row benchmark dataset (D-02) — flagged in the plan's own `prohibitions` for cross-plan visibility.
- No blockers.

---
*Phase: 06-end-to-end-verification-benchmark-ci-docs*
*Completed: 2026-08-30*

## Self-Check: PASSED

All created files confirmed present on disk; both task commits (`f00b694`, `225742b`) confirmed in git history.
