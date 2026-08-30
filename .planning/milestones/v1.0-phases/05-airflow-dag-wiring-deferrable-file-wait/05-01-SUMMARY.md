---
phase: 05-airflow-dag-wiring-deferrable-file-wait
plan: 01
subsystem: infra
tags: [airflow, taskflow, filesensor, deferrable-operator, rest-api, docker-compose, oracle]

# Dependency graph
requires:
  - phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
    provides: csv_processor.engine.process(file_path, config) -> ProcessingResult, the single
      atomic detect->parse->validate->normalize->chunk->load(Oracle) entrypoint this plan's
      process_csv_task wraps
provides:
  - "airflow/dags/csv_ingest.py -- the one, config-driven csv_ingest DAG (D-01), proven end-to-end
    against the real running stack for a live triggered customers run"
  - "airflow/dags/_common/{paths,reporting}.py -- pure-Python, unit-tested helpers (dataset/
    config-path validation, glob re-resolution, log-line formatting)"
  - "docker-compose.yml fixes: configs/ mount, ORACLE_DSN/ORACLE_APP_USER/
    ORACLE_APP_USER_PASSWORD, AIRFLOW_CONN_FS_DEFAULT, AIRFLOW__CORE__EXECUTION_API_SERVER_URL,
    AIRFLOW__API_AUTH__JWT_SECRET -- five gaps that blocked any Airflow task from ever running in
    this multi-container stack, all newly discovered by this plan's own live-trigger verification"
affects: [06-e2e-benchmark-ci-docs]

actuals:
  tokens: 6178
  tasks: 2
  commits: 2

tech-stack:
  added: []
  patterns:
    - "airflow.sdk (@dag/@task/Param/get_current_context), not airflow.models/airflow.decorators
      -- Airflow 3.3.1's stable Task SDK import surface"
    - "FileSensor(deferrable=True) as the one class-based task in an otherwise all-@task DAG;
      process_csv_task independently re-globs for the matched file rather than reading
      wait_for_file's XCom (which is a bare bool)"
    - "DAG-level input validation (dataset enum + config_path allowlist) happens inside the
      load_config_task body, one layer above csv_processor's own load_config(), never inside
      csv_processor itself"

key-files:
  created:
    - airflow/dags/csv_ingest.py
    - airflow/dags/_common/__init__.py
    - airflow/dags/_common/paths.py
    - airflow/dags/_common/reporting.py
    - tests/unit/dags/__init__.py
    - tests/unit/dags/conftest.py
    - tests/unit/dags/test_dag_helpers.py
    - tests/unit/dags/test_load_config_helpers.py
    - tests/unit/dags/test_report_result_format.py
  modified:
    - docker-compose.yml
    - docker/airflow/Dockerfile

key-decisions:
  - "Verification script uses airflow.dag_processing.dagbag.BundleDagBag(bundle_path=...), not
    the plan's literal airflow.models.DagBag(dag_folder=...) -- the plain DagBag never adds the
    dags folder to sys.path, so the DAG's own `from _common import paths, reporting` (Airflow-3-
    correct, per 05-RESEARCH.md's own guidance) fails to import under it. Airflow's real
    dag-processor uses the bundle-aware path internally (confirmed via its logs successfully
    importing csv_ingest.py with zero errors) -- BundleDagBag mirrors that, not a workaround."
  - "docker/airflow/Dockerfile's single pip install split into two calls: Airflow-constrained
    packages (oracledb/pydantic/providers) under --constraint, and csv_processor's own
    clevercsv/charset-normalizer/chardet pins installed separately, unconstrained. Airflow's
    constraints-3.3.1 branch is a moving target (re-pinned periodically upstream, not an
    immutable tag) and had drifted to require charset-normalizer==3.4.9, conflicting with this
    project's already-approved, uv.lock-pinned 3.5.1 -- a spurious ResolutionImpossible on
    rebuild, unrelated to package legitimacy."
  - "Three additional docker-compose.yml env vars beyond the plan's own documented ORACLE_DSN/
    configs-mount gaps, all found only by actually live-triggering a DAG run (no prior phase in
    this project ever executed a real Airflow task): AIRFLOW_CONN_FS_DEFAULT (fs_default was
    never registered), AIRFLOW__CORE__EXECUTION_API_SERVER_URL (localhost default doesn't route
    from airflow-scheduler to the airflow-apiserver container), and
    AIRFLOW__API_AUTH__JWT_SECRET (each container was auto-generating its own random jwt_secret,
    so every scheduler-signed Task Execution API token failed apiserver's signature check)."

requirements-completed: [DAG-01, DAG-02, DAG-03, DAG-04, DAG-05]

coverage:
  - id: D1
    description: "csv_ingest DAG runs load_config_task -> route_after_config -> wait_for_file ->
      process_csv_task -> load_results_task -> report_result_task in order for a live triggered
      customers run, calling only csv_processor.config.loader.load_config() and
      csv_processor.engine.process() -- no CSV/Oracle logic reimplemented in DAG code"
    requirement: DAG-01
    verification:
      - kind: e2e
        ref: "live POST /api/v2/dags/csv_ingest/dagRuns trigger -> dagRuns/{id} state == success,
          taskInstances all state == success (verified this session)"
        status: pass
    human_judgment: false
  - id: D2
    description: "A dataset value outside {customers, orders}, or a config_path resolving outside
      configs/datasets/ (including an absolute path), results in a CONFIGURATION_ERROR-shaped
      early exit reaching report_result_task's log -- never an unhandled exception or a stuck/
      failed Airflow task"
    requirement: DAG-02
    verification:
      - kind: e2e
        ref: "live trigger with dataset=malicious rejected at trigger time by Param enum (HTTP
          422); live trigger with config_path=/etc/passwd reaches DagRun state == success with
          report_result_task logging status=CONFIGURATION_ERROR (verified this session)"
        status: pass
    human_judgment: false
  - id: D3
    description: "wait_for_file is declared deferrable=True and, when the target file is absent
      at trigger time, its task instance reports Airflow state deferred via the REST API
      taskInstances endpoint"
    requirement: DAG-03
    verification:
      - kind: e2e
        ref: "live trigger with the customers CSV fixture temporarily moved away ->
          taskInstances/wait_for_file state == deferred, confirmed via REST API polling
          (verified this session)"
        status: pass
    human_judgment: false
  - id: D4
    description: "report_result_task's log line contains dataset=, file=, status=, total=,
      valid=, invalid=, and duration=, proven by format_summary_log()'s unit test and by the live
      triggered run's load_results_task XCom content"
    requirement: DAG-04
    verification:
      - kind: unit
        ref: "tests/unit/dags/test_report_result_format.py#test_format_summary_log_contains_all_required_fields"
        status: pass
      - kind: e2e
        ref: "docker compose logs airflow-scheduler -- dataset=customers file=customers_20260829.csv
          status=SUCCESS_WITH_INVALID_ROWS total=100 valid=90 invalid=10 duration=0.14s
          (verified this session)"
        status: pass
    human_judgment: false
  - id: D5
    description: "csv_ingest.py contains no dataset-specific branch -- route_after_config's only
      conditional keys off config validity, never dataset identity, so the same file structurally
      supports both datasets by construction"
    requirement: DAG-05
    verification:
      - kind: unit
        ref: "tests/unit/dags/test_dag_helpers.py#test_resolve_matched_file_works_for_both_dataset_patterns"
        status: pass
    human_judgment: false

duration: 25min
completed: 2026-08-29
status: complete
---

# Phase 5 Plan 1: Airflow DAG Wiring & Deferrable File-Wait Summary

**One config-driven `csv_ingest` Airflow TaskFlow DAG with a deferrable `FileSensor`, proven end-to-end via three live triggered runs (success, CONFIGURATION_ERROR early-exit, and deferred-file-wait) against the real docker-compose stack.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-08-29T19:31:19Z
- **Completed:** 2026-08-29T19:47:50Z
- **Tasks:** 2
- **Files modified:** 11

## Accomplishments

- Built the single, config-driven `csv_ingest` DAG (`load_config_task -> route_after_config ->
  wait_for_file -> process_csv_task -> load_results_task -> report_result_task`) that delegates
  the entire CSV/Oracle sequence to `csv_processor.engine.process()`, with zero dataset-specific
  branching (D-01/D-05).
- Wired the deferrable `FileSensor` (D-04) and confirmed via a live trigger with the fixture file
  temporarily removed that `wait_for_file` genuinely reports Airflow state `deferred` via the
  REST API — not just a class attribute.
- Proved the `CONFIGURATION_ERROR` early-exit path end-to-end: an absolute `config_path` (a
  path-traversal-adjacent attack) is rejected by `resolve_safe_config_path` (T-05-01), and the
  DagRun still reaches `state == success` with `report_result_task` logging the error shape —
  never an unhandled exception or a stuck task.
- Found and fixed five real infrastructure gaps in `docker-compose.yml` — two the plan's own
  research had already flagged (`ORACLE_DSN`/credentials, `configs/` mount), and three genuinely
  new ones only surfaced by actually triggering a live DAG run for the first time in this
  project's history: `fs_default` never registered, the Task Execution API server URL defaulting
  to `localhost` (unreachable from a sibling container), and each container minting its own
  random JWT signing secret (so no scheduler-signed task token could ever validate against
  apiserver).
- Unit-tested every pure-Python `_common/` helper, including the security-relevant negative cases
  (absolute-path bypass, path traversal, unknown dataset) — 11/11 new tests pass, full 210-test
  unit suite stays green.

## Task Commits

1. **Task 1: csv_ingest DAG wired end-to-end, one live triggered run (customers)** - `89fc786` (feat)
2. **Task 2: Unit tests for the pure-Python _common helpers (DAG-02/DAG-04/DAG-05)** - `4599d7c` (test)

**Plan metadata:** (this commit)

## Files Created/Modified

- `airflow/dags/csv_ingest.py` - The one `@dag`-decorated `csv_ingest` DAG (D-01/DAG-01..05)
- `airflow/dags/_common/paths.py` - `resolve_matched_file`, `validate_dataset`,
  `resolve_safe_config_path` — pure, zero-Airflow-import helpers
- `airflow/dags/_common/reporting.py` - `format_summary_log` (DAG-04)
- `airflow/dags/_common/__init__.py` - Package init (empty)
- `docker-compose.yml` - `configs/` mount, `ORACLE_DSN`/`ORACLE_APP_USER`/
  `ORACLE_APP_USER_PASSWORD`, `AIRFLOW_CONN_FS_DEFAULT`,
  `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, `AIRFLOW__API_AUTH__JWT_SECRET`
- `docker/airflow/Dockerfile` - Split pip install to isolate `csv_processor`'s own
  clevercsv/charset-normalizer/chardet pins from Airflow's drifting constraints file
- `tests/unit/dags/{__init__,conftest}.py` - Test package init + shared `dataset_configs` fixture
- `tests/unit/dags/test_dag_helpers.py` - `resolve_matched_file` tests (DAG-05)
- `tests/unit/dags/test_load_config_helpers.py` - `validate_dataset`/`resolve_safe_config_path`
  tests (DAG-02, T-05-01/T-05-02)
- `tests/unit/dags/test_report_result_format.py` - `format_summary_log` tests (DAG-04)

## Decisions Made

- Verification of the DAG structure used `airflow.dag_processing.dagbag.BundleDagBag` (which
  adds `bundle_path` to `sys.path`, matching what Airflow's real dag-processor does internally)
  instead of the plan's literal `airflow.models.DagBag(dag_folder=...)`, which never adds the DAG
  folder to `sys.path` and so cannot resolve `csv_ingest.py`'s `from _common import paths,
  reporting` relative import. Confirmed via the real `airflow-dag-processor` container's own logs
  that it imports `csv_ingest.py` with zero errors — `BundleDagBag` mirrors production behavior,
  it's not a workaround.
- `docker/airflow/Dockerfile`'s single `pip install` was split into two calls so
  `csv_processor`'s own `clevercsv`/`charset-normalizer`/`chardet` pins install unconstrained by
  Airflow's `constraints-3.3.1` branch, which had drifted (it's a periodically-updated branch,
  not an immutable tag) to require `charset-normalizer==3.4.9` — conflicting with this project's
  already-approved, `uv.lock`-pinned `3.5.1` and producing a spurious `ResolutionImpossible` on
  image rebuild.
- Three docker-compose env vars beyond the plan's own two documented gaps were added after
  actually live-triggering a DAG run exposed them (`AIRFLOW_CONN_FS_DEFAULT`,
  `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, `AIRFLOW__API_AUTH__JWT_SECRET`) — see Deviations
  below for full detail.
- `AIRFLOW__API_AUTH__JWT_SECRET` uses a fixed local-dev literal (not a random/generated value)
  so every container shares the same signing key deterministically across rebuilds — acceptable
  per this project's explicit local-dev-only, 127.0.0.1-bound scope (INFRA-03).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `data/` directory owned by root, unwritable by the executing user**
- **Found during:** Task 1, before the live-trigger verification (`uv run python
  generator/generate_csv.py --dataset customers` needed to write a fixture file)
- **Issue:** `./data/` was owned `root:root` from a prior root-run container operation; the
  non-root host user running this session's commands got `Permission denied` on write.
- **Fix:** `docker run --rm -v .../data:/data alpine:3.20 chown -R 1000:1000 /data`
- **Files modified:** none (filesystem ownership only, not repo content)
- **Committed in:** n/a (not a repo change)

**2. [Rule 3 - Blocking] `docker/airflow/Dockerfile`'s pip install hit `ResolutionImpossible`**
- **Found during:** Task 1, rebuilding the Airflow image to pick up the current
  `csv_processor` package (the running containers were built from a stale pre-Phase-3 copy
  containing only `__init__.py`)
- **Issue:** Airflow's `constraints-3.3.1` branch had drifted to pin `charset-normalizer==3.4.9`,
  conflicting with the Dockerfile's own already-approved `charset-normalizer==3.5.1` pin.
- **Fix:** Split the single `pip install` into two calls — Airflow-constrained packages under
  `--constraint`, `csv_processor`'s own detection-library pins (`clevercsv`/
  `charset-normalizer`/`chardet`) installed separately, unconstrained.
- **Files modified:** `docker/airflow/Dockerfile`
- **Verification:** `docker compose build` succeeds; `csv_processor` package fully present in
  the container afterward (`config/`, `engine.py`, `load.py`, etc., not just `__init__.py`).
- **Committed in:** `89fc786` (Task 1 commit)

**3. [Rule 3 - Blocking] `_common` package not importable via the plan's literal `DagBag` check**
- **Found during:** Task 1, running the plan's own automated `DagBag` structure-verify snippet
- **Issue:** `airflow.models.DagBag(dag_folder=...)` never adds the dag folder to `sys.path`
  (confirmed by reading `airflow.dag_processing.dagbag`'s source) — `from _common import paths,
  reporting` failed with `ModuleNotFoundError` even though the DAG code itself is correct and the
  real dag-processor container imports it fine.
- **Fix:** Used `airflow.dag_processing.dagbag.BundleDagBag(bundle_path=Path("/opt/airflow/dags"),
  dag_folder="/opt/airflow/dags")` for the structure-verification step instead — this is the
  class Airflow's own bundle-aware loading path uses, confirmed by the real
  `airflow-dag-processor` container's logs showing a clean import of `csv_ingest.py`.
- **Files modified:** none (verification methodology only, DAG code unchanged)
- **Committed in:** n/a (not a repo change)

**4. [Rule 2/3 - Missing critical / Blocking] Three docker-compose env vars needed for ANY task
to execute in this multi-container stack**
- **Found during:** Task 1's live-trigger verification — the very first triggered run's
  `load_config_task` was `SIGKILL`'d with `httpx.ConnectError: [Errno 111] Connection refused`,
  then (after the first fix) `airflow.sdk.api.client.ServerResponseError: Invalid auth token`
- **Issue:** (a) `fs_default` was never registered as an Airflow Connection — `airflow-init` only
  runs `db migrate`, which Airflow 3.x no longer auto-creates default connections during; (b)
  `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` was unset, defaulting to `http://localhost:8080/
  execution/`, which does not route from the `airflow-scheduler` container to the
  `airflow-apiserver` container; (c) `AIRFLOW__API_AUTH__JWT_SECRET` was unset, so each container
  auto-generated its own random signing key, meaning a task JWT minted by `airflow-scheduler`
  could never pass `airflow-apiserver`'s signature verification.
- **Fix:** Added `AIRFLOW_CONN_FS_DEFAULT: "fs://"`, `AIRFLOW__CORE__EXECUTION_API_SERVER_URL:
  "http://airflow-apiserver:8080/execution/"`, and `AIRFLOW__API_AUTH__JWT_SECRET: "<fixed
  local-dev literal>"` to `airflow-common-env`.
- **Files modified:** `docker-compose.yml`
- **Verification:** Live triggered run reaches `state == success` with all six task instances
  `success`; `wait_for_file` independently confirmed reporting `deferred` when the target file is
  absent.
- **Committed in:** `89fc786` (Task 1 commit)

---

**Total deviations:** 4 auto-fixed (3 Rule 3 - blocking, 1 combined Rule 2/3 - missing critical
infrastructure)
**Impact on plan:** All four were necessary preconditions for the plan's own live-trigger
verification requirement to be met at all — none represent scope creep into Phase 6 territory
(HTTP-trigger E2E test, benchmark, CI, docs remain untouched and out of scope here).

## Issues Encountered

None beyond what's captured in Deviations above — every blocker found during this session was a
genuine infrastructure gap (never previously exercised, since no prior phase in this project ever
triggered a real Airflow DAG run) rather than a defect in the DAG code itself, which parsed and
ran correctly on the first live trigger once the infrastructure gaps were closed.

## User Setup Required

None - no external service configuration required. All fixes are within this repo's own
`docker-compose.yml`/`Dockerfile` and apply automatically to any fresh `docker compose up -d
--wait`.

## Next Phase Readiness

- `csv_ingest` DAG is live, unpaused, and proven for both the success and config-error paths for
  `customers`; `orders` uses the identical DAG by construction (DAG-05, unit-tested via
  `resolve_matched_file` parametrized over both datasets) but has not itself been live-triggered
  in this plan — Phase 6's own E2E test is the natural place to add that second live proof.
- Phase 6 (HTTP-trigger E2E test, benchmark, CI, docs) can build directly on this phase's proven
  REST API trigger flow (`/auth/token` -> `Bearer` -> `POST .../dagRuns`) and the now-fixed
  docker-compose stack — no known blockers.
- The DAG was left unpaused (`is_paused: false`) on the live stack as a side effect of this
  session's verification triggers; Phase 6 or a fresh `docker compose down -v && up` will reset
  this to Airflow's default-paused state if that matters for a clean CI run.

## Known Stubs

None.

## Threat Flags

None — every new surface introduced by this plan (`dataset`/`config_path` runtime `conf`,
`fs_default`'s file-wait path) was already anticipated and mitigated per the plan's own
`<threat_model>` (T-05-01 through T-05-04), all verified working via the live CONFIGURATION_ERROR
and deferred-file tests above.

## Self-Check: PASSED

All 9 created files confirmed present on disk; both task commit hashes (`89fc786`, `4599d7c`)
confirmed present in `git log`.

---
*Phase: 05-airflow-dag-wiring-deferrable-file-wait*
*Completed: 2026-08-29*
