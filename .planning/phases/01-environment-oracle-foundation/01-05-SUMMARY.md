---
phase: 01-environment-oracle-foundation
plan: 5
subsystem: infra
tags: [docker-compose, healthcheck, airflow, urllib, retry-backoff, unittest, gap-closure]

# Dependency graph
requires:
  - phase: 01-01
    provides: "docker-compose.yml stack, scripts/verify_environment.py's verify_airflow_auth()"
  - phase: 01-03
    provides: "custom-image-based airflow-apiserver/scheduler/dag-processor/triggerer services"
provides:
  - "Real, app-level healthchecks on all four Airflow services so docker compose up --wait genuinely reflects readiness"
  - "verify_airflow_auth() retries a bounded, evidence-backed cold-start OSError/ConnectionResetError race with backoff, never surfacing a raw traceback"
  - "tests/test_verify_environment.py — project's first test file, deterministic regression coverage for the retry/clean-failure/no-retry-on-HTTPError behavior"
affects: ["phase-2-config-contract", "phase-3-csv-engine", "future-phases-using-make-verify-or-make-smoke-test"]

# Actuals (#2632)
actuals:
  tokens: 2600
  tasks: 2
  commits: 3

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "docker-compose healthcheck: block matches Apache Airflow's own upstream docker-compose.yaml pattern (curl --fail for apiserver, airflow jobs check --job-type <X>Job --hostname \"$$HOSTNAME\" for scheduler/triggerer/dag-processor), verified via Context7 against apache/airflow's airflow-core/docs/howto/docker-compose/docker-compose.yaml rather than invented"
    - "Bounded exponential-backoff retry for a transient, evidence-backed client-side network race: module-level tuning constants (AUTH_RETRY_ATTEMPTS/AUTH_RETRY_BASE_DELAY_SECONDS/AUTH_RETRY_MAX_DELAY_SECONDS), never retry on the non-transient exception class (HTTPError), always raise the same clean AssertionError style on exhaustion"
    - "tests/ loads a non-package script module via importlib.util.spec_from_file_location + module_from_spec, not a normal import — project's first test, stdlib unittest + unittest.mock only, no framework choice preempted"

key-files:
  created:
    - tests/test_verify_environment.py
  modified:
    - docker-compose.yml
    - scripts/verify_environment.py

key-decisions:
  - "Healthcheck test commands and interval/timeout/retries/start_period values copied verbatim from Apache Airflow's own upstream docker-compose.yaml (confirmed via Context7), not invented — including DagProcessorJob as the job-type for the standalone dag-processor service (Airflow 3.x), manually confirmed valid (airflow jobs check --job-type DagProcessorJob exits non-zero for \"no such job\", not \"invalid job-type\")"
  - "Second except clause broadened from urllib.error.URLError alone to (urllib.error.URLError, OSError) rather than adding a ConnectionResetError-specific special case — OSError is the confirmed common superclass per the debug session's MRO trace, so this is the structural fix, not a narrow patch"
  - "Retry budget (6 attempts, ~23s worst-case backoff) sized at 2.3x the debug session's empirically measured 12.66s cold-start gap; HTTPError (genuine auth rejection) is explicitly excluded from retry and still fails on the first attempt with zero added delay"

patterns-established:
  - "tests/ directory + importlib.util.spec_from_file_location loading convention for testing scripts/ scripts that aren't installed packages — reusable if later phases add more standalone scripts under scripts/"

requirements-completed: [INFRA-01, INFRA-03]

coverage:
  - id: D1
    description: "docker-compose.yml defines real, app-level healthcheck: blocks for airflow-apiserver, airflow-scheduler, airflow-dag-processor, and airflow-triggerer (previously none of the four had one), matching Airflow's own upstream pattern"
    requirement: INFRA-01
    verification:
      - kind: integration
        ref: "docker compose down -v && docker compose up -d --wait (took ~40s, was <1s before; all four services report Health.Status=healthy via docker inspect, not null)"
        status: pass
    human_judgment: false
  - id: D2
    description: "verify_airflow_auth() catches OSError (confirmed common superclass of ConnectionResetError) from the response-read phase, retries with bounded exponential backoff, and only ever raises a clean AssertionError — never a raw uncaught traceback — once the retry budget is exhausted; HTTPError is never retried"
    requirement: INFRA-03
    verification:
      - kind: unit
        ref: "tests/test_verify_environment.py::VerifyAirflowAuthTests::test_retries_transient_connection_reset_then_succeeds"
        status: pass
      - kind: unit
        ref: "tests/test_verify_environment.py::VerifyAirflowAuthTests::test_retry_budget_exhausted_raises_clean_assertion_error"
        status: pass
      - kind: unit
        ref: "tests/test_verify_environment.py::VerifyAirflowAuthTests::test_http_error_is_not_retried"
        status: pass
    human_judgment: false
  - id: D3
    description: "Three consecutive full cold-start cycles (docker compose down -v && up -d --wait && verify_environment.py) all pass cleanly with no raw traceback, closing the debug session's 'may be timing-sensitive, not guaranteed every run' caveat -- UAT gap G-01-1 resolved"
    requirement: INFRA-01
    verification:
      - kind: integration
        ref: "for i in 1 2 3; do docker compose down -v; docker compose up -d --wait; uv run python scripts/verify_environment.py; done -- all 3 iterations printed the 3 OK lines with no FAILED/traceback"
        status: pass
    human_judgment: false

# Metrics
duration: 20min
completed: 2026-08-28
status: complete
---

# Phase 1 Plan 5: Gap Closure — Cold-Start Healthcheck + verify_airflow_auth() Retry Summary

**Closed UAT gap G-01-1 by adding real app-level healthchecks to all four Airflow services (matching Airflow's own upstream docker-compose.yaml pattern) and broadening `verify_airflow_auth()`'s exception handling to retry a bounded, evidence-backed `OSError`/`ConnectionResetError` cold-start race with backoff — proven by 3 consecutive clean cold starts and a new 3-test regression suite (this project's first test file).**

## Performance

- **Duration:** ~20 min (19:06 root-cause context read → 19:24 last task commit; excludes the ~3.5 min spent on the 3x cold-start verification loop)
- **Started:** 2026-08-28T19:06:00+02:00
- **Completed:** 2026-08-28T19:24:43+02:00
- **Tasks:** 2/2 completed
- **Files modified:** 3 (1 modified for Task 1, 1 created + 1 modified for Task 2)

## Accomplishments

- Added real `healthcheck:` blocks to `airflow-apiserver` (curl against `/api/v2/monitor/health`), `airflow-scheduler`/`airflow-triggerer`/`airflow-dag-processor` (`airflow jobs check --job-type <X>Job`), matching Apache Airflow's own official docker-compose.yaml pattern (verified via Context7), closing the false-Healthy race that previously let `docker compose up -d --wait` return before the apps inside were actually ready
- Manually confirmed `DagProcessorJob` is a valid `airflow jobs check` job-type before committing to it in the healthcheck (`docker compose exec airflow-dag-processor airflow jobs check --job-type DagProcessorJob --hostname test` exits non-zero for "no such job", not "invalid job-type")
- Broadened `verify_airflow_auth()`'s exception handling from `(HTTPError, URLError)` to `(HTTPError, (URLError, OSError))` with a bounded (6-attempt, ~23s worst-case) exponential-backoff retry loop for the `URLError`/`OSError` branch — the structural fix for a `ConnectionResetError` raised during urllib's response-read phase, which is never wrapped as `URLError`
- Wrote `tests/test_verify_environment.py` — the project's first test file — proving the retry-then-succeed, retry-budget-exhausted, and HTTPError-not-retried cases deterministically via mocked `urlopen`, following full TDD RED → GREEN discipline
- Ran the plan's full verification: 3 consecutive `docker compose down -v && up -d --wait && verify_environment.py` cycles, all clean, no raw traceback, no `FAILED:` output

## Task Commits

Each task was committed atomically (Task 2 followed TDD RED → GREEN):

1. **Task 1: Real healthchecks for airflow-apiserver/scheduler/dag-processor/triggerer** - `1eb841f` (feat)
2. **Task 2 (RED): Add failing regression tests for verify_airflow_auth() retry behavior** - `0845f98` (test)
3. **Task 2 (GREEN): Broaden verify_airflow_auth() to retry OSError with backoff** - `f887943` (feat)

_No REFACTOR commit was needed — the GREEN implementation required no further cleanup._

## Files Created/Modified

- `docker-compose.yml` - added `healthcheck:` blocks to `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer` (interval=30s, timeout=10s, retries=5, start_period=30s, matching upstream); no other lines touched
- `scripts/verify_environment.py` - added `import time`; added `AUTH_RETRY_ATTEMPTS`/`AUTH_RETRY_BASE_DELAY_SECONDS`/`AUTH_RETRY_MAX_DELAY_SECONDS` module constants; rewrote `verify_airflow_auth()`'s body as a bounded retry loop, broadening the second `except` clause to `(urllib.error.URLError, OSError)` while keeping the `HTTPError` branch's immediate-raise, unretried behavior unchanged
- `tests/test_verify_environment.py` - new: 3 `unittest.TestCase` methods covering retry-then-succeed, retry-exhausted, and HTTPError-not-retried, loading `verify_environment.py` via `importlib.util.spec_from_file_location`

## Decisions Made

- Healthcheck shapes and timing values (`interval`/`timeout`/`retries`/`start_period`) copied verbatim from Apache Airflow's own upstream `docker-compose.yaml` (confirmed live via Context7 against `apache/airflow`'s docs repo) rather than invented — including using `DagProcessorJob` for the standalone dag-processor service, which the upstream doc doesn't show directly (Airflow 3.x's standalone dag-processor split is newer) but which was manually confirmed to be a real, accepted job-type string before committing to it
- `OSError` chosen as the broadened exception class (not a `ConnectionResetError`-specific catch) because the debug session's MRO trace confirmed `OSError` is the correct common ancestor — this is the structural fix the debug session recommended, not a narrow special case
- Retry budget sized at 2.3x the debug session's empirically measured 12.66s gap (6 attempts, 1.0/2.0/4.0/8.0/8.0s backoff, ~23s worst case) — bounded so a genuinely down apiserver still fails within a documented window, not indefinitely (see threat register T-01-07 in 01-05-PLAN.md)

## Deviations from Plan

None - plan executed exactly as written. Both tasks' `<action>`/`<behavior>` specs were followed precisely; the manual `DagProcessorJob` verification step called for in Task 1's `<action>` was run and confirmed valid before being written into the healthcheck.

## Issues Encountered

None. The docker-compose stack was already up from prior plans; Task 1's own verification required a `docker compose down -v` + fresh `up -d --wait` cycle, which surfaced no problems. The plan-level 3x cold-start verification loop also completed with zero retries actually triggered in `verify_airflow_auth()` — the healthcheck fix alone was sufficient to close the race in this session's runs, with the retry logic serving as the evidence-backed defensive backstop the debug session called for (both fixes are independently required per the plan's prohibitions, even though only one was empirically exercised in these 3 runs).

## User Setup Required

None - no external service configuration required. Existing developers with a running stack from prior plans should run `docker compose up -d --wait` (or `make up`) to pick up the new healthchecks on next restart; a `docker compose down -v && docker compose up -d --wait` (or `make smoke-test`) cold start is not required but is exactly what this plan's own verification ran 3x cleanly.

## Known Stubs

None.

## Next Phase Readiness

UAT gap G-01-1 is fully closed: `docker compose up -d --wait` now genuinely reflects airflow-apiserver/scheduler/dag-processor/triggerer readiness (never falsely reports Healthy before the app is serving), and `scripts/verify_environment.py` never produces a raw uncaught traceback on a cold-start race — it either succeeds after transparently retrying, or fails cleanly with `FAILED: ...`. Phase 1 is now fully verified (16/16 UAT tests passing, 0 open gaps) and ready to transition to Phase 2 (Config Contract). `tests/test_verify_environment.py` establishes a reusable `importlib.util.spec_from_file_location` pattern for testing any future standalone `scripts/*.py` module without preempting Phase 3's TEST-01 formal test framework choice.

---
*Phase: 01-environment-oracle-foundation*
*Completed: 2026-08-28*

## Self-Check: PASSED

All 3 claimed files found on disk (docker-compose.yml, scripts/verify_environment.py,
tests/test_verify_environment.py). All 3 task commits found in git log (1eb841f, 0845f98, f887943).
