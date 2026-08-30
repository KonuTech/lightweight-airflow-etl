---
status: complete
phase: 01-environment-oracle-foundation
source: [01-01-SUMMARY.md, 01-02-SUMMARY.md, 01-03-SUMMARY.md, 01-04-SUMMARY.md, 01-05-SUMMARY.md]
started: 2026-08-28T16:46:44Z
updated: 2026-08-28T17:20:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Cold Start Smoke Test
expected: |
  Kill any running server/service. Clear ephemeral state (temp DBs, caches, lock files).
  Start the application from scratch. Server boots without errors, any seed/migration
  completes, and a primary query (health check, homepage load, or basic API call) returns
  live data.
result: issue
reported: "Ran `make smoke-test` (reset && up && verify) myself, on the user's request. `docker compose up -d --wait` reported `airflow-apiserver` Healthy, but the immediately-following `make verify` (scripts/verify_environment.py) hit `ConnectionResetError: [Errno 104] Connection reset by peer` on the /auth/token request, with a raw uncaught traceback rather than a clean failure message. A bare retry of `make verify` (no restart) passed cleanly with no changes — confirming this is a transient cold-start race: the apiserver's Docker healthcheck reports Healthy slightly before it's actually ready to serve all HTTP endpoints. Separately, this also reveals a residual gap in the just-shipped WR-03 fix (01-REVIEW-FIX.md): verify_airflow_auth() now catches HTTPError and URLError, but ConnectionResetError propagates from deep inside http.client's getresponse() and is not wrapped as URLError by urllib in this failure path, so it still produces a raw traceback instead of a clean 'FAILED: ...' message."
severity: major

### 2. Package legitimacy review (oracledb, pydantic, apache-airflow-providers-standard)
expected: Reviewed and approved before install (D-04, Plan 01-01)
result: pass
source: confirmed-live
note: "User typed 'approved' at this checkpoint during execution after reviewing RESEARCH.md's audit table and pypi.org project pages — not re-asked here."

### 3. Package legitimacy review (apache-airflow-providers-oracle)
expected: Reviewed and approved before install (Plan 01-03, added mid-execution)
result: pass
source: confirmed-live
note: "User typed 'approved' at this checkpoint during execution after reviewing PyPI/GitHub evidence — not re-asked here."

### 4. [01-01/D1] Full docker-compose stack boots healthy
expected: postgres, oracle, airflow-apiserver/scheduler/dag-processor/triggerer all healthy/running, no redis/airflow-worker/flower present
result: pass
source: automated
coverage_id: D1

### 5. [01-01/D2] INGESTION_METADATA table exists
expected: Confirmed via USER_TABLES (not DDL exit status)
result: pass
source: automated
coverage_id: D2

### 6. [01-01/D3] admin/admin dual authentication
expected: Authenticates against both Oracle (python-oracledb) and Airflow's REST API
result: pass
source: automated
coverage_id: D3

### 7. [01-02/D1] Full 5-table Oracle schema with INTERVAL partitioning
expected: CUSTOMERS_VALID/INVALID, ORDERS_VALID/INVALID exist in ADMIN schema with daily INTERVAL partitioning on INGESTED_AT
result: pass
source: automated
coverage_id: D1

### 8. [01-02/D2] verify_environment.py confirms full schema
expected: All 5 tables + expected columns verified via USER_TABLES/ALL_TAB_COLUMNS
result: pass
source: automated
coverage_id: D2

### 9. [01-03/D1] Custom Airflow image builds with all dependencies
expected: Dockerfile builds FROM apache/airflow:3.3.1-python3.12 with oracledb, pydantic, apache-airflow-providers-standard, apache-airflow-providers-oracle, and csv_processor scaffold
result: pass
source: automated
coverage_id: D1

### 10. [01-03/D2] docker-compose builds from custom Dockerfile
expected: Airflow runtime services build from the custom image and stack boots healthy
result: pass
source: automated
coverage_id: D2

### 11. [01-03/D3] oracle_default Connection registered and testable
expected: Visible/testable in Airflow, independent of csv_processor
result: pass
source: automated
coverage_id: D3

### 12. [01-03/D4] No regression after custom-image swap
expected: verify_environment.py still passes after the image swap
result: pass
source: automated
coverage_id: D4

### 13. [01-04/D1] Makefile targets
expected: up/down/reset/logs work; make down preserves volumes, make reset removes them
result: pass
source: automated
coverage_id: D1

### 14. [01-04/D2] Empirically observed resource requirements documented
expected: docs/environment.md documents real docker stats/system df figures, distinct from vendor-floor numbers
result: pass
source: automated
coverage_id: D2

### 15. [01-04/D3] README getting-started section
expected: Links to docs/environment.md, preserves pre-existing Notes & Q&A content
result: pass
source: automated
coverage_id: D3

### 16. [01-04/D4] Full fresh-clone phase-gate verification
expected: Clean docker compose down --volumes && make up brings all services healthy; all 5 tables + dual-auth confirmed; ports bound to 127.0.0.1 only
result: pass
source: automated
coverage_id: D4

## Summary

total: 16
passed: 15
issues: 1
pending: 1
skipped: 0
blocked: 0

## Gaps

- gap_id: G-01-1
  truth: "Server boots without errors, any seed/migration completes, and a primary query (health check, homepage load, or basic API call) returns live data."
  status: resolved
  resolved_by: "01-05-PLAN.md"
  resolved_at: "2026-08-28"
  reason: "User reported: airflow-apiserver reports Docker-healthy before it can actually serve /auth/token — first request after a fresh `make up` hits ConnectionResetError with a raw traceback (retry with no changes passes cleanly). Also exposes that verify_airflow_auth()'s just-shipped URLError catch (01-REVIEW-FIX.md WR-03) doesn't cover ConnectionResetError, which isn't wrapped as URLError in this failure path."
  severity: major
  test: 1
  root_cause: "Two independent causes, both required to reproduce this exact symptom. (1) airflow-apiserver (and scheduler/dag-processor/triggerer) have NO healthcheck: block in docker-compose.yml, so `docker compose up --wait` treats the container as Healthy the instant the process starts (<1s), not once uvicorn is actually serving (~12.66s later in this session, confirmed via docker inspect + log-timestamp correlation). (2) On Docker Desktop/WSL2 specifically, the port-forwarding proxy accepts the TCP connection during that gap and resets it rather than refusing it outright (604/604 polling attempts across the full gap raised ConnectionResetError, never ConnectionRefusedError) — confirmed via `docker info` (Operating System: Docker Desktop). Separately, scripts/verify_environment.py::verify_airflow_auth()'s URLError catch (WR-03, commit d7d0882) cannot structurally catch this: CPython's urllib.request.AbstractHTTPHandler.do_open() only wraps OSError as URLError around the h.request() (connect+send) phase — h.getresponse() (where this ConnectionResetError is actually raised, during response-read) is outside that wrap and re-raises unchanged. ConnectionResetError's MRO (ConnectionResetError -> ConnectionError -> OSError -> Exception) never includes URLError."
  artifacts:
    - path: "docker-compose.yml"
      issue: "airflow-apiserver (and scheduler/dag-processor/triggerer) services have no healthcheck: block, so --wait's readiness signal is false (reports Healthy at container start, not app readiness)"
    - path: "scripts/verify_environment.py"
      issue: "verify_airflow_auth()'s except clause (HTTPError, URLError) cannot catch a getresponse()-phase ConnectionResetError — structurally out of scope for that wrap, not a matter of adding one more exception type without also reconsidering the retry strategy"
  missing:
    - "Add a real healthcheck: to airflow-apiserver (e.g. curl against an actual health endpoint) with a start_period long enough to cover Airflow's bootstrap, so --wait reflects true readiness"
    - "Broaden or retry verify_airflow_auth()'s exception handling to also handle ConnectionResetError/OSError from the response-read phase, so a cold-start race produces a clean FAILED message instead of a raw traceback"
  debug_session: ".planning/debug/apiserver-auth-connreset.md"
