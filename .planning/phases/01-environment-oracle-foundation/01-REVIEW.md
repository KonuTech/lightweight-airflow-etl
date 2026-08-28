---
phase: 01-environment-oracle-foundation
reviewed: 2026-08-28T17:34:22Z
depth: standard
files_reviewed: 3
files_reviewed_list:
  - docker-compose.yml
  - scripts/verify_environment.py
  - tests/test_verify_environment.py
findings:
  critical: 0
  warning: 3
  info: 1
  total: 4
status: issues_found
---

# Phase 01: Code Review Report (Gap Closure — G-01-1)

**Reviewed:** 2026-08-28T17:34:22Z
**Depth:** standard
**Files Reviewed:** 3
**Status:** issues_found

## Summary

This is a narrow gap-closure review of Plan 01-05, which added `healthcheck:` blocks to the
four Airflow services (`airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`,
`airflow-triggerer`) and broadened `verify_airflow_auth()`'s retry logic to cover `OSError`
(not just `urllib.error.URLError`) with bounded exponential backoff, to fix a cold-start race
(G-01-1) where `docker compose up --wait` reported these services healthy before the apiserver
was actually able to serve a response.

The core fix is sound: the healthcheck commands and timing (verified against Airflow's own
upstream `docker-compose.yaml` per the commit message) correctly gate `--wait`, the retry loop
is correctly bounded (exactly `AUTH_RETRY_ATTEMPTS` attempts, confirmed by running the test
suite via `uv run python -m unittest tests.test_verify_environment -v`, which passes 3/3 and
shows the exact 1s/2s/4s/8s/8s backoff sequence the commit message claims), and `HTTPError` is
correctly excluded from the retry (it's checked in an earlier, more specific `except` clause
before the broadened `(URLError, OSError)` clause, so a genuine 401 still fails fast).

However, the exception-handling broadening is incomplete relative to its own stated goal
("never a raw uncaught traceback"): it only covers errors during `urlopen()`'s connect/send
phase and OSError-derived read-phase errors, but not the other read-phase exception type
Python's own `http.client` can raise when a connection is torn down mid-body
(`http.client.IncompleteRead`, which is not an `OSError` subclass) or malformed content
(`json.JSONDecodeError`). There's also a missing socket-level timeout on the retried
`urlopen()` call itself, and a test-reliability gap in how the new test file loads the module
under test. None of these are proven to reproduce in practice (the debug session only measured
`ConnectionResetError`), so none rise to Critical, but they're real gaps in the fix's coverage
worth closing.

## Warnings

### WR-01: Read-phase errors not covered by `OSError` broadening can still crash uncaught

**File:** `scripts/verify_environment.py:98-124`
**Issue:** The retry loop's second `except` clause was broadened from `URLError` to
`(urllib.error.URLError, OSError)` specifically because the comment/commit message note that
errors raised during the response-read phase (`response.read()`) are never wrapped by urllib as
`URLError`. That's correct for `ConnectionResetError` (an `OSError` subclass, confirmed via
`ConnectionResetError.__mro__`), but the same read phase can also raise
`http.client.IncompleteRead` when the connection is closed cleanly after only part of a
`Content-Length`-declared body has been received — a very plausible variant of the exact
cold-start race this fix targets. `IncompleteRead`'s MRO is
`(IncompleteRead, HTTPException, Exception, BaseException, object)` — it is **not** an
`OSError`, so it is not caught here. Likewise, `json.loads(response.read().decode(...))` can
raise `json.JSONDecodeError`/`UnicodeDecodeError` on a truncated-but-non-empty body, which is
also uncaught. Either case would propagate as a raw, uncaught exception out of
`verify_airflow_auth()`, past `main()` (which only catches `AssertionError`), producing exactly
the kind of ugly uncaught traceback this gap-closure fix was written to eliminate — just from a
sibling exception type instead of `ConnectionResetError`.
**Fix:**
```python
except (
    urllib.error.URLError,
    OSError,
    http.client.IncompleteRead,
    json.JSONDecodeError,
    UnicodeDecodeError,
) as exc:
    ...
```
(with `import http.client` added). Alternatively, wrap `response.read()` and `json.loads(...)`
in a narrower inner `try` so read/parse failures are explicitly routed through the same
retry-or-raise path as connection failures, rather than relying on the exception type union to
happen to line up.

### WR-02: No timeout on the retried `urlopen()` call — a hang defeats the bounded-retry design

**File:** `scripts/verify_environment.py:100`
**Issue:** `urllib.request.urlopen(request)` is called with no `timeout=` argument, so it uses
`socket.getdefaulttimeout()`, which is `None` (block forever) unless something else in the
process has changed the global default. The whole point of `AUTH_RETRY_ATTEMPTS` /
`AUTH_RETRY_BASE_DELAY_SECONDS` / `AUTH_RETRY_MAX_DELAY_SECONDS` is to bound the total wall-clock
time this function can spend (the commit message calculates a ~23s worst case). But that budget
only bounds the *sleeps between* attempts — if any single `urlopen()` call hangs (e.g., TCP
handshake completes but the ASGI app never responds, rather than resetting the connection), the
function can block indefinitely on that one attempt, silently blowing the entire time budget and
hanging `make up`/CI. This is an easy gap to close and is directly relevant to the "bounded,
evidence-backed" design goal stated in the same function's docstring.
**Fix:**
```python
with urllib.request.urlopen(request, timeout=10) as response:
```

### WR-03: Test module unnecessarily coupled to `oracledb` via whole-module `exec_module`

**File:** `tests/test_verify_environment.py:31-35`
**Issue:** The test file loads `scripts/verify_environment.py` via
`importlib.util.spec_from_file_location` + `exec_module`, which executes the *entire* module
top-to-bottom, including its unconditional `import oracledb` (verify_environment.py:24) — even
though all 3 tests in this file exercise only `verify_airflow_auth()`, which has no dependency on
Oracle at all. Confirmed reproducible: running `python3 -m unittest tests.test_verify_environment`
in a plain interpreter without the project's `oracledb` dependency installed fails at collection
time with `ModuleNotFoundError: No module named 'oracledb'`, not because of anything wrong with
the retry logic under test, but purely from this incidental coupling. (It does pass cleanly under
`uv run python -m unittest tests.test_verify_environment -v` where `oracledb` is present per
`pyproject.toml`.) If this test ever runs in a lighter CI stage that doesn't install the full
Oracle driver stack (a plausible future split given the project has no Oracle in these 3 tests),
the failure will look like a broken environment rather than a signal about the code under test.
**Fix:** Either (a) make the `oracledb` import in `verify_environment.py` lazy — move
`import oracledb` inside `main()` (the only place it's used) so the module can be loaded for
testing `verify_airflow_auth()` without the Oracle driver installed — or (b) if the module-level
import is intentional/required elsewhere, document the `oracledb`-install dependency directly in
this test file's docstring so a future collection failure here is immediately diagnosable instead
of surprising.

## Info

### IN-01: Repeated healthcheck timing block across 4 services (docker-compose.yml)

**File:** `docker-compose.yml:78-113`
**Issue:** `interval: 30s` / `timeout: 10s` / `retries: 5` / `start_period: 30s` is duplicated
verbatim across `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, and
`airflow-triggerer` — only the `test:` command differs between them. The file already
establishes a YAML-anchor convention (`x-airflow-common`) for exactly this kind of shared
configuration, so this is a missed opportunity for consistency rather than a new pattern.
**Fix:**
```yaml
x-airflow-job-healthcheck: &airflow-job-healthcheck
  interval: 30s
  timeout: 10s
  retries: 5
  start_period: 30s

services:
  airflow-scheduler:
    healthcheck:
      <<: *airflow-job-healthcheck
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
```

---

_Reviewed: 2026-08-28T17:34:22Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
