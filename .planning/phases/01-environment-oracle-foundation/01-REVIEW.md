---
phase: 01-environment-oracle-foundation
reviewed: 2026-08-28T00:00:00Z
depth: standard
files_reviewed: 18
files_reviewed_list:
  - .env.example
  - .gitignore
  - .python-version
  - Makefile
  - README.md
  - airflow/dags/.gitkeep
  - docker-compose.yml
  - docker/airflow/Dockerfile
  - docker/airflow/simple_auth_manager_passwords.json.generated
  - docker/oracle/init/01_ingestion_metadata.sql
  - docker/oracle/init/02_customers.sql
  - docker/oracle/init/03_orders.sql
  - docs/environment.md
  - packages/csv-processor/pyproject.toml
  - packages/csv-processor/src/csv_processor/__init__.py
  - pyproject.toml
  - scripts/verify_environment.py
  - src/lightweight_airflow_etl/__init__.py
  - uv.lock
findings:
  critical: 0
  warning: 3
  info: 3
  total: 6
status: issues_found
---

# Phase 01: Code Review Report

**Reviewed:** 2026-08-28T00:00:00Z
**Depth:** standard
**Files Reviewed:** 18
**Status:** issues_found

## Summary

Reviewed the environment/Oracle foundation for Phase 1: docker-compose stack, Airflow/Oracle
Dockerfile, Oracle DDL init scripts, `.env.example`, `verify_environment.py`, and the two package
scaffolds. Overall the stack is well-documented and the loopback port bindings, `.gitignore`
coverage, and parameterized SQL in `verify_environment.py` are correctly implemented — no critical
security or data-loss issues were found in the *default* configuration (`admin`/`admin`
end-to-end).

The issues found are latent/consistency defects that will surface the moment someone follows the
project's own stated instructions to customize credentials, plus one genuinely dead configuration
variable tied directly to a documented workaround (the passwords-file permission gotcha). None of
these break the default `make up` happy path today, which is why none are classified Critical, but
each represents incorrect behavior a future user/phase is likely to hit.

## Warnings

### WR-01: `AIRFLOW_CONN_ORACLE_DEFAULT` hardcodes `admin:admin`, ignoring `ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD`

**File:** `docker-compose.yml:15`
**Issue:** The Oracle connection registered for Airflow is a literal string:
```yaml
AIRFLOW_CONN_ORACLE_DEFAULT: "oracle://admin:admin@oracle:1521/?service_name=FREEPDB1&encoding=UTF-8&threaded=False&events=False"
```
Meanwhile the Oracle container itself derives its application-schema credentials from
`${ORACLE_APP_USER:-admin}` / `${ORACLE_APP_USER_PASSWORD:-admin}` (lines 49-50), and
`.env.example` explicitly invites customization: *"Placeholder `admin`/`admin` values are already
correct for local dev; no editing required **unless you want different credentials**."* If a user
changes `ORACLE_APP_USER` or `ORACLE_APP_USER_PASSWORD` in `.env` as invited, the Oracle container's
`APP_USER`/`APP_USER_PASSWORD` change accordingly, but `AIRFLOW_CONN_ORACLE_DEFAULT` still points at
the literal `admin:admin` — the registered Connection now has stale/wrong credentials, and
`airflow connections test oracle_default` (the exact D-11 verification workflow documented in
`docs/environment.md`) starts failing with an opaque `ORA-01017: invalid username/password` and no
indication of why, since nothing else in the stack surfaces the mismatch.
**Fix:** Interpolate the same env vars used for the Oracle container itself:
```yaml
AIRFLOW_CONN_ORACLE_DEFAULT: "oracle://${ORACLE_APP_USER:-admin}:${ORACLE_APP_USER_PASSWORD:-admin}@oracle:1521/?service_name=FREEPDB1&encoding=UTF-8&threaded=False&events=False"
```

### WR-02: `AIRFLOW_UID` defined in `.env.example` but never referenced — dead config tied to a documented permission gotcha

**File:** `.env.example` (`AIRFLOW_UID=50000`), cross-referenced against `docker-compose.yml`
**Issue:** `.env.example` defines `AIRFLOW_UID=50000` with the comment *"Matches the base
apache/airflow image's non-root `airflow` user uid -- avoids bind-mount permission friction on
`./airflow/dags`."* However, `docker-compose.yml` never references `${AIRFLOW_UID}` anywhere — there
is no `user: "${AIRFLOW_UID}:0"` (the standard pattern from Airflow's own official
`docker-compose.yaml`, which this variable name/value is clearly borrowed from). The variable is
pure dead configuration today.

This is the direct root cause of the workaround documented immediately below it in
`docs/environment.md` ("Known First-Boot Gotcha: Permission Error on the Passwords File") — had
`AIRFLOW_UID` actually been wired to a `user:` directive on the airflow services, the container
would run as the host UID and the bind-mount permission mismatch (and the resulting need to
`chmod 666` the passwords file to a world-writable mode) likely wouldn't occur in the first place.
As written, the comment misleads a reader into believing this variable is already doing its stated
job.
**Fix:** Either wire it up —
```yaml
x-airflow-common:
  &airflow-common
  ...
  user: "${AIRFLOW_UID:-50000}:0"
```
— or, if intentionally left unused pending a later phase, update the `.env.example` comment to say
so explicitly rather than describing behavior that isn't implemented.

### WR-03: `verify_airflow_auth()` only catches `HTTPError`, not connection-level failures

**File:** `scripts/verify_environment.py:79-85`
**Issue:**
```python
try:
    with urllib.request.urlopen(request) as response:
        body = json.loads(response.read().decode("utf-8"))
except urllib.error.HTTPError as exc:
    raise AssertionError(...) from exc
```
`urllib.error.HTTPError` is only raised for a valid HTTP response with a non-2xx status. If the
`airflow-apiserver` container isn't up/reachable at all (e.g., still starting, wrong port, network
issue), `urlopen` raises `urllib.error.URLError` (`HTTPError`'s superclass) instead, which is not
caught here. That exception propagates out of `verify_airflow_auth()` and out of `main()`
uncaught — the `if __name__ == "__main__":` block only catches `AssertionError`
(`scripts/verify_environment.py:126-130`), so the script exits with a raw, unfriendly traceback
instead of the tool's own "FAILED: ..." format, defeating the stated purpose of this script as a
clean, reusable verification entrypoint for Phase 4.
**Fix:**
```python
except (urllib.error.HTTPError, urllib.error.URLError) as exc:
    detail = exc.read().decode("utf-8") if isinstance(exc, urllib.error.HTTPError) else str(exc)
    raise AssertionError(f"Airflow auth request failed: {detail}") from exc
```

## Info

### IN-01: Redundant `environment: <<: *airflow-common-env` merge on `airflow-init`

**File:** `docker-compose.yml:69-70`
**Issue:** `airflow-init` already inherits `environment` (along with `build`/`volumes`/`depends_on`)
via `<<: *airflow-common` (line 64). Re-declaring `environment: <<: *airflow-common-env` (lines
69-70) merges the exact same env map back in, producing an identical result — it's dead
duplication, not a functional override (unlike the deliberate `depends_on` override two lines
above, which does need to differ to avoid a self-referential dependency cycle).
**Fix:** Remove the redundant `environment:` block from `airflow-init` since it adds nothing beyond
what `<<: *airflow-common` already provides.

### IN-02: `simple_auth_manager_passwords.json.generated` left world-writable (0666) beyond what the documented workaround requires

**File:** `docker/airflow/simple_auth_manager_passwords.json.generated` (mode `0666`)
**Issue:** `docs/environment.md`'s documented fix for the first-boot permission gotcha is
`chmod 666`, and the file on disk matches that. `0666` grants write access to *any* local user on
the host, not just the container's mapped UID — broader than necessary even for a throwaway
local-dev file. `0664` (group-writable, or matching whatever GID the container maps to) would
satisfy the same bind-mount need without granting arbitrary-other-user write access on a
multi-user host. Low real-world impact given the single-developer WSL2 scope this project
explicitly targets, but worth tightening now that WR-02 (wiring up `AIRFLOW_UID`) would likely
remove the need for a loosened-permissions workaround altogether.
**Fix:** If WR-02 is addressed, re-verify whether the `chmod 666` workaround is still needed at all;
if it remains needed, prefer `0664` over `0666`.

### IN-03: Placeholder author email and description in `pyproject.toml`

**File:** `pyproject.toml:5-8`
**Issue:** `description = "Add your description here"` and
`authors = [{ name = "Konrad Borowiec", email = "konrad.borowiec@example.com" }]` are unedited
`uv init` scaffold defaults — the `@example.com` address is a placeholder, not a real project
contact, and the description doesn't describe the project.
**Fix:** Fill in a real one-line description and either a real contact address or omit the
`authors` field if not needed for this internal project.

---

_Reviewed: 2026-08-28T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
