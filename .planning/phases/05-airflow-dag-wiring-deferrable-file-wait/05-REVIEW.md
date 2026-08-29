---
phase: 05-airflow-dag-wiring-deferrable-file-wait
reviewed: 2026-08-29T20:25:13Z
depth: standard
files_reviewed: 15
files_reviewed_list:
  - Makefile
  - airflow/dags/_common/__init__.py
  - airflow/dags/_common/paths.py
  - airflow/dags/_common/reporting.py
  - airflow/dags/csv_ingest.py
  - docker-compose.yml
  - docker/airflow/Dockerfile
  - docs/airflow-dag.md
  - packages/csv-processor/src/csv_processor/source.py
  - scripts/trigger_dag.sh
  - tests/unit/dags/__init__.py
  - tests/unit/dags/conftest.py
  - tests/unit/dags/test_dag_helpers.py
  - tests/unit/dags/test_load_config_helpers.py
  - tests/unit/dags/test_report_result_format.py
  - tests/unit/test_source_undetermined_encoding.py
findings:
  critical: 0
  warning: 4
  info: 3
  total: 7
status: issues_found
---

# Phase 05: Code Review Report

**Reviewed:** 2026-08-29T20:25:13Z
**Depth:** standard
**Files Reviewed:** 15
**Status:** issues_found

## Summary

Reviewed the `csv_ingest` DAG wiring (`airflow/dags/csv_ingest.py`), its `_common` helper modules
(`paths.py`, `reporting.py`), the phase's regression fix to `source.py` (undetermined-encoding
`LookupError` guard), the docker-compose/Dockerfile infra changes, `scripts/trigger_dag.sh`, and
the new unit tests.

The `source.py` encoding fix is correct and narrowly scoped (only computes `detected_name` when
`enc_detection.source == "detected"`, matching the regression test
`test_prepare_source_defers_to_config_when_encoding_undetermined`). The path-traversal defenses in
`_common/paths.py` (`resolve_safe_config_path`'s absolute-path-first rejection, then
join-then-resolve-then-`is_relative_to` check) are sound and match the documented T-05-01 threat
model, with tests covering the traversal and absolute-path bypass cases.

I traced the DAG's branch/skip mechanics carefully (Airflow's `skip_all_except` skips a
branch-operator's direct children only when they are **not** also reachable downstream of the
selected branch — `report_result_task` is reachable via `wait_for_file → process_csv_task →
load_results_task`, so the `route >> report` direct edge does not cause it to be incorrectly
skipped on the success path). No blocker-level defect was found in that graph. However, I did find
one real gap in `report_result_task`'s coverage (WR-01) and an exception-handling gap in
`load_config_task` that compounds it (WR-02), plus a JSON-injection robustness gap in
`scripts/trigger_dag.sh` (WR-03) and a missing dataset/config cross-check (WR-04). No critical
(security/data-loss/crash-guaranteed) issues were found.

## Warnings

### WR-01: `report_result_task`'s trigger rule silently drops the "real failure" case

**File:** `airflow/dags/csv_ingest.py:128-137`
**Issue:** `report_result_task` runs on `trigger_rule="none_failed_min_one_success"`, and its two
direct upstreams are `route` (the branch task) and `final_result_dict` (downstream of
`load_results_task`). The docstring/docs (`docs/airflow-dag.md:33-34`) state this trigger rule
makes it "fire on both the success path and the config-error early-exit path" — but a third,
entirely realistic terminal state is never covered: if `wait_for_file` genuinely times out
(`timeout=3600`) or fails for any other reason, that FAILED state propagates as `upstream_failed`
through `process_csv_task` → `load_results_task`. Since one of `report_result_task`'s direct
upstreams (`load_results_task`) is then `upstream_failed`, `none_failed_min_one_success`'s
"none_failed" condition is violated, so `report_result_task` itself becomes `upstream_failed` and
never runs. The one task in this DAG whose entire purpose is "always log a final summary line"
(DAG-04) silently produces **no log line at all** for the most common real-world failure mode
(file never arrives / processing genuinely blows up).
**Fix:** Give `report_result_task` a trigger rule that also fires on upstream failure (e.g.
`trigger_rule="all_done"`), and branch internally on whichever XCom is actually available (or add
a `None`-safe default) so it can log a `FAILED`/`unknown`-shaped line instead of being skipped
entirely:
```python
@task(trigger_rule="all_done")
def report_result_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    outcome = (
        ti.xcom_pull(task_ids="load_results_task")
        or ti.xcom_pull(task_ids="load_config_task")
        or {"status": "TASK_FAILURE", "dataset": ctx["params"]["dataset"], "file_name": "",
            "total_rows": 0, "valid_rows": 0, "invalid_rows": 0, "duration_seconds": 0.0}
    )
    logging.getLogger("airflow.task").info(reporting.format_summary_log(outcome))
```

### WR-02: `load_config_task` only catches `(ValueError, ConfigurationError)` — other failures crash ungracefully and compound WR-01

**File:** `airflow/dags/csv_ingest.py:55-69`
**Issue:** D-08's stated contract is "domain failures don't fail the task", implemented by
catching exactly `(ValueError, ConfigurationError)`. But `resolve_safe_config_path` itself has a
gap that lets a non-error `config_path` slip past validation: passing `config_path="configs/datasets"`
(the bare directory, no filename) passes both the absolute-path check and the
`is_relative_to(allowed_root)` check (a path is trivially relative to itself), and is then handed
to `load_config()`, which will raise `IsADirectoryError`/`OSError` — a type not in the caught
tuple. That exception (and any other `load_config()` failure mode outside `ConfigurationError`,
e.g. a permissions error or a JSON decode error not wrapped by the loader) propagates uncaught,
failing `load_config_task` itself with a raw traceback instead of the intended
`CONFIGURATION_ERROR`-shaped dict — and per WR-01, this also means `report_result_task` never runs
to log anything either.
**Fix:** Either broaden `resolve_safe_config_path` to also reject a resolved path that is not a
regular file (`resolved.is_file()`), or widen `load_config_task`'s except clause to a documented,
broader exception set (e.g. `except (ValueError, ConfigurationError, OSError):`) so every
config-loading failure mode degrades to the same `CONFIGURATION_ERROR` dict.

### WR-03: `scripts/trigger_dag.sh` builds JSON via unescaped shell interpolation

**File:** `scripts/trigger_dag.sh:52`
**Issue:**
```bash
-d "{\"conf\": {\"dataset\": \"${DATASET}\", \"config_path\": \"${CONFIG_PATH}\"}, \"logical_date\": null}"
```
`$DATASET`/`$CONFIG_PATH` are interpolated directly into a hand-built JSON string with no
escaping. A value containing a double quote or backslash (e.g. `config_path='configs/datasets/x".json'`)
produces malformed or semantically-altered JSON sent to the trigger endpoint. Low real-world
severity (this is a local, trusted-operator dev script), but it is the standard JSON-injection
anti-pattern and the fix is essentially free since `jq` is already a hard dependency of this
script.
**Fix:**
```bash
BODY=$(jq -n --arg dataset "$DATASET" --arg config_path "$CONFIG_PATH" \
  '{conf: {dataset: $dataset, config_path: $config_path}, logical_date: null}')
DAG_RUN_ID=$(curl -s -X POST "${AIRFLOW_TRIGGER_URL}" \
  -H "Authorization: Bearer ${JWT_TOKEN}" -H "Content-Type: application/json" \
  -d "$BODY" | jq -r '.dag_run_id')
```

### WR-04: no cross-check between the `dataset` param and the config identity resolved from `config_path`

**File:** `airflow/dags/csv_ingest.py:35-38, 107-116`
**Issue:** `dataset` (restricted by `Param(..., enum=["customers", "orders"])`) and `config_path`
(an arbitrary allowlisted-directory string) are two independent runtime-conf fields with no
consistency check between them. A caller can trigger with `dataset="orders"` and
`config_path="configs/datasets/customers.json"`; `process_csv_task` then globs
`config.file_pattern` (customers' pattern, e.g. `customers_*.csv*`) inside
`/opt/airflow/data/orders/` — silently finding nothing (routine "no match" placeholder path) or,
if a dataset ever shared a file-naming convention, silently processing the wrong file against the
wrong schema/validation rules. This is a config-authoring foot-gun with only a low-severity
practical blast radius (the trigger endpoint already requires authenticated admin access), but it
is unvalidated input that the DAG happily accepts and processes without complaint.
**Fix:** In `load_config_task`, after loading `config`, assert the loaded config's own declared
dataset name (if `DatasetConfig` carries one) matches the `dataset` param, and return
`CONFIGURATION_ERROR` on mismatch rather than silently proceeding.

## Info

### IN-01: redundant `environment:` re-declaration in `docker-compose.yml`'s `airflow-init` service

**File:** `docker-compose.yml:104-111`
**Issue:** `airflow-init` merges `<<: *airflow-common` (which already carries
`environment: *airflow-common-env`), then separately re-declares `environment: {<<: *airflow-common-env}`.
This second declaration is a no-op copy of what `<<: *airflow-common` already brought in — dead,
copy-pasted duplication.
**Fix:** Drop the redundant `environment:` block from `airflow-init`; the merged-in one from
`<<: *airflow-common` already applies.

### IN-02: `sys.path.insert` boilerplate duplicated across three test files instead of centralized in `conftest.py`

**File:** `tests/unit/dags/test_dag_helpers.py:15-16`, `tests/unit/dags/test_load_config_helpers.py:17-18`, `tests/unit/dags/test_report_result_format.py:11-12`
**Issue:** Each of the three test modules repeats the identical
`sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags"))` boilerplate before importing from
`_common`. `tests/unit/dags/conftest.py` already exists for this directory and would be the
natural place to do this once (e.g. in a session-scoped autouse fixture, or simply at conftest
module scope, which pytest guarantees runs before test collection imports in the same directory).
**Fix:** Move the `sys.path.insert` call into `tests/unit/dags/conftest.py` once; drop it from the
three individual test files.

### IN-03: hardcoded local-dev credentials/secrets in `docker-compose.yml`

**File:** `docker-compose.yml:7-8, 30, 75-76, 89-91`
**Issue:** `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "admin:admin"`,
`AIRFLOW__API_AUTH__JWT_SECRET: "csv-ingest-local-dev-shared-jwt-secret-not-for-prod"`,
`POSTGRES_PASSWORD: airflow`, and `ORACLE_PASSWORD`/`ORACLE_APP_USER_PASSWORD` default to
`admin`/`admin`. These match the project's own documented convention (single admin/admin
credential, 127.0.0.1-bound stack, explicitly local-dev-only per inline comments), so this is not
flagged as a real vulnerability — recorded here only because it matches the standard
hardcoded-secret pattern scan and to make explicit that it is a deliberate, scoped exception rather
than an oversight.
**Fix:** None required given the documented scope; if this compose file is ever adapted for a
non-127.0.0.1-bound or shared environment, these values must move to `.env`/secrets management.

---

_Reviewed: 2026-08-29T20:25:13Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
