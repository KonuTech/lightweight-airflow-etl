---
phase: 09-hourly-orchestrator-dag-csv-generate-schedule
reviewed: 2026-09-01T23:35:56Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - Makefile
  - airflow/dags/_common/generate_schedule_helpers.py
  - airflow/dags/csv_generate_schedule.py
  - docs/airflow-dag.md
  - tests/unit/dags/test_generate_schedule_helpers.py
findings:
  critical: 1
  warning: 3
  info: 3
  total: 7
status: issues_found
---

# Phase 9: Code Review Report

**Reviewed:** 2026-09-01T23:35:56Z
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found

## Summary

`generate_schedule_helpers.py`'s three pure functions (`derive_seed`, `format_cascade_summary`,
`retention_sweep`) are well-tested and correctly isolated from Airflow — no defects found there
beyond one glob-permissiveness edge case. `csv_generate_schedule.py`'s task graph, bind-parameterized
SQL, and `retention_task`'s best-effort contract are sound. The one finding that matters most came
from tracing `TriggerDagRunOperator`'s actual `skip_when_already_exists`/`reset_dag_run` semantics
(installed `apache-airflow-providers-standard` source, not just the docstring) against how an
operator would realistically recover from a failed chain-trigger task: the combination used here
can silently convert a genuinely failed cascade into a reported "success" on retry, which undermines
the FK-ordering guarantee (SCHED-03) this phase is built around and defeats `summary_task`'s own
observability purpose. The remaining findings are error-handling/diagnosability gaps in
`generate_task`'s subprocess invocation and a data-loss-adjacent edge case in the retention glob.

## Critical Issues

### CR-01: `skip_when_already_exists=True` + default `reset_dag_run=False` silently masks a genuinely failed chain-trigger as cascading "success" on retry

**File:** `airflow/dags/csv_generate_schedule.py:102-138`
**Issue:**

All three `TriggerDagRunOperator` instances (`trigger_customers`, `trigger_orders`,
`trigger_report_ready`) set `skip_when_already_exists=True`, `retries=0`, and a deterministic
`trigger_run_id` (`{{ dag_run.run_id }}__customers` etc.), but never set `reset_dag_run=True`
(it defaults to `False`).

Traced against the installed `apache-airflow-providers-standard==1.17.0` source
(`trigger_dagrun.py`): when `execute()` finds a DagRun already exists for the deterministic
`trigger_run_id`, the operator's own contract is state-blind — it does not check whether that
prior DagRun *succeeded* or *failed*, only that it *exists* (`DagRunAlreadyExists`). With
`reset_dag_run=False`, the `af_2` code path is exactly:

```python
except DagRunAlreadyExists as e:
    if self.reset_dag_run:
        ...  # clear + rerun
    else:
        if self.skip_when_already_exists:
            raise AirflowSkipException(...)
        raise e
```

and the `af_3` path (the one actually exercised on this project's pinned Airflow 3.3.1) forwards
the identical `reset_dag_run`/`skip_when_already_exists` values unchanged via
`DagRunTriggerException` to the same execution-side handler — same documented contract, same
state-blind skip decision.

Concretely: `trigger_customers` runs, creates the child `csv_ingest` DagRun, and that child run
ends in `FAILED` (e.g. a transient Oracle outage). Because `failed_states` defaults to `[FAILED]`
and `wait_for_completion=True`, `trigger_customers` itself is marked `failed`. `retries=0` means
Airflow will never auto-retry it — the standard operator recovery action here is a manual "Clear"
in the UI. On that clear/retry, `execute()` runs again, tries to (re)create a DagRun with the
*same* deterministic `run_id`, hits `DagRunAlreadyExists` (the failed child run still exists), and
— since `reset_dag_run` is `False` — takes the `skip_when_already_exists` branch and raises
`AirflowSkipException`. `trigger_customers` becomes `skipped`, not re-triggered and not failed.

Downstream, `trigger_orders`/`trigger_report_ready` use the operator default trigger rule
(`all_success`), so a `skipped` immediate upstream cascades them to `skipped` too. `summary_task`
and `retention_task` use `trigger_rule="none_failed_min_one_success"`, which requires *at least
one* upstream success; with the entire chain skipped, they also resolve to `skipped`. A DagRun
whose leaf tasks are all `skipped`/`success` (never `failed`) is reported as an overall `success`
by Airflow — so the exact recovery action an operator would take after a real ingestion failure
(clear the failed task and retry) silently converts that hour's genuine data-pipeline failure into
a reported cascade **success**, with no customers/orders data actually re-ingested and no summary
line ever logged. This directly defeats SCHED-03's FK-ordering guarantee (an operator could believe
`customers` committed when it never did) and `summary_task`'s whole observability purpose.

The DAG's own module docstring states the `skip_when_already_exists=True` intent as "a retry never
double-triggers the same cascade run" (D-07) — that reasoning only holds for retrying an
already-*succeeded* trigger (e.g. a downstream XCom hiccup after the child DAG completed); it does
not account for retrying a trigger whose child DagRun *failed*, which is the far more common reason
an operator would retry in the first place.

**Fix:** Do not rely on `skip_when_already_exists` alone to decide retry behavior. Either:

```python
# Option A: let a manual retry actually re-attempt a failed child run.
trigger_customers = TriggerDagRunOperator(
    task_id="trigger_customers",
    trigger_dag_id="csv_ingest",
    conf={"dataset": "customers", "config_path": "configs/datasets/customers.json"},
    trigger_run_id="{{ dag_run.run_id }}__customers",
    skip_when_already_exists=True,
    reset_dag_run=True,      # clear + rerun the existing child DagRun instead of blind-skipping it
    wait_for_completion=True,
    deferrable=True,
    fail_when_dag_is_paused=True,
    poke_interval=10,
    retries=0,
)
```

or, if `reset_dag_run=True` is undesirable (e.g. it also clears/reruns an already-*successful* run,
which may not be wanted), replace the built-in skip with an explicit pre-check task that queries the
child DAG's own DagRun state for `trigger_run_id` and only proceeds/skips when it was previously
`success`, failing loudly (not skipping) when it was previously `failed`. At minimum, document this
gap in `docs/airflow-dag.md`'s SCHED-03/D-07 sections so operators know a plain UI "Clear" on a
failed `trigger_*` task does not actually retry anything.

## Warnings

### WR-01: `generate_task`'s subprocess failure diagnostics are captured then discarded

**File:** `airflow/dags/csv_generate_schedule.py:84-100`
**Issue:** `subprocess.run([...], check=True, capture_output=True, text=True)` redirects
`generate_csv.py`'s stdout/stderr into the returned `CompletedProcess` (so they never reach the
Airflow task log directly), and `check=True` raises `subprocess.CalledProcessError` on any non-zero
exit — but that exception is never caught. `CalledProcessError.__str__` only prints the command and
return code (`"Command '[...]' returned non-zero exit status 1."`); it does **not** include
`exc.stdout`/`exc.stderr`. When `generate_csv.py` fails for any reason (bad config, disk full, a
real bug), the task log shows only the exit code with zero information about *why* — actively worse
for debugging than not capturing output at all, since the child's own error message is discarded.
**Fix:**
```python
try:
    subprocess.run([...], check=True, capture_output=True, text=True)
except subprocess.CalledProcessError as exc:
    logging.getLogger("airflow.task").error(
        "generate_csv.py failed (exit %s):\nstdout:\n%s\nstderr:\n%s",
        exc.returncode, exc.stdout, exc.stderr,
    )
    raise
```

### WR-02: `generate_task`'s subprocess has no explicit timeout

**File:** `airflow/dags/csv_generate_schedule.py:84-100`
**Issue:** `subprocess.run(...)` is called with no `timeout=` argument. The only bound on a hung
`generate_csv.py` process is the DAG-level `dagrun_timeout=timedelta(minutes=45)`, which fails the
*DagRun* bookkeeping-wise but does not reliably terminate an already-spawned child OS process inside
a running task — depending on executor/worker behavior this can leave an orphaned `generate_csv.py`
process consuming resources well past the point the DagRun is reported as timed out/failed.
**Fix:** Pass an explicit `timeout=` (e.g. a few minutes, well under `dagrun_timeout`) and catch
`subprocess.TimeoutExpired` alongside `CalledProcessError` per WR-01, so the subprocess is killed
deterministically rather than relying on DAG-level bookkeeping.

### WR-03: `retention_sweep`'s glob pattern over-matches non-canonical filenames, risking unintended deletion

**File:** `airflow/dags/_common/generate_schedule_helpers.py:93`
**Issue:** `base_dir.glob(f"{dataset}_*.csv*")` matches any filename containing the dataset prefix
and the literal substring `.csv` anywhere before the end, not just the exact
`<dataset>_<YYYYMMDD>.csv`/`.csv.gz` shapes `generate_csv.py` actually produces. A file such as
`customers_20260101.csv.bak` or `customers_20260101.csv.orig` (e.g. a manual backup an operator
makes following the established naming convention) matches the glob, has its embedded date parsed
successfully by the subsequent `datetime.strptime(date_token, "%Y%m%d")`, and — if older than the
30-day cutoff — is silently `unlink()`-ed by `retention_task`'s best-effort sweep, with no signal
to the operator beyond a routine `logger.info("retention: deleted ...")` line indistinguishable from
a normal generated-file cleanup. This is a real (if narrow) unintended-data-loss path: the function's
own docstring promises to delete only "``<dataset>_<YYYYMMDD>.csv``/``<dataset>_<YYYYMMDD>.csv.gz``"
files, but the implementation's glob is broader than that contract.
**Fix:** Match the exact two suffixes the generator produces, e.g.:
```python
import re

_FILENAME_RE = re.compile(r"^(\d{8})\.csv(\.gz)?$")

for path in base_dir.glob(f"{dataset}_*"):
    m = _FILENAME_RE.match(path.name.removeprefix(f"{dataset}_"))
    if m is None:
        skipped.append((path, "does not match <dataset>_<YYYYMMDD>.csv[.gz]"))
        continue
    date_token = m.group(1)
    ...
```
No test in `tests/unit/dags/test_generate_schedule_helpers.py` currently exercises this
over-matching case (see IN-03).

## Info

### IN-01: `_LATEST_INGESTION_SQL`'s "latest" row has no deterministic tiebreaker

**File:** `airflow/dags/csv_generate_schedule.py:38-44`
**Issue:** `ORDER BY processed_at DESC FETCH FIRST 1 ROW ONLY` with no secondary sort key means
two `ingestion_metadata` rows sharing an identical `processed_at` timestamp (plausible if
`processed_at` has coarser precision than actual insert timing, or under a fast retry) can be
returned in either order across executions, making `summary_task`'s logged line non-deterministic
in that edge case.
**Fix:** Add a stable tiebreaker, e.g. `ORDER BY processed_at DESC, id DESC` (or whatever
monotonic primary/surrogate key `ingestion_metadata` has).

### IN-02: Seed-derivation clock (`logical_date`) and generated-file naming clock (wall-clock `date.today()`) are different clocks

**File:** `airflow/dags/csv_generate_schedule.py:70-100` (cross-referenced against
`generator/generate_csv.py`'s `output_path()`, not itself in this review's file list)
**Issue:** `generate_task` derives its RNG seed from the DagRun's `logical_date`/`run_after` (D-04,
for retry-reproducibility), but the subprocess it invokes (`generate_csv.py --correlated`, no
`--today` override exists) names its output file from `date.today()` — real wall-clock date, not
`logical_date`. Near a UTC day boundary these two clocks can disagree (e.g. a `23:00` scheduled run
whose task execution slips past midnight), so the seed used to generate the data and the date
embedded in the resulting filename can come from different calendar days. This is pre-existing
`generate_csv.py` behavior, not introduced by this phase's files, but `csv_generate_schedule.py` is
the caller that newly relies on both properties holding together (deterministic seed *and*
deterministic same-day filename) — worth a cross-file note for whoever next touches either file.
**Fix:** Not urgent to change now; if it ever causes an observed retention/glob mismatch, thread
`logical_date`'s date through `generate_csv.py` as an explicit `--today` override instead of relying
on wall-clock `date.today()`.

### IN-03: `verify-phase9`'s live `BundleDagBag` check only asserts `deferrable`/`fail_when_dag_is_paused` on one of three `TriggerDagRunOperator` tasks

**File:** `Makefile:134-135`
**Issue:** The phase-gate script asserts
`dag.get_task('trigger_customers').deferrable is True` and
`.fail_when_dag_is_paused is True`, but never checks the same two properties on
`trigger_orders`/`trigger_report_ready`, even though `csv_generate_schedule.py` sets them
identically on all three operators. A future edit that accidentally drops `deferrable=True` or
`fail_when_dag_is_paused=True` from just `trigger_orders` or `trigger_report_ready` would pass this
gate undetected.
**Fix:**
```python
for tid in ("trigger_customers", "trigger_orders", "trigger_report_ready"):
    t = dag.get_task(tid)
    assert t.deferrable is True, tid
    assert t.fail_when_dag_is_paused is True, tid
```

---

_Reviewed: 2026-09-01T23:35:56Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
