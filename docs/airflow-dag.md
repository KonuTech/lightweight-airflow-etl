# The Airflow DAGs: Task Graphs, Triggering, and Live Verification Evidence

This document covers all three Airflow DAGs this project runs: the config-driven `csv_ingest` DAG
(`airflow/dags/csv_ingest.py`, D-01) — its task graph, how to trigger it, and the reproducible
commands used to prove DAG-03 (deferrable file-wait genuinely reports Airflow state `deferred`)
and DAG-05 (the identical, unmodified DAG file supports a second dataset by construction) — the
`report_ready` DAG (`airflow/dags/report_ready.py`, D-26), which senses when both datasets
have ingested data and materializes the business report — and the `csv_generate_schedule` DAG
(`airflow/dags/csv_generate_schedule.py`), the hourly orchestrator that generates fresh CSVs and
chain-triggers both of the other two, unmodified DAGs. Full HTTP-to-Oracle-rows automated
end-to-end testing is Phase 6's job (TEST-03) — this document only records manual/live
verification evidence gathered directly against the real running docker-compose stack.

## `csv_ingest` DAG

### Task Graph

```
load_config_task -> route_after_config -> wait_for_file -> process_csv_task -> load_results_task -> report_result_task
                                        \-> report_result_task   (CONFIGURATION_ERROR early exit)
```

- `load_config_task` — validates runtime `conf` (`dataset`, `config_path`) and loads the
  referenced dataset's `config.json`. On a `(ValueError, ConfigurationError)` it returns a
  `CONFIGURATION_ERROR`-shaped dict instead of letting the exception propagate.
- `route_after_config` — the **only** conditional in the graph. It branches on config validity
  (`status == CONFIGURATION_ERROR` or not), **never** on dataset identity (D-01/D-05) — the same
  DAG file structurally supports both `customers` and `orders` with zero dataset-specific
  branching.
- `wait_for_file` — a deferrable `FileSensor` (D-04). Glob-matches
  `/opt/airflow/data/<dataset>/<file_pattern>`; when the file is absent it releases its worker
  slot and defers to the triggerer, reporting Airflow task state `deferred` (proven live below).
- `process_csv_task` — the sole Oracle-writing integration point. Independently re-globs for the
  matched file (never reads `wait_for_file`'s XCom, which is a bare bool) and calls
  `csv_processor.engine.process()` exactly once.
- `load_results_task` — a thin XCom pass-through; never imports `csv_processor.load`/`oracledb`,
  never calls `process()` again.
- `report_result_task` — logs `dataset=`, `file=`, `status=`, `total=`, `valid=`, `invalid=`, and
  `duration=` (DAG-04). Runs on `trigger_rule="none_failed_min_one_success"` so it fires on both
  the success path and the config-error early-exit path.

### Triggering the DAG

`scripts/trigger_dag.sh <dataset> <config_path>` reuses the exact `/auth/token` → `Authorization:
Bearer` auth flow already proven in `scripts/verify_environment.py` (`AIRFLOW_AUTH_TOKEN_URL` =
`http://localhost:8080/auth/token`, `admin`/`admin`) — no new auth mechanism. It prints the
triggered `dag_run_id` to stdout only (diagnostics go to stderr), so a caller can capture it via
command substitution:

```bash
RUN_ID=$(scripts/trigger_dag.sh customers configs/datasets/customers.json)
RUN_ID=$(scripts/trigger_dag.sh orders configs/datasets/orders.json)
```

`AIRFLOW_BASE_URL` defaults to `http://localhost:8080`, overridable via env var.

**One-time setup note:** a brand-new Airflow metadata database (a fresh `postgres-db-volume`, e.g.
after `make reset`) starts with every DAG paused by default. `scripts/trigger_dag.sh` triggers a
run regardless, but the scheduler will never execute a paused DAG's tasks — the run sits in
`queued` state indefinitely. Unpause once per fresh environment:

```bash
JWT=$(curl -s -X POST "http://localhost:8080/auth/token" -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' | jq -r '.access_token')
curl -s -X PATCH "http://localhost:8080/api/v2/dags/csv_ingest" \
  -H "Authorization: Bearer ${JWT}" -H "Content-Type: application/json" \
  -d '{"is_paused": false}'
```

`make verify-phase5` (unit suite + a live `BundleDagBag` structure check) requires `make up`
first, same as `verify-phase4`.

### Live Verification Evidence

#### DAG-03: `wait_for_file` genuinely reports Airflow state `deferred`

Triggered the `orders` dataset **before** any `orders_*.csv*` fixture file existed on disk, then
polled the task instance endpoint every ~3s:

```bash
RUN_ID=$(scripts/trigger_dag.sh orders configs/datasets/orders.json)

JWT=$(curl -s -X POST "http://localhost:8080/auth/token" -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' | jq -r '.access_token')

curl -s -H "Authorization: Bearer ${JWT}" \
  "http://localhost:8080/api/v2/dags/csv_ingest/dagRuns/${RUN_ID}/taskInstances/wait_for_file" \
  | jq -r '.state'
```

Observed on the second poll (`state` field, live REST response — not a class attribute check):

```json
{
  "task_id": "wait_for_file",
  "dag_run_id": "manual__2026-08-29T20:10:03.118165+00:00",
  "state": "deferred",
  "operator": "FileSensor",
  "trigger": {
    "classpath": "airflow.providers.standard.triggers.file.FileTrigger",
    "triggerer_id": 10
  },
  "rendered_fields": {
    "filepath": "/opt/airflow/data/orders/orders_*.csv*"
  }
}
```

This confirms the sensor is genuinely triggerer-managed (the `trigger`/`triggerer_job` fields are
only ever populated for a deferred task instance) — not a worker-slot poll loop.

#### DAG-05: the identical, unmodified `csv_ingest.py` completes the `orders` dataset

Once `deferred` was observed, the fixture file was generated (`uv run python
generator/generate_csv.py --dataset orders`), and the same run was polled to completion:

```bash
curl -s --max-time 120 -H "Authorization: Bearer ${JWT}" -H "Accept: application/x-ndjson" \
  "http://localhost:8080/api/v2/dags/csv_ingest/dagRuns/${RUN_ID}/wait?result=load_results_task&interval=1" \
  | tail -1
```

**API note (Airflow 3.3.1, this environment):** the `wait` endpoint's `interval` query parameter
is required with no default — this environment's `openapi.json` confirms `interval` has no
default value, unlike the shorthand `?result=load_results_task` form assumed elsewhere. Pass
`&interval=1` explicitly (seconds between state polls).

Final response:

```json
{
  "state": "success",
  "results": {
    "load_results_task": {
      "status": "SUCCESS_WITH_INVALID_ROWS",
      "dataset": "orders",
      "checksum": "cfa476de6e145685a845896389d9f3f068a5686736468f05336b932988a5df52",
      "file_name": "orders_20260829.csv",
      "total_rows": 100,
      "valid_rows": 90,
      "invalid_rows": 10,
      "duration_seconds": 0.259
    }
  }
}
```

`state == "success"` and `results.load_results_task.dataset == "orders"` with
`results.load_results_task.status == "SUCCESS_WITH_INVALID_ROWS"` — proving DAG-05 for the second
dataset using the exact same, unmodified `csv_ingest.py` already proven for `customers` in
Plan 05-01, with zero dataset-specific code paths.

#### API note: `POST .../dagRuns` requires an explicit `logical_date`

This environment's Airflow 3.3.1 REST API marks `logical_date` as a **required** (but nullable)
field on `TriggerDAGRunPostBody` — omitting it entirely returns HTTP 422 (`Field required`).
`scripts/trigger_dag.sh` passes `"logical_date": null` explicitly in the trigger body so Airflow
auto-assigns the trigger time, matching UI/CLI-triggered runs.

## `report_ready` DAG

`airflow/dags/report_ready.py` (D-26) is dataset-agnostic — it takes no runtime `conf` and is
triggered on demand or run on a schedule, independently of any `csv_ingest` run.

### Task Graph

```
wait_for_both_datasets -> build_report_task
```

- `wait_for_both_datasets` — a `ReportReadySensor` (`airflow/dags/_common/oracle_partition_trigger.py`).
  Its `execute()` immediately defers to `OraclePartitionReadyTrigger`, a custom `BaseTrigger` —
  `apache-airflow-providers-oracle` ships no sensor of its own, so this is the only path to a
  deferrable check here. The trigger polls (`oracledb.connect_async()`, never a blocking call, so
  it never stalls the triggerer's shared event loop) `SELECT COUNT(DISTINCT dataset) FROM
  ingestion_metadata WHERE dataset IN ('customers', 'orders') AND TRUNC(processed_at) =
  TRUNC(SYSDATE)` every 30 seconds until the count reaches 2, then yields a `TriggerEvent`.
  `TRUNC(SYSDATE)` — the real wall-clock date — is used throughout, never `logical_date`/
  `data_interval` (this DAG has no meaningful logical date of its own).
- `build_report_task` — a plain `@task` that opens a normal (blocking) Oracle connection (correct
  here — it's a worker task, not the triggerer), runs the same business-report SQL every other
  path in this project shares (`docs/oracle.md`'s "Business Report Evidence"), and logs each
  returned row via `logging.getLogger("airflow.task")`.

### Live Verification Evidence

`tests/e2e/test_report_ready_dag.py` proves, against the real running stack: the sensor reaches
Airflow task state `deferred` before either dataset has ingested; it remains `deferred` after only
ONE dataset has ingested (proving it waits for both, not either); and the DAG run completes
successfully once BOTH datasets have ingested data for the current partition.

## `csv_generate_schedule` DAG

`airflow/dags/csv_generate_schedule.py` is the hourly orchestrator: `@dag(schedule="@hourly",
catchup=False, max_active_runs=1)`. Each run generates a fresh, correlated `customers`+`orders`
CSV pair, then sequentially chain-triggers the existing, unmodified `csv_ingest`/`report_ready`
DAGs above via `TriggerDagRunOperator` — it never duplicates their logic. `rows` and
`invalid_ratio` are operator-overridable DAG `Param`s (SCHED-08), defaulting to `100` and `0.1`
respectively.

### Task Graph

```
generate_task -> trigger_customers -> trigger_orders -> trigger_report_ready -> summary_task -> retention_task
```

- `generate_task` — invokes `generator/generate_csv.py --correlated` as a subprocess, seeded via
  `derive_seed(logical_date)` (D-04) so a retry of this task regenerates byte-identical data for
  the same DagRun's `logical_date`.
- `trigger_customers` — a `TriggerDagRunOperator(trigger_dag_id="csv_ingest", conf={"dataset":
  "customers", ...})`. Uses a deterministic `trigger_run_id` (`{{ dag_run.run_id }}__customers`,
  D-06), `skip_when_already_exists=True` (D-07) so a retry never double-triggers the same cascade
  run, and `deferrable=True` (D-08) so the wait releases its worker slot to the triggerer instead
  of blocking one, matching this project's existing "defer, never block" convention.
- `trigger_orders` — identical shape to `trigger_customers`, targeting the `orders` dataset. Runs
  strictly after `trigger_customers` fully commits (SCHED-03) — Phase 7's DB-level `BEFORE INSERT`
  trigger on `orders_valid` rejects any row whose `customer_id` doesn't already exist in
  `customers_valid`.
- `trigger_report_ready` — the same deterministic-`trigger_run_id`/`skip_when_already_exists`/
  `deferrable` shape (D-06/D-07/D-08), targeting `report_ready` instead of `csv_ingest`. Runs only
  after both dataset ingests complete.
- `summary_task` — queries `ingestion_metadata` directly via a bind-parameterized Oracle query for
  both datasets' latest ingestion (D-12/D-13/D-14) and logs one cascade summary line built by
  `format_cascade_summary()`, independent of any XCom pulled from the triggered DAGs.
- `retention_task` — best-effort deletes CSVs older than 30 days from `data/customers/` and
  `data/orders/` (D-16/D-17/D-18), logging each deletion/skip; never fails the DagRun even if
  cleanup itself fails, per its best-effort contract. It is the final task in each hourly
  `csv_generate_schedule` cascade run.

### Live Verification Evidence

#### SCHED-03: sequential chain-trigger completes end-to-end, deferred states observed

Triggered a real, fresh `csv_generate_schedule` run against the running docker-compose stack
(same auth flow as `csv_ingest`'s own DAG-03/DAG-05 evidence above), then polled each of the three
chain-trigger tasks in strict sequence (`trigger_customers` -> `trigger_orders` ->
`trigger_report_ready`, each becomes pollable once its predecessor starts):

```bash
JWT=$(curl -s -X POST "http://localhost:8080/auth/token" -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' | jq -r '.access_token')

curl -s -X POST "http://localhost:8080/api/v2/dags/csv_generate_schedule/dagRuns" \
  -H "Authorization: Bearer ${JWT}" -H "Content-Type: application/json" \
  -d '{"logical_date": null}'
```

```json
{
  "dag_run_id": "manual__2026-09-01T23:15:36.309509+00:00",
  "state": "queued",
  "logical_date": null
}
```

Polling `trigger_customers`'s task-instance endpoint caught a genuine `deferred` state (the
triggerer-managed `trigger`/`triggerer_id` fields are only ever populated for a deferred task
instance, same proof pattern as DAG-03's `FileTrigger` evidence):

```bash
curl -s -H "Authorization: Bearer ${JWT}" \
  "http://localhost:8080/api/v2/dags/csv_generate_schedule/dagRuns/${RUN_ID}/taskInstances/trigger_customers" \
  | jq '{state, trigger}'
```

```json
{
  "state": "deferred",
  "trigger": {
    "id": 21,
    "classpath": "airflow.providers.standard.triggers.external_task.DagStateTrigger",
    "kwargs": "{}",
    "created_date": "2026-09-01T23:15:37.917855Z",
    "queue": null,
    "triggerer_id": 1
  }
}
```

**Note:** the observed `trigger.classpath` is
`airflow.providers.standard.triggers.external_task.DagStateTrigger` -- the real deferred-mode
trigger `TriggerDagRunOperator(deferrable=True, wait_for_completion=True)` uses on this pinned
`apache-airflow-providers-standard==1.17.0`, not the `dag_run.DagStateTrigger` path speculated in
this plan's own interface notes before live-verification. `trigger_customers` reached `success`
seconds later.

`trigger_orders` was also caught in a genuine `deferred` state on its first poll, then reached
`success`:

```json
{
  "state": "deferred",
  "trigger": {
    "id": 22,
    "classpath": "airflow.providers.standard.triggers.external_task.DagStateTrigger",
    "kwargs": "{}",
    "created_date": "2026-09-01T23:15:52.436505Z",
    "queue": null,
    "triggerer_id": 1
  }
}
```

`trigger_report_ready` reached `success` before a `deferred` poll could be captured (this
project's low default `rows=100` makes both child DAGs -- and, for `report_ready`, its 30-second
`OraclePartitionReadyTrigger` poll -- fast enough that the sensor's condition was already true by
the first poll) -- a valid outcome per this task's own acceptance criteria, not a failure; the
load-bearing proof is strict ordering + overall success, not that every single task is caught
mid-defer.

The underlying `csv_ingest` child runs genuinely processed the fresh, correlated CSV pair
`generate_task` produced in this same run (not a stale leftover fixture -- see the "Environment
note" below):

```json
{
  "status": "SUCCESS_WITH_INVALID_ROWS",
  "dataset": "customers",
  "file_name": "customers_20260901.csv.gz",
  "total_rows": 100,
  "valid_rows": 90,
  "invalid_rows": 10
}
```

```json
{
  "status": "SUCCESS_WITH_INVALID_ROWS",
  "dataset": "orders",
  "file_name": "orders_20260901.csv.gz",
  "total_rows": 100,
  "valid_rows": 90,
  "invalid_rows": 10
}
```

Waiting for the overall DagRun to a terminal state (`result=trigger_report_ready&interval=2`)
confirmed genuine end-to-end completion:

```bash
curl -s --max-time 120 -H "Authorization: Bearer ${JWT}" -H "Accept: application/x-ndjson" \
  "http://localhost:8080/api/v2/dags/csv_generate_schedule/dagRuns/${RUN_ID}/wait?result=trigger_report_ready&interval=2" \
  | tail -1
```

```json
{
  "state": "success"
}
```

All six tasks (`generate_task`, `trigger_customers`, `trigger_orders`, `trigger_report_ready`,
`summary_task`, `retention_task`) reached `success` in strict order, with the three chain-trigger
tasks' timestamps confirming `trigger_orders` started only after `trigger_customers` fully ended
(`23:15:51` -> `23:15:52`), and `trigger_report_ready` only after `trigger_orders` fully ended
(`23:16:05` -> `23:16:06`) -- proving SCHED-03's ordering requirement (Phase 7's DB-level `BEFORE
INSERT` trigger on `orders_valid` requires `customers_valid` to already contain the referenced
`customer_id`s).

**Upstream issue check (STATE.md's recorded blocker):** none of the four flagged upstream
GitHub issues (#60049, #57756, #38353, #52247 -- open `TriggerDagRunOperator(deferrable=True)`
bugs at the time Phase 9 was researched) reproduced during this live run. All three deferred
chain-trigger tasks transitioned cleanly `queued` -> `deferred` -> `success` with no stuck/
zombie/duplicate-trigger behavior observed, on the exact pinned
`apache-airflow-providers-standard==1.17.0` / Airflow 3.3.1 combination this phase targets. The
MEDIUM-confidence research finding is corrected to live-confirmed working.

**Environment note (Rule 1/3 fix applied during this plan, not a DAG behavior change):** this
live-verification run initially failed twice before producing the evidence above, for two
unrelated pre-existing issues discovered only by attempting a real trigger:

1. Three stray, incorrectly-named leftover CSV fixture files (`customers_1788283829821223445.csv`,
   `customers_1788283847307977164.csv`, `orders_1788283847307977164.csv`, `orders_20260901.csv`
   uncompressed) from earlier manual testing sessions were sitting in `data/customers/`/
   `data/orders/` (both fully `.gitignore`d, untracked runtime directories -- confirmed via
   `git status --short -- data/` before deletion). `_common/paths.py`'s `resolve_matched_file()`
   deterministically re-globs for the **sorted-first** match on every `process_csv_task` run (by
   design, Phase 5) -- these stale files' names sorted before the current hourly
   `<dataset>_20260901.csv.gz` pair, so every real `@hourly`-scheduled run for the past several
   hours (`20:00`/`21:00`/`22:00` UTC) was silently reprocessing old data instead of the freshly
   generated pair, and `orders`'s stale file's `customer_id`s no longer existed in the
   continuously-regenerating `customers_valid` table, so every real hourly `orders` ingestion hit
   Phase 7's `BEFORE INSERT` FK trigger and returned `DATABASE_ERROR` -- `report_ready`'s sensor
   then waited forever for an `orders` `ingestion_metadata` row that never arrived, and each
   `csv_generate_schedule` run failed on its 45-minute `dagrun_timeout`. Deleted the four stale
   files (untracked, gitignored runtime artifacts, not source) so the sorted-first glob resolves
   to the genuine current-hour pair.
2. `generate_task`'s `derive_seed(ctx["dag_run"].logical_date)` raised
   `AttributeError: 'NoneType' object has no attribute 'strftime'` -- Airflow 3.x's
   `logical_date` is genuinely `None` for a manually/API-triggered run started with this plan's
   own documented `{"logical_date": null}` body (confirmed live against the pinned SDK's
   `DagRunProtocol` type: `logical_date: AwareDatetime | None` vs. `run_after: AwareDatetime`,
   the one field Airflow 3.x guarantees non-null on every DagRun). Fixed in
   `airflow/dags/csv_generate_schedule.py`'s `generate_task()` to fall back to `run_after` when
   `logical_date` is `None`, preserving D-04's retry-reproducibility for scheduled runs (which
   always carry a real `logical_date`) while making manually/API-triggered runs -- exactly this
   plan's own trigger pattern -- work correctly too. `csv_ingest.py`/`report_ready.py` were not
   touched (SCHED-06); `_common/generate_schedule_helpers.py`'s `derive_seed()` itself needed no
   change, only its caller's argument.

Both fixes were verified via `uv run pytest tests/unit/dags/ -x` (18 passed) and a live
`BundleDagBag` structural check (`DAGBAG_OK`) before the successful run captured above.

## Full HTTP-to-Oracle-rows automated testing

This document records this phase's own manual/live verification evidence only. Fully automated
end-to-end tests (HTTP trigger → DAG → CSV → Oracle → VALID/INVALID tables for `csv_ingest`,
asserted via a real test runner rather than manual curl commands, plus the `report_ready`
deferral/completion proof above) live under `tests/e2e/`.
