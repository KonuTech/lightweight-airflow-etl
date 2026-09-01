# Phase 9: Hourly Orchestrator DAG (`csv_generate_schedule`) - Research

**Researched:** 2026-09-01
**Domain:** Airflow-native DAG-to-DAG orchestration (`TriggerDagRunOperator` chain-triggering) on
Airflow 3.3.1 / `apache-airflow-providers-standard==1.17.0`, plus an in-process CSV generator task
and a plain-Oracle-SQL summary/retention pass.
**Confidence:** HIGH — this phase's environment prerequisites (Phase 8) are already complete and
verified in this checkout; the two previously-open technical uncertainties this research pass
targeted (`TriggerDagRunOperator`'s exact deferral mechanism on Airflow 3.x, and the full parameter
API at the pinned provider version) were resolved by reading the **actual pinned wheel source**,
not inferred from docs or older-version behavior.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** One shared `rows` Param drives both datasets' row counts (mirrors
  `generate_correlated_datasets(customers_rows=args.rows, orders_rows=args.rows)` — the CLI has no
  asymmetric-row-count path today). Default `rows=100` (matches `generate_csv.py`'s own `--rows`
  default).
- **D-02:** `invalid_ratio` is also exposed as a DAG Param (default `0.1`, matching the CLI
  default).
- **D-03:** The generate task invokes `generator/generate_csv.py` via `subprocess.run([sys.executable,
  "/opt/airflow/generator/generate_csv.py", "--correlated", "--rows", ..., "--invalid-ratio", ...,
  "--seed", ..., "--compress"], check=True, capture_output=True, text=True)` — per PITFALLS.md
  Pitfall 6, never as a Python import.
- **D-04:** Seed is derived from the DAG run's own `logical_date`, not wall-clock time at execution:
  `int(logical_date.strftime("%Y%m%d%H"))`, obtained via `get_current_context()["dag_run"].logical_date`
  — same `get_current_context()` access pattern `csv_ingest.py`'s `load_config_task` already uses.
  Different real hours always get different seeds (SCHED-02); a manual retry/re-trigger of the SAME
  scheduled hour reproduces identical content.
- **D-05:** The generate task's subprocess call includes `--compress` (gzip). Verified safe with
  zero config changes: `configs/datasets/{customers,orders}.json`'s `file_pattern` already matches
  `.gz`, and `csv_processor/compression.py` already handles decompression.
- **D-06:** Each of the three `TriggerDagRunOperator` tasks sets an explicit, deterministic
  `trigger_run_id` derived from the parent run's own identity (e.g. `"{{ dag_run.run_id }}__customers"`,
  Jinja-templated) — fixes the auto-generated `run_id` being wall-clock-derived at task-attempt time.
- **D-07:** `skip_when_already_exists=True` on all three trigger tasks. `reset_dag_run` stays
  unset/`False`.
- **D-08:** All three trigger tasks use `deferrable=True` (locked at ROADMAP level) alongside
  `wait_for_completion=True`.
- **D-09:** `csv_generate_schedule`'s own tasks use `retries=0` (explicit).
- **D-10:** `csv_generate_schedule` sets `dagrun_timeout=timedelta(minutes=45)`.
- **D-11:** No `is_paused_upon_creation` override — relies on the existing
  `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false` (Phase 8).
- **D-12:** A dedicated summary task re-queries Oracle directly (reads `ingestion_metadata`) for
  each dataset's row counts, rather than pulling XCom from the triggered DagRuns.
- **D-13:** The summary task selects the latest `processed_at` row per dataset from
  `ingestion_metadata` — safe because `max_active_runs=1` guarantees no concurrent writer.
- **D-14:** The summary line's `report_ready` component is a simple heartbeat (`report_ready=OK`),
  not a re-run of the business-report SQL. Runs with `trigger_rule="none_failed_min_one_success"`.
- **D-15:** Scope addition — Phase 9 also adds a retention/cleanup task for `data/customers/` and
  `data/orders/`. Needs a new requirement ID during planning.
- **D-16:** Retention window is 30 days — deletes any dated CSV (`.csv` or `.csv.gz`) older than 30
  days from `data/customers/` and `data/orders/`.
- **D-17:** Cleanup runs as a new task inside `csv_generate_schedule` itself, at the end of each
  hourly cascade.
- **D-18:** Cleanup is best-effort — never fails the overall DagRun. Log the error and move on.
- **D-19:** `csv_ingest` and `report_ready` keep their current `dag_id`s — explicitly NOT changed
  this phase.

### Claude's Discretion

- Exact REQ-ID for D-15's retention requirement (`SCHED-09` vs. a new `RETAIN-01` prefix) —
  see "New Requirement ID" section below for this research's recommendation.
- Exact `poke_interval` for the three deferrable `TriggerDagRunOperator` tasks — no user-facing
  behavioral difference at this project's scale; pick a value consistent with `FileSensor`'s existing
  `poke_interval=10`.
- Exact retention-task implementation shape (glob + `Path.unlink()` loop vs. something more
  structured).
- Exact wording/placement of `docs/airflow-dag.md` updates documenting the new DAG.

### Deferred Ideas (OUT OF SCOPE)

- Centralize `_BUSINESS_REPORT_SQL` into one shared module (currently duplicated across
  `report_ready.py`, `regenerate_readme_summary.py`, `verify_evidence.sql`) — future phase.
- Rename `csv_ingest`/`report_ready` to more descriptive DAG ids — future phase/milestone, would
  need a REQUIREMENTS.md change authorizing modification of those files.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SCHED-01 | `csv_generate_schedule` runs `@hourly`, `catchup=False`, no manual step | `@dag(...)` decorator params confirmed (Code Examples §1); Phase 8 environment already supports container-side generation |
| SCHED-02 | Fresh, non-duplicate CSV pair each hour (seed varies) | D-04's `logical_date`-derived seed confirmed safe — `dag_run.logical_date` is a plain, tz-aware `datetime.datetime` (verified against `airflow.sdk.types.DagRunProtocol`, not `pendulum.DateTime` as CONTEXT.md speculated); `.strftime("%Y%m%d%H")` works directly, no extra import |
| SCHED-03 | Sequential chain-trigger customers → orders → `report_ready`, each waits for completion | `TriggerDagRunOperator`'s exact AF3 (`AIRFLOW_V_3_0_PLUS`) code path read from the pinned `1.17.0` wheel — confirms `wait_for_completion=True` + `deferrable=True` genuinely releases the worker slot on this exact pinned version (see State of the Art) |
| SCHED-04 | `max_active_runs=1` on the new DAG | `max_active_runs: int` confirmed as the exact `@dag(...)`/`DAG(...)` param name (Code Examples §1) |
| SCHED-05 | `fail_when_dag_is_paused=True` on trigger tasks | Confirmed present and functional at `apache-airflow-providers-standard==1.17.0` (requires Airflow 3.2.0+, satisfied by pinned 3.3.1) — read directly from operator `__init__` |
| SCHED-06 | `csv_ingest.py`/`report_ready.py` stay byte-for-byte unmodified | Confirmed: their `conf={"dataset": ..., "config_path": ...}` contract (customers/orders) and no-`conf` contract (`report_ready`) read directly from both files' current source; new DAG's trigger `conf=` payloads must match exactly |
| SCHED-07 | One-line cascade summary log | `format_summary_log()` pattern (`_common/reporting.py`) is the template; D-12/D-13's Oracle-direct query is the data source (SQL in Code Examples §4) |
| SCHED-08 | Row counts + invalid-ratio configurable via DAG `Param`s | `Param(default, description=None, **kwargs)` signature confirmed from `airflow.sdk.definitions.param` source — `type="integer"`/`type="number"` schema kwargs work exactly like `csv_ingest.py`'s existing `Param(..., type="string", enum=[...])` |
| SCHED-10 (new, this research's recommendation — see below) | Retention: delete CSVs older than 30 days from `data/{customers,orders}/` | Filename convention `<dataset>_<YYYYMMDD>.csv[.gz]` confirmed from `generate_csv.py`'s `output_path()`; best-effort task pattern in Code Examples §5 |
</phase_requirements>

## Summary

Phase 8 already completed every environment prerequisite this DAG needs: `docker-compose.yml`
already mounts `./generator:/opt/airflow/generator:ro`, `PYTHONPATH` is already
`/opt/airflow/dags:/opt/airflow`, `faker==40.37.0` is already in the Dockerfile's constrained pip
install, and `airflow-init` already `chown -R`s `/opt/airflow/data` to `50000:0` with `chmod -R 777`
on every `docker compose up`. **No environment changes are needed in this phase** — it is purely a
new DAG file (`airflow/dags/csv_generate_schedule.py`) plus a `docs/airflow-dag.md` update.

The single most valuable finding of this research pass is a **correction to the prior milestone
research's `TriggerDagRunOperator` analysis**: SUMMARY.md/ARCHITECTURE.md/STACK.md all quoted and
reasoned about the operator's `execute()`/`wait_for_completion` blocking-`while`-loop code path —
but that exact code (`_trigger_dag_af_2`, with its own `self.defer(...)` call) is the **Airflow-2.x
compatibility branch**, gated behind `if not AIRFLOW_V_3_0_PLUS`. This project runs Airflow 3.3.1,
which is `AIRFLOW_V_3_0_PLUS`, so the code that actually executes is `_trigger_dag_af_3`, which does
something structurally different: it raises a `DagRunTriggerException` that the Task SDK's
`task_runner.py` catches and hands to `_handle_trigger_dag_run()`. That function (read directly from
the matching `apache-airflow-task-sdk` package) is what actually implements
`wait_for_completion`/`deferrable`/`skip_when_already_exists` for Airflow 3.x — and it confirms,
by direct inspection, that `deferrable=True` really does return a `DEFERRED` state message to the
supervisor (freeing the worker slot) rather than looping in-process. This raises confidence in
CONTEXT.md's D-08 (`deferrable=True`) from the prior research's MEDIUM to this research's **HIGH**
for the specific pinned-version combination this project uses (see State of the Art below) — though
the phase's own "live-verify against the exact pinned version" instruction in the phase description
should still be executed as a real end-to-end run, not skipped, since this was read from a matching
provider wheel in a sibling checkout, not built and run inside this project's own container this
session.

**Primary recommendation:** Write `csv_generate_schedule.py` following `csv_ingest.py`'s exact
structural conventions (`@dag`, `@task`, `get_current_context()`, `_common.paths`/`_common.reporting`
reuse), using the exact `TriggerDagRunOperator` parameter names and `Param` schema syntax verified
against the pinned `1.17.0` source in this document's Code Examples section, and add a `verify-phase9`
Makefile target mirroring `verify-phase5`'s `BundleDagBag` structural-check pattern.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Hourly scheduling / cron trigger | Airflow Scheduler (orchestration tier) | — | `@dag(schedule="@hourly")` — pure Airflow-native scheduling, no external cron |
| CSV generation (fake data) | Airflow Worker (task-execution tier, subprocess) | Filesystem (`data/`) | `generate_csv.py` runs as a subprocess of the Airflow worker task, writes to the shared bind-mounted `data/` volume |
| Chain-triggering downstream DAGs | Airflow Scheduler + Triggerer (deferred-task tier) | Airflow Worker (task-execution tier, when not deferred) | `TriggerDagRunOperator(deferrable=True)` hands the wait off to the Triggerer process, not the Worker |
| CSV → Oracle ingestion (customers/orders) | Airflow Worker (task-execution tier) + Oracle (data tier) | — | Unchanged — delegated entirely to the existing, unmodified `csv_ingest` DAG |
| Business-report generation | Airflow Worker (task-execution tier) + Oracle (data tier) | — | Unchanged — delegated entirely to the existing, unmodified `report_ready` DAG |
| Cascade summary logging | Airflow Worker (task-execution tier) + Oracle (data tier, read-only query) | — | New summary task queries `ingestion_metadata` directly, independent of `csv_ingest`/`report_ready`'s own internals |
| CSV retention/cleanup | Airflow Worker (task-execution tier) + Filesystem (`data/`) | — | Pure `pathlib`/`datetime`, no new tier, no new dependency |

## Standard Stack

### Core

No new libraries or provider packages this phase. Every dependency this DAG needs is already
installed as of Phase 8:

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `apache-airflow-providers-standard` | `==1.17.0` (already pinned, `docker/airflow/Dockerfile`) | `TriggerDagRunOperator` | Ships the only operator needed for DAG-to-DAG chain-triggering; already imported successfully elsewhere in this project (`FileSensor`) [VERIFIED: read directly from the pinned wheel's `trigger_dagrun.py`, `pip download apache-airflow-providers-standard==1.17.0` this session] |
| `faker` | `==40.37.0` (already pinned, `docker/airflow/Dockerfile`, added Phase 8) | Used *inside* `generate_csv.py`, not directly by this phase's new DAG code | Already installed; no action needed [VERIFIED: `docker/airflow/Dockerfile` read directly] |

### Supporting

None — no new imports beyond `subprocess`, `sys`, `pathlib`, `datetime`, `logging` (stdlib) and
`csv_processor.load`, `_common.paths`, `_common.reporting` (already-installed in-repo modules).

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `subprocess.run([sys.executable, ...], check=True, ...)` (D-03, locked) | In-process `from generator.generate_csv import main; main(["--correlated", ...])` | Both now technically work at Phase 8's environment state (PYTHONPATH extended to `/opt/airflow`) — CONTEXT.md's D-03 already locked subprocess per PITFALLS.md Pitfall 6's reasoning; not re-litigated here. Note: `docker-compose.yml`'s own Phase 8 comment says the mount exists so the DAG "can... run it in-process" — this is stale/aspirational commentary from Phase 8, superseded by Phase 9's own locked D-03; no action needed on the comment unless the planner wants to correct it for accuracy. |
| Pure-Oracle `ingestion_metadata` query for the summary task (D-12/D-13, locked) | Pulling XCom from the triggered `csv_ingest`/`report_ready` DagRuns via `TriggerDagRunOperator`'s own XCom push (`trigger_run_id`) | XCom-pulling would require `dag_id`+`run_id`-scoped `xcom_pull(dag_id=..., task_ids=..., run_id=...)` calls reaching into another DAG's internal task IDs — tighter coupling than D-12 wants; not pursued |

**Installation:** None required — no new packages.

## Package Legitimacy Audit

No external packages are installed by this phase. All dependencies (`apache-airflow-providers-standard==1.17.0`,
`faker==40.37.0`) were already vetted and installed during Phase 8. This section is intentionally
empty; the Package Legitimacy Gate protocol does not apply.

## Architecture Patterns

### System Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────────────────────┐
│  Airflow Scheduler (@hourly trigger, catchup=False, max_active_runs=1)             │
└──────────────────────────────────┬──────────────────────────────────────────────────┘
                                    │ fires new DagRun
                                    ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│  csv_generate_schedule  (dagrun_timeout=45min)                                     │
│                                                                                      │
│  [1] generate_task (@task)                                                         │
│      seed = int(logical_date.strftime("%Y%m%d%H"))          <- D-04                │
│      subprocess.run([sys.executable,                        <- D-03                │
│        "/opt/airflow/generator/generate_csv.py", "--correlated",                   │
│        "--rows", str(rows_param), "--invalid-ratio", str(ratio_param),             │
│        "--seed", str(seed), "--compress"], check=True, ...)                        │
│      writes: /opt/airflow/data/{customers,orders}/*_<YYYYMMDD>.csv.gz              │
│         │                                                                            │
│         ▼                                                                            │
│  [2] trigger_customers (TriggerDagRunOperator)                                     │
│      trigger_dag_id="csv_ingest", conf={"dataset":"customers","config_path":...}   │
│      trigger_run_id="{{ dag_run.run_id }}__customers"        <- D-06                │
│      skip_when_already_exists=True                           <- D-07                │
│      wait_for_completion=True, deferrable=True               <- D-08                │
│      fail_when_dag_is_paused=True                             <- SCHED-05           │
│         │  (raises DagRunTriggerException -> Task SDK -> triggers csv_ingest,      │
│         │   defers to Triggerer via DagStateTrigger, worker slot freed)            │
│         ▼                                                                            │
│      ══════════> [EXISTING, UNMODIFIED] csv_ingest(dataset=customers) DagRun       │
│                   (load_config -> wait_for_file -> process_csv -> load -> report)  │
│         │  (waits until DagRunState terminal)                                       │
│         ▼                                                                            │
│  [3] trigger_orders (TriggerDagRunOperator)  -- same shape, dataset=orders         │
│         │  MUST run after [2] fully commits (Phase 7 FK-existence DB trigger        │
│         │   on orders_valid requires customers_valid populated first)              │
│         ▼                                                                            │
│      ══════════> [EXISTING, UNMODIFIED] csv_ingest(dataset=orders) DagRun          │
│         │                                                                            │
│         ▼                                                                            │
│  [4] trigger_report_ready (TriggerDagRunOperator)                                  │
│      trigger_dag_id="report_ready", conf=None (no params contract)                 │
│         │                                                                            │
│         ▼                                                                            │
│      ══════════> [EXISTING, UNMODIFIED] report_ready DagRun                        │
│                   (ReportReadySensor -> build_report_task, logs business report)   │
│         │                                                                            │
│         ▼                                                                            │
│  [5] summary_task (@task, trigger_rule="none_failed_min_one_success")     <- D-12   │
│      SELECT dataset, total_rows, valid_rows, invalid_rows, processed_at            │
│      FROM ingestion_metadata WHERE dataset=:d                                      │
│      ORDER BY processed_at DESC FETCH FIRST 1 ROW ONLY   (per dataset)   <- D-13   │
│      logs: "customers=... orders=... report_ready=OK"                    <- D-14   │
│         │                                                                            │
│         ▼                                                                            │
│  [6] retention_task (@task, best-effort, never raises)                    <- D-15   │
│      glob data/{customers,orders}/*.csv* -> parse <dataset>_<YYYYMMDD>             │
│      -> delete if older than 30 days                                     <- D-16   │
└───────────────────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

No new directories. One new file:
```
airflow/dags/
├── csv_generate_schedule.py   # NEW — this phase's only new source file
├── csv_ingest.py              # UNCHANGED (SCHED-06)
├── report_ready.py            # UNCHANGED (SCHED-06)
└── _common/
    ├── paths.py                # reused as-is (DATA_ROOT)
    └── reporting.py             # pattern reused (not imported directly — new formatter, same shape)
```

### Pattern 1: Thin TaskFlow DAG delegates to existing tooling, no business logic inline

**What:** `csv_generate_schedule.py` follows `csv_ingest.py`'s and `report_ready.py`'s exact
established shape: `@dag`-decorated function, `@task`-decorated inner functions,
`get_current_context()` for runtime params, imports from `_common`.
**When to use:** Always, in this codebase — it's the project's one established DAG-authoring
convention, verified across all three existing/planned DAGs.
**Example:**
```python
# Source: airflow/dags/csv_ingest.py (this repo, read directly)
from _common import paths, reporting
from airflow.sdk import Param, dag, get_current_context, task

@dag(
    dag_id="csv_ingest",
    schedule=None,
    catchup=False,
    params={
        "dataset": Param("customers", type="string", enum=["customers", "orders"]),
        "config_path": Param("configs/datasets/customers.json", type="string"),
    },
)
def csv_ingest() -> None:
    ...
```

### Pattern 2: `TriggerDagRunOperator` on Airflow 3.x — exact confirmed parameter set

**What:** The full constructor signature, read directly from the pinned
`apache-airflow-providers-standard==1.17.0` wheel (`pip download ... --no-deps`, extracted and read
this session — not WebSearch, not training data).
**Confirmed present, all needed for this phase:**
```python
# Source: apache-airflow-providers-standard==1.17.0's own trigger_dagrun.py,
# TriggerDagRunOperator.__init__ signature (VERIFIED: extracted from the
# actual downloaded wheel this session)
def __init__(
    self,
    *,
    trigger_dag_id: str,
    trigger_run_id: str | None = None,             # templated (see template_fields below)
    conf: dict | None = None,                        # templated
    logical_date=NOTSET,
    run_after=NOTSET,
    reset_dag_run: bool = False,
    wait_for_completion: bool = False,
    poke_interval: int = 60,
    allowed_states: list[str] | None = None,          # default [DagRunState.SUCCESS]
    failed_states: list[str] | None = None,            # default [DagRunState.FAILED]
    skip_when_already_exists: bool = False,
    fail_when_dag_is_paused: bool = False,             # requires Airflow 3.2.0+ on AF3 (satisfied: 3.3.1)
    note: str | None = None,
    deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
    openlineage_inject_parent_info: bool = True,
    **kwargs,
) -> None: ...

template_fields: Sequence[str] = (
    "trigger_dag_id", "trigger_run_id", "logical_date", "conf",
    "wait_for_completion", "skip_when_already_exists",
)
```
**Confirms, for this phase's planning:**
- `trigger_run_id` IS a `template_fields` entry — D-06's `"{{ dag_run.run_id }}__customers"` Jinja
  templating works exactly as CONTEXT.md assumed. [VERIFIED: read directly from `template_fields`]
- `skip_when_already_exists` exists exactly under that name, exactly the boolean CONTEXT.md D-07
  assumed. [VERIFIED]
- `fail_when_dag_is_paused` exists and is usable — the pinned Airflow 3.3.1 is `>= 3.2.0`, so the
  `NotImplementedError` guard in `__init__` (`if fail_when_dag_is_paused and AIRFLOW_V_3_0_PLUS and
  not AIRFLOW_V_3_2_PLUS: raise NotImplementedError(...)`) does not trigger. [VERIFIED]
- `deferrable`, `wait_for_completion`, `poke_interval`, `conf`, `allowed_states`, `failed_states`,
  all exist exactly as named. [VERIFIED]
- **No `execution_timeout` parameter on this operator specifically** — that's a generic
  `BaseOperator` kwarg (`**kwargs` absorbs it), not something `TriggerDagRunOperator` defines itself;
  it works, but isn't listed in this operator's own docstring. Use Airflow's standard
  `execution_timeout=timedelta(...)` `BaseOperator` kwarg if per-task timeouts (distinct from the
  DAG-level `dagrun_timeout`) are wanted — not required by CONTEXT.md's locked decisions, so
  optional.

### Pattern 3: `conf=` payload contract for the two trigger targets

**What:** `csv_ingest`'s own `params={...}` block (read directly, see Pattern 1) defines exactly two
keys, both strings: `dataset` (enum `["customers", "orders"]`) and `config_path`. `report_ready`
takes **no** `params` at all — its `@dag(...)` decorator has zero `params=` kwarg.
**Example:**
```python
# customers trigger — mirrors csv_ingest's own params contract exactly
TriggerDagRunOperator(
    task_id="trigger_customers",
    trigger_dag_id="csv_ingest",
    conf={"dataset": "customers", "config_path": "configs/datasets/customers.json"},
    trigger_run_id="{{ dag_run.run_id }}__customers",
    skip_when_already_exists=True,
    wait_for_completion=True,
    deferrable=True,
    fail_when_dag_is_paused=True,
    poke_interval=10,  # matches FileSensor's existing poke_interval convention
)

# report_ready trigger — no conf at all (report_ready has no params={} block)
TriggerDagRunOperator(
    task_id="trigger_report_ready",
    trigger_dag_id="report_ready",
    trigger_run_id="{{ dag_run.run_id }}__report_ready",
    skip_when_already_exists=True,
    wait_for_completion=True,
    deferrable=True,
    fail_when_dag_is_paused=True,
    poke_interval=10,
)
```

### Pattern 4: `@dag(...)` decorator's exact `dagrun_timeout`/`max_active_runs` param names

**What:** Read directly from `airflow.sdk.definitions.dag`'s `DAG`/`@dag` implementation (via the
matching-provider-version sibling checkout's installed `apache-airflow-task-sdk`).
```python
# Source: airflow/sdk/definitions/dag.py (VERIFIED: read directly, matching
# provider version 1.17.0 sibling checkout)
max_active_runs: int = attrs.field(factory=_config_int_factory("core", "max_active_runs_per_dag"))
dagrun_timeout: timedelta | None = attrs.field(...)

# @dag(...) overload signature confirms both are accepted directly:
def dag(
    ...,
    max_active_runs: int = ...,
    ...,
    dagrun_timeout: timedelta | None = None,
    ...,
) -> ...: ...
```
```python
from datetime import timedelta

@dag(
    dag_id="csv_generate_schedule",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,                       # SCHED-04
    dagrun_timeout=timedelta(minutes=45),    # D-10
    params={
        "rows": Param(100, type="integer", minimum=1),        # D-01, SCHED-08
        "invalid_ratio": Param(0.1, type="number", minimum=0.0, maximum=1.0),  # D-02, SCHED-08
    },
)
def csv_generate_schedule() -> None:
    ...
```
`Param(default, description=None, **kwargs)`'s constructor (read directly from
`airflow/sdk/definitions/param.py`) treats every kwarg beyond `default`/`description`/`source` as
the JSON-schema (`self.schema = kwargs`), confirming `type="integer"`/`type="number"` +
`minimum`/`maximum` work exactly like `csv_ingest.py`'s existing `type="string", enum=[...]` usage —
same mechanism, different schema keys. [VERIFIED]

### Pattern 5: `get_current_context()["dag_run"].logical_date` — exact type, confirmed

**What:** CONTEXT.md's D-04 speculated this is a `pendulum.DateTime`. Read directly against
`airflow.sdk.types.DagRunProtocol` (the Task SDK's typed interface for the runtime `dag_run` context
object): `logical_date: AwareDatetime | None`. `AwareDatetime` is a Pydantic type alias that
validates as a standard, timezone-aware `datetime.datetime` — **not** `pendulum.DateTime**.
`.strftime("%Y%m%d%H")` is a plain `datetime.datetime` method and works with no extra import beyond
what `csv_ingest.py` already imports (`get_current_context` from `airflow.sdk`).
[VERIFIED: `airflow/sdk/types.py`, `DagRunProtocol.logical_date` field, read directly against the
matching-provider-version sibling checkout — this corrects, rather than merely confirms, CONTEXT.md's
own speculative "pendulum.DateTime" framing.]
```python
@task
def generate_task() -> dict[str, object]:
    ctx = get_current_context()
    logical_date = ctx["dag_run"].logical_date          # tz-aware datetime.datetime
    seed = int(logical_date.strftime("%Y%m%d%H"))        # D-04, no extra import needed
    ...
```

### Anti-Patterns to Avoid

- **Reaching into `csv_ingest`/`report_ready`'s own task IDs via `xcom_pull(dag_id=..., task_ids=...)`
  for the summary task:** rejected by D-12 — tight coupling to another DAG's internal task shape.
  Query `ingestion_metadata` directly instead (Pattern 6/Code Examples §4).
- **Passing `reset_dag_run=True` on any trigger task:** explicitly out of scope (REQUIREMENTS.md's
  Out of Scope table) — combined with `deferrable=True` and an explicit `trigger_run_id`, this is a
  documented upstream bug pattern (apache/airflow#57756). Never set it.
- **Assuming `TriggerDagRunOperator`'s `wait_for_completion=True` blocking-`while`-loop code (as
  described in prior SUMMARY.md/ARCHITECTURE.md/STACK.md research) is what actually runs on this
  pinned Airflow 3.3.1 install:** it is not — that's the AF2-compat branch. See State of the Art.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Waiting for another DAG's run to finish before proceeding | A custom polling loop against `DagRun`/Airflow's REST API | `TriggerDagRunOperator(wait_for_completion=True, deferrable=True, allowed_states=..., failed_states=...)` | Already ships in the pinned provider, already handles the deferred-vs-blocking distinction, already exposes `failed_states` for clean failure propagation |
| Deriving a per-run-unique seed | `time.time()`/`uuid.uuid4()`-based seed (non-reproducible, breaks "retry reproduces the same content" property D-04 wants) | `int(logical_date.strftime("%Y%m%d%H"))` | Ties directly to the DAG run's own identity — reproducible on retry, distinct across real hours |
| Parsing dated CSV filenames for retention | A regex/dateutil-based generic date parser | `datetime.strptime(stem.split("_")[-1], "%Y%m%d")` against the known, fixed `<dataset>_<YYYYMMDD>.csv[.gz]` convention (from `generate_csv.py`'s own `output_path()`) | The filename format is fully controlled and fixed by this same codebase — no need for a general-purpose parser |
| Retention/cleanup task's error handling | A custom retry/circuit-breaker library | Plain `try`/`except Exception: logging.getLogger("airflow.task").warning(...)` per file, never re-raising (D-18) | Airflow's own task-level `retries=0` (D-09) already governs the *task's* own retry policy; the requirement here is "never let one bad file block the DagRun," which a simple per-file try/except satisfies completely |

**Key insight:** Every capability this phase needs already exists in this codebase or the pinned
Airflow provider — the entire phase is wiring, not new engineering. The only genuinely new logic is
the retention task's glob-and-delete loop and the summary task's SQL query, both trivial.

## Common Pitfalls

This phase's pitfalls were already extensively catalogued in `.planning/research/PITFALLS.md`
(Pitfalls 1, 6, 7, 8 apply directly; see that file for full detail). This section adds corrections
and confirmations found during this research pass, not a re-derivation.

### Pitfall (correction): Prior research's `TriggerDagRunOperator` worker-slot analysis quoted the wrong code path for this project's actual pinned Airflow version

**What goes wrong (if unaddressed):** SUMMARY.md/ARCHITECTURE.md/STACK.md's "Integration Point 2"
and "Open Decision" sections both quote and reason from this exact code block:
```python
if self.wait_for_completion:
    if self.deferrable:
        self.defer(trigger=DagStateTrigger(...), method_name="execute_complete")
    while True:
        ...
```
attributing it to "the operator's `execute()` method," and conclude that `deferrable=False` "doesn't
matter at this scale" while flagging `deferrable=True` as carrying multiple open upstream bugs.

**Why it happens:** That exact code lives in `TriggerDagRunOperator._trigger_dag_af_2` — a method
gated behind `if not AIRFLOW_V_3_0_PLUS` inside `execute()`. This project runs Airflow 3.3.1, which
**is** `AIRFLOW_V_3_0_PLUS` (True) — the method that actually runs is `_trigger_dag_af_3`, which does
not call `self.defer()` at all. Instead it raises `DagRunTriggerException(**kwargs)` (defined in
`airflow.sdk.exceptions`), carrying `deferrable=self.deferrable` as one of its fields. The Task SDK's
`task_runner.py` catches this exception (`except DagRunTriggerException as drte:`) and dispatches to
`_handle_trigger_dag_run(drte, ...)`, which is where `wait_for_completion`/`deferrable`/
`skip_when_already_exists` are *actually* implemented for Airflow 3.x — a structurally different
code path than the one the prior research quoted.

**How to avoid:** When implementing/verifying this phase, treat `_handle_trigger_dag_run()`
(`airflow/sdk/execution_time/task_runner.py`, read directly this session from a sibling checkout
pinned to the identical `apache-airflow-providers-standard==1.17.0`) as the authoritative reference,
not the AF2-branch code quoted in prior research. Confirmed behavior, read directly:
- `skip_when_already_exists=True` + a `DAGRUN_ALREADY_EXISTS` error from the supervisor →
  `TaskInstanceState.SKIPPED` (not raised as an exception) — matches D-07 exactly.
- `wait_for_completion=True` + `deferrable=True` → returns a `TaskDeferred(trigger=DagStateTrigger(...,
  run_ids=[drte.dag_run_id], ...), method_name="execute_complete")`, which `_defer_task()` converts
  into a `DeferTask` message sent to the supervisor and a `TaskInstanceState.DEFERRED` return value —
  **the task process itself returns here; it does not loop.** This is the same mechanism
  `FileSensor(deferrable=True)` already uses successfully in `csv_ingest`, now confirmed (not
  assumed) to apply identically to `TriggerDagRunOperator` on this exact pinned version.
- `wait_for_completion=True` + `deferrable=False` → the blocking `while True: time.sleep(poke_interval)`
  loop, but this loop is now inside `_handle_trigger_dag_run()` in the Task SDK's worker-side runner,
  not the operator's own `execute()` — functionally equivalent to what prior research described
  (still occupies the worker), but a different call site.

**Confidence:** HIGH — read directly from the pinned wheel (`apache-airflow-providers-standard==1.17.0`,
downloaded via `pip download` this session) and a sibling checkout's installed
`apache-airflow-task-sdk==1.3.0` (one patch version behind the latest `1.3.1`, but matching provider
version `1.17.0` exactly — the `DagRunTriggerException`/`_handle_trigger_dag_run` mechanism is not
the kind of thing that changes across a single task-sdk patch release). **Not independently verified
by actually building and running this project's own container this session** — the phase
description's instruction to live-verify against the exact pinned combination during
planning/execution should still be followed as a real end-to-end DAG run, this static source-reading
only substantially de-risks that verification, it doesn't replace it.

**This directly affects `TriggerDagRunOperator`'s upstream bug applicability (#60049, #57756, #38353,
#52247):** all four issues are Airflow-3.x-era reports; since this project's exact pinned code (both
provider `1.17.0` and the matching task-sdk) already routes through the `DagRunTriggerException`
mechanism rather than a raw `self.defer()` call inside the operator, and the specific failure #60049
describes ("defers even when `wait_for_completion=False`") is structurally impossible in the read
code (`if drte.wait_for_completion: if drte.deferrable: ...` — deferral is nested inside the
`wait_for_completion` check), this specific bug does not reproduce in the code actually read this
session. The other three issues were not individually re-tested against this exact combination and
should still be treated as open until the phase's own live-verification pass confirms no reproduction.

## Code Examples

### 1. `@dag(...)` header — schedule, params, timeout

```python
# Source: pattern verified against airflow/sdk/definitions/dag.py (matching
# provider-version sibling checkout) + csv_ingest.py's existing @dag(...) usage
from datetime import timedelta

from airflow.sdk import Param, dag, get_current_context, task

@dag(
    dag_id="csv_generate_schedule",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,                        # SCHED-04
    dagrun_timeout=timedelta(minutes=45),      # D-10
    params={
        "rows": Param(100, type="integer", minimum=1),
        "invalid_ratio": Param(0.1, type="number", minimum=0.0, maximum=1.0),
    },
)
def csv_generate_schedule() -> None:
    ...
```

### 2. Generate task — subprocess invocation, D-03/D-04/D-05

```python
# Source: subprocess shape mirrors PITFALLS.md Pitfall 6's recommendation,
# confirmed compatible with this repo's actual Dockerfile Python environment
# (apache/airflow:3.3.1-python3.12 base image, sys.executable resolves to
# that same interpreter inside the airflow-scheduler/worker container).
import subprocess
import sys

@task(retries=0)
def generate_task() -> None:
    ctx = get_current_context()
    logical_date = ctx["dag_run"].logical_date       # tz-aware datetime.datetime, confirmed
    seed = int(logical_date.strftime("%Y%m%d%H"))     # D-04
    rows = ctx["params"]["rows"]
    invalid_ratio = ctx["params"]["invalid_ratio"]

    subprocess.run(
        [
            sys.executable,
            "/opt/airflow/generator/generate_csv.py",
            "--correlated",
            "--rows", str(rows),
            "--invalid-ratio", str(invalid_ratio),
            "--seed", str(seed),
            "--compress",
        ],
        check=True,             # raises CalledProcessError on non-zero exit -- fails the task,
                                 # no explicit AirflowException import/raise needed (confirmed:
                                 # this repo has zero existing AirflowException usage anywhere;
                                 # letting subprocess.run's own exception propagate is sufficient
                                 # and consistent with the codebase's existing minimal-import style)
        capture_output=True,
        text=True,
    )
```
Note: `sys.executable` inside the built image resolves to the same Python the Airflow worker itself
runs under (`apache/airflow:3.3.1-python3.12` base image has one Python interpreter; no venv
switching happens between the scheduler/worker process and its own subprocess calls) — no special
handling needed. Environment variables (including `PYTHONPATH`) are inherited by default by
`subprocess.run` (no `env=` override needed, and none should be added — `generate_csv.py` doesn't
need `PYTHONPATH` itself since it's invoked as a script, not imported).

### 3. Trigger tasks — full parameter set, D-06/D-07/D-08/SCHED-03/SCHED-05

```python
# Source: parameter names/behavior VERIFIED against pinned
# apache-airflow-providers-standard==1.17.0 wheel source, read directly this session
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

trigger_customers = TriggerDagRunOperator(
    task_id="trigger_customers",
    trigger_dag_id="csv_ingest",
    conf={"dataset": "customers", "config_path": "configs/datasets/customers.json"},
    trigger_run_id="{{ dag_run.run_id }}__customers",   # D-06, confirmed templated
    skip_when_already_exists=True,                       # D-07
    wait_for_completion=True,
    deferrable=True,                                     # D-08
    fail_when_dag_is_paused=True,                         # SCHED-05
    poke_interval=10,
)

trigger_orders = TriggerDagRunOperator(
    task_id="trigger_orders",
    trigger_dag_id="csv_ingest",
    conf={"dataset": "orders", "config_path": "configs/datasets/orders.json"},
    trigger_run_id="{{ dag_run.run_id }}__orders",
    skip_when_already_exists=True,
    wait_for_completion=True,
    deferrable=True,
    fail_when_dag_is_paused=True,
    poke_interval=10,
)

trigger_report_ready = TriggerDagRunOperator(
    task_id="trigger_report_ready",
    trigger_dag_id="report_ready",
    # report_ready.py has no `params={}` block at all -- no conf= needed here.
    trigger_run_id="{{ dag_run.run_id }}__report_ready",
    skip_when_already_exists=True,
    wait_for_completion=True,
    deferrable=True,
    fail_when_dag_is_paused=True,
    poke_interval=10,
)

generate_task() >> trigger_customers >> trigger_orders >> trigger_report_ready
```

### 4. Summary task — Oracle-direct query, D-12/D-13/D-14, SCHED-07

```python
# Source: table schema read directly from docker/oracle/init/01_ingestion_metadata.sql;
# connection helper read directly from packages/csv-processor/src/csv_processor/load.py
# (the same get_connection() report_ready.py's build_report_task already uses)
import logging

from csv_processor import load

_LATEST_INGESTION_SQL = """
SELECT total_rows, valid_rows, invalid_rows, processed_at
FROM ingestion_metadata
WHERE dataset = :dataset
ORDER BY processed_at DESC
FETCH FIRST 1 ROW ONLY
"""

@task(trigger_rule="none_failed_min_one_success", retries=0)
def summary_task() -> None:
    connection = load.get_connection()
    try:
        cursor = connection.cursor()
        parts = []
        for dataset in ("customers", "orders"):
            cursor.execute(_LATEST_INGESTION_SQL, dataset=dataset)
            row = cursor.fetchone()
            if row is None:
                parts.append(f"{dataset}=NO_DATA")
            else:
                total, valid, invalid, _processed_at = row
                parts.append(f"{dataset}=total:{total},valid:{valid},invalid:{invalid}")
    finally:
        connection.close()
    parts.append("report_ready=OK")  # D-14: heartbeat only, reaching this task means
                                       # trigger_report_ready already succeeded
    logging.getLogger("airflow.task").info(" ".join(parts))
```
`ingestion_metadata`'s columns, confirmed directly from `docker/oracle/init/01_ingestion_metadata.sql`:
`id`, `dataset`, `file_name`, `checksum`, `processed_at` (`TIMESTAMP WITH TIME ZONE`), `total_rows`,
`valid_rows`, `invalid_rows`, `status`. `UNIQUE (dataset, checksum)` is the idempotency constraint;
no index explicitly on `(dataset, processed_at)` beyond the implicit PK/unique-constraint indexes —
acceptable at this project's row-count scale (no performance concern flagged).

### 5. Retention task — best-effort, never fails the DagRun, D-15..D-18

```python
# Source: filename convention read directly from generator/generate_csv.py's
# output_path(): `_DATA_DIR / dataset / f"{dataset}_{day:%Y%m%d}.csv"`
# (D-05's --compress appends ".gz" on top of that same stem).
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

_RETENTION_DAYS = 30
_DATA_ROOT = Path("/opt/airflow/data")  # matches _common/paths.py's DATA_ROOT exactly

@task(trigger_rule="none_failed_min_one_success", retries=0)
def retention_task() -> None:
    logger = logging.getLogger("airflow.task")
    cutoff = datetime.now(UTC) - timedelta(days=_RETENTION_DAYS)
    for dataset in ("customers", "orders"):
        base_dir = _DATA_ROOT / dataset
        for path in base_dir.glob(f"{dataset}_*.csv*"):
            try:
                # "<dataset>_<YYYYMMDD>.csv" or "<dataset>_<YYYYMMDD>.csv.gz"
                date_token = path.name.removeprefix(f"{dataset}_").split(".", 1)[0]
                file_date = datetime.strptime(date_token, "%Y%m%d").replace(tzinfo=UTC)
                if file_date < cutoff:
                    path.unlink()
                    logger.info("retention: deleted %s (older than %d days)", path, _RETENTION_DAYS)
            except Exception:  # noqa: BLE001 -- D-18: best-effort, never raise
                logger.warning("retention: failed to process %s, skipping", path, exc_info=True)
```
Deliberately broad `except Exception` here (not the narrow `oracledb.OperationalError`-style
precision used elsewhere in this codebase) — D-18 explicitly wants *any* per-file failure (a
permission error, an unparseable filename, a race with a concurrent read) logged and skipped, never
propagated. This is the one place in this phase where a broad catch is the *correct* choice, not an
anti-pattern — contrast with Pitfall 9's guidance (narrow catch) which applies to a different
concern (masking genuine Oracle bugs), not this filesystem-housekeeping context.

### 6. `verify-phase9` Makefile target (structural check, mirrors `verify-phase5`)

```makefile
# Source: pattern copied from Makefile's existing verify-phase5 target (read directly)
verify-phase9:     ## Phase 9's own combined local gate: unit suite + live DagBag structure check (requires `make up` first)
	uv run pytest tests/unit/ -x
	docker compose exec -T airflow-scheduler python -c "\
from pathlib import Path; \
from airflow.dag_processing.dagbag import BundleDagBag; \
b = BundleDagBag(bundle_path=Path('/opt/airflow/dags'), dag_folder='/opt/airflow/dags'); \
assert not b.import_errors, b.import_errors; \
dag = b.dags['csv_generate_schedule']; \
required = {'generate_task','trigger_customers','trigger_orders','trigger_report_ready','summary_task','retention_task'}; \
assert required.issubset(set(dag.task_ids)), dag.task_ids; \
assert dag.max_active_runs == 1, dag.max_active_runs; \
assert dag.get_task('trigger_customers').deferrable is True; \
print('DAGBAG_OK')"
```

## New Requirement ID (D-15's scope addition)

**Recommendation: `SCHED-10`**, not `SCHED-09` (already taken by the deferred `FileSensor`
timeout-vs-hourly-period item in REQUIREMENTS.md's "Future Requirements" section — CONTEXT.md's own
canonical references explicitly warn not to confuse these) and not a new `RETAIN-` prefix.
Rationale: `SCHED-10` continues the phase's own existing prefix (mirrors Phase 8's `ENV-03` precedent
— that addition stayed under the *same* prefix as the phase it was bundled into, `ENV`, rather than
inventing a new one), keeps all of Phase 9's requirements contiguous in the traceability table, and
the retention task genuinely *is* part of "scheduling automation" scope (D-17 locks it as a task
inside `csv_generate_schedule` itself, not a separate DAG/mechanism) — a `RETAIN-` prefix would imply
a broader, standalone retention *feature* this phase doesn't actually build. Add to
REQUIREMENTS.md's v1.1 Requirements/Scheduling section: `SCHED-10: csv_generate_schedule deletes CSV
files older than 30 days from data/customers/ and data/orders/ at the end of each hourly cascade,
without failing the DagRun if cleanup itself fails.`

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `apache-airflow-task-sdk`'s exact version installed inside this project's own built image matches the `1.3.0` read from the sibling checkout closely enough that `_handle_trigger_dag_run()`'s behavior is identical | State of the Art / Common Pitfalls | If this project's actual installed task-sdk differs meaningfully (e.g. pulls `1.3.1` or a version with a behavioral fix/regression in this exact function), the deferred-mode confidence claim could be slightly stale. Low risk: task-sdk version is pinned transitively by the `apache/airflow:3.3.1-python3.12` base image, which should resolve to the same minor-version family regardless of checkout. Mitigate by running the phase's own required live-verification pass (already mandated by the phase description) before considering `deferrable=True` fully proven in this project's actual container. |
| A2 | The three still-open upstream issues (#57756, #38353, #52247) that were not individually re-tested against this exact pinned combination do not reproduce | Common Pitfalls | If one does reproduce, a trigger task could get stuck in `deferred` state indefinitely. Mitigate exactly as the phase description already instructs: live-verify (a real triggered run, watched through the Airflow UI, not just a structural `BundleDagBag` check) before considering the chain-trigger implementation done. |
| A3 | `dagrun_timeout=timedelta(minutes=45)` combined with the actual runtime of generate + 2×ingest + report_ready comfortably fits inside the hourly cadence | Code Examples §1 | If the real cascade (especially with `deferrable=True`'s extra Triggerer round-trip latency) runs meaningfully longer than the low-tens-of-seconds benchmark cited in prior research, 45 minutes may be tight or (less likely) too generous. Low risk given SUMMARY.md's cited benchmark (`780K rows/sec` bulk-loading) and this project's tiny fixture sizes (rows=100 default); no action needed unless a live run is observed running close to the timeout. |

**If empty:** N/A — see table above; three assumptions logged, all low-risk and already covered by
the phase description's own mandated live-verification step.

## Open Questions

1. **Exact behavior of `apache-airflow-task-sdk` inside this project's own actual built image**
   - What we know: The pinned provider (`apache-airflow-providers-standard==1.17.0`) source was read
     directly from the actual downloaded wheel — HIGH confidence, exact match. The task-sdk
     implementation of `_handle_trigger_dag_run()` was read from a *sibling* checkout
     (`/mnt/c/.../airflow-platform`) that happens to pin the identical provider version, with
     task-sdk at `1.3.0` (latest available is `1.3.1`).
   - What's unclear: Whether this project's own `apache/airflow:3.3.1-python3.12` base image
     resolves task-sdk to exactly `1.3.0`, a different nearby version, or whether any behaviorally
     relevant change exists between those versions for this specific code path.
   - Recommendation: Non-blocking for planning — write the DAG per this research's confirmed
     parameter API, then execute the phase description's own mandated live-verification pass (a
     real hourly-triggered run, watched through the Airflow UI, confirming `deferred` state appears
     correctly and the chain completes) as part of this phase's own acceptance criteria, exactly as
     already instructed at the phase-description level.

2. **`poke_interval` exact value for the three trigger tasks**
   - What we know: CONTEXT.md leaves this to Claude's Discretion, suggesting consistency with
     `FileSensor`'s existing `poke_interval=10`.
   - What's unclear: Nothing technical — this is a pure style/consistency choice with no functional
     stakes at this project's scale (three sequential deferred waits, each on a sub-minute-runtime
     downstream DAG).
   - Recommendation: Use `poke_interval=10`, matching `FileSensor`'s established value exactly (used
     throughout Code Examples §3 above).

## Environment Availability

All dependencies this phase needs were already verified available during Phase 8's own
`verify-phase8` gate (container-exec import + data write-access checks). Re-confirmed by direct
source read this session, not re-executed live:

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `apache-airflow-providers-standard` | `TriggerDagRunOperator` | ✓ | 1.17.0 (pinned, `docker/airflow/Dockerfile`) | — |
| `generator/generate_csv.py` mount + subprocess-invocable | `generate_task` (D-03) | ✓ | mounted `:ro` at `/opt/airflow/generator` (Phase 8) | — |
| `data/` write access (uid 50000:gid 0) | `generate_task`'s `write_staged()`, `retention_task`'s `Path.unlink()` | ✓ | `chmod -R 777` via `airflow-init` (Phase 8) | — |
| Oracle connectivity (`ORACLE_DSN`/`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD`) | `summary_task` | ✓ | already configured, used by `report_ready.py` today | — |

**Missing dependencies with no fallback:** None.
**Missing dependencies with fallback:** None — this phase adds zero new external dependencies.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (already configured, `[tool.pytest.ini_options]` in root `pyproject.toml`) |
| Config file | `pyproject.toml` (`testpaths = ["tests"]`, `pythonpath = ["."]`) |
| Quick run command | `uv run pytest tests/unit/ -x` |
| Full suite command | `uv run pytest tests/unit/ tests/integration/ tests/e2e/ -x` (integration/e2e require `make up` first) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SCHED-01 | DAG parses, `schedule="@hourly"`, `catchup=False` | structural (live `BundleDagBag`) | `make verify-phase9` (Code Examples §6) | ❌ Wave 0 (new Makefile target) |
| SCHED-02 | Seed varies per logical_date, distinct checksums | unit | `pytest tests/unit/dags/test_generate_schedule_helpers.py::test_seed_varies_by_hour -x` | ❌ Wave 0 |
| SCHED-03 | Sequential chain-trigger order (customers before orders before report_ready) | structural + e2e | `make verify-phase9` (task_ids present) + a real live-triggered run (manual, phase acceptance) | ❌ Wave 0 (structural), manual for e2e |
| SCHED-04 | `max_active_runs=1` | structural | `make verify-phase9`'s `dag.max_active_runs == 1` assertion | ❌ Wave 0 |
| SCHED-05 | `fail_when_dag_is_paused=True` on trigger tasks | structural | extend `make verify-phase9`'s DagBag check with `dag.get_task('trigger_customers').fail_when_dag_is_paused is True` | ❌ Wave 0 |
| SCHED-06 | `csv_ingest.py`/`report_ready.py` unmodified | code review | `git diff --stat` shows zero changes to those two files (plan-checker/code-review gate) | N/A (process check, not a test file) |
| SCHED-07 | One-line cascade summary log | unit | `pytest tests/unit/dags/test_generate_schedule_helpers.py::test_summary_format -x` (extract the formatter into a plain function first, per `format_summary_log()`'s own pattern) | ❌ Wave 0 |
| SCHED-08 | `rows`/`invalid_ratio` DAG Params | structural | `make verify-phase9`'s DagBag check (`dag.params["rows"].value == 100`) | ❌ Wave 0 |
| SCHED-10 (retention) | Deletes CSVs older than 30 days, never raises | unit | `pytest tests/unit/dags/test_generate_schedule_helpers.py::test_retention_deletes_old_files -x` and `::test_retention_never_raises` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `uv run pytest tests/unit/dags/ -x`
- **Per wave merge:** `make verify-phase9` (unit suite + live DagBag structural check, requires
  `make up`)
- **Phase gate:** `make verify-phase9` green, plus one real live-triggered/manually-triggered
  end-to-end run watched through the Airflow UI (confirming `deferred` states appear correctly for
  all three trigger tasks and the chain completes) before `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `tests/unit/dags/test_generate_schedule_helpers.py` — covers SCHED-02 (seed derivation as a
  plain, testable function extracted from `generate_task`), SCHED-07 (summary-line formatter, mirror
  `_common/reporting.py::format_summary_log()`'s pattern — extract a pure `format_cascade_summary()`
  helper rather than inlining string-building in the task body), SCHED-10 (retention date-parsing +
  deletion logic as pure functions, independently testable without a live Airflow context or Oracle
  connection).
- [ ] `Makefile`'s `verify-phase9` target — new, mirrors `verify-phase5`'s exact shape (see Code
  Examples §6).
- [ ] `docs/airflow-dag.md` update documenting `csv_generate_schedule` — not a test file, but listed
  here since CONTEXT.md's canonical references call it out as this phase's doc-update obligation.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | This phase adds no new auth surface — Oracle credentials reused verbatim from `csv_processor.load`'s existing env-var-first helpers |
| V3 Session Management | No | Not applicable — no new session/cookie handling |
| V4 Access Control | No | No new access-control surface; `conf=` payloads are fixed, hardcoded dicts in the new DAG's own source (`{"dataset": "customers", ...}`), never derived from untrusted external input |
| V5 Input Validation | Yes | DAG `Param`s (`rows`, `invalid_ratio`) already get Airflow's own JSON-Schema-based validation via `type="integer"/minimum=1` and `type="number"/minimum=0.0/maximum=1.0` (same mechanism `csv_ingest.py`'s existing `Param(..., enum=[...])` already relies on) — no custom validation needed |
| V6 Cryptography | No | No new cryptographic operations — checksum hashing (`sha256_file`) is pre-existing, unchanged, in `csv_processor.load` |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| SQL injection in the summary task's `ingestion_metadata` query | Tampering | Already mitigated by design: `cursor.execute(_LATEST_INGESTION_SQL, dataset=dataset)` uses `oracledb`'s native bind-parameter substitution (`:dataset`), never string interpolation — `dataset` itself is a hardcoded literal (`"customers"`/`"orders"`) from a `for` loop, not external input, so there is no injection surface at all in this phase's new SQL |
| Path traversal in the retention task's filename parsing | Tampering | `retention_task` only ever globs `_DATA_ROOT / dataset / f"{dataset}_*.csv*"` (a fixed, hardcoded pattern under a fixed root) and calls `Path.unlink()` on the glob's own returned `Path` objects — never constructs a path from unsanitized external input, so `resolve_safe_config_path()`-style traversal guarding (used elsewhere in this codebase for genuinely untrusted `conf`-supplied paths) is not needed here |
| Subprocess argument injection in `generate_task`'s `subprocess.run([...])` call | Tampering | All arguments passed to `subprocess.run` are either hardcoded literals (`"--correlated"`, `"--compress"`) or `str()`-cast values already validated by Airflow's own Param JSON-Schema (`rows`/`invalid_ratio`) or derived from `logical_date` (Airflow-controlled, not user-supplied) — the list-form `subprocess.run([...])` (not `shell=True`) already prevents shell-injection by construction, consistent with this codebase's existing convention of never using `shell=True` anywhere |

## Sources

### Primary (HIGH confidence)
- This repository's own committed source, read directly this session: `airflow/dags/csv_ingest.py`,
  `airflow/dags/report_ready.py`, `airflow/dags/_common/paths.py`, `airflow/dags/_common/reporting.py`,
  `generator/generate_csv.py` (full `build_parser`/`main`/`output_path`/`write_staged`/
  `generate_correlated_datasets`), `docker/airflow/Dockerfile`, `docker-compose.yml`,
  `docker/oracle/init/01_ingestion_metadata.sql`, `packages/csv-processor/src/csv_processor/load.py`,
  `Makefile` (`verify-phase5`/`verify-phase6`/`verify-phase8` targets), `pyproject.toml`
  (`[tool.pytest.ini_options]`), `tests/unit/dags/conftest.py`, `tests/unit/test_generate_csv.py`,
  `.planning/REQUIREMENTS.md`, `.planning/STATE.md`, `.planning/config.json`.
- `apache-airflow-providers-standard==1.17.0`'s actual `trigger_dagrun.py` — downloaded via
  `pip download apache-airflow-providers-standard==1.17.0 --no-deps` this session, extracted, read
  in full (constructor signature, `template_fields`, `execute()`, `_trigger_dag_af_3`,
  `_trigger_dag_af_2`).
- `airflow.sdk.exceptions.DagRunTriggerException` and `airflow.sdk.execution_time.task_runner
  ._handle_trigger_dag_run()`/`_defer_task()` — read directly from a sibling checkout
  (`/mnt/c/Users/borow/VSC/projects/airflow-platform/.venv`) pinned to the identical
  `apache-airflow-providers-standard==1.17.0`, task-sdk `1.3.0`.
- `airflow.sdk.types.DagRunProtocol` — read directly from the same sibling checkout, confirming
  `logical_date: AwareDatetime | None`.
- `airflow.sdk.definitions.dag` (`DAG`/`@dag`'s `max_active_runs`/`dagrun_timeout` attrs) and
  `airflow.sdk.definitions.param.Param.__init__` — read directly from the same sibling checkout.

### Secondary (MEDIUM confidence)
- `.planning/research/SUMMARY.md`, `.planning/research/PITFALLS.md`, `.planning/research/ARCHITECTURE.md`,
  `.planning/research/STACK.md` — this milestone's own prior upfront research, treated as the
  starting point per this phase's task framing; one section (`TriggerDagRunOperator` worker-slot
  analysis) is corrected above with HIGH-confidence direct-source-reading, not merely cited.
- Sibling checkout's task-sdk pinned at `1.3.0` vs. this project's own (unverified this session)
  actual resolved version — see Assumptions Log A1.

### Tertiary (LOW confidence)
- None new this session — the open upstream GitHub issues (#57756, #38353, #52247) remain cited from
  prior research (WebSearch-sourced originally) and were not independently re-tested; treated as
  still-open per Assumptions Log A2, not elevated or downgraded by this pass.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — zero new dependencies, both already-installed packages' exact behavior
  verified by reading the actual pinned wheel source.
- Architecture: HIGH — every integration point (subprocess invocation, `conf=` contract,
  `TriggerDagRunOperator` parameter API, `@dag`/`Param` signatures) verified against actual source,
  not inferred.
- Pitfalls: HIGH for the corrected `TriggerDagRunOperator` deferral-mechanism finding (direct source
  read); MEDIUM for the three still-open upstream issues' applicability to this exact pinned
  combination (not independently re-tested this session — carried forward from prior research).

**Research date:** 2026-09-01
**Valid until:** 30 days (stable, pinned-version-based findings; re-verify if
`apache-airflow-providers-standard` or the base Airflow image version changes)
