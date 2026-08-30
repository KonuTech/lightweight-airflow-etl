# Phase 5: Airflow DAG Wiring & Deferrable File-Wait - Research

**Researched:** 2026-08-29
**Domain:** Airflow 3.x TaskFlow DAG authoring, deferrable sensors, REST API triggering
**Confidence:** HIGH (in-repo integration points, all verified by reading actual code/config this
session) / MEDIUM (Airflow 3.x API specifics, verified against Context7 docs and the exact pinned
provider source on GitHub, cross-checked against the project's own pinned versions)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** Exactly **one** DAG definition/dag_id (e.g. `csv_ingest`), fully parameterized by
  runtime `conf` (`dataset` name + `config` path) — not two dataset-specific DAG files/dag_ids.
  `ARCHITECTURE.md`'s two-DAG-file sketch is superseded on this one point only.
- **D-02:** `process_csv` calls `csv_processor.engine.process(file_path, config)` directly — this
  single call already performs the entire Oracle load in one transaction. **No task in this DAG
  may open a second Oracle connection or re-attempt any insert.** `load_results` stays a thin
  pass-through task (receives the `ProcessingResult` XCom, prepares what `report_result` needs) —
  a no-op-shaped implementation is expected and fine.
- **D-03:** `process()` never raises for any of its 7 closed `Status` outcomes. `process_csv` must
  **not** raise `AirflowFailException` (or let any exception propagate) for a known-Status outcome
  — every closed-enum result, success or failure, flows through `load_results` → `report_result`.
  The Airflow task itself only genuinely FAILs (retry/alert) on a truly unexpected, uncaught
  exception — a real bug, not a modeled domain outcome.
- **D-04:** Use stock `airflow.providers.standard.sensors.filesystem.FileSensor(deferrable=True)`
  for `wait_for_file` — no custom `BaseTrigger`. **Researcher confirmation required:** whether/how
  `filepath` can be templated with a value from runtime `conf`, since `dag_run.conf` isn't
  available until DAG-run time and `FileSensor` is class-based, not a `@task`. **Resolved below in
  Pattern 2 — YES, `filepath` is Jinja-templated.**
- **D-05:** File input path is not a `config.json` field — it's `/opt/airflow/data/<dataset>/`
  inside the container (already mounted). The DAG derives this path from the `dataset` runtime-conf
  value, joined with the dataset's own `config.json`'s `file_pattern` for the glob.
- **D-06:** `poke_interval`/`timeout` for the deferred sensor are Claude's discretion.
- **D-07:** `report_result`'s summary is satisfied via Airflow task logging — no
  Slack/email/external notification.
- **D-08:** `load_config` validates both halves of `conf` (`dataset`, `config` path) and loads the
  referenced `config.json` via `csv_processor.config.load_config`, catching `ConfigurationError`
  and surfacing a `CONFIGURATION_ERROR`-shaped early exit. XCom payload is
  `config.model_dump(mode="json")`, never a live Pydantic instance.
- **D-09** (= Phase 3 D-31): `file_pattern` already matches compressed variants
  (`customers_*.csv*`, `orders_*.csv*`) — no special-casing needed in the glob.
- **D-10** (= Phase 3 D-08): `source_file`/`file_name` on `ProcessingResult` is the basename only.
- **D-11** (= Phase 4 D-01): Re-processing an already-recorded file returns the original recorded
  outcome via `process()` itself — no DAG-level idempotency check needed.
- **D-12** (= Phase 4 D-02): One Oracle connection per `process()` call, opened/closed inside the
  function — never held across DAG tasks.

### Claude's Discretion

- Exact `dag_id` string, task function names/file layout inside `airflow/dags/` and
  `airflow/dags/_common/` (a `dag_factory.py`-style single-DAG builder, or an inline `@dag`
  function — both fine now that D-01 settles on one DAG).
- Whether `load_results` ends up as a genuinely separate function/task or is folded tightly into
  `process_csv`'s immediate next step — constrained only by D-02 and DAG-01's literal task-name
  list (the UI/API must still show a `load_results` step).
- Exact Jinja-templated `filepath` expression for `FileSensor` and how `dag_run.conf` reaches it.
- Whether `report_result` formats its summary as a plain log line, structured JSON log, or both.

### Deferred Ideas (OUT OF SCOPE)

None — discussion stayed within phase scope (single `--auto` pass, no new scope surfaced).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DAG-01 | TaskFlow DAG (`load_config` → `wait_for_file` → `process_csv` → `load_results` → `report_result`) delegates to `csv_processor`, not implementing CSV logic itself | Pattern 1 (single param-driven `@dag`), Code Examples (full DAG skeleton); `process()`'s exact signature/behavior verified in engine.py |
| DAG-02 | Triggerable via a single HTTP request (Airflow REST API), passing dataset name + config path as runtime `conf` | Pattern 5 (REST API trigger shape, verified against `verify_environment.py`'s existing `/auth/token` pattern and Context7 docs); Pitfall 6 (`conf` vs `params` distinction) |
| DAG-03 | File-wait via a deferrable operator/trigger, non-blocking, releases the worker slot | Pattern 2 (`FileSensor(deferrable=True)`, template_fields verified against pinned provider source) |
| DAG-04 | Concise human-readable summary after processing (dataset, file, row counts, duration, status) | `ProcessingResult`'s exact field list verified in models.py; Pattern 1's `report_result` example |
| DAG-05 | Identical DAG definition works for both datasets purely by config, no dataset-specific branches | Pattern 1 (single `@dag`, `dataset`/`config` come from `params`/`conf`, never hard-coded); Don't Hand-Roll (no per-dataset `if` branches) |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- `csv_processor` has zero Airflow imports anywhere — `airflow/dags/` is the only place Airflow
  imports may appear in this codebase (already an established, verified pattern from Phases 2-4).
- Two-tier reuse discipline (vendor-then-strip vs. read-the-algorithm) applied to the reference
  repo — not directly relevant to this phase's DAG code (the reference repo's `airflow/dags/`
  is explicitly Tier B: "read only for the sequence... write a smaller orchestrator," skip
  `kpo.py`/`tracing_kpo.py`, no Kubernetes here).
- Oracle driver is `python-oracledb` thin mode, pinned exact version — already installed
  (`oracledb==4.0.2`), no new pin needed this phase.
- LocalExecutor only — no Celery/Kubernetes; this phase's single-DAG, in-process `process_csv`
  call is consistent with that.

## Summary

This phase wires exactly one Airflow 3.3.1 TaskFlow DAG around Phase 4's already-complete
`csv_processor.engine.process()` entrypoint. Every genuinely new Airflow-specific question
`05-CONTEXT.md` flagged for this research pass has a concrete, source-verified answer:

1. **`FileSensor.filepath` IS Jinja-templated** — `template_fields: Sequence[str] = ("filepath",)`
   on the pinned `apache-airflow-providers-standard==1.17.0` source — so
   `filepath="/opt/airflow/data/{{ dag_run.conf['dataset'] }}/{{ params.file_pattern }}"` (or
   equivalent) resolves at task-render time from runtime `conf`, confirming D-04's flagged
   uncertainty with a definite yes.
2. **`FileSensor` never surfaces the matched file's path via XCom** — its `poke()` returns a plain
   `bool` (`True`/`False`), never the resolved path. `process_csv` cannot rely on
   `wait_for_file.output`; it must independently re-resolve the matching file (a `pathlib.Path.glob`
   call using the same pattern) once the sensor confirms existence. This is the single most
   important non-obvious finding in this research pass — the planner must account for it in
   `process_csv`'s design, not assume `wait_for_file`'s XCom carries a usable path.
3. **A real, verified infrastructure gap exists**: `docker-compose.yml`'s `airflow-common-env`
   never sets `ORACLE_DSN` as a container environment variable. `csv_processor.load.oracle_dsn()`
   falls back to `"localhost:1521/FREEPDB1"` when unset — inside the Airflow containers, `localhost`
   does **not** resolve to the `oracle` service (a separate container on the compose network).
   Every `process_csv` task will hit `DATABASE_ERROR` until `docker-compose.yml` adds
   `ORACLE_DSN: "oracle:1521/FREEPDB1"` (and forwards `ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD`
   as real container env vars, not just compose-file interpolation variables) to
   `airflow-common-env`. This must be an explicit task in the plan, not an assumption.
4. **`csv_processor.config.load_config` requires a keyword-only `defaults_path` argument**
   (`load_config(path: Path, *, defaults_path: Path) -> DatasetConfig`) — `ARCHITECTURE.md`'s
   Pattern 1 sketch (`load_config(Path(config_path))`) is a one-argument oversimplification; the
   real signature needs `configs/defaults.json` passed explicitly. `load_config` is the actual
   integration point, more authoritative than the architecture sketch.
5. **Airflow 3's REST API trigger endpoint is `POST /api/v2/dags/{dag_id}/dagRuns`**, auth via
   `POST /auth/token` (username/password → JWT `access_token`, used as `Authorization: Bearer`) —
   this exact flow is already proven working in this repo's own `scripts/verify_environment.py`.
6. **Import paths for Airflow 3.3.1**: `@dag`/`@task`/`DAG`/`Param`/`get_current_context` live in
   `airflow.sdk` (the new stable interface — `airflow.models`/`airflow.decorators` are the
   deprecated 2.x paths). `FileSensor` stays in
   `airflow.providers.standard.sensors.filesystem` (provider packages are not re-exported through
   `airflow.sdk`).

**Primary recommendation:** Build one `@dag`-decorated DAG in `airflow/dags/csv_ingest.py` with
`params={"dataset": Param(...), "config_path": Param(...)}` as manually-triggerable defaults,
`wait_for_file = FileSensor(filepath="{{ ... }}", deferrable=True)` as the one class-based task,
and four `@task` functions around it. Keep all non-Airflow-specific logic (path-building, XCom
dict shaping) as plain, unit-testable functions in `airflow/dags/_common/`, called from inside the
thin task bodies — mirrors `ARCHITECTURE.md` Anti-Pattern 1's discipline. Fix the `ORACLE_DSN` gap
in `docker-compose.yml` as part of this phase's own scope (it blocks DAG-01/DAG-04's "a completed
run" success criterion regardless of whose job it technically was).

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| HTTP trigger / auth (DAG-02) | API / Backend (Airflow's own REST API + `SimpleAuthManager`) | — | Airflow's REST API server *is* the backend tier here; no custom API wrapper (explicitly out of scope per REQUIREMENTS.md) |
| Runtime `conf` validation (`dataset`, `config` path) | API / Backend (`load_config` task) | — | First task in the DAG, before any file I/O; the natural "reject bad input early" boundary |
| Non-blocking file-wait (DAG-03) | API / Backend (Airflow triggerer process) | — | `FileSensor(deferrable=True)` hands off to the triggerer's async event loop, not the worker — this is Airflow's own scheduler/triggerer tier, not application code |
| CSV parse/validate/normalize/load (DAG-01's actual work) | API / Backend (`csv_processor.engine.process()`, in-process) | Database/Storage (Oracle inserts happen inside this same call) | Already built in Phase 3/4; this phase only calls it |
| Persistence (`<DATASET>_VALID`/`INVALID`/`ingestion_metadata`) | Database / Storage (Oracle) | — | Owned entirely by `csv_processor.load`, one connection per `process()` call |
| Result reporting (DAG-04) | API / Backend (Airflow task logs) | — | No external notification tier — logs only, per D-07 |

## Standard Stack

### Core

No new packages are introduced by this phase. Every library this phase's DAG code needs is
already pinned and installed via `docker/airflow/Dockerfile` (approved in Phase 1's package
legitimacy checkpoint — see `.planning/STATE.md` "Phase 01: 01-01/01-03" decisions).

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `apache-airflow` | `3.3.1` (base image tag) [VERIFIED: docker/airflow/Dockerfile:1] | Orchestration runtime, `@dag`/`@task` TaskFlow API | Already the project's chosen executor/version — DAG-01..05's entire surface |
| `apache-airflow-providers-standard` | `1.17.0` [VERIFIED: docker/airflow/Dockerfile:6] | `FileSensor` (`airflow.providers.standard.sensors.filesystem.FileSensor`) | Ships `deferrable=True` support out of the box for the local filesystem — D-04's whole basis |
| `apache-airflow-providers-oracle` | `4.6.2` [VERIFIED: docker/airflow/Dockerfile:7] | Registers `oracle_default` Connection for UI visibility only | `csv_processor` never touches this Connection object (stays Airflow-agnostic, ENGINE-09) — DAG code doesn't need to import this provider directly |
| `csv_processor` (local package) | workspace-local, `pip install --no-deps` [VERIFIED: docker/airflow/Dockerfile:13-15] | `engine.process()`, `config.load_config()` — the two functions this DAG calls | Already built and tested (Phases 2-4) |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| stdlib `pathlib.Path` | — | Building `/opt/airflow/data/<dataset>/` paths, globbing for the matched file after `wait_for_file` succeeds (Pitfall 1) | Every place a filesystem path needs joining/globbing in `_common/` helpers |
| stdlib `json`/plain `dict` | — | XCom payload shaping (`config.model_dump(mode="json")`, `result.model_dump(mode="json")`) | Every task boundary that crosses XCom (Pattern 1 in ARCHITECTURE.md, reused here) |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `FileSensor(deferrable=True)` | Hand-rolled `BaseTrigger` (async polling) | Already rejected by D-04 — no requirement `FileSensor` can't express; a custom trigger only earns its complexity if content-inspection (not just existence) is ever needed |
| `params`-declared defaults | Reading `dag_run.conf` directly with no `params` declaration | `params` gives the Airflow UI a trigger form with named fields and JSON-Schema validation on manual trigger; a bare `dag_run.conf` dict has none of that — cheap to add, worth it for DAG-02's "single HTTP request" ergonomics |

**Installation:** None — no `pip install`/`uv add` changes needed this phase. If the `ORACLE_DSN`
gap fix (below) is the only `docker-compose.yml` change, no image rebuild is required either
(environment variables, not installed packages).

**Version verification:** `apache-airflow-providers-standard==1.17.0`'s `FileSensor` source was
fetched directly from its GitHub tag (`providers-standard/1.17.0`) this session — see Pattern 2.
`apache-airflow==3.3.1`'s TaskFlow/`airflow.sdk` behavior was cross-checked against Context7's
`/apache/airflow` docs at the closest available indexed version (`3.1.6`); the two Airflow minor
versions share the same `airflow.sdk` import surface (no breaking TaskFlow/`params`/`FileSensor`
changes between 3.1 and 3.3 per the Airflow 3.x release notes reviewed).

## Package Legitimacy Audit

**Not applicable this phase.** No new external packages are introduced — every dependency this
phase's code imports (`apache-airflow`, `apache-airflow-providers-standard`, `oracledb`,
`csv_processor`) was already installed and legitimacy-checkpointed in Phase 1
(`.planning/STATE.md`: *"01-01: Package legitimacy checkpoint approved — oracledb==4.0.2,
pydantic==2.13.4, apache-airflow-providers-standard==1.18.0 installed at pinned versions"*, later
corrected to `1.17.0` in `01-03`). The planner does not need a `checkpoint:human-verify` task for
any package this phase touches.

## Architecture Patterns

### System Architecture Diagram

```
HTTP POST /auth/token {username, password}
    │
    ▼
Airflow REST API  ──► JWT access_token
    │
    ▼
HTTP POST /api/v2/dags/{dag_id}/dagRuns
  Authorization: Bearer <token>
  body: { "conf": { "dataset": "customers", "config_path": "configs/datasets/customers.json" } }
    │
    ▼
┌─────────────────────────────── csv_ingest DAG (single dag_id, DAG-01/DAG-05) ──────────────────┐
│                                                                                                   │
│  load_config (@task)                                                                             │
│    reads params["dataset"]/params["config_path"] (conf-overridden) via get_current_context()     │
│    calls csv_processor.config.load_config(path, defaults_path=configs/defaults.json)              │
│    → CONFIGURATION_ERROR early-exit shape on failure (D-03/D-08)                                  │
│    → XCom: config.model_dump(mode="json")                                                         │
│         │                                                                                          │
│         ▼                                                                                          │
│  wait_for_file (FileSensor, class-based, deferrable=True — NOT a @task, DAG-03)                    │
│    filepath = Jinja-templated from dag_run.conf['dataset'] + config's file_pattern (Pattern 2)     │
│    poke() returns bool only — NO file path in its XCom (Pitfall 1)                                 │
│    while waiting: releases worker slot, resumes via the triggerer process                          │
│         │                                                                                          │
│         ▼                                                                                          │
│  process_csv (@task)                                                                              │
│    rehydrates DatasetConfig from XCom dict                                                         │
│    RE-RESOLVES the matched file path itself (glob against the same pattern, Pitfall 1)             │
│    calls csv_processor.engine.process(file_path, config)  ── the ONE integration point            │
│      into detect→parse→validate→normalize→chunk→load(Oracle), already atomic (D-02/D-12)          │
│    never raises for a known Status (D-03) — always returns ProcessingResult                        │
│    → XCom: result.model_dump(mode="json")                                                          │
│         │                                                                                          │
│         ▼                                                                                          │
│  load_results (@task) — thin pass-through (D-02: no second Oracle write)                           │
│         │                                                                                          │
│         ▼                                                                                          │
│  report_result (@task) — logs dataset/file/row counts/duration/status (DAG-04, D-07: logs only)    │
│                                                                                                     │
└────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

```
airflow/
└── dags/
    ├── csv_ingest.py           # DAG-01/DAG-05: the ONE @dag definition (D-01)
    └── _common/
        ├── __init__.py
        ├── paths.py             # build_data_dir(dataset), resolve_matched_file(dir, pattern) — plain, unit-testable, no Airflow import
        └── xcom.py               # (optional) thin wrappers around model_dump/model_validate if reused across tasks
```

Deliberately smaller than `ARCHITECTURE.md`'s original two-DAG-file sketch (`dag_factory.py`
building two DAGs, `customers_ingest.py`/`orders_ingest.py`) — D-01 supersedes that; there is now
exactly one DAG file, so a "factory shared by two DAG files" has no reason to exist. `_common/`
survives as the home for pure-Python helpers the single DAG file itself calls — testable without
importing Airflow at all (see Validation Architecture).

### Pattern 1: Single param-driven `@dag`, `params` for the trigger UI, `dag_run.conf` for actual runtime values

**What:** Declare `params={"dataset": Param(...), "config_path": Param(...)}` on the `@dag`
decorator so Airflow's UI/API trigger form has named, JSON-Schema-validated fields with sensible
defaults — but the *actual* values used by tasks at run time always come from the merged
`params`/`dag_run.conf` context (accessed via `get_current_context()["params"]`, not the `dag`
object itself — the `dag` object at parse time only ever holds the *defaults*, per Airflow's own
documented warning below).

**When to use:** Any DAG meant to be triggered with different values per run over the REST API
(DAG-02) while still working with zero `conf` (i.e., falling back to declared defaults) — exactly
this project's `dataset`/`config_path` case.

**Verified constraint (do not violate):** *"Dag-level parameters are the default values passed on
to tasks... users might try to access the manually-provided parameter values using the `dag`
object, but this will only ever contain the default values. To ensure that the manually-provided
values are accessed, use a template variable such as `params` or `ti` within your task."*
[CITED: Context7 /apache/airflow/3.1.6, `airflow-core/docs/core-concepts/params.rst`] — i.e., never
read `dag.params["dataset"]` inside a task body expecting the run's actual conf-supplied value;
always go through `get_current_context()["params"]` (inside a `@task` function body) or Jinja
`{{ params.dataset }}` (inside a template field).

**Example (import paths verified — see Metadata):**
```python
# Source: Context7 /apache/airflow/3.1.6, task-sdk/docs/index.rst "Migrate DAG and Task Imports
# from Airflow 2.x to 3.x" — airflow.sdk is Airflow 3's recommended stable import surface,
# replacing airflow.models.DAG / airflow.decorators.task.
from airflow.sdk import dag, task, Param, get_current_context

@dag(
    dag_id="csv_ingest",
    schedule=None,
    params={
        "dataset": Param("customers", type="string", enum=["customers", "orders"]),
        "config_path": Param("configs/datasets/customers.json", type="string"),
    },
    catchup=False,
)
def csv_ingest():
    @task
    def load_config() -> dict:
        ctx = get_current_context()
        dataset = ctx["params"]["dataset"]
        config_path = ctx["params"]["config_path"]
        ...  # csv_processor.config.load_config(Path(config_path), defaults_path=...)

    ...

csv_ingest()
```

### Pattern 2: `FileSensor.filepath` IS Jinja-templated — resolves D-04's flagged open question

**What:** `apache-airflow-providers-standard==1.17.0`'s `FileSensor` declares
`template_fields: Sequence[str] = ("filepath",)` — quoted directly from the pinned-version source
fetched this session [CITED: raw.githubusercontent.com/apache/airflow, tag
`providers-standard/1.17.0`,
`providers/standard/src/airflow/providers/standard/sensors/filesystem.py`]. Airflow's templating
engine renders any field named in `template_fields` against the task's Jinja context (which
includes `dag_run.conf` and `params`) **before** `poke()`/deferral ever runs — so a value that only
exists at DAG-run time can flow into a class-based operator's constructor argument, exactly the
capability D-04 needed confirmed.

**Constraint (also verified):** *"the parameters from `dag_run.conf` can only be used in a template
field of an operator, limiting their direct use to templated fields"* [CITED: Context7
/apache/airflow/3.1.6, `airflow-core/docs/core-concepts/dag-run.rst`]. This means `filepath` MUST
be written as a Jinja string, never as an f-string interpolating a Python-side variable captured
at DAG-parse time (there is no such variable — `dag_run.conf` doesn't exist yet at parse time).

**Example:**
```python
# Source: apache-airflow-providers-standard 1.17.0 pinned source (filesystem.py) +
# Context7 /apache/airflow/3.1.6 core-concepts/dag-run.rst templating example
from airflow.providers.standard.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_default",
    filepath="{{ params.dataset }}/{{ ti.xcom_pull(task_ids='load_config')['file_pattern'] }}",
    deferrable=True,
    poke_interval=10,
    # fs_default's FSHook base path should be set to /opt/airflow/data (Airflow Connection,
    # not this project's own config) so `filepath` only needs to add "<dataset>/<pattern>".
)
```

**Trade-off / open implementation detail (Claude's discretion, flagged for the planner):** the
glob pattern itself (`customers_*.csv*`) lives inside `config.json`, not in `params`/`conf`
directly — so `filepath`'s Jinja expression either (a) pulls it from `load_config`'s XCom via
`ti.xcom_pull(...)` inside the template (verified idiom, XCom values ARE available to Jinja
templates via `ti.xcom_pull`), or (b) the DAG re-derives the pattern from a small `dataset →
file_pattern` lookup embedded in the DAG file itself (duplicates `config.json`'s own field — less
correct), or (c) `params` declares `file_pattern` as its own trigger-time field alongside
`dataset`/`config_path` (simplest, but duplicates data the config file already owns). Recommend
(a): keep `file_pattern` sourced from the one place it's already validated (`config.json` via
`load_config`), accepting the minor coupling of `wait_for_file` depending on `load_config`'s XCom
output.

### Pattern 3: `process_csv` must independently re-resolve the matched file — `FileSensor` never hands it one

**What:** `FileSensor.poke()`'s only return values are `True`/`False`
[CITED: apache-airflow-providers-standard 1.17.0 pinned source, `filesystem.py` `poke()` method,
fetched and quoted verbatim this session — see Pitfall 1 below]. There is no `PokeReturnValue`
carrying the matched path, and Airflow's default XCom push for a sensor stores the return value of
`poke()`/`execute()` — a bare boolean, not a path. `process_csv` (or a small step just before it)
must call `pathlib.Path(base_dir).glob(file_pattern)` itself and take the (only expected) match —
the exact same pattern the sensor already confirmed exists.

**When to use:** Always, for this DAG. Do not design `process_csv` to expect a file path arriving
via `wait_for_file.output`/XCom pull — it will not be there.

**Trade-off:** A small amount of duplicated globbing logic (sensor confirms existence, next task
re-globs to get the actual name) — acceptable and in fact necessary, since `process()` needs one
concrete `Path`, and `FileSensor` was never designed to hand that back.

### Pattern 4: Fix `ORACLE_DSN` before this phase can pass its own success criteria

**What:** `csv_processor.load.oracle_dsn()` reads
`os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")`
[VERIFIED: packages/csv-processor/src/csv_processor/load.py:61] — quoted verbatim. `oracle_user()`
reads `os.environ.get("ORACLE_APP_USER", "admin")` [VERIFIED: load.py:69] and `oracle_password()`
reads `os.environ.get("ORACLE_APP_USER_PASSWORD", "admin")` [VERIFIED: load.py:75].
`docker-compose.yml`'s `airflow-common-env` block (lines 5-20) sets `AIRFLOW_CONN_ORACLE_DEFAULT`
(for Airflow's own Connection UI, D-11 from `04-CONTEXT.md`) but **never sets `ORACLE_DSN`,
`ORACLE_APP_USER`, or `ORACLE_APP_USER_PASSWORD` as actual container environment variables**
[VERIFIED: docker-compose.yml:1-33, full grep of the file for these three names found zero
matches outside the `oracle:` service's own block (lines 49-54, a different service) and the
`AIRFLOW_CONN_ORACLE_DEFAULT` interpolation]. Compose's `${VAR}` syntax inside a YAML value string
interpolates at `docker compose` parse time from the *host's* environment/`.env` file — it does
**not** also export `VAR` itself as a container environment variable unless `VAR` is separately
listed under `environment:`.

**Consequence if unfixed:** every `process_csv` task, running inside an Airflow worker container,
calls `load.get_connection()` → `oracledb.connect(dsn="localhost:1521/FREEPDB1", ...)` —
`localhost` inside the `airflow-scheduler`/`airflow-apiserver` container does not route to the
`oracle` compose service. Connection refused → `oracledb.Error` → `process()` returns
`Status.DATABASE_ERROR` for every single run, and DAG-04's "a completed run's logs show a
SUCCESS/etc. status" success criterion is never met with real data.

**Fix (add to this phase's plan, not assumed pre-existing):**
```yaml
# docker-compose.yml, inside x-airflow-common's environment block
ORACLE_DSN: "oracle:1521/FREEPDB1"
ORACLE_APP_USER: "${ORACLE_APP_USER:-admin}"
ORACLE_APP_USER_PASSWORD: "${ORACLE_APP_USER_PASSWORD:-admin}"
```
No image rebuild needed (env var change only, not an installed-package change) — `docker compose
up -d` (or a targeted `docker compose up -d --force-recreate airflow-scheduler
airflow-apiserver`) picks it up.

### Pattern 5: REST API trigger — proven auth flow already exists in this repo

**What:** This project's own `scripts/verify_environment.py` already implements and proves the
exact two-step auth flow DAG-02 needs: `POST http://localhost:8080/auth/token` with
`{"username": "admin", "password": "admin"}` returns a JSON body containing `access_token`
[VERIFIED: scripts/verify_environment.py:37-39,134-146 — `AIRFLOW_AUTH_TOKEN_URL =
"http://localhost:8080/auth/token"`, `AIRFLOW_USER = "admin"`, `AIRFLOW_PASSWORD = "admin"`,
`payload = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD})`]. This matches
Context7's documented flow exactly: *"Generate JWT Token with Credentials using cURL... POST
`/auth/token` with username/password... returns `access_token`"* [CITED: Context7
/apache/airflow/3.1.6, `airflow-core/docs/core-concepts/auth-manager/simple/token.rst` and
`airflow-core/docs/security/api.rst`]. The trigger call itself is
`POST /api/v2/dags/{dag_id}/dagRuns` with `Authorization: Bearer <access_token>` and a JSON body
whose `conf` field carries `{"dataset": ..., "config_path": ...}` [CITED: Context7
/apache/airflow/3.1.6, `clients/python/README.md` — request body accepts `conf` (object,
optional), `run_id` (optional), and in Airflow 3, `logical_date` defaults to `None` if omitted
rather than being auto-generated, per the `3.1.6` `RELEASE_NOTES.rst` entry reviewed].

**Example (end-to-end trigger, for docs/manual testing, not DAG code itself):**
```bash
# Source: this repo's own scripts/verify_environment.py (auth flow) +
# Context7 /apache/airflow/3.1.6 docker-compose/index.rst curl pattern
ENDPOINT_URL="http://localhost:8080"
JWT_TOKEN=$(curl -s -X POST "${ENDPOINT_URL}/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' | jq -r '.access_token')

curl -X POST "${ENDPOINT_URL}/api/v2/dags/csv_ingest/dagRuns" \
  -H "Authorization: Bearer ${JWT_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"conf": {"dataset": "customers", "config_path": "configs/datasets/customers.json"}}'
```

**Trade-off:** None significant — this is exactly the surface `SimpleAuthManager` (already
configured in Phase 1, `AIRFLOW__CORE__AUTH_MANAGER` in `docker-compose.yml:7`) is designed for.
No custom FastAPI wrapper needed (explicitly out of scope per REQUIREMENTS.md's Out of Scope
table).

### Anti-Patterns to Avoid

- **Reading `dag_run.conf`/`params` values via the `dag` object inside a task body.** The `dag`
  object only ever carries parse-time defaults (verified constraint in Pattern 1) — always go
  through `get_current_context()["params"]` or Jinja templating.
- **Assuming `wait_for_file`'s XCom carries the matched file path.** It doesn't (Pattern 3) —
  `process_csv` must re-resolve it.
- **Passing a live `DatasetConfig`/`ProcessingResult` Pydantic instance through XCom.** Both models
  are `frozen=True, extra="forbid"` specifically so `.model_dump(mode="json")` round-trips safely
  — never skip the dict conversion (carried forward from `ARCHITECTURE.md` Pattern 1/3, unchanged
  by this phase).
- **Building two DAG files "to keep them simple."** Explicitly superseded by D-01 — one `dag_id`,
  parameterized.
- **Letting a modeled `Status` outcome raise `AirflowFailException`.** D-03 is explicit: only a
  genuinely unexpected exception should fail the Airflow task.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Non-blocking file-wait | A custom `asyncio` polling loop or `BaseTrigger` subclass | `FileSensor(deferrable=True)` | Already ships glob support + deferral; D-04 confirmed no gap exists for this project's plain-glob patterns |
| REST trigger auth | A custom FastAPI/Flask wrapper issuing its own tokens | Airflow's own `/auth/token` + `SimpleAuthManager` | Explicitly out of scope per REQUIREMENTS.md; this repo already proves the flow works (`verify_environment.py`) |
| Config/result serialization for XCom | Custom `json.dumps`/manual dict-shaping per field | `model.model_dump(mode="json")` / `Model.model_validate(dict)` | Both models are purpose-built (`frozen`, `extra="forbid"`) for exactly this round-trip; already the established convention from Phases 2 and 4 |
| Idempotency / re-processing detection | A DAG-level "has this file been seen" check before calling `process_csv` | Nothing — `process()` already handles it internally (D-01/D-11) | Duplicating this logic in the DAG risks disagreeing with `process()`'s own `ingestion_metadata` check |

**Key insight:** Every "don't hand-roll" item in this phase reduces to *"Phase 4 (or Airflow
itself) already solved this — the DAG's only job is thin wiring."* The one place this phase adds
genuinely new logic is the small `_common/paths.py` glob-resolution helper (Pattern 3), which has
no existing solution to reuse because `FileSensor` was deliberately not designed to provide it.

## Common Pitfalls

### Pitfall 1: Expecting `FileSensor` to hand `process_csv` the matched file path
**What goes wrong:** A design that does `file_path = wait_for_file.output` (mirroring the
`.output` pattern shown in Airflow's own dynamic-task-mapping docs for other operators) silently
receives `True`, not a path — `process(True, config)` then fails in a confusing way (`Path` coercion
error, not a clean `FILE_NOT_FOUND`).
**Why it happens:** `FileSensor.poke()`'s source
(`apache-airflow-providers-standard==1.17.0`, quoted verbatim below) shows every return path is a
bare bool:
```python
def poke(self, context: Context) -> bool:
    self.log.info("Poking for file %s", self.path)
    for path in glob(self.path, recursive=self.recursive):
        if os.path.isfile(path):
            ...
            return True
        for _, _, files in os.walk(path):
            if files:
                return True
    return False
```
[CITED: raw.githubusercontent.com/apache/airflow, tag `providers-standard/1.17.0`,
`providers/standard/src/airflow/providers/standard/sensors/filesystem.py`]
**How to avoid:** `process_csv` (or a tiny helper called at its start) globs
`base_dir/file_pattern` itself and takes the match — same pattern `FileSensor` already used to
confirm existence, just re-run once more to get the actual filename.
**Warning signs:** A `TypeError`/`AttributeError` inside `process()` on the `file_path` argument,
or a `process_csv` implementation that tries to `ti.xcom_pull(task_ids="wait_for_file")` and gets
`True`/`False` instead of a string.

### Pitfall 2: `ORACLE_DSN` never reaching the Airflow containers
**What goes wrong:** Every DAG run's `process_csv` task returns `Status.DATABASE_ERROR` even
though Oracle itself is healthy and reachable from the host.
**Why it happens:** `docker-compose.yml`'s `${ORACLE_APP_USER:-admin}`-style interpolation only
resolves the compose *file's own* string values at parse time — it does not also forward
`ORACLE_APP_USER` as a container-visible env var, and `ORACLE_DSN` is never referenced in
`docker-compose.yml` at all (verified by grep — zero matches). `load.oracle_dsn()`'s default,
`"localhost:1521/FREEPDB1"`, is correct for a host-side script (`verify_environment.py`, which
does connect to `localhost:1521`) but wrong inside any Airflow container.
**How to avoid:** Add the three env vars explicitly to `airflow-common-env` in `docker-compose.yml`
(Pattern 4) as part of this phase's own plan — do not assume it already works because
`verify_environment.py`'s Oracle check passes (that check runs on the host, not inside a
container).
**Warning signs:** `make verify`/`scripts/verify_environment.py` passing (host-side, uses
`localhost`) while every triggered DAG run still shows `DATABASE_ERROR`.

### Pitfall 3: Treating `dag_run.conf` as available inside a `@task` function's default arguments
**What goes wrong:** Writing `def load_config(dataset: str = "{{ dag_run.conf['dataset'] }}")` —
the Jinja string is never rendered because `@task` function parameters are only templated if
explicitly passed a templated string as an argument at call time (`load_config(dataset="{{
params.dataset }}")` inside the DAG body, or reading `get_current_context()` inside the function
body) — a bare Python default value is never Jinja-rendered.
**Why it happens:** TaskFlow functions are still ordinary Python functions; templating happens at
the Airflow-argument-binding layer (`template_fields`/explicit templated call arguments), not via
magic on function *default* values.
**How to avoid:** Either call `get_current_context()["params"]` inside the `@task` function body
(Pattern 1's example), or explicitly pass `"{{ params.dataset }}"` as a call-site argument when
invoking the decorated function inside the `@dag` body (both are documented, verified patterns).
**Warning signs:** `dataset`/`config_path` showing up as the literal string `"{{ dag_run.conf...
}}"` in logs instead of the actual conf value.

### Pitfall 4: Calling `load_config` with the old one-argument signature `ARCHITECTURE.md` sketches
**What goes wrong:** `load_config(Path(config_path))` raises `TypeError: load_config() missing 1
required keyword-only argument: 'defaults_path'`.
**Why it happens:** `ARCHITECTURE.md` Pattern 1's example predates the real implementation;
`csv_processor.config.loader.load_config`'s actual signature is
`load_config(path: Path, *, defaults_path: Path) -> DatasetConfig`
[VERIFIED: packages/csv-processor/src/csv_processor/config/loader.py:39] — quoted verbatim, and
`configs/defaults.json` genuinely exists and is merged in (shallow merge, dataset keys win)
[VERIFIED: configs/defaults.json exists with a `csv:` block, confirmed by direct read this
session].
**How to avoid:** `load_config` task calls
`load_config(Path(config_path), defaults_path=Path("configs/defaults.json"))` — the DAG needs to
know this second path too (a small constant in `_common/`, not something the runtime `conf` needs
to supply, since it's the same for both datasets).
**Warning signs:** A `TypeError` at DAG-run time (not DAG-parse time, since the call only happens
inside the `@task` function body) on the very first task.

### Pitfall 5: A domain `Status` failure incorrectly failing the Airflow task
**What goes wrong:** Wrapping `process()`'s call in a `try/except` that re-raises on
`Status.DATABASE_ERROR`/`Status.INVALID_FILE` etc. — this makes Airflow mark the task FAILED,
trigger retries/alerting for what is actually a clean, already-fully-handled domain outcome, and
never reaches `load_results`/`report_result` for that run (breaking DAG-04's "a completed run's
logs show status" guarantee for anything but the two success statuses).
**Why it happens:** Muscle memory from operators where "the function raised" is the normal failure
signal — but `process()`'s docstring is explicit that it *never* raises
[VERIFIED: packages/csv-processor/src/csv_processor/engine.py:191-195 — *"Returns: A
`ProcessingResult` with exactly one of the 7 closed `Status` values -- never raises; every
exception this function's own sequence can produce is caught and translated into a status
instead."*].
**How to avoid:** `process_csv` simply returns `result.model_dump(mode="json")` for every possible
`Status` — no status-based branching into an exception. D-03 is explicit and locked.
**Warning signs:** DAG runs for a deliberately-malformed fixture file show as FAILED (red) in the
Airflow UI instead of SUCCESS (green) with a `CONFIGURATION_ERROR`/`INVALID_FILE` status visible
in `report_result`'s log line.

### Pitfall 6: Unvalidated `dataset`/`config_path` conf values (security-relevant, see Security Domain)
**What goes wrong:** A caller of the REST trigger endpoint supplies `config_path:
"/etc/passwd"` or `dataset: "../../etc"` — nothing in `load_config`/`load.py` currently restricts
the path to `configs/datasets/`, and nothing restricts `dataset` to the two known values
(`customers`, `orders`) before it's joined into the `/opt/airflow/data/<dataset>/` glob path.
**Why it happens:** `load_config`'s own signature accepts any `Path` — it has no allow-list logic,
by design (it's a generic loader, not a request-input validator); that responsibility sits
one layer up, in this phase's own `load_config` **task**, which is new code this phase writes.
**How to avoid:** `load_config`'s task body should validate `dataset` against `params`'s declared
`enum=["customers", "orders"]` (Airflow's own Param JSON-Schema validation already rejects an
unlisted value at trigger time if `params` is declared with `enum`, per Pattern 1) and resolve
`config_path` to confirm it stays under `configs/datasets/` (e.g.
`Path(config_path).resolve().is_relative_to(Path("configs/datasets").resolve())`) before ever
calling `load_config`.
**Warning signs:** A trigger request with a `config_path` outside `configs/` succeeding instead of
being rejected as `CONFIGURATION_ERROR`.

## Code Examples

### Full DAG skeleton (illustrative — exact task bodies/helper names are Claude's discretion)
```python
# Source: synthesized from Context7 /apache/airflow/3.1.6 (task-sdk/docs/index.rst,
# airflow-core/docs/core-concepts/{params,dag-run,taskflow}.rst) + this project's own
# in-repo verified integration points (engine.py, config/loader.py, models.py)
from __future__ import annotations

from pathlib import Path

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Param, dag, get_current_context, task

from csv_processor.config.loader import load_config
from csv_processor.config.models import DatasetConfig
from csv_processor.engine import process
from csv_processor.models import ProcessingResult

_DEFAULTS_PATH = Path("configs/defaults.json")
_DATA_ROOT = Path("/opt/airflow/data")


@dag(
    dag_id="csv_ingest",
    schedule=None,
    catchup=False,
    params={
        "dataset": Param("customers", type="string", enum=["customers", "orders"]),
        "config_path": Param("configs/datasets/customers.json", type="string"),
    },
)
def csv_ingest():
    @task
    def load_config_task() -> dict:
        ctx = get_current_context()
        config_path = Path(ctx["params"]["config_path"])
        # Pitfall 6: confirm config_path stays under configs/datasets/ before loading.
        config = load_config(config_path, defaults_path=_DEFAULTS_PATH)
        return config.model_dump(mode="json")

    config_dict = load_config_task()

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        fs_conn_id="fs_default",
        filepath=(
            "{{ params.dataset }}/"
            "{{ ti.xcom_pull(task_ids='load_config_task')['file_pattern'] }}"
        ),
        deferrable=True,
        poke_interval=10,
    )

    @task
    def process_csv_task(config_dict: dict) -> dict:
        ctx = get_current_context()
        dataset = ctx["params"]["dataset"]
        config = DatasetConfig.model_validate(config_dict)
        # Pitfall 1: re-resolve the matched file -- wait_for_file's XCom is a bare bool.
        candidates = sorted((_DATA_ROOT / dataset).glob(config.file_pattern))
        file_path = candidates[0]  # exactly one expected file per triggered run
        result: ProcessingResult = process(file_path, config)
        return result.model_dump(mode="json")

    result_dict = process_csv_task(config_dict)
    wait_for_file >> result_dict  # class-based task -> @task dependency (not .output pull)

    @task
    def load_results_task(result_dict: dict) -> dict:
        return result_dict  # D-02: thin pass-through, no second Oracle write

    @task
    def report_result_task(result_dict: dict) -> None:
        import logging

        logger = logging.getLogger("airflow.task")
        logger.info(
            "dataset=%s file=%s status=%s total=%s valid=%s invalid=%s duration=%.2fs",
            result_dict["dataset"],
            result_dict["file_name"],
            result_dict["status"],
            result_dict["total_rows"],
            result_dict["valid_rows"],
            result_dict["invalid_rows"],
            result_dict["duration_seconds"],
        )

    report_result_task(load_results_task(result_dict))


csv_ingest()
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| `airflow.models.DAG` / `airflow.decorators.task` imports | `airflow.sdk.DAG` / `airflow.sdk.task` | Airflow 3.0's Task SDK split [CITED: Context7 /apache/airflow/3.1.6, task-sdk/docs/index.rst "Migrate DAG and Task Imports from Airflow 2.x to 3.x"] | This project's Airflow 3.3.1 pin means the 2.x import paths are already deprecated — use `airflow.sdk` from the start, no migration needed |
| REST API auto-generating `logical_date` when omitted | `logical_date` defaults to `None` if not explicitly provided on trigger | Airflow 3.0 [CITED: Context7 /apache/airflow/3.1.6, `RELEASE_NOTES.rst`] | This project's trigger calls never need to supply `logical_date` — omit it entirely, matches an unscheduled (`schedule=None`), purely manually/API-triggered DAG |
| `PythonSensor`/classic sensor subclassing for simple polling | `@task.sensor` decorator (TaskFlow-native) | Ongoing Airflow 3.x guidance [CITED: Context7 /apache/airflow/3.1.6, `providers/standard/docs/sensors/python.rst`] | Not directly relevant here — `FileSensor` is used as a class-based operator specifically *because* deferral requires it (Pattern 2), not `@task.sensor` |

**Deprecated/outdated:** `from airflow import dag` (bare top-level import, shown only in Airflow's
own docs as a "legacy/deprecated" example) — use `from airflow.sdk import dag` instead.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `fs_conn_id="fs_default"`'s underlying `FSHook` base path is configured (or defaults) such that `filepath` only needs `<dataset>/<pattern>` rather than the full `/opt/airflow/data/<dataset>/<pattern>` — not verified against this project's actual `fs_default` Connection (none registered yet; only `oracle_default` is confirmed registered in `docker-compose.yml`) | Pattern 2, Code Examples | If `fs_default`'s base path is unset/wrong, `filepath` needs the full absolute path instead — a one-line fix, but the planner should verify the `fs_default` Connection's `extra.path` (or register one) before assuming the shorter relative form works |
| A2 | Airflow 3.1 and 3.3's `airflow.sdk`/`params`/`FileSensor` template-field behavior are unchanged between those minor versions (no indexed Context7 docs exist for 3.3.x specifically) | Standard Stack "Version verification", Pattern 1/2 | Low risk — Airflow 3.x minor releases have not historically broken `TaskFlow`/`params`/sensor templating; still worth a quick `pip show apache-airflow` / doc-diff spot-check inside the actual container before finalizing task bodies |
| A3 | `ti.xcom_pull(task_ids='load_config_task')['file_pattern']` is valid inside a Jinja `template_fields` string (i.e., `ti` and `xcom_pull` are available in the FileSensor's render context) — not independently proven working end-to-end in this repo (no DAG code exists yet to test against) | Pattern 2, Code Examples | If this specific idiom doesn't render as expected, option (c) from Pattern 2 (`file_pattern` as its own `params` field) is the safe fallback — flagged explicitly as a fallback above |

## Open Questions

1. **Is a `fs_default` Airflow Connection already registered, or does this phase need to register
   one?**
   - What we know: `AIRFLOW_CONN_ORACLE_DEFAULT` is registered via env var in `docker-compose.yml`
     — the same mechanism (`AIRFLOW_CONN_FS_DEFAULT`) could register `fs_default` the same way.
     `FileSensor`'s docs say the connection ID defaults to `fs_default` and Airflow ships a
     built-in `fs_default` Connection out of the box in most installs, but this project's specific
     `AIRFLOW__CORE__LOAD_EXAMPLES: "false"` /clean-init setup was not directly checked for its
     presence.
   - What's unclear: Whether the default `fs_default` Connection's base path is `/` (making
     `filepath` need the full `/opt/airflow/data/...` path) or unset.
   - Recommendation: Plan should include a one-command verification step (`airflow connections get
     fs_default` inside the container, or equivalent) before finalizing whether `filepath` uses a
     relative or absolute path — cheap to check, avoids guessing (ties to A1).

2. **Should `load_config` also validate that `config_path` resolves under `configs/datasets/`
   (Pitfall 6), or is Param's `enum=["customers","orders"]` on `dataset` alone sufficient given
   `config_path` is a separate, unconstrained `params` field?**
   - What we know: `params`' JSON-Schema `enum` only constrains `dataset`; `config_path` as
     declared in the Code Examples skeleton has no `enum`/pattern constraint, so it remains an
     open string field vulnerable to path traversal (Pitfall 6/Security Domain) unless the task
     body validates it explicitly.
   - What's unclear: Whether the project wants `config_path` to be a free-form field at all (given
     `dataset` alone is enough to derive the config path deterministically:
     `configs/datasets/<dataset>.json`) — this would remove the traversal vector entirely by
     deriving the path from the already-validated `dataset` enum instead of accepting a second,
     independent path string over HTTP.
   - Recommendation: Consider deriving `config_path` from `dataset` (`f"configs/datasets/{dataset}.json"`)
     rather than accepting it as an independent `conf` field — DAG-02's literal wording ("passing
     dataset name and config path as runtime conf") does ask for both, so this is presented as an
     option for the planner/user to weigh, not a recommendation to silently drop DAG-02's literal
     two-field contract.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker Desktop / `docker compose` stack (Airflow + Oracle) | Running/testing the DAG at all | Not verified running in this research session (no `docker compose up` executed) | `apache/airflow:3.3.1-python3.12` base image, pinned | — |
| `apache-airflow` (local `.venv`, root `pyproject.toml`) | Local `uv run pytest` DAG-structure tests | ✗ — not in `uv.lock`/root `.venv` at all [VERIFIED: `uv.lock` contains no `apache-airflow` entry; root `pyproject.toml`'s `dependencies` list has no Airflow package] | — | See Validation Architecture — DAG-parse/structure tests must run via `docker compose exec` against the already-built Airflow container, or a new `dev` dependency group must be added |
| `fs_default` Airflow Connection | `FileSensor`'s `fs_conn_id` (default value) | Not verified — see Open Question 1 | — | Register via `AIRFLOW_CONN_FS_DEFAULT` env var (mirrors the existing `AIRFLOW_CONN_ORACLE_DEFAULT` pattern) if the built-in default proves insufficient |
| `ORACLE_DSN`/`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD` (container env vars) | `process_csv`'s Oracle connection | ✗ — confirmed absent from `docker-compose.yml` (Pattern 4/Pitfall 2) | — | None — this is a blocking gap the plan must close, not a tool with a viable fallback |

**Missing dependencies with no fallback:**
- `ORACLE_DSN` (+ the two credential env vars) on the Airflow containers — must be added to
  `docker-compose.yml` as part of this phase's plan (Pattern 4).

**Missing dependencies with fallback:**
- Local `apache-airflow` for DAG-structure unit tests — fallback is running such tests via `docker
  compose exec` against the container that already has Airflow installed, rather than adding a
  second, drift-prone local install (see Validation Architecture Wave 0 Gaps).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 [VERIFIED: pyproject.toml `[dependency-groups] dev = ["pytest==9.1.1"]`] |
| Config file | root `pyproject.toml` `[tool.pytest.ini_options]` — `testpaths = ["tests"]`, `pythonpath = ["."]` |
| Quick run command | `uv run pytest tests/unit/dags/ -x` (new subdirectory this phase adds) |
| Full suite command | `uv run pytest tests/unit/ -x` (existing project convention, per `Makefile`'s `verify-phaseN` targets) |

**Key constraint (Environment Availability, above):** `apache-airflow` is not installed in the
root `.venv` — only inside the Docker image. This splits DAG-01..05's testable surface into two
tiers:

1. **Pure-Python helpers with zero Airflow import** (`_common/paths.py`'s glob-resolution logic,
   any `dataset`-to-`config_path` derivation, `config_path` traversal validation) — fully
   unit-testable in the existing local `.venv`, no new dev dependency needed. This is the bulk of
   this phase's genuinely new logic per Pattern 3/Don't Hand-Roll.
2. **DAG-structure/import validity** (does `csv_ingest.py` parse without error, does it expose
   exactly one `dag_id`, do the five named tasks exist, is there no dataset-specific `if` branch)
   — requires an actual `airflow` import (e.g. `airflow.models.DagBag` or `dag.test()`). Recommend
   running this tier via `docker compose exec airflow-scheduler python -c "..."` (or an
   equivalent one-off container invocation), consistent with the project's existing zero-Airflow-
   in-local-venv discipline (`ENGINE-09`'s spirit extended informally to the whole root `.venv`),
   rather than adding `apache-airflow` as a ~200MB local dev dependency that must then be kept in
   lockstep with the Dockerfile's own pin (a fourth place to drift, on top of the "three-place
   dependency rule" Phase 2's own RESEARCH.md already flagged for `csv_processor`).

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| DAG-01 | DAG defines exactly the 5 named tasks in the documented order, calls `csv_processor` (not raw SQL/CSV logic) | dag-structure (needs Airflow) | `docker compose exec airflow-scheduler python -c "from airflow.models import DagBag; b = DagBag(dag_folder='/opt/airflow/dags'); assert not b.import_errors; assert set(b.dags['csv_ingest'].task_ids) == {'load_config_task','wait_for_file','process_csv_task','load_results_task','report_result_task'}"` | ❌ Wave 0 |
| DAG-02 | Runtime `conf` (`dataset`, `config_path`) reaches `load_config`'s task body correctly | unit (no Airflow needed if `load_config_task`'s inner logic is factored into a plain function taking `dataset`/`config_path` strings) | `pytest tests/unit/dags/test_load_config_helpers.py -x` | ❌ Wave 0 |
| DAG-03 | `wait_for_file` is `deferrable=True` | dag-structure (needs Airflow) | Same `DagBag` check above, asserting `dag.get_task('wait_for_file').deferrable is True` | ❌ Wave 0 |
| DAG-04 | `report_result`'s log line contains dataset/file/row counts/duration/status | unit (factor the log-formatting into a plain function, test its string output directly) | `pytest tests/unit/dags/test_report_result_format.py -x` | ❌ Wave 0 |
| DAG-05 | Same DAG definition works for both datasets with no branch — proven by running the plain helper functions (path-building, config-loading) against both `customers.json` and `orders.json` | unit (parametrized over both dataset configs) | `pytest tests/unit/dags/test_dag_helpers.py -x -k "customers or orders"` | ❌ Wave 0 |

Full end-to-end proof (HTTP trigger → real DAG run → Oracle rows) is explicitly **TEST-03, Phase
6's job** — this phase's own validation stays at the unit/dag-structure tier described above.

### Sampling Rate
- **Per task commit:** `uv run pytest tests/unit/dags/ -x` (fast, no Airflow/Docker needed for the
  pure-Python-helper tier)
- **Per wave merge:** Full unit suite (`uv run pytest tests/unit/ -x`) + the `DagBag`
  import/structure check via `docker compose exec` (requires `make up` first)
- **Phase gate:** Both tiers green, plus a manual smoke trigger (the Pattern 5 curl example)
  against a real running stack before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/unit/dags/__init__.py` + `tests/unit/dags/conftest.py` — new test subdirectory, no
      Airflow import needed for the pure-Python-helper tier
- [ ] `tests/unit/dags/test_dag_helpers.py` — covers DAG-05 (both-dataset parametrization) and the
      `_common/paths.py` glob-resolution helper (Pitfall 1's fix)
- [ ] `tests/unit/dags/test_load_config_helpers.py` — covers DAG-02's conf-validation logic
      (including Pitfall 6's traversal check, extracted as a plain function)
- [ ] `tests/unit/dags/test_report_result_format.py` — covers DAG-04's summary formatting
- [ ] A `Makefile` target (e.g. `verify-phase5`) that runs the local unit tier plus the
      `docker compose exec` DAG-structure check, following the established `verify-phaseN`
      convention (`Makefile:51`'s own comment: *"Later phases (2-6) add targets here"*)
- [ ] Framework install: none — pytest is already present; no new dependency needed for the
      pure-Python-helper tier

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-------------------|
| V2 Authentication | yes (already satisfied, not this phase's job) | Airflow's `SimpleAuthManager`, already configured in Phase 1 (`docker-compose.yml:7-8`) — this phase's trigger call reuses it, doesn't implement auth itself |
| V3 Session Management | yes (already satisfied) | JWT `access_token` from `/auth/token`, Airflow-managed — no session state this phase introduces |
| V4 Access Control | no | Single `admin` user, local dev scope (INFRA-03) — no per-resource access control needed at this project's scale |
| V5 Input Validation | **yes — this phase's own responsibility** | `dataset` constrained via `Param(..., enum=[...])` (Airflow-enforced JSON Schema at trigger time); `config_path` requires an explicit `is_relative_to()` check inside `load_config_task` (Pitfall 6 — no existing library does this for us) |
| V6 Cryptography | no | No cryptographic operations in this phase's own code (JWT signing is Airflow's own internal concern) |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|----------------------|
| Path traversal via `config_path` runtime `conf` (e.g. `"../../etc/passwd"` or an absolute path outside `configs/datasets/`) | Tampering / Information Disclosure | Resolve the path and assert `Path(config_path).resolve().is_relative_to(Path("configs/datasets").resolve())` before calling `load_config` — reject with a `CONFIGURATION_ERROR`-shaped early exit (consistent with D-03/D-08) rather than attempting the read |
| Arbitrary `dataset` value used to build the `FileSensor` glob path (e.g. `dataset="../../"`) | Tampering | `Param(..., enum=["customers", "orders"])` on the `dataset` param — Airflow validates trigger-time values against the declared JSON Schema `enum` before the DAG run is even created, closing this off before it ever reaches path-building code |
| Overly permissive `core.dag_run_conf_overrides_params` allowing any `conf` key to silently override any `params` default, including ones not intended to be externally settable | Tampering | Default Airflow behavior (`dag_run_conf_overrides_params=True`) is fine here since this DAG intentionally exposes exactly `dataset`/`config_path` as its whole externally-settable surface — just don't declare additional internal-only `params` fields that a caller could then also override [CITED: Context7 /apache/airflow/3.1.6, `airflow-core/docs/core-concepts/params.rst` "Runtime Modification Control"] |

## Sources

### Primary (HIGH confidence — verified in-repo, read directly this session)
- `docker/airflow/Dockerfile` — exact pinned versions (`apache/airflow:3.3.1-python3.12`,
  `apache-airflow-providers-standard==1.17.0`, `apache-airflow-providers-oracle==4.6.2`)
- `docker-compose.yml` — full `airflow-common-env` block, `AIRFLOW_CONN_ORACLE_DEFAULT`, confirmed
  absence of `ORACLE_DSN`/`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD` as container env vars
- `packages/csv-processor/src/csv_processor/engine.py` — `process()`'s exact signature, docstring
  ("never raises"), full status-handling sequence
- `packages/csv-processor/src/csv_processor/models.py` — `Status` enum (7 members, exact string
  values), `ProcessingResult`'s exact field list
- `packages/csv-processor/src/csv_processor/config/loader.py` — `load_config`'s exact
  keyword-only-`defaults_path` signature
- `packages/csv-processor/src/csv_processor/config/models.py` — `DatasetConfig`'s exact field list
  (`dataset`, `file_pattern`, `columns`, `oracle`, `processing`)
- `packages/csv-processor/src/csv_processor/load.py` — `oracle_dsn()`/`oracle_user()`/
  `oracle_password()`'s exact `os.environ.get(...)` calls and defaults
- `configs/datasets/customers.json`, `configs/datasets/orders.json`, `configs/defaults.json` — real
  config shapes, `file_pattern`/`oracle` table name values
- `scripts/verify_environment.py` — the working `/auth/token` auth flow already proven in this repo
- `.planning/REQUIREMENTS.md`, `.planning/STATE.md`, `.planning/phases/05-.../05-CONTEXT.md` — phase
  scope, requirement text, locked decisions

### Secondary (MEDIUM confidence — Context7 official docs, and pinned-version GitHub source read via WebFetch)
- Context7 `/apache/airflow` (indexed version `3.1.6`, closest available to the project's pinned
  `3.3.1`) — `task-sdk/docs/{index,examples}.rst`, `airflow-core/docs/core-concepts/{dags,dag-run,
  params,taskflow}.rst`, `airflow-core/docs/tutorial/taskflow.rst`,
  `airflow-core/docs/security/api.rst`, `airflow-core/docs/core-concepts/auth-manager/simple/
  token.rst`, `airflow-core/docs/authoring-and-scheduling/deferring.rst`,
  `providers/standard/docs/sensors/file.rst`, `clients/python/README.md`, `RELEASE_NOTES.rst`
- `raw.githubusercontent.com/apache/airflow/providers-standard/1.17.0/providers/standard/src/
  airflow/providers/standard/sensors/filesystem.py` — fetched directly via WebFetch, quoted
  verbatim for `template_fields`, `__init__` signature, and `poke()`'s full body. This is the
  actual pinned-version primary source, not documentation prose — treated as MEDIUM/CITED per this
  project's provenance rules (the WebFetch mechanism itself is scored LOW by the confidence seam
  since it cannot generically confirm authoritativeness of an arbitrary URL, but the fetched
  content is the official Apache Airflow GitHub repository at the exact pinned tag — see Metadata
  for the explicit reasoning)

### Tertiary (LOW confidence)
- None used as the basis for any claim in this document — every finding above is either read
  directly from this repo's own files this session, or fetched from an Airflow-official source
  (Context7's indexed docs or the exact pinned-version GitHub source) this session.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new packages, all versions read directly from `Dockerfile`
- Architecture (DAG structure, `params`/`conf`, `FileSensor` templating): MEDIUM — Context7 docs
  are indexed at Airflow `3.1.6`, one minor version behind this project's pinned `3.3.1`; no
  known breaking changes to the specific APIs used here between those versions, but not
  independently re-verified against a running `3.3.1` instance in this research session
- `FileSensor` template_fields/poke() behavior specifically: MEDIUM-leaning-HIGH — read directly
  from the exact pinned-version (`1.17.0`) source file via WebFetch, the single most
  load-bearing external claim in this document (Pattern 2/3, Pitfall 1); the seam's automated
  confidence classifier scores the WebFetch fetch mechanism LOW by default (it cannot generically
  distinguish an arbitrary URL from an authoritative one), but the actual content fetched is the
  official Apache Airflow GitHub repository at the exact pinned provider tag — functionally
  equivalent to reading an in-repo vendored copy of the same file. Tagged `[CITED]` rather than
  `[VERIFIED]` throughout this document to stay consistent with the seam's literal output, but
  the planner should treat this specific claim as effectively HIGH confidence given the source.
- Pitfalls (`ORACLE_DSN` gap, `load_config` signature, "never raises" contract): HIGH — every one
  read directly from this repo's own source this session, with exact file:line citations and
  verbatim quotes
- Security domain: MEDIUM — ASVS category applicability reasoned from the actual attack surface
  this phase introduces (`dataset`/`config_path` as untrusted HTTP input); mitigations are
  standard Airflow `Param` JSON-Schema validation + a straightforward path-containment check, not
  independently verified against a live exploit attempt

**Research date:** 2026-08-29
**Valid until:** 30 days (Airflow 3.x is a stable major line; this project's own pinned versions
in `Dockerfile`/`pyproject.toml` are the actual source of truth going forward — re-verify only if
those pins change)
