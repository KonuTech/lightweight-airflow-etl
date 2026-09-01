# Architecture Research: Hourly CSV-Generation-and-Ingestion Orchestrator DAG

**Domain:** Airflow-native scheduling/orchestration integration (v1.1 milestone) — not a new
ecosystem/stack question, a "how does this new DAG wire into this repo's actual docker-compose
and DAG topology" question.
**Researched:** 2026-09-01
**Confidence:** HIGH for docker-compose/Dockerfile/permission mechanics (verified against this
repo's real files + Airflow's own official docker-compose.yaml, Context7-sourced); HIGH for
`TriggerDagRunOperator` worker-slot semantics (verified against `apache/airflow`'s own source via
Context7); MEDIUM for the exact `faker` pip-install placement (untested against this project's own
constraints file — flagged as a build-order verification step, not asserted as certain).

## Standard Architecture

### System Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     airflow-scheduler / airflow-dag-processor            │
│  (existing 5-service x-airflow-common topology, LocalExecutor, uid 50000)│
├──────────────────────────────────────────────────────────────────────────┤
│  NEW: csv_generate_schedule  (schedule="@hourly")                        │
│  ┌────────────────┐  ┌─────────────────┐  ┌────────────────┐  ┌────────┐│
│  │ generate_task   │→│ trigger_customers│→│ trigger_orders │→│trigger_ ││
│  │ (@task, in-     │  │ (TriggerDagRun-  │  │ (TriggerDagRun-│  │report_ ││
│  │  process import │  │  Operator, wait_ │  │  Operator, wait│  │ready   ││
│  │  of generator.  │  │  for_completion= │  │  _for_complet.=│  │(Trigger││
│  │  generate_csv)  │  │  True)           │  │  True)         │  │DagRun) ││
│  └────────────────┘  └─────────────────┘  └────────────────┘  └────────┘│
│         │                     │                     │               │    │
│         ▼                     ▼                     ▼               ▼    │
│  writes CSVs to        triggers existing      triggers existing  triggers│
│  /opt/airflow/data/    csv_ingest DAG run      csv_ingest DAG run existing│
│  {customers,orders}/   (conf: dataset=         (conf: dataset=    report_│
│  via write_staged()    customers)              orders)            ready  │
├──────────────────────────────────────────────────────────────────────────┤
│  UNCHANGED: csv_ingest (schedule=None)   UNCHANGED: report_ready         │
│  (config→wait_for_file→process→load→report)  (poll ingestion_metadata → │
│                                                 build_report_task)        │
└──────────────────────────────────────────────────────────────────────────┘
```

`csv_generate_schedule` is the only new DAG. `csv_ingest` and `report_ready` are consumed exactly
as they exist today — triggered via `TriggerDagRunOperator`, the same mechanism their existing
`schedule=None` design already anticipates (they're "manually/API-triggered only," and a DAG-to-DAG
trigger is just another trigger source, no different in kind from the REST API trigger Phase 5
already proved).

### Component Responsibilities

| Component | Responsibility | Real file in this repo |
|-----------|----------------|-------------------------|
| `csv_generate_schedule` (new) | Hourly cron entrypoint: generate fresh CSVs in-process, then chain-trigger the 3 downstream DAGs sequentially | `airflow/dags/csv_generate_schedule.py` (new) |
| `generate_task` | Calls `generator.generate_csv.main(["--correlated"])` in-process (mirrors `process_csv_task`'s existing in-process-call pattern, not a subprocess/BashOperator) | new `@task` inside the new DAG file |
| `trigger_customers`/`trigger_orders`/`trigger_report_ready` | `TriggerDagRunOperator` instances, sequential, `wait_for_completion=True` | new, inside the same DAG file |
| `csv_ingest` (unchanged) | Detect→parse→validate→load one dataset's CSV into Oracle | `airflow/dags/csv_ingest.py` (no changes) |
| `report_ready` (unchanged) | Poll for both datasets' current-day partition, log business report | `airflow/dags/report_ready.py` (no changes) |
| `docker-compose.yml` (modified) | Mount `generator/`, extend `PYTHONPATH`, fix `data/` ownership at `airflow-init` | root of repo |
| `docker/airflow/Dockerfile` (modified) | Add `faker` to the image's installed packages | `docker/airflow/Dockerfile` |

## Integration Point 1 — Exposing `generator/generate_csv.py` in the container

**Verified against the actual file** (`generator/generate_csv.py` lines 33-35):

```python
_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"
_DATA_DIR = _REPO_ROOT / "data"
```

This is a load-bearing detail: `_REPO_ROOT` is computed as *two levels up* from the script's own
file location (`generate_csv.py`'s directory, then its parent). If `generator/` is bind-mounted at
exactly `/opt/airflow/generator`, then inside the container `_REPO_ROOT` resolves to
`/opt/airflow` — which is **already** the parent of the two mounts that exist today:
`./configs:/opt/airflow/configs:ro` and `./data:/opt/airflow/data`. In other words, mounting
`generator/` at that one specific path makes the script's own path-relative logic land on the
existing mounts with zero code changes. Mounting it anywhere else (e.g. nested under `dags/`) would
silently break `_CONFIGS_DIR`/`_DATA_DIR` resolution.

**Required changes:**

1. **`docker-compose.yml`** — add one line to `x-airflow-common.volumes`, alongside the existing
   `./data`/`./configs` mounts:
   ```yaml
   - ./generator:/opt/airflow/generator
   ```
   (Read-write, not `:ro` — `write_staged()` only ever writes under `/opt/airflow/data/`, never
   under `/opt/airflow/generator/`, so this mount only needs to be readable; mounting it writable
   is harmless but read-only is the tighter, more correct choice — recommend `:ro` here, unlike
   `./data`.)

2. **`docker-compose.yml`** — extend `PYTHONPATH` in `x-airflow-common-env`:
   ```yaml
   PYTHONPATH: "/opt/airflow/dags:/opt/airflow"
   ```
   `generator/` has no `__init__.py` (confirmed: this repo's own root `pyproject.toml` already
   documents `generator/`, `tools/`, and `airflow/dags/` as namespace packages resolved relative to
   the repo root for mypy's `explicit_package_bases`/`mypy_path` settings — the same convention
   applies at runtime). For `from generator.generate_csv import main` to resolve inside a DAG task,
   `/opt/airflow` (the parent of the mounted `generator/` directory) must be on `sys.path`. Keep
   `/opt/airflow/dags` first in the list — the triggerer's existing `_common` import (documented in
   this file's own comment block, "found via a real live deferral of report_ready's custom
   trigger") depends on that entry already being present; this only *adds* a second path segment,
   it doesn't replace the first.

3. **`docker/airflow/Dockerfile`** — add `faker` to the installed packages, pinned to the exact
   version already locked in the root `pyproject.toml`/`uv.lock` (`faker==40.37.0`) — same "pin an
   exact version, matched to what's already approved elsewhere in the repo" discipline this
   Dockerfile already applies to `oracledb`/`pydantic`/`clevercsv`/etc. Try it in the **first,
   Airflow-constrained** `pip install` call first (alongside `oracledb`/`pydantic`):
   ```dockerfile
   RUN pip install --no-cache-dir \
         "oracledb==4.0.2" \
         "pydantic==2.13.4" \
         "faker==40.37.0" \
         "apache-airflow-providers-standard==1.17.0" \
         "apache-airflow-providers-oracle==4.6.2" \
       --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
   ```
   `faker` is not an Airflow dependency, so it's very unlikely to appear in Airflow's own
   constraints file at all (unlike `clevercsv`/`chardet`/`charset-normalizer`, which were moved to
   the *second*, unconstrained `pip install` specifically because their exact pinned versions
   conflicted with Airflow's constraints-resolved versions of those same packages — verified by
   reading the Dockerfile's own comment). **Verification step, not a certainty:** if this first
   `pip install` throws `ResolutionImpossible` on `faker` when the image is actually rebuilt, move
   it to the second (unconstrained) `pip install` call instead, mirroring the existing
   `clevercsv`/`chardet` treatment exactly. This is a MEDIUM-confidence recommendation precisely
   because it wasn't build-tested as part of this research pass — flag it as the first thing to
   confirm when this phase actually runs `make rebuild`.

4. **DAG-side invocation** — call `generate_csv.main()` **in-process**, not via `BashOperator`/
   `subprocess`, mirroring `csv_ingest.py`'s own established pattern of calling
   `csv_processor.engine.process()` directly inside a `@task` rather than shelling out:
   ```python
   from generator.generate_csv import main as generate_csv_main

   @task
   def generate_task() -> None:
       exit_code = generate_csv_main(["--correlated"])
       if exit_code != 0:
           raise AirflowException(f"generate_csv.py exited {exit_code}")
   ```
   `main()` returns `int` (0 on success) rather than calling `sys.exit()` itself — only the
   `if __name__ == "__main__":` guard at the bottom of `generate_csv.py` wraps it in
   `raise SystemExit(main())`, so calling `main([...])` directly from a task body is safe and
   already the file's own documented calling convention. A genuine generation failure (e.g. the
   `ValueError: cannot generate correlated orders: valid-customer pool is empty` raised inside
   `generate_correlated_datasets()`) propagates as a real exception and fails the task outright —
   correctly, since (unlike `csv_ingest`'s `process()`) generation failure has no equivalent
   "domain status that shouldn't fail the task" concept; there's nothing to report if there's no
   file.

## Integration Point 2 — `TriggerDagRunOperator` + `wait_for_completion=True` + `LocalExecutor`

**Verified directly against `apache/airflow`'s own source** (via Context7,
`providers/standard/src/airflow/providers/standard/operators/trigger_dagrun.py`):

```python
if self.wait_for_completion:
    if self.deferrable:
        self.defer(trigger=DagStateTrigger(...), method_name="execute_complete")
    while True:
        self.log.info("Waiting for %s on %s to become allowed state %s ...", ...)
        time.sleep(self.poke_interval)
        dag_run.refresh_from_db()
        state = dag_run.state
        if state in self.failed_states:
            raise AirflowException(...)
        if state in self.allowed_states:
            return
```

**Answer: yes, it blocks a worker slot for the entire wait — unless `deferrable=True` is also set.**
`deferrable` defaults to `False`. With the default, `wait_for_completion=True` runs a genuine
blocking `while True: time.sleep(poke_interval)` loop *inside the LocalExecutor worker process*
that's executing the `TriggerDagRunOperator` task — that process (and the worker slot it occupies)
is unavailable for any other task for the full duration of the triggered DAG run. This is the exact
same "held slot" cost as a non-deferrable `BaseSensorOperator` in `mode="poke"` — confirmed via
Airflow's own docs (`authoring-and-scheduling/deferring.rst`, Context7): "Standard operators and
sensors occupy a worker slot for their entire duration, even when idle." Setting `deferrable=True`
instead routes through `DagStateTrigger` on the triggerer (the same component `report_ready`'s
custom `OraclePartitionReadyTrigger` already runs on) and does **not** hold a worker slot — Airflow's
own scheduler concurrency accounting (`ConcurrencyMap.load`, Context7-verified) explicitly excludes
`DEFERRED`-state task instances from `dag_run_active_tasks_map`, the structure that counts against
`max_active_tasks`/parallelism.

**Does it matter at this project's tiny scale? No — recommend `deferrable=False` (the default) for
v1.1.** Reasoning:
- LocalExecutor's default `[core] parallelism` is 32 concurrent task slots; this hourly chain is
  the only pipeline in the project and runs one task at a time by construction (`generate_task >>
  trigger_customers >> trigger_orders >> trigger_report_ready`), so it never contends with itself
  or anything else for slots.
- The benchmark work already on record (`docs/benchmark.md`, 780K rows/sec bulk-loading) means the
  full chain — generate a couple hundred rows, ingest customers, ingest orders, poll+build the
  report — completes in low tens of seconds, once per hour. A single worker slot held for that
  duration, once an hour, is immaterial against a 32-slot budget with nothing else competing for it.
- `deferrable=True` on `TriggerDagRunOperator` has multiple **open, unresolved upstream bugs** as of
  this research pass — GitHub issues [#60049](https://github.com/apache/airflow/issues/60049)
  (defers even when `wait_for_completion=False`), [#57756](https://github.com/apache/airflow/issues/57756)
  (stuck deferred state combined with `reset_dag_run`), and [#52247](https://github.com/apache/airflow/issues/52247)
  (deferred trigger tasks stuck in 3.0.2). Given this repo's own working style — "verify by
  actually running it, not assumed" (see PROJECT.md's Key Decisions on the FileSensor/deferrable
  research) — adopting `deferrable=True` here would need its own live-verification pass against the
  exact pinned `apache-airflow-providers-standard==1.17.0`/Airflow `3.3.1` combination before being
  trusted, for a benefit (freeing one worker slot for tens of seconds, hourly) that doesn't
  materially matter at this scale. **Recommendation: ship `deferrable=False` (implicit default) for
  v1.1; revisit only if a future milestone adds enough concurrent DAG activity that worker-slot
  pressure becomes real.**

**One real (non-hypothetical) side-effect to flag, not fix, in this phase:** `csv_ingest`'s own
`report_result_task` uses `trigger_rule="none_failed_min_one_success"` and — per PROJECT.md's Key
Decisions table — `process_csv_task` never fails the DAG for any of `process()`'s domain-status
outcomes (`INVALID_FILE`, `CONFIGURATION_ERROR`, etc.). That means a `csv_ingest` run can reach
`DagRunState.SUCCESS` even when the file was invalid or misconfigured. `TriggerDagRunOperator`'s
default `allowed_states` includes `SUCCESS`, so `trigger_customers`/`trigger_orders` will report
success back to `csv_generate_schedule` even on a domain failure inside `csv_ingest` — the hourly
chain will happily proceed to `report_ready` even after a "silent" ingestion failure. This is not a
new bug introduced by this milestone (it's an existing, already-recorded design choice from Phase
5), but chaining DAGs together is what makes it newly *reachable* unattended, hourly, with nobody
watching the UI. Worth a follow-up (log-based alerting, or checking `result_dict["status"]` some
other way) in a later milestone — out of scope to fix as part of wiring the orchestrator itself.

## Integration Point 3 — Fixing `data/`'s root-owned permissions at the compose level

**The problem, precisely:** `./data:/opt/airflow/data` is a bind mount. On a genuinely fresh clone,
`./data` does not exist on the host yet. Docker Engine auto-creates it **as root** the moment the
first container with that mount starts (already independently confirmed live in this repo, per
`docs/environment.md`'s "Known First-Boot Gotcha" section and the CI fix in commit `caf5bfa`). The
existing fix — `mkdir -p data/customers data/orders` as a manual/CI pre-step, done by the *host*
user — only works because, until now, **generation happens on the host** (`make generate` runs
`generator/generate_csv.py` as the host user, which writes as that host user; the container only
ever *reads* from `data/`, and `755`-style read+traverse permissions on a host-user-owned directory
are sufficient for the container's uid 50000 to read files it didn't create). v1.1 changes this
premise: `generate_task` runs generation **inside the container**, as uid 50000. Writing into
`data/customers/.staging/` etc. now requires uid 50000 to have *write* access, not just traversal —
being merely able to `cd` into a host-user-owned, mode-755 directory is not enough to create files
in it. Two failure modes compound on a fresh clone:
1. The top-level `data/` itself, if Docker auto-creates it as root before anything else runs, blocks
   uid 50000 from creating *any* new subdirectory directly under it (this is the literal gap the
   milestone context names).
2. Even the existing `mkdir -p data/customers data/orders` host-side workaround doesn't fully solve
   the *new* problem: those directories, once pre-created by the host user, are owned by the host
   user (not uid 50000) at default `755` — sufcient for the container to read/traverse, but **not**
   to write, since only the owner (not "other") gets write bits at `755`.

**What this project already tried (and why it's not the durable fix going forward):** a documented
manual `mkdir -p data/customers data/orders` step, done by a human or CI, before first
`docker compose up`. This is host-side, easy to forget on a genuinely fresh clone (as its own
"Known First-Boot Gotcha" write-up admits), and — per the analysis above — was only ever sufficient
because nothing needed to *write* into those directories from inside the container until now.

**Recommended fix — adopt the same pattern Apache Airflow's own official quick-start
`docker-compose.yaml` uses for exactly this class of problem** (verified via Context7 against
`apache/airflow`'s `airflow-core/docs/howto/docker-compose/docker-compose.yaml`): give the
`airflow-init` service a **root-user override** so it can `chown` the bind-mounted directory to
match the non-root `AIRFLOW_UID` every other service runs as, *before* any other service starts.
Airflow's own file does exactly this for `logs`/`dags`/`plugins`/`config`:
```yaml
airflow-init:
  entrypoint: /bin/bash
  user: "0:0"
  command:
    - -c
    - |
      mkdir -v -p /opt/airflow/{logs,dags,plugins,config}
      chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/
      ...
```
Applied to this repo's own `airflow-init` service (which today only runs `command: db migrate`,
inheriting the anchor's non-root `user: "${AIRFLOW_UID:-50000}:0"` — meaning it currently has no
permission to `chown` a root-owned host directory at all), the minimal targeted change is:

```yaml
airflow-init:
  <<: *airflow-common
  user: "0:0"                      # override the anchor's non-root user for this service only
  depends_on:
    postgres:
      condition: service_healthy
  command: >
    bash -c "mkdir -p /opt/airflow/data/customers /opt/airflow/data/orders &&
             chown -R ${AIRFLOW_UID:-50000}:0 /opt/airflow/data &&
             exec airflow db migrate"
  environment:
    <<: *airflow-common-env
```

No `entrypoint:` override is needed here (unlike Airflow's own full quick-start file, which
overrides `entrypoint: /bin/bash` too) — the base `apache/airflow` image's default entrypoint script
already execs whatever command it's given verbatim when that command isn't one of its own
recognized subcommands (`webserver`, `scheduler`, `db`, ...); `bash -c "..."` is passed straight
through. `${AIRFLOW_UID:-50000}` inside the `command:` string is resolved by **docker compose
itself**, at compose-file-parse time, from the project's `.env` file (the same substitution
mechanism the existing `user: "${AIRFLOW_UID:-50000}:0"` line already relies on) — it does not need
to exist as a container-internal environment variable for this to work.

**Why this is more robust than the current manual step, not just a rearrangement of it:**
- `chown -R` fixes ownership recursively — it repairs a genuinely-fresh, root-owned `data/`
  (mkdir'd by this same command, then immediately chowned) *and* repairs ownership drift on a
  long-lived clone where some files/dirs under `data/` were created by the host user (uid 1000, via
  `make generate`) and others will now be created by the container (uid 50000) — both ownership
  origins converge to uid 50000 after every `docker compose up`/`make up`, since `airflow-init` runs
  this on every startup, not just the first one (idempotent: `mkdir -p` and `chown -R` are both
  safe to repeat).
- It runs *before* any other service, because `airflow-apiserver`/`airflow-scheduler`/etc. already
  gate on `airflow-init: condition: service_completed_successfully` in the existing
  `x-airflow-common.depends_on` block — no new dependency wiring needed, this ordering is already
  in place.
- It requires zero action from a human on a fresh clone (no `mkdir -p data/customers data/orders`
  step to remember) — closing exactly the "genuinely fresh clone doesn't hit this" gap the milestone
  context asks about. The existing `docs/environment.md` "Known First-Boot Gotcha: Permission Error
  Creating `data/<dataset>/`" section and the CI pre-create step in `.github/workflows/ci.yml`
  become **removable** once this lands (a currently-necessary workaround becomes dead weight, not a
  belt-and-suspenders redundancy — since the CI step runs `mkdir -p data/customers data/orders` as
  the **runner user**, `docker compose up`'s subsequent `airflow-init` `chown -R` would immediately
  reassign that ownership to uid 50000 anyway, so leaving the CI step in place is harmless but
  redundant, not actively wrong).

**Alternatives considered and rejected:**
- *Named volume instead of bind mount for `data/`* — rejected: `make generate`, `tests/e2e/`
  fixtures, and `scripts/verify_evidence.sql`'s evidence capture all currently depend on host-side
  processes reading/writing `./data/` directly (the existing `D-06` comment in `docker-compose.yml`
  says this explicitly: "generate_csv.py writes to `./data/<dataset>/` on the host"). A named volume
  is only directly accessible from inside a container, which would break every host-side script that
  isn't run through Docker.
- *A one-off manual `chmod -R 777 data/`* — rejected: works, but is the same class of "must remember
  it, host-only, undocumented-until-it-bites-someone" fix this repo has already twice hit (the
  passwords-file `chmod 666` and the current `mkdir -p data/customers data/orders`) — compose-level
  automation via `airflow-init` removes the human dependency entirely rather than adding a third
  instance of the same pattern.

## Anti-Patterns to Avoid

### Anti-Pattern 1: Mounting or copying `generator/` at the wrong container path

**What people do:** Bind-mount `generator/` under `/opt/airflow/dags/generator` (reasoning "it's
DAG-adjacent code") or `COPY` it into the image the way `packages/csv-processor/` is copied.
**Why it's wrong:** `generate_csv.py`'s `_REPO_ROOT = Path(__file__).resolve().parent.parent` is a
hard-coded two-levels-up computation. Mounting one level too deep (or too shallow) silently changes
`_CONFIGS_DIR`/`_DATA_DIR` to point somewhere that doesn't have the `configs`/`data` mounts already
in place — the failure mode is not an import error, it's a confusing `FileNotFoundError` at config
load or file write time, deep inside otherwise-correct code.
**Instead:** Mount at exactly `/opt/airflow/generator` — this is the one path where the script's
existing, unmodified path math lines up with mounts that already exist for other reasons.

### Anti-Pattern 2: Reaching for `deferrable=True` on `TriggerDagRunOperator` by default

**What people do:** Since this project has an established preference for deferrable operators
(`FileSensor(deferrable=True)`, the custom `OraclePartitionReadyTrigger`), it's tempting to reflexively
set `deferrable=True` here too, treating it as "the more correct choice, full stop."
**Why it's wrong:** Unlike `FileSensor`, which this project already live-verified as sufficient,
`TriggerDagRunOperator`'s `deferrable` mode has multiple open upstream bugs on file as of this
research pass (see Integration Point 2). Enabling it here would be adopting an unverified, actively
buggy code path for a benefit (freeing a worker slot for tens of seconds, once an hour, in a
32-slot-parallelism LocalExecutor with nothing else running) that doesn't matter at this project's
scale.
**Instead:** Use the default (`deferrable=False`). Revisit only if a future milestone's real,
observed worker-slot contention makes it matter — and even then, budget time to live-verify against
the exact pinned provider version first, this project's own established discipline.

### Anti-Pattern 3: Treating the `data/` permission fix as a one-time manual step

**What people do:** Document "run `mkdir -p data/customers data/orders` before first boot" and
consider the gap closed (this is literally what this repo already did once).
**Why it's wrong:** It was sufficient when only host processes wrote into `data/`. It stops being
sufficient the moment a container-side process needs write access too — and it depends on a human
(or CI script) remembering to run it, every time a volume gets wiped (`make reset`/`make destroy`),
not just on the very first clone.
**Instead:** Fix it inside `docker-compose.yml` itself, in a service that already runs before
everything else and already gates all other services' startup (`airflow-init`) — see Integration
Point 3.

## Build Order for This Phase

The dependency here is strict, not stylistic — the DAG cannot be verified to actually run until the
environment can support it:

1. **`docker-compose.yml`: `airflow-init` chown fix (Integration Point 3)** — land and verify first,
   independent of everything else. Verify via `make destroy && make up` (the deepest teardown target
   this repo already has, per the `Makefile` diff currently staged) against a genuinely fresh
   `./data` state, then `ls -la data/ data/customers data/orders` from the host to confirm uid 50000
   ownership. This has zero dependency on the DAG or the generator mount and can be proven correct
   on its own.
2. **`docker-compose.yml`: mount `generator/` + `PYTHONPATH` extension, `docker/airflow/Dockerfile`:
   add `faker` (Integration Point 1)** — land together (they're both "make the container able to run
   `generate_csv.py`" changes), then verify with `make rebuild` (already a Makefile target) followed
   by a one-off manual exec check:
   ```bash
   docker compose exec -T airflow-scheduler python -c \
     "from generator.generate_csv import main; print(main(['--correlated']))"
   ```
   proving the import path and the `faker` install both resolve correctly *before* wiring it into a
   real DAG — isolates any `ResolutionImpossible`/`ModuleNotFoundError` surprises from DAG-authoring
   mistakes.
3. **New `airflow/dags/csv_generate_schedule.py` DAG (Integration Point 2)** — only after 1 and 2 are
   independently proven. Structural verification first (mirroring this repo's own
   `verify-phase5`/`verify-phase7` Makefile pattern: a live `BundleDagBag` import-error check), then
   a real live-triggered/live-scheduled run watched end-to-end through the Airflow UI, the same
   "don't trust it structurally, watch it actually run" discipline this repo has applied at every
   prior DAG-introducing phase (Phase 5's `csv_ingest`, Phase 7's `report_ready`).
4. **`OraclePartitionReadyTrigger.run()` exception-handling fix** (the other Active v1.1 item, per
   PROJECT.md) — independent of 1-3, can land in parallel or in either order; noted here only to be
   explicit that it's not a blocking dependency for the orchestrator DAG itself.

## Integration Points

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| `csv_generate_schedule.generate_task` ↔ `generator.generate_csv` | Direct in-process Python call (`main(["--correlated"])`), not subprocess | Requires `/opt/airflow` on `PYTHONPATH`; requires `generator/` mounted at exactly `/opt/airflow/generator` |
| `csv_generate_schedule` ↔ `csv_ingest` (×2) | `TriggerDagRunOperator(wait_for_completion=True)`, `conf={"dataset": ..., "config_path": ...}` | Sequential (customers before orders) — required by the Phase 7 DB-level `BEFORE INSERT` FK trigger on `orders_valid`, which needs `customers_valid` rows to already exist |
| `csv_generate_schedule` ↔ `report_ready` | `TriggerDagRunOperator(wait_for_completion=True)`, empty `conf` | Runs last; `report_ready`'s own deferrable sensor still independently polls `ingestion_metadata`, so this trigger is really just "kick it off now" rather than a strict data dependency |
| `airflow-init` (root, `user: "0:0"`) ↔ every other Airflow service (`user: "${AIRFLOW_UID:-50000}:0"`) | Shared bind-mounted host directory (`./data`), ownership fixed by the former before the latter start | Existing `depends_on: airflow-init: condition: service_completed_successfully` on the shared anchor already enforces this ordering — no new dependency wiring needed |

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| None new | — | This milestone introduces no new external service — it wires existing in-repo DAGs together and fixes environment/permission gaps. Oracle and the Airflow metadata Postgres are unchanged. |

## Sources

- This repo's own files (read directly, not assumed): `docker-compose.yml`, `docker/airflow/Dockerfile`,
  `generator/generate_csv.py`, `docs/environment.md`, `airflow/dags/_common/paths.py`,
  `airflow/dags/csv_ingest.py`, `airflow/dags/report_ready.py`, `pyproject.toml`, `Makefile`,
  `.env.example`, `.github/workflows/ci.yml`, `.planning/PROJECT.md`
- `apache/airflow` GitHub source, `providers/standard/src/airflow/providers/standard/operators/trigger_dagrun.py`
  (Context7, HIGH confidence) — the `wait_for_completion`/`deferrable` blocking-loop-vs-`DagStateTrigger`
  code path
- `apache/airflow` GitHub source, `airflow-core/src/airflow/jobs/scheduler_job_runner.py`
  (Context7, HIGH confidence) — `ConcurrencyMap.load`, confirming `DEFERRED` task instances don't
  count toward worker-slot/`max_active_tasks` accounting
- `airflow-core/docs/authoring-and-scheduling/deferring.rst` (Context7, HIGH confidence) — "Standard
  operators and sensors occupy a worker slot for their entire duration, even when idle."
- `apache/airflow`'s own official `airflow-core/docs/howto/docker-compose/docker-compose.yaml`
  (verified via WebFetch/Context7, HIGH confidence) — the `airflow-init` `user: "0:0"` +
  `mkdir`/`chown -R "${AIRFLOW_UID:-50000}:0"` pattern this research recommends adapting
- [`TriggerDagRunOperator` defers even when `wait_for_completion=False` · Issue #60049](https://github.com/apache/airflow/issues/60049)
- [Deferrable mode of `TriggerDagRunOperator` stays stuck if used with `reset_dag_run` · Issue #57756](https://github.com/apache/airflow/issues/57756)
- [Deferred trigger tasks seem to get stuck in Airflow 3.0.2 · Issue #52247](https://github.com/apache/airflow/issues/52247)
- This repo's own git history: `caf5bfa fix(06): pre-create data/ subdirectories in CI to avoid
  root-owned bind mount` — confirms the exact failure mode this research addresses was already hit
  once, in CI, for the host-only-write case

---
*Architecture research for: hourly CSV-generation-and-ingestion orchestrator DAG (v1.1 milestone)*
*Researched: 2026-09-01*
