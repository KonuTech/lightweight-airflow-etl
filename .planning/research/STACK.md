# Stack Research

**Domain:** Airflow orchestrator DAG — in-process CSV generation task + sequential chain-triggering of existing DAGs
**Researched:** 2026-09-01
**Confidence:** HIGH (verified against this repo's own pinned versions + this session's live-confirmed import; MEDIUM on TriggerDagRunOperator's `deferrable` behavior, flagged below)

This is a delta document. It assumes everything in "Existing validated capabilities" (Airflow
3.3.1, `apache-airflow-providers-standard==1.17.0`, `apache-airflow-providers-oracle==4.6.2`,
`python-oracledb==4.0.2`, `csv_processor`, `TriggerDagRunOperator` import path) is already settled
and does **not** re-litigate it. It covers only what's net-new for the `csv_generate_schedule` DAG.

## Recommended Stack

### Core Technologies

No new frameworks. This milestone adds one library dependency (`faker`) and two integration
changes (a volume mount, a `PYTHONPATH` extension) to the existing Docker image — no new operator
types, no new provider packages.

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| `faker` | `==40.37.0` | Realistic-looking fake string values (`fake.name()`, `fake.word()`, `fake.country()`) inside `generator/generate_csv.py` | **Must match, not just approximate, the version already pinned in root `pyproject.toml`/`uv.lock`** (confirmed via `grep`: `faker==40.37.0` in both `pyproject.toml:14` and `uv.lock`). Using a different version inside the container than the one the repo's own `uv.lock` resolved would reintroduce exactly the kind of drift the Dockerfile's own comment about `clevercsv`/`charset-normalizer`/`chardet` already warns against — two environments generating CSVs with the same `--seed` should be able to produce byte-identical output, and `Faker`'s corpus/algorithm has changed between major versions historically. |
| `TriggerDagRunOperator` | (ships in already-pinned `apache-airflow-providers-standard==1.17.0`) | Chain-trigger `csv_ingest` (customers), `csv_ingest` (orders), and `report_ready` sequentially, blocking each on the prior run's completion | Already confirmed importable in this exact built image this session (`airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator`) — no new package to add. Supports `wait_for_completion=True` plus `poke_interval`, `allowed_states`, `failed_states` natively; no custom sensor/trigger needed for "wait for each to complete before proceeding." |

### Supporting Libraries

None. `generate_csv.py`'s only non-stdlib imports are `faker` (new to the image) and
`csv_processor.config` (already installed in the image via the Dockerfile's existing
`pip install --no-deps packages/csv-processor/` line — no change needed there). `pyyaml==6.0.3`,
also in root `pyproject.toml`, is unused by `generate_csv.py` and by `csv_processor.config.loader`
(verified: `loader.py`'s only imports are `json`, `pathlib`, `pydantic`, and `csv_processor.config.*`
— confirmed via direct read, no `yaml`/`pyyaml` import anywhere in `csv_processor`). Do not add
`pyyaml` to the Docker image for this milestone; it would be dead weight with no import site.

### Development Tools

No new dev tooling. The existing `make lint` (ruff + mypy) and `make verify-phaseN` pattern
already covers `generator/` (root `pyproject.toml`'s `[[tool.mypy.overrides]]` already lists
`generator.*` in the strict-per-module block) and will pick up the new orchestrator DAG file the
same way `verify-phase5`'s `BundleDagBag` check already covers `csv_ingest.py`.

## Installation

```dockerfile
# docker/airflow/Dockerfile — add faker to the EXISTING constrained pip install line.
# Verified: `faker` does not appear at all in Airflow's own
# constraints-3.3.1/constraints-3.12.txt (checked via `curl | grep -i "^faker"` — zero
# matches), unlike chardet/charset-normalizer, which DO appear there with different pins
# than csv-processor needs (that's why those two get a SEPARATE unconstrained pip call).
# Because faker has no competing constraint entry, --constraint has nothing to override
# for it, so it is safe to add to the SAME constrained install line as oracledb/pydantic/
# the two provider packages -- no ResolutionImpossible risk, no need for a second call.
RUN pip install --no-cache-dir \
      "oracledb==4.0.2" \
      "pydantic==2.13.4" \
      "apache-airflow-providers-standard==1.17.0" \
      "apache-airflow-providers-oracle==4.6.2" \
      "faker==40.37.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
 && pip install --no-cache-dir \
      "clevercsv==0.8.5" \
      "charset-normalizer==3.5.1" \
      "chardet==7.6.0"
```

```yaml
# docker-compose.yml — x-airflow-common.volumes: mount generator/ alongside the
# existing airflow/dags, data, configs mounts.
volumes:
  - ./docker/airflow/simple_auth_manager_passwords.json.generated:/opt/airflow/simple_auth_manager_passwords.json.generated
  - ./airflow/dags:/opt/airflow/dags
  - ./data:/opt/airflow/data
  - ./configs:/opt/airflow/configs:ro
  # NEW: mount at /opt/airflow/generator (not /opt/airflow/dags/generator) --
  # generate_csv.py computes `_REPO_ROOT = Path(__file__).resolve().parent.parent`,
  # which for /opt/airflow/generator/generate_csv.py resolves to /opt/airflow. That
  # makes _CONFIGS_DIR = /opt/airflow/configs and _DATA_DIR = /opt/airflow/data --
  # the SAME two paths already mounted above. Mounting anywhere else breaks that
  # parent-parent arithmetic and silently points the generator at the wrong dirs.
  - ./generator:/opt/airflow/generator
  - airflow-logs:/opt/airflow/logs

x-airflow-common-env:
  # EXTEND (do not replace) the existing PYTHONPATH -- /opt/airflow/dags is still
  # needed for the triggerer's `_common` import (Phase 7 decision, see Dockerfile/
  # compose comments); add /opt/airflow so `generator` (a namespace package, no
  # __init__.py at its top level -- see root pyproject.toml's own mypy comment on
  # this) resolves as `generator.generate_csv` from inside the new DAG file.
  PYTHONPATH: "/opt/airflow/dags:/opt/airflow"
```

No `pip install` step is needed for `generator/` itself — it's a plain namespace package (matching
the existing `tools/` convention noted in root `pyproject.toml`), imported directly off
`PYTHONPATH`, not an installed distribution. Do not add a `pyproject.toml`/`setup.py` under
`generator/` to make it "installable" — that would be new packaging machinery this repo doesn't use
anywhere else for a directory this small.

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|--------------------------|
| Mount `./generator` read-write into the image, import it as a Python module from a `@task` | `COPY generator/ /opt/airflow/generator/` baked into the Dockerfile at build time | Only if `generator/` were meant to be immutable/versioned independently of the DAG code being iterated on. This repo's own precedent (`airflow/dags` and `configs` are both live-mounted, not `COPY`'d) is bind-mount for anything actively edited during development; `csv-processor` is the one thing that IS `COPY`'d, because it's an installed *package* with a build step (`pip install --no-deps`), not a plain script tree. `generator/` is a script tree like `airflow/dags/`, not a package like `csv-processor` — mount, don't copy. |
| In-process `@task` calling `generator.generate_csv`'s functions directly (`load_config` + `generate_correlated_datasets` + `write_staged`) | Shell out via `BashOperator`/`subprocess.run(["python", "generator/generate_csv.py", "--correlated"])` | Never, for this repo. `csv_ingest.py`'s `process_csv_task` already establishes the pattern of importing the engine and calling it as a Python function in-process (never shelling to a CLI) — mirroring that here keeps the "thin TaskFlow DAG delegates to a reusable Python engine" philosophy consistent across both DAGs, and gives structured return values (paths, row counts) for XCom instead of parsing subprocess stdout. |
| Plain synchronous `TriggerDagRunOperator(wait_for_completion=True, deferrable=False)` (poke, not defer) for the three chain-trigger steps | `TriggerDagRunOperator(..., deferrable=True)` | Only after live-verifying deferral behavior in this exact stack. `deferrable=True` on `TriggerDagRunOperator` has multiple still-open upstream issues as of Airflow 3.x (apache/airflow#60049 — defers even when `wait_for_completion=False`; apache/airflow#57756 — deferred mode "stuck" combined with `reset_dag_run`; apache/airflow#38353 — deferred `wait_for_completion` "not working as expected"). This project's LocalExecutor runs a small, hourly-cadence, single-active-DAG-run workload — there is no worker-slot-scarcity problem `deferrable=True` would actually solve here, unlike `FileSensor(deferrable=True)`'s genuine win in `csv_ingest` (which can sit for up to an hour waiting on an external file). Poke (`deferrable=False`, the operator's own default) with a `poke_interval` tuned to the expected sub-minute `csv_ingest`/`report_ready` runtime is simpler and avoids a known-flaky code path. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|--------------|
| `PythonVirtualenvOperator` (or `ExternalPythonOperator`) to run `generate_csv.py` in an isolated venv because it needs `faker` | Explicitly out of scope per this task's own framing, and unnecessary here — `faker` has zero conflicting transitive requirements with anything already in the image (confirmed: absent from Airflow's own constraints file entirely), so there's no dependency-isolation problem to solve. `PythonVirtualenvOperator` would also require pip/venv-building tooling inside the scheduler/worker container and add real per-task-run latency (venv creation) for a library that installs in seconds at image-build time. | Add `faker==40.37.0` straight to the existing Dockerfile pip-install layer (already the pattern for `oracledb`/`pydantic`/the two providers). |
| `apt`/`uv`/`poetry`/any other package manager inside `docker/airflow/Dockerfile` | The image already exclusively uses plain `pip install` (base `apache/airflow` image ships pip; the Dockerfile never introduces `uv` or `poetry` even though the rest of the repo uses `uv` for local dev) — introducing a second package manager inside the image for one library breaks that one established convention for no benefit. | Extend the existing `pip install --no-cache-dir ... --constraint ...` line. |
| A brand-new `apache-airflow-providers-*` package to get "a trigger-and-wait operator" | Already have it — `TriggerDagRunOperator` ships in `apache-airflow-providers-standard`, already pinned at `1.17.0`, already confirmed importable in the built image this session. | `from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator` |
| Baking `generator/` into the image via `COPY` (like `csv-processor`) | `generator/` is edited alongside DAG code during active development, not a versioned installable package — `COPY`-ing it would require an image rebuild (`make rebuild`) on every generator tweak, unlike every other actively-edited path in this repo (`airflow/dags`, `configs`), which are live bind-mounts. | Bind-mount `./generator:/opt/airflow/generator`, same treatment as `./airflow/dags` and `./configs`. |
| Writing generated CSVs directly via `output_path()` (skipping `write_staged()`) from inside the new DAG task | Phase 7's own recorded decision: `write_staged()` (staging path + atomic same-filesystem `rename()`) is "the one write path for every production CSV writer" specifically so a file is never visible to a watching `FileSensor` mid-write. The new orchestrator task calling `generate_correlated_datasets()` must still finish by calling `write_staged()` for both datasets, exactly like `generate_csv.py main()`'s own `--correlated` branch does. | Call `write_staged(generated, config, dataset, compress=...)` for both `customers` and `orders`, same as the CLI's own `--correlated` code path. |

## Stack Patterns by Variant

**If the new task calls `generator.generate_csv`'s functions directly (recommended):**
- `from generator.generate_csv import generate_correlated_datasets, write_staged` inside the new
  `@task` function body (not at DAG-parse top-level, to keep DAG-file import errors isolated the
  same way `csv_ingest.py` keeps `csv_processor` imports scoped to what's actually needed at parse
  time — though note `csv_ingest.py` itself imports `csv_processor.engine` at module top-level, so
  either placement is consistent with existing precedent; prefer top-level for parse-time import
  error visibility, matching `csv_ingest.py`).
- Load `customers_config`/`orders_config` via `csv_processor.config.loader.load_config` against
  `/opt/airflow/configs/datasets/{customers,orders}.json` — the exact same `_common.paths.
  CONFIGS_ROOT`/`DATASETS_CONFIG_ROOT` constants `csv_ingest.py` already uses, not
  `generate_csv.py`'s own host-relative `_CONFIGS_DIR` (which happens to resolve to the same place
  under the recommended mount, but reusing `_common.paths` keeps one source of truth for the
  container-side path convention rather than two independently-correct-by-coincidence ones).

**If a future milestone needs the generator to run in a genuinely separate Python environment
(e.g. a different Faker major version than the DAG code needs):**
- That's the point at which `PythonVirtualenvOperator`/`ExternalPythonOperator` becomes justified.
  Not needed now — flagged here only so this isn't re-litigated from scratch later.

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|------------------|-------|
| `faker==40.37.0` | Airflow `3.3.1` / `apache-airflow-providers-standard==1.17.0` (constraints-3.3.1/constraints-3.12.txt) | Confirmed via direct fetch of the pinned constraints file: `faker` has **zero** entry in `constraints-3.3.1/constraints-3.12.txt` (only `PyYAML==6.0.3` appears from the two names checked) — no version conflict is possible via `--constraint`; safe to add to the same constrained `pip install` call as `oracledb`/`pydantic`/the two providers, unlike `chardet`/`charset-normalizer` (which DO appear in that file with different pins than `csv-processor` needs, hence their separate unconstrained call). |
| `faker==40.37.0` (image) | `faker==40.37.0` (root `pyproject.toml`/`uv.lock`, used by local `make generate` via `uv run`) | Must stay identical, not just "compatible" — this is a determinism requirement (D-14/D-06 style seed-reproducibility), not a looser SemVer-range compatibility question. Any future bump to root `pyproject.toml`'s `faker` pin must be mirrored into the Dockerfile in the same change. |
| `TriggerDagRunOperator(deferrable=True)` | Airflow `3.3.1` / `apache-airflow-providers-standard==1.17.0` | MEDIUM confidence only — WebSearch-sourced (no Context7/official-docs paragraph found explicitly confirming defer behavior is bug-free at these exact pinned versions); multiple still-open upstream GitHub issues describe deferred-mode `TriggerDagRunOperator` edge cases across Airflow 3.0.x-3.1.x. Recommendation above is to use `deferrable=False` (poke) specifically to sidestep this uncertainty rather than resolve it. |

## Sources

- `/home/konutec/projects/lightweight-airflow-etl/pyproject.toml` (root) — confirmed `faker==40.37.0`, confirmed `pyyaml==6.0.3` present but unused by anything this milestone touches
- `/home/konutec/projects/lightweight-airflow-etl/uv.lock` — confirmed `faker` resolves to `40.37.0` with no version drift from the `pyproject.toml` pin
- `/home/konutec/projects/lightweight-airflow-etl/docker/airflow/Dockerfile` — existing constrained/unconstrained two-call pip pattern, reused rather than reinvented
- `/home/konutec/projects/lightweight-airflow-etl/docker-compose.yml` — existing volume mounts (`airflow/dags`, `data`, `configs`) and `PYTHONPATH` convention, extended rather than replaced
- `/home/konutec/projects/lightweight-airflow-etl/generator/generate_csv.py` — confirmed `_REPO_ROOT`/`_CONFIGS_DIR`/`_DATA_DIR` path arithmetic, confirmed only non-stdlib imports are `faker` and `csv_processor.config`, confirmed `write_staged()`/`generate_correlated_datasets()`/`--correlated` as the functions/entry point to call from the new task
- `/home/konutec/projects/lightweight-airflow-etl/airflow/dags/csv_ingest.py` — the existing TaskFlow-delegates-to-engine pattern mirrored for the new generate task
- `/home/konutec/projects/lightweight-airflow-etl/airflow/dags/_common/paths.py` — `DATA_ROOT`/`CONFIGS_ROOT`/`DATASETS_CONFIG_ROOT` constants to reuse instead of duplicating path logic
- `/home/konutec/projects/lightweight-airflow-etl/packages/csv-processor/src/csv_processor/config/loader.py` — confirmed zero `yaml`/`pyyaml` import
- `curl https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt | grep -i "^faker\|^pyyaml"` — live-fetched, HIGH confidence: `faker` absent, `PyYAML==6.0.3` present
- [TriggerDagRunOperator API docs (stable)](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/_api/airflow/providers/standard/operators/trigger_dagrun/index.html) — MEDIUM confidence, constructor signature (`wait_for_completion`, `poke_interval`, `allowed_states`, `failed_states`, `deferrable` defaults) confirmed via WebFetch of the "stable" version, not the pinned `1.17.0` docs snapshot specifically
- [apache/airflow#60049 — TriggerDagRunOperator defers even when wait_for_completion=False](https://github.com/apache/airflow/issues/60049) — MEDIUM confidence, WebSearch-sourced open issue
- [apache/airflow#57756 — Deferrable mode of TriggerDagRunOperator stays stuck if used with reset_dag_run](https://github.com/apache/airflow/issues/57756) — MEDIUM confidence, WebSearch-sourced open issue
- [apache/airflow#38353 — TriggerDagRunOperator not working as expected in defer with wait_for_completion](https://github.com/apache/airflow/issues/38353) — MEDIUM confidence, WebSearch-sourced open issue
- [apache/airflow#47949 — Support deferral mode for TriggerDagRunOperator with Task SDK](https://github.com/apache/airflow/issues/47949) — MEDIUM confidence, WebSearch-sourced, context on deferral maturity for this operator

---
*Stack research for: hourly CSV-generation-and-ingestion orchestrator DAG (v1.1 milestone)*
*Researched: 2026-09-01*
