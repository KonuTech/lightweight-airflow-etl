<!-- EXEC-SUMMARY:START -->

# Executive Summary

Live evidence of a working HTTP-trigger -> Airflow DAG -> Oracle ETL pipeline
(TEST-03/DOC-01), regenerated automatically after every merge to `main`
(D-11/D-12) by `scripts/regenerate_readme_summary.py` via
`.github/workflows/readme-summary.yml`, using the default `GITHUB_TOKEN`
(never a PAT -- D-13). Last regenerated: `2026-08-29T23:01:16.635950+00:00`.

### Latest ingestion per dataset

| Dataset | File Name | Total Rows | Valid Rows | Invalid Rows | Status | Processed At (UTC) |
|---|---|---|---|---|---|---|
| customers | customers_1788044460631453295.csv | 25 | 20 | 5 | SUCCESS_WITH_INVALID_ROWS | 2026-08-29T23:01:02.972938 |
| orders | orders_1788044470578841735.csv | 25 | 20 | 5 | SUCCESS_WITH_INVALID_ROWS | 2026-08-29T23:01:13.327505 |

### Deferred-wake proof

`wait_for_file` reported Airflow task state `deferred` for the `customers` dataset (`dag_run_id=manual__2026-08-29T23:00:56.578632+00:00`) at `2026-08-29T23:01:00.631444+00:00` -- confirmed BEFORE the fixture file existed on disk, proving the non-blocking file-wait genuinely deferred rather than short-circuited against an already-present file.

### Customers x Orders business report (top 10)

Region is `customers.country` (no literal `region` column exists in this
schema -- explicit substitution, not silently assumed, D-10). Grouped by
region and month-of-`orders.order_date`; see `scripts/verify_evidence.sql`
for the full, un-truncated report and `make verify-evidence` to reproduce it.

| Region | Order Month | Order Count | Total Amount | Avg Amount |
|---|---|---|---|---|
| Afghanistan | 2000-12 | 2 | 7934157756.6 | 3967078878.3 |
| Afghanistan | 2001-02 | 1 | 4272709782.63 | 4272709782.63 |
| Afghanistan | 2001-06 | 1 | 8160460647.95 | 8160460647.95 |
| Afghanistan | 2001-10 | 2 | 14176463922.86 | 7088231961.43 |
| Afghanistan | 2002-01 | 2 | 4263714943.94 | 2131857471.97 |
| Afghanistan | 2003-05 | 1 | 6976301727.6 | 6976301727.6 |
| Afghanistan | 2005-04 | 1 | 4886050136.08 | 4886050136.08 |
| Afghanistan | 2005-06 | 3 | 9506165486.04 | 3168721828.68 |
| Afghanistan | 2005-10 | 1 | 593639016.35 | 593639016.35 |
| Afghanistan | 2006-03 | 1 | 9893762532.9 | 9893762532.9 |

<!-- EXEC-SUMMARY:END -->

# Lightweight Airflow CSV→Oracle ETL Platform

A small, local Airflow environment that detects, parses, validates, and bulk-loads generated CSV
files into Oracle Database Free, orchestrated by a thin Airflow TaskFlow DAG. See `.planning/PROJECT.md`
for the full project context and scope.

## Getting Started

Prerequisites: Docker Desktop (with WSL2 integration enabled), GNU Make.

```bash
git clone <this-repo-url>
cd lightweight-airflow-etl
cp .env.example .env

# One-time: recreate the gitignored Airflow auth passwords file (no automated
# mechanism exists for this yet — see docs/environment.md "First-Clone Setup Gaps")
mkdir -p docker/airflow
echo '{"admin": "admin"}' > docker/airflow/simple_auth_manager_passwords.json.generated

make up
```

This brings up the full stack (Airflow LocalExecutor + its metadata DB + Oracle Database Free).
Airflow's UI/API is at `http://localhost:8080` (`admin`/`admin`); Oracle is reachable at
`localhost:1521/FREEPDB1` (`admin`/`admin`).

For CPU/RAM/disk requirements, `.wslconfig` sizing, networking caveats, and first-boot
troubleshooting (including a known permission gotcha on first boot), see
**[docs/environment.md](docs/environment.md)**.

This README covers only what Phase 1 (environment setup) delivers — the full clone-to-first-ingest
walkthrough (DAG triggering, CSV processing, Oracle loading) is documented once those phases land.

## Notes & Q&A

### Q: To run Python operators on a different executor than Airflow's own scheduler/executor, do we have to use DockerOperator, or is there another architecture approach?

There are a few standard patterns, from heaviest to lightest isolation:

1. **`KubernetesPodOperator`** — each task run gets its own pod, fully separate container. Used by the sibling reference project (`airflow-platform`). Explicitly out of scope here — no Kubernetes at all in this project.
2. **`DockerOperator`** — each task run gets its own Docker container, spun up by the scheduler/worker via the Docker socket (needs the Airflow container to have Docker-in-Docker or docker-socket-mount access). Same idea as K8s but simpler infra, still a distinct container per task.
3. **`ExternalPythonOperator`** (`@task.external_python`) — runs the callable using a *different, pre-existing* Python interpreter/venv on the same machine/container. No new container, just a different `python` binary path. Lighter than Docker, still gives dependency isolation.
4. **`PythonVirtualenvOperator`** (`@task.virtualenv`) — builds an ephemeral venv on the fly per task run (pip-installs whatever deps you declare), then tears it down. Same host, no persistent isolation.
5. **Plain `@task` / `PythonOperator` under `LocalExecutor`** — what this project actually does. `LocalExecutor` already runs each task as a separate OS subprocess forked from the scheduler, so you get process-level isolation (a crash in one task doesn't kill the scheduler), but it's still the same container, same Python environment, same installed packages as Airflow itself.

This project deliberately picked option 5 — `CLAUDE.md` explicitly says `process_csv` "runs in-process under Airflow's LocalExecutor instead" of `KubernetesPodOperator`, and Kubernetes is called out as out of scope. That's why Plan 01-03 builds a custom Airflow image with `csv-processor` installed directly into Airflow's own environment, rather than reaching for `DockerOperator`/K8s — it keeps the "lightweight" framing: no docker-socket access needed from inside Airflow, no per-task container overhead, just a Python function call. If you ever wanted a middle ground without going full-Docker/K8s, `ExternalPythonOperator` would be the next lightest option — but it's not part of the current plan.

### Q: Would switching to `PythonVirtualenvOperator` (`@task.virtualenv`) from plain `@task`/`PythonOperator` under `LocalExecutor` result in very slow file processing due to ephemeral venvs?

Yes, almost certainly — and that's exactly why this project doesn't use it.

**Where the slowdown comes from:**
1. **Venv creation**: `PythonVirtualenvOperator` builds a fresh virtualenv via `virtualenv`/`venv` and `pip install`s its `requirements` list on every task execution (unless you opt into the `venv_cache_path` caching feature added in newer Airflow versions, which reuses a venv keyed by a hash of the requirements — but even then, the *first* run per cache-key still pays full cost, and cache misses happen anytime requirements/versions drift).
2. **Subprocess spawn + interpreter startup**: even with a cached venv, each call still forks a brand-new Python process pointing at a different interpreter — slower than calling a function already loaded in the running process, but this part is comparatively cheap (milliseconds to ~1s).
3. **Serialization boundary**: arguments and return values cross the process boundary via pickling to/from a temp file, which adds overhead proportional to what you pass — not usually a big deal here if you're just passing file paths/config, not whole dataframes.

For CSV ingestion specifically, cost #1 dominates. If this pipeline processes files frequently (one task per file, or a scheduled DAG run per batch), you'd pay venv-build/pip-install latency on a large fraction of task executions — easily seconds to tens of seconds each, dwarfing the actual CSV parse/validate/load work for small-to-medium files.

Compare that to the plain `@task`/`LocalExecutor` approach this project uses: `csv-processor` is already installed into the Airflow image's own environment (Plan 01-03), so calling `process_csv` pays only normal Python import + function-call cost — no venv build, no pip install, no extra interpreter spin-up beyond what `LocalExecutor` already does per task.

So this is a real, deliberate trade-off in the architecture, not just a "keep it simple" preference: `PythonVirtualenvOperator` would work, but it would meaningfully hurt throughput on a per-file (or per-batch) ingestion workload — which is why `CLAUDE.md`'s in-process/`LocalExecutor` decision is the right one for this project's actual access pattern.
