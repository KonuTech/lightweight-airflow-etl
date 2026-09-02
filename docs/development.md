# Development: Local Workflow, Contribution Notes, CI Troubleshooting

This document covers all three things D-17 asks for in one place: (a) the local dev
workflow — running the test suites, resetting Oracle, regenerating fixtures, linting/
type-checking before pushing; (b) architecture/contribution notes — code layout and how to add a
new dataset; and (c) CI/troubleshooting — exactly what CI runs (copied verbatim, never
paraphrased, from `.github/workflows/ci.yml`) and how to debug a failing PR check locally. See
`docs/architecture.md` for the system-level design and `docs/environment.md` for resource
sizing/first-boot setup.

## Local Dev Workflow

### Running the test suites

Three suites, in increasing order of what they need running:

```bash
uv run pytest tests/unit/ -x          # no Docker/Oracle/Airflow needed
uv run pytest tests/integration/ -x   # needs `make up` (real Oracle, no Airflow trigger)
uv run pytest tests/e2e/ -x           # needs `make up` (real Oracle + real Airflow HTTP trigger)
```

`tests/unit/` covers config/parsing/type-conversion/validation logic with no external
dependencies. `tests/integration/` exercises `csv_processor.load`/`engine.process()` against a
real running Oracle container (never mocked). `tests/e2e/` (TEST-03) is the strongest proof: it
triggers the real `csv_to_oracle_ingest` DAG over Airflow's REST API, polls for a genuine `deferred`
`wait_for_file` state *before* dropping the fixture file, then asserts real row counts in
`<dataset>_valid`/`<dataset>_invalid` — not just `DagRun.state == "success"`.

### Resetting the Oracle container

```bash
make reset   # docker compose down -v -- full wipe, stops containers AND removes volumes
make up      # docker compose up -d --wait -- brings the full stack back up fresh
```

`make down` (containers only, volumes stay) is for a quick stop/restart; `make reset` is for
"start genuinely clean" — e.g. after a schema change to `docker/oracle/init/*.sql`, since those
scripts only run on a fresh volume's first boot.

### Regenerating fixtures

```bash
make generate         # deterministic business-row CSVs for both datasets (customers, orders)
make fixtures          # materialize the byte-level fixture corpus + its digest oracle
make fixtures-verify   # regenerate the corpus to a temp dir, diff SHA-256 against the committed oracle
```

### Linting and type-checking locally, before pushing

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
```

Or all three at once via `make lint` (see "Reproducing CI Locally" below — this is the exact same
sequence CI runs).

## Architecture / Contribution Notes

### Code layout

```
packages/csv-processor/   -- the reusable, Airflow-agnostic CSV engine (ENGINE-09).
                              Zero airflow.* imports anywhere in this tree.
airflow/dags/              -- the config-driven csv_to_oracle_ingest DAG, the customers_orders_report DAG (senses both
                              datasets' ingestion, materializes the business report), and
                              _common/ helpers (including the custom OraclePartitionReadyTrigger).
                              Thin orchestration only -- zero CSV/Oracle logic of its own.
benchmark/                 -- throwaway naive-vs-bulk Oracle write comparison (TEST-04).
                              Never imported by csv_processor; deliberately outside it.
scripts/                   -- operational scripts: trigger_dag.sh, verify_environment.py,
                              verify_evidence.sql, regenerate_readme_summary.py, dag_polling.py
generator/                 -- deterministic CSV fixture generator (GEN-01)
configs/                   -- per-dataset config.json + shared defaults.json (docs/configuration.md)
docker/                    -- Dockerfile(s) + Oracle init DDL (docker/oracle/init/*.sql)
tests/unit/ tests/integration/ tests/e2e/ -- see "Running the test suites" above
```

Every module in `packages/csv-processor/src/csv_processor/` follows the same docstring
convention used throughout this repo: cite the decision ID (`D-01`, `ENGINE-07`, `T-04-01`, etc.)
a piece of code implements, and explain *why* a line exists, not just what it does — see
`load.py`/`engine.py`'s own module docstrings for the pattern to match. New modules should follow
the same convention.

### How to add a new dataset

Per `DAG-05`'s zero-branching design, adding a dataset never requires a DAG code change:

1. Write `configs/datasets/<name>.json` — follow `docs/configuration.md`'s two real examples
   (`customers.json`/`orders.json`) for the exact shape.
2. Add the matching Oracle DDL: `<name>_valid`/`<name>_invalid` tables (mirror
   `docker/oracle/init/02_customers.sql`'s structure — `INTERVAL` daily partitioning on
   `ingested_at`) plus a widened-`_invalid`-columns migration (mirror
   `docker/oracle/init/04_widen_invalid_columns.sql` — every `_invalid` data column becomes
   nullable `VARCHAR2` at its current size, see `docs/oracle.md` for why).
3. Add the new dataset name to `airflow/dags/csv_to_oracle_ingest.py`'s `dataset` `Param`'s `enum` list —
   the **only** DAG-file edit a new dataset ever needs.
4. `make reset && make up` (fresh Oracle volume, so the new init scripts actually run), then
   `uv run python generator/generate_csv.py --dataset <name>` to produce a fixture file and
   `scripts/trigger_dag.sh <name> configs/datasets/<name>.json` to trigger a real run.

No other file changes — `process_csv_task` calls `csv_processor.engine.process()` generically for
whatever `dataset`/`config_path` the runtime `conf` carries.

### Coding conventions

Matches `CLAUDE.md`'s own established style: docstrings cite the decision ID they implement and
explain *why*, not just *what*; every Pydantic model in `csv_processor.config`/`csv_processor
.models` is `frozen=True, extra="forbid"`; SQL identifiers built from config-sourced strings are
always re-validated via `is_safe_identifier()` immediately before interpolation (defense-in-depth,
never trust config-load-time validation alone — see `docs/oracle.md`'s bulk-insert section).

## CI / Troubleshooting

### What CI actually runs

Two required checks, both triggered `on: pull_request` (never `pull_request_target` — a
forked-repo PR never gains write access or secrets under `pull_request`'s default semantics; no
`permissions:` block anywhere in `ci.yml`, workflow- or job-level).

**`lint-type-unit`** — the exact `run:` steps, copied verbatim from `.github/workflows/ci.yml`
(never paraphrased — a paraphrased command that silently differs from the real one is exactly the
drift DOC-01 exists to prevent):

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pytest tests/unit/ -x
```

(Preceded by `actions/checkout@v7.0.1`, `astral-sh/setup-uv@v10.0.1`, `uv sync --locked` — no
Docker, no Oracle, no Airflow.)

**`oracle-e2e`** — stands up this project's own, real, **unmodified** `docker-compose.yml` stack
inside the CI runner (never GitHub Actions' native `services:` key — see the comment at the top of
`ci.yml` for why: no `depends_on` ordering between service containers, and every service container
starts before `actions/checkout` even runs, so it can't bind-mount `docker/oracle/init/*.sql` or
`airflow/dags/`). Exact `run:` steps, copied verbatim:

```bash
cp .env.example .env
mkdir -p docker/airflow
echo '{"admin": "admin"}' > docker/airflow/simple_auth_manager_passwords.json.generated
```
```bash
docker compose version
```
```bash
docker compose up -d --wait
```
```bash
uv run pytest tests/e2e/ -x
```
```bash
docker compose down -v
```

Reproduce the whole `oracle-e2e` job locally by running exactly those commands in order (after
`uv sync --locked`) against a fresh clone — this is the same sequence `make verify-phase6` runs
(see the Makefile).

### Configuring required status checks (manual, one-time repo setup)

A workflow with `lint-type-unit`/`oracle-e2e` jobs running `on: pull_request` does **not**, by
itself, block a PR merge if either job fails — "required" status checks are a **repository
setting**, separate from the workflow YAML. Enable them once, manually:

1. Repo **Settings → Branches → Branch protection rules** → add/edit a rule for `main`.
2. Enable **"Require status checks to pass before merging"**.
3. Search for and select both job names exactly as they appear in `ci.yml`: **`lint-type-unit`**
   and **`oracle-e2e`**.

The clearest sign this step was skipped: a PR merges successfully despite a red CI check.

### CI disk pressure fallback (only if actually observed)

This project's own measured footprint is ~14.8 GB (images + volumes, `docs/environment.md`'s
Phase 1 measurement) — comfortably within a local dev machine's free disk, but CI runners' free
disk headroom is tighter and less predictable. If `docker compose up`/`pull` ever fails in the
`oracle-e2e` job specifically with `no space left on device` (not observed as of this writing —
this is a documented fallback, not a pre-applied fix), switch `docker-compose.yml`'s Oracle image
tag to `gvenzl/oracle-free:23.26.2-slim-faststart` (1.24 GB vs. the current `23.26.2-faststart`'s
1.55 GB).

### A note on Action version pinning

Every third-party GitHub Action this project uses is pinned to an exact **version tag**
(`astral-sh/setup-uv@v10.0.1`, `stefanzweifel/git-auto-commit-action@v7.2.0`,
`actions/checkout@v7.0.1`), not a full commit SHA. This is a deliberate choice for this project's
local-dev-scale risk profile (06-RESEARCH.md's Security Domain section) — version-tag pinning
already prevents silent floating-tag drift (`@v10` resolving to whatever `v10.x.y` is newest at
run time), which is the main practical risk at this project's scale. Full SHA-pinning (immune even
to a maintainer re-tagging an existing version, at the cost of needing a Dependabot-style updater
to keep hashes current) is noted here as optional future hardening, not a gap in the current setup.

## Reproducing CI Locally (`make lint`, `make verify-phase6`)

```bash
make lint             # uv run ruff check . && uv run ruff format --check . && uv run mypy .
make benchmark         # uv run python -m benchmark.run_benchmark --mode bulk --rows 100000
make verify-evidence   # docker compose exec -T oracle sqlplus ... < scripts/verify_evidence.sql
make verify-phase6     # unit + e2e suites, then lint, then verify-evidence -- requires `make up` first
```

`make verify-phase6` is this project's final combined local gate — see the Makefile's own comment
on the target for the exact composition and why it mirrors `verify-phase4`/`verify-phase5`'s
established "unit suite first, then phase-specific live checks" shape.
