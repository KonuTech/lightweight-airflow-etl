# Stack Research

**Domain:** Local Airflow CSV→Oracle ETL platform (thin TaskFlow DAG + reusable CSV processing engine + Oracle bulk load)
**Researched:** 2026-08-28
**Confidence:** MEDIUM-HIGH (core versions verified against PyPI's live index; Airflow/oracledb/Pydantic API shapes verified against Context7-fetched official docs; Oracle image tag and CI conventions verified via web search only — see per-row confidence)

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Apache Airflow | 3.3.1 (`apache-airflow`) | Orchestrator — TaskFlow DAG, LocalExecutor, Triggerer, REST API | Current stable 3.x line. Airflow 3 moved the REST API to `/api/v2/...` (v1 is gone) and ships the Triggerer as a first-class component the docker-compose quick-start already wires up — both are load-bearing for this project's two hard requirements (HTTP trigger with runtime `conf`, deferrable file-wait). Confidence: MEDIUM (Context7 `/apache/airflow` docs, official GitHub source) |
| `apache-airflow-providers-standard` | 1.18.0 | Ships `FileSensor` (with `deferrable=True`) and other core sensors/operators | As of Airflow 2.8+/3.x, filesystem/bash/python operators moved out of airflow-core into this provider — must be installed explicitly, it's not bundled implicitly the way "core" once was. Gives an off-the-shelf deferrable file sensor instead of hand-rolling one from scratch (see Pattern note below). Confidence: MEDIUM |
| Python | 3.12 | Runtime for Airflow, `csv_processor` package, generator, tests | Airflow 3.3.1 supports 3.10–3.14 (`!=3.15`); oracledb, Pydantic 2, ruff, mypy, pytest, Faker, clevercsv, charset-normalizer all declare 3.9/3.10+ compatibility. 3.12 is the sweet spot: mature wheel availability for all C-extension-backed deps (oracledb, clevercsv), one full year+ of ecosystem shakeout past 3.13, and avoids being an early adopter on whichever Python Airflow's own CI has least soak time on. Confidence: HIGH (PyPI classifiers cross-checked directly) |
| `oracledb` (python-oracledb) | 4.0.2 | Oracle Database driver — connections + bulk load via `executemany()` | Oracle's own actively maintained driver, official successor to `cx_Oracle` (which is in maintenance-only mode). Default **thin mode** needs no separate Oracle Client install — critical for a "clone and go" WSL/Docker Desktop setup. `cursor.executemany(sql, rows, batcherrors=True)` is the array-bind bulk-insert primitive; Oracle has no `COPY`, so this *is* the bulk path. Confidence: MEDIUM (Context7 `/oracle/python-oracledb` docs) / version number HIGH (PyPI) |
| Pydantic | 2.13.4 | Validate `config.json` once per run, before CSV processing starts | v2's `model_validate_json()` / `model_validate()` raises a single `ValidationError` that collects **every** field error at once (`err.errors()`), matching the spec's "fail fast on config, but tell me everything wrong with it" requirement. Deliberately **not** used per-CSV-row (see What NOT to Use). Confidence: MEDIUM (Context7 `/pydantic/pydantic` docs) / version HIGH (PyPI) |
| `gvenzl/oracle-free` (Docker image) | `23.26.2-faststart` (pin exact digit-version + faststart, not `latest`) | Oracle Database Free container for docker-compose | De facto community-standard Oracle Free image (built from Oracle's own binaries via the `oracle/docker-images` project, referenced directly in the seed spec). The `-faststart` variant ships a pre-baked datafile set that cuts container boot from minutes to ~10-20s — meaningful for a local dev inner loop and CI. Use the non-`slim` full variant unless image size becomes a real constraint; `slim` strips components (e.g., some NLS/character-set and Java options) that are cheap insurance to keep during early development. Confidence: LOW (web search only — Docker Hub tag listing corroborates the version/tag scheme but this needs a one-time manual pull-and-boot check before locking it into docker-compose) |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `clevercsv` | 0.8.5 | CSV dialect (delimiter/quoting) sniffing | Vendor per the two-tier reuse decision — reference repo's `detect/dialect.py` uses this and is near-zero-coupling to port. Use for the "detect delimiter/quote-char" step before parsing; don't hand-roll dialect sniffing. |
| `charset-normalizer` | 3.5.1 | Encoding detection for CSV files of unknown/mixed encoding | Vendor per two-tier reuse (`detect/encoding.py`). Modern, MIT-licensed, faster and more accurate than `chardet` for this use case; use it rather than assuming `utf-8` blindly, since generated files may deliberately include edge-case encodings. |
| `Faker` | 40.37.0 | Deterministic-ish realistic test data (names, etc.) in the CSV generator | Use with a fixed `Faker.seed(n)` to keep the generator's "valid" rows deterministic across runs while still looking like realistic data; combine with plain `random.Random(seed)` for the numeric/date fields and the deliberate invalid-row injection, which Faker doesn't model well. |
| `python-dateutil` | current (2.9.x) | Fallback flexible date parsing during **normalization exploration only** — not for validation | Do NOT use for the actual date **validator** (spec explicitly wants strict `datetime.strptime(value, configured_format)` rejection of anything not matching the configured format — that's the whole point of catching bad dates). Only reach for it if the CSV generator itself needs to emit varied-but-plausible date strings. |
| `pytest` | 9.1.1 | Test runner — unit, Oracle integration, end-to-end | Standard choice; no rationale needed beyond "this is what everyone uses." Pair with `pytest-asyncio` if the custom Trigger's `run()` gets a direct unit test (most Trigger testing instead goes through Airflow's own trigger-test harness / integration test). |
| `testcontainers` (Python, `testcontainers[oracle]` if available, else raw `docker` SDK) or a docker-compose-based CI job | current | Spin up a real Oracle container for integration tests | Spec explicitly requires "Oracle integration tests against a real container, not mocked." `testcontainers-python` is the standard way to do this from pytest with automatic container lifecycle management; alternative is a docker-compose service that CI starts before the test job (simpler, less new dependency surface — worth deciding in the phase that builds Oracle integration tests, not now). |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| `uv` (0.12.7) | Python package/dependency manager, lockfile, venv | Fast, single-binary, `pyproject.toml`-native; `uv sync --locked` in CI enforces the lockfile hasn't drifted. Standard 2026 default over pip/poetry for new Python projects. |
| `ruff` (0.16.5) | Lint + format (replaces flake8 + black + isort) | One Rust-based tool for both `ruff check` and `ruff format --check`; this is what "minimal CI" means in practice now — one tool, two invocations. |
| `mypy` (2.3.1) | Static type checking | Spec requires type hints throughout the CSV engine; `mypy` is the conventional enforcement tool for a `pyproject.toml`-based package. (`pyrefly` is a newer, faster alternative gaining traction — stick with `mypy` for now since it's the safer, better-documented default; revisit only if CI type-check time becomes a real bottleneck.) |
| `astral-sh/setup-uv` GitHub Action (`@v10.0.1`, pin the exact immutable tag) | Installs `uv` in CI | As of `setup-uv` v8+, releases are immutable per-version tags — floating major-version tags (`@v8`) no longer resolve the way they used to on many other actions, so pin the full `vX.Y.Z` tag exactly. |
| `docker-compose` | Local orchestration of Airflow + Airflow metadata DB + Oracle Free | Project deliverable per PROJECT.md — not pre-provisioned. Base it on Airflow's own official `docker-compose.yaml` quick-start (adds `airflow-triggerer` service, which this project actually needs, unlike a plain scheduler+webserver+worker setup) plus one more service block for `gvenzl/oracle-free`. |

## Installation

```bash
# Core
uv add "apache-airflow==3.3.1" "apache-airflow-providers-standard==1.18.0" \
       "oracledb==4.0.2" "pydantic==2.13.4"

# Supporting (CSV engine + generator)
uv add "clevercsv==0.8.5" "charset-normalizer==3.5.1" "Faker==40.37.0"

# Dev dependencies
uv add --dev "pytest==9.1.1" "ruff==0.16.5" "mypy==2.3.1"
```

Note: Airflow's own installation is normally pinned against its published constraints file
(`https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt`) rather
than resolved freely — verify this constraints URL exists for 3.3.1/Python 3.12 before finalizing
`pyproject.toml`, since Airflow's dependency graph is notoriously easy to break without it.

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|--------------------------|
| `oracledb` thin mode | `oracledb` thick mode (with Oracle Instant Client) | Only if a feature thin mode doesn't support is needed (e.g., Native Network Encryption, some older wallet-based auth styles) — not expected for this project's local dev-only Oracle Free target. |
| `oracledb` (successor driver) | `cx_Oracle` | Never for new code — `cx_Oracle` is in maintenance mode and Oracle itself directs new projects to `python-oracledb`. Already excluded per PROJECT.md's pinned decision. |
| Airflow `FileSensor(deferrable=True)` from `apache-airflow-providers-standard` | Hand-written custom `BaseTrigger` + custom sensor operator | Use the hand-written custom Trigger if the file-wait needs project-specific semantics the stock FileSensor doesn't give you cheaply — e.g., matching a glob/regex **pattern** (`customers_*.csv`) rather than one exact path, or returning which specific file matched via the `TriggerEvent` payload for downstream tasks. Given the spec's requirement to resolve a *pattern*, expect to actually need a small custom Trigger subclassing the same async-polling shape FileSensor uses, not the stock sensor verbatim — treat FileSensor's source as the reference implementation to copy the shape of. |
| `gvenzl/oracle-free:23.26.2-faststart` | `container-registry.oracle.com/database/free` (Oracle's own official registry) | Use Oracle's own registry image if there's an organizational policy requiring images be pulled only from Oracle-controlled registries (some enterprises require this for license/support reasons). For a personal local WSL/Docker Desktop project, `gvenzl/oracle-free` is lower friction (no Oracle account/login wall to pull) and is explicitly built from Oracle's own binaries, not reverse-engineered. |
| `ruff` + `mypy` | `pyrefly` (Meta's newer, faster type checker) or `pyright` | Consider `pyrefly`/`pyright` if CI type-check time on a larger codebase becomes noticeably slow; not a concern at this project's scale (one small package + DAGs + tests). |
| `uv` | `poetry`, plain `pip` + `venv` | `poetry` if the team has existing poetry tooling/CI muscle memory; plain `pip`/`venv` only for the absolute simplest throwaway scripts — neither offers `uv`'s speed or lockfile-enforced-in-CI story for a project this CI-conscious. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|--------------|
| `cx_Oracle` | Maintenance-mode driver; Oracle itself recommends migrating off it; no new features, no thin mode | `oracledb` (python-oracledb) |
| Pydantic validation **per CSV row** | Model construction cost per row at 100K+ rows is real overhead, and Pydantic's raise-on-first-invalid-field model fights the spec's "collect invalid rows and continue" requirement — you'd be catching `ValidationError` per row just to extract fields back out again | Plain, fast row-level validation functions (dataclass/TypedDict + explicit type-cast + collect-errors-in-a-list pattern) driven by the same schema the Pydantic `config.json` model already parsed once |
| Airflow's legacy `/api/v1/...` REST endpoints or Airflow-2-style basic auth examples | Airflow 3's REST API is `/api/v2/...`; v1 documentation/examples floating around from Airflow 2 tutorials will silently 404 or hit the wrong auth flow | `/api/v2/dags/{dag_id}/dagRuns` with Airflow 3's auth manager (token-based `simple_auth_manager` for local dev, or FAB if configured) |
| One-off `KubernetesPodOperator`/`kpo.py`-style patterns from the reference repo | No Kubernetes in this project at all | Airflow's `@task` TaskFlow decorator running `process_csv()` in-process under LocalExecutor |
| Docker image tag `latest` for `gvenzl/oracle-free` or for the Airflow image | Non-reproducible builds — "works on my machine today, breaks next week" is explicitly the failure mode PROJECT.md calls out to avoid | Pin explicit tags everywhere: `gvenzl/oracle-free:23.26.2-faststart`, `apache/airflow:3.3.1-python3.12` (or whatever exact Airflow image tag matches the chosen Python) |
| `chardet` for encoding detection | Slower and less actively maintained than the modern alternative; reference repo already moved on | `charset-normalizer` |
| Celery/Redis, `CeleryExecutor`, `KubernetesExecutor` | No horizontal scaling need at this project's scale; adds services (broker, workers) with nothing to justify the operational cost | `LocalExecutor` (already pinned in PROJECT.md) |

## Stack Patterns by Variant

**If the file-wait needs to match a glob pattern (`customers_*.csv`) and report back which file matched:**
- Write a small custom `BaseTrigger` (async `run()`, `asyncio.sleep`-based poll loop, `pathlib.Path.glob()` — never a blocking `time.sleep`) that yields a `TriggerEvent` carrying the resolved file path.
- Because the stock `FileSensor(deferrable=True)` checks one exact `filepath`, not a pattern — copying its *shape* (defer/resume lifecycle, serialization contract) is right; using it unmodified is not enough for this spec's file-pattern requirement.

**If Oracle integration tests need to run in GitHub Actions (not just locally):**
- Use a `services:` block in the workflow YAML running `gvenzl/oracle-free:23.26.2-faststart` (faststart matters here — a normal Oracle Free boot can eat 2+ minutes of CI time per run) with a `wait-for-it`/healthcheck loop before running `pytest -m oracle`.
- Because starting Oracle as a docker-compose stack from within the job is more moving parts than a single `services:` container Actions already manages for you.

## Version Compatibility

| Package A | Compatible With | Notes |
|-----------|------------------|-------|
| `apache-airflow==3.3.1` | Python `>=3.10,!=3.15` (verified via PyPI classifiers: 3.10–3.14 supported) | Target 3.12 — comfortably inside the supported range with the widest wheel/tooling maturity. |
| `apache-airflow==3.3.1` | `apache-airflow-providers-standard==1.18.0` | Install both explicitly; the provider is not bundled by default in 3.x the way filesystem/bash operators once were treated as "core." |
| `oracledb==4.0.2` | Python `>=3.9` | No conflict with the 3.12 target. |
| `pydantic==2.13.4` | Python `>=3.9` | No conflict; also confirm any Airflow-pinned Pydantic version in Airflow's constraints file doesn't collide — Airflow itself depends on Pydantic internally for some Airflow 3 API server models, so resolve `pyproject.toml` against Airflow's official constraints file (see Installation note) rather than letting `uv` freely pick a Pydantic version that Airflow's own server code wasn't tested against. |
| `clevercsv==0.8.5` | Python `>=3.9.0` | No conflict. |
| Airflow's Triggerer component | Requires `LocalExecutor` or any executor — Triggerer is a separate always-on service regardless of executor choice | docker-compose must include an `airflow-triggerer` service alongside scheduler/webserver/(local)worker; this is easy to forget when adapting an older Airflow 2 docker-compose example that predates deferrable operators being common. |

## Sources

- Context7 `/apache/airflow` (benchmark score 75.3, High reputation) — REST API v2 dagRuns trigger + conf payload shape, Deferrable Operators & Triggers authoring guide (`airflow-core/docs/authoring-and-scheduling/deferring.rst`), `FileSensor` deferrable mode (`providers/standard/docs/sensors/file.rst`). Confidence: MEDIUM.
- Context7 `/oracle/python-oracledb` (benchmark score 79.1, High reputation) — `executemany()` + `batcherrors=True` + `getbatcherrors()` pattern, `setinputsizes()`, `batch_size` chunking, thin-mode error semantics (`doc/src/user_guide/batch_statement.md`, `samples/notebooks/3-DML.ipynb`). Confidence: MEDIUM.
- Context7 `/pydantic/pydantic` (benchmark score 79.9, High reputation) — `model_validate_json`, `ValidationError.errors()` collecting all field errors at once. Confidence: MEDIUM.
- PyPI JSON API (`pypi.org/pypi/<pkg>/json`), queried live 2026-08-28, for exact current version numbers and `requires_python` classifiers of: `apache-airflow`, `apache-airflow-providers-standard`, `oracledb`, `pydantic`, `pytest`, `ruff`, `mypy`, `uv`, `Faker`, `clevercsv`, `charset-normalizer`. This is the canonical package registry, not a search-engine summary — treat version numbers pulled this way as HIGH confidence even though the generic provider-confidence tiering tool has no dedicated "registry API" bucket for it.
- Docker Hub API (`hub.docker.com/v2/repositories/gvenzl/oracle-free/tags`), queried live 2026-08-28, for current tag list — confirms `23.26.2` is the latest version tag with `full`/`slim`/`faststart` variants. Confidence: MEDIUM for the tag list itself (Docker Hub API is authoritative for what tags exist), LOW carried over from the web-search-sourced interpretation of what each variant strips/keeps — worth a one-time manual `docker run` sanity check before locking into docker-compose.
- Web search (WebSearch tool, no Context7/registry entry) — Oracle Database Free image ecosystem overview (gvenzl/oci-oracle-free GitHub, geraldonit.com release-announcement blog posts), and 2026 GitHub Actions Python CI conventions (`astral-sh/setup-uv` immutable-tag policy, `uv sync --locked` + ruff + mypy + pytest pipeline shape). Confidence: LOW — corroborated by a second independent hit each (Docker Hub tag list for Oracle; multiple independent blog/handbook sources converging on the same `uv`+`ruff`+`mypy`+`pytest` shape for CI) but not sourced from an authoritative registry/doc, so treat as directionally correct rather than pinned fact — re-verify `astral-sh/setup-uv`'s exact latest tag at implementation time since action releases move faster than this research's cache TTL.

---
*Stack research for: Local Airflow CSV→Oracle ETL platform*
*Researched: 2026-08-28*
