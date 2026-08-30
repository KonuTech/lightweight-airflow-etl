# Phase 6: End-to-End Verification, Benchmark, CI & Docs - Research

**Researched:** 2026-08-29
**Domain:** Automated e2e testing over Airflow's REST API, a genuine naive-vs-bulk Oracle benchmark, GitHub Actions CI (lint/type/unit + real Oracle+Airflow service containers as a required PR check), CI-driven README auto-commit, and full topic-doc documentation.
**Confidence:** MEDIUM-HIGH (GitHub Actions syntax/permissions/action tags verified live against GitHub's REST API and official docs this session; Airflow/oracledb/Pydantic mechanics verified via Context7 official docs and by reading this project's own installed `airflow.sdk` package inside the running container; benchmark harness design derived by reading this project's own `engine.py`/`load.py` source, not assumed)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Benchmark (TEST-04)**
- D-01: Genuine, separate naive-loop Oracle baseline (single `cursor.execute()` per row in a loop) — NOT `executemany()` run with `chunk_size=1`.
- D-02: Benchmark dataset is `customers` (6-column schema) at ~100K rows. `generator/generate_csv.py --rows 100000` needs no new profile.
- D-03: Benchmark calls `csv_processor.engine.process()` / `csv_processor.load` directly, bypassing Airflow entirely.
- D-04: Naive-loop baseline lives in a new top-level `benchmark/` directory (`benchmark/naive_loader.py` + `benchmark/run_benchmark.py`) — not inside `packages/csv-processor/`, not inside `tests/benchmark/`.
- D-05: Results committed to `docs/benchmark.md` — side-by-side comparison table (rows/sec, peak memory, Oracle load time), explicit speedup ratio, run metadata, and a raw per-chunk timing breakdown for the chunked run.

**End-to-end test & CI scope (TEST-03, CI-01)**
- D-06 (scope expansion, user-approved): GitHub Actions spins up Oracle + Airflow as real service containers and runs the e2e test in the PR pipeline itself, not just locally. Reversibility: costly.
- D-07: The Oracle+e2e job is a required status check, on every PR (not just main pushes).
- D-08: e2e test must prove the full trigger chain: file appears → deferred `wait_for_file` genuinely wakes (not polling) → `process_csv` runs → correct/incorrect rows land in Oracle. Stronger than DAG-03's Phase 5 structural proof — needs real Oracle query results.

**Evidence & Executive Summary (new, user-initiated, folds into TEST-03/DOC-01)**
- D-09: Evidence captured by querying Oracle directly via a committed SQL script + Makefile target (e.g. `scripts/verify_evidence.sql` + `make verify-evidence`) — reproducible, not ad hoc.
- D-10: A customers⋈orders business report is part of that evidence — join on `customer_id`, aggregate by region (Claude's substitution: `customers.country`, no literal region column) and date (month-of-`orders.order_date`), with a count and numeric metrics (order count + total/average `amount`). Does not reopen the unenforced-FK out-of-scope decision — read-only JOIN for reporting evidence.
- D-11: README.md gets a top "Executive Summary" section: (a) total/valid/invalid row counts per dataset from the latest run, (b) a deferred-wake proof line, (c) the business report, top N rows only.
- D-12 (non-default choice): Executive Summary is live/regenerated, not a static snapshot — regenerated via CI auto-committing the refreshed section to README.md after every successful merge to `main`. Reversibility: costly.
- D-13: CI auto-commit step uses `[skip ci]` in the bot's commit message; needs `permissions: contents: write` (or equivalent PAT) — flagged for this research to confirm exact syntax/permissions.

**Lint/type-check scope (CI-01)**
- D-14 (non-default choice): `mypy`/`ruff` cover the whole repo including `airflow/dags/`. Versions pinned: `ruff==0.16.5`, `mypy==2.3.1`, via `uv add --dev`.

**Docs (DOC-01)**
- D-15: Full topic-doc split — README.md + `docs/architecture.md` + `docs/configuration.md` + `docs/csv-engine.md` + `docs/oracle.md` + `docs/development.md`, in addition to the already-existing `docs/airflow-dag.md` and `docs/environment.md`.
- D-16: README.md is "summary + links," not a full walkthrough — short overview + links into topic docs. Executive Summary (D-11/D-12) sits at the top, ahead of the summary/links content.
- D-17: `docs/development.md` covers local dev workflow (tests, reset, fixtures, lint/type locally), architecture/contribution notes (layout, adding a dataset, conventions), and CI/troubleshooting (what CI runs, how to debug a failing PR check locally).

### Claude's Discretion
- Exact peak-memory measurement method for the benchmark (`tracemalloc`, `resource.getrusage`, `psutil`, `/usr/bin/time -v`).
- Exact GitHub Actions workflow YAML structure (job names, matrix strategy, `uv` caching approach) — `astral-sh/setup-uv@v10.0.1` pinned per STACK.md, re-verify at implementation time.
- Exact content/SQL of `scripts/verify_evidence.sql` and the Executive Summary's marker syntax in README.md.
- Whether the deferred-wake proof (D-11b) is captured by the same e2e test/evidence script or a separate check.
- `verify-phase6` Makefile target composition.

### Deferred Ideas (OUT OF SCOPE)
None — discussion stayed within phase scope. The Executive Summary / business-report additions are scope expansions within Phase 6, not new capabilities for a different phase.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| TEST-03 | An end-to-end test exercises the full path: HTTP request → DAG run → config → file detection → `csv_processor` → Oracle VALID/INVALID tables | Architecture Patterns §1 (e2e test harness reusing `scripts/trigger_dag.sh`'s REST flow), Code Examples §1-2, Pitfalls §1/§4 |
| TEST-04 | A performance benchmark at ~100K rows compares row-by-row vs. chunked/bulk processing and records rows/sec, peak memory, and Oracle load time | Architecture Patterns §2 (benchmark harness design reusing `process_chunks()`), Code Examples §3, Don't Hand-Roll |
| CI-01 | A minimal GitHub Actions pipeline runs lint, type check, and unit tests on every PR (widened by D-06/D-07 to include Oracle+e2e as a required check) | Architecture Patterns §3-4, Code Examples §4-6, Pitfalls §2/§3/§5/§6, Package Legitimacy Audit |
| DOC-01 | README and `docs/` let a new developer go from `git clone` to a completed HTTP-triggered ingestion with no undocumented manual steps | Architecture Patterns §5, Pitfalls §7 |
</phase_requirements>

## Summary

This phase has two genuinely separate engineering deliverables (a benchmark harness, an e2e test) sitting inside a much larger CI/docs completion-quality wrapper. The benchmark question is already answered by reading this project's own code: `csv_processor.engine.process_chunks(file_path, config)` [VERIFIED: packages/csv-processor/src/csv_processor/engine.py:33-35] is the public generator both the naive and chunked benchmark paths should reuse for CSV parsing/validation — the *only* thing that should differ between the two runs is the Oracle write strategy (`load.insert_rows()`'s `executemany()` vs. a new `benchmark/naive_loader.py` function doing one `cursor.execute()` per row). This isolates exactly the variable PITFALLS.md's "Performance Trap 1" and D-01 are both about, without confounding the comparison with two different CSV-parsing implementations.

The CI question is more consequential: GitHub Actions' native `services:` key does **not** support `depends_on`/ordering between service containers [CITED: community sources, cross-checked], and every service container starts **before any workflow step runs — including `actions/checkout`** [CITED: GitHub Docs "About service containers"], so a `services:`-block Oracle container cannot bind-mount this project's own `docker/oracle/init/*.sql` scripts the way `docker-compose.yml` already does locally. Given the project's 6-service Airflow+Oracle topology already depends on `depends_on: condition: service_healthy` chains and container-DNS hostnames (`oracle`, `airflow-apiserver`) that `docker-compose.yml` already fixed after real live-trigger debugging in Phase 5, the lowest-risk path is to **run the project's own `docker-compose.yml` unmodified as a CI step** (`docker compose up -d --wait`, after checkout) for the Oracle+e2e job, rather than hand-translating the stack into GitHub Actions' native `services:` syntax. This satisfies D-06's "real service containers" intent (they are still real, healthchecked containers) while reusing infrastructure already proven to work, with zero drift risk between local dev and CI.

The auto-commit mechanism (D-12/D-13) has one load-bearing correction to the CONTEXT.md's own framing: a commit made with the **default `GITHUB_TOKEN`** does not trigger further Actions runs at all [CITED: stefanzweifel/git-auto-commit-action README] — GitHub suppresses `push`-triggered workflow runs from `GITHUB_TOKEN`-authored commits specifically to prevent this exact infinite-loop class. `[skip ci]` is real GitHub syntax but is only strictly load-bearing if a PAT is used instead of the default token; recommend the default token + `permissions: contents: write` scoped to the one job, with `[skip ci]` kept in the commit message anyway as explicit, defense-in-depth documentation of intent (matches D-13's literal ask, costs nothing).

**Primary recommendation:** Reuse `docker-compose.yml` unmodified as the CI mechanism for Oracle+Airflow (not GitHub Actions' native `services:` key); reuse `process_chunks()` for both benchmark paths, varying only the Oracle write call; use the default `GITHUB_TOKEN` (not a PAT) for the README auto-commit.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| E2E test's HTTP trigger + polling | CI workflow step (test runner) | Airflow REST API (already-running container) | The e2e test is a thin REST client exactly like `scripts/trigger_dag.sh` — it must live in the test/CI tier, never inside `csv_processor` or the DAG itself |
| Deferred-wake proof | Airflow REST API (`taskInstances` endpoint) | e2e test assertion | Already proven manually in Phase 5 (`docs/airflow-dag.md`); this phase automates the same REST query into an assertion, not a new mechanism |
| Benchmark's CSV parse/validate | `csv_processor.engine.process_chunks()` (existing) | `benchmark/run_benchmark.py` (orchestration only) | Must NOT be reimplemented for the benchmark — reusing the real, tested generator keeps the comparison honest (only the DB-write strategy varies) |
| Benchmark's naive Oracle write | `benchmark/naive_loader.py` (new, throwaway) | — | Deliberately outside `csv_processor` (D-04) — never confused with the reusable engine |
| Benchmark's bulk Oracle write | `csv_processor.load.insert_rows()` (existing) | — | Already the real, tested `executemany()` path (LOAD-01/LOAD-02) — reused verbatim, not reimplemented |
| Lint/type/unit CI job | GitHub Actions workflow (no service containers) | `uv`/`ruff`/`mypy`/`pytest` (existing tooling) | Fast job, no external dependencies — matches CI-01's literal minimal scope |
| Oracle+e2e CI job | GitHub Actions workflow step running `docker compose up` | The project's own `docker-compose.yml` (existing, unmodified) | Airflow's 5-process topology + Oracle's init-script volume mount don't map onto GH Actions' native `services:` key (no `depends_on`, services start before checkout) — compose-in-CI avoids reinventing already-solved infrastructure |
| Evidence capture (SQL query + business report) | `scripts/verify_evidence.sql` + a thin Python/SQL runner (new) | Oracle (already running from the e2e job) | Read-only queries against tables `process()` already populated — no new write path |
| Executive Summary regeneration | A new CI job/step (push-to-main only) | README.md (existing) | Must run AFTER a real ingestion run so the numbers are genuine, not synthetic — sequenced after the Oracle+e2e job's own ingestion, on `push` to `main` only |
| README auto-commit | GitHub Actions job with `permissions: contents: write` | `stefanzweifel/git-auto-commit-action@v7.2.0` (or a manual `git commit`/`push`) | Default `GITHUB_TOKEN`, not a PAT — avoids both secret management and the infinite-loop risk by construction |

## Standard Stack

### Core

| Library / Action | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `ruff` | 0.16.5 | Lint + format | Already pinned in STACK.md/PROJECT.md; re-verified live this session — `0.16.5` is still PyPI's current latest version for `ruff` [VERIFIED: pypi.org/pypi/ruff/json, queried 2026-08-29]. |
| `mypy` | 2.3.1 | Static type checking | Already pinned; re-verified live this session — `2.3.1` is still PyPI's current latest version for `mypy` [VERIFIED: pypi.org/pypi/mypy/json, queried 2026-08-29]. |
| `astral-sh/setup-uv` | `v10.0.1` | Installs `uv` in CI | Confirmed still the latest immutable release tag via GitHub's own Releases API this session (`published_at: 2026-08-14T08:55:32Z`, `immutable: true`) [VERIFIED: api.github.com/repos/astral-sh/setup-uv/releases]. STACK.md's pin needs no update. |
| `actions/checkout` | `v7` (verify exact patch, e.g. `v7.0.1`) | Checks out the repo before any other step | Latest tag confirmed live via GitHub Releases API this session: `v7.0.1`, published `2026-07-20` [VERIFIED: api.github.com/repos/actions/checkout/releases]. |
| `stefanzweifel/git-auto-commit-action` | `v7.2.0` | Commits + pushes the regenerated Executive Summary section back to README.md | Latest tag confirmed live via GitHub Releases API this session: `v7.2.0`, published `2026-06-28` [VERIFIED: api.github.com/repos/stefanzweifel/git-auto-commit-action/releases]. Handles the default-committer-identity and diff-detection boilerplate so the workflow step doesn't hand-roll `git add`/`git commit`/`git push` + no-op detection. |
| `docker compose` (v2 CLI plugin) | Pre-installed on `ubuntu-latest` | Stands up the project's own 6-service stack inside the CI job | [ASSUMED — low risk] `ubuntu-latest` GitHub-hosted runners ship Docker Engine + the `docker compose` v2 CLI plugin pre-installed; verify with a cheap `docker compose version` as the CI job's first step so a missing/mismatched version fails fast and visibly rather than mid-job. |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `resource` (stdlib) | n/a | Peak-RSS memory measurement for the benchmark | Recommended over `tracemalloc` for the benchmark's memory metric: `tracemalloc` only tracks Python-heap allocations and would **undercount** memory used by `oracledb`'s C-extension buffers during `executemany()`'s array binding — `resource.getrusage(resource.RUSAGE_SELF).ru_maxrss` captures true peak RSS for the whole process (Linux `ru_maxrss` is in KB), giving an apples-to-apples number across both the naive and chunked runs regardless of where the memory is allocated. |
| `pydantic.mypy` (bundled with `pydantic`, not a separate install) | matches installed `pydantic==2.13.4` | mypy plugin so `mypy` correctly understands Pydantic v2's `ConfigDict`/`model_validator`-generated `__init__` signatures | Without this plugin, mypy under `disallow_untyped_defs`/strict-ish settings will misreport or miss errors on every `DatasetConfig`/`ProcessingResult`-shaped model construction across the whole repo — required, not optional, once D-14 puts `csv_processor.config`/`csv_processor.models` under whole-repo mypy [VERIFIED: Context7 /pydantic/pydantic, docs/integrations/mypy.md]. |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|-------------|-----------|----------|
| `docker compose up -d --wait` as a CI step (Oracle+Airflow) | GitHub Actions native `services:` block per container | Native `services:` blocks start before `actions/checkout` (can't bind-mount `docker/oracle/init/*.sql` or `airflow/dags/`) and don't support `depends_on` ordering between services [CITED, cross-checked] — would require re-deriving all of Phase 5's already-solved container-DNS/health-order fixes a second time, in a different syntax, with real drift risk against `docker-compose.yml`. |
| `resource.getrusage` for peak memory | `psutil.Process().memory_info().rss` sampled in a loop | `psutil` is a real, well-known package but is a new dependency for a one-off benchmark script; `resource` is stdlib and gives the same peak-RSS number with zero new dependency surface. Use `psutil` only if a continuous (not just peak) memory timeline is wanted later — not required by D-05's spec (peak memory only). |
| `stefanzweifel/git-auto-commit-action` for the README write-back | Hand-rolled `git config`/`git add`/`git commit`/`git push` steps | The action is a thin, well-known wrapper (confirmed live via its README this session) that already handles no-op-diff detection (skips the commit if nothing changed) and default bot-identity — hand-rolling saves zero real complexity and adds more surface to get wrong (e.g. forgetting to skip an empty commit). |
| Default `GITHUB_TOKEN` for the auto-commit | A Personal Access Token (PAT) stored as a repo secret | A PAT-authored commit DOES trigger downstream workflow runs (looks like a human push), reintroducing exactly the infinite-loop risk D-13 is trying to prevent, and adds secret-rotation/scope-management overhead with no benefit here — only reach for a PAT if the auto-commit must itself re-trigger CI (not needed; the Executive Summary regen only needs to run once per merge). |

**Installation:**
```bash
uv add --dev "ruff==0.16.5" "mypy==2.3.1"
```
(`pytest==9.1.1` is already a dev dependency from Phase 1 — see `pyproject.toml`'s existing `[dependency-groups]`.)

**Version verification:** All four version-pinned externals above (`ruff`, `mypy`, `astral-sh/setup-uv`, `stefanzweifel/git-auto-commit-action`, `actions/checkout`) were re-verified live this session (2026-08-29) directly against PyPI's JSON API and GitHub's Releases API — not re-derived from training data or STACK.md's cached 2026-08-28 research. No drift found; all pins are current as of this research date.

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | Verdict | Disposition |
|---------|----------|-----|-----------|-------------|---------|-------------|
| `ruff` | PyPI | Long-established (Astral's canonical Rust-based linter/formatter; PyPI project registered years ago) | Not returned by the automated check (PyPI doesn't expose weekly-download counts the way npm does) | `https://docs.astral.sh/ruff` | `SUS` (raw signals: `too-new`, `unknown-downloads`) — **false positive**, see note | Approved override |
| `mypy` | PyPI | Long-established (the canonical Python type checker, maintained since 2016) | Not returned (same PyPI limitation) | `https://www.mypy-lang.org/` | `SUS` (raw signals: `too-new`, `unknown-downloads`) — **false positive**, see note | Approved override |

**Note on the `SUS` verdicts above:** the automated `gsd-tools query package-legitimacy check --ecosystem pypi` seam flagged both packages `too-new`/`unknown-downloads` because its `publishedAt` signal reads the *latest release's* publish timestamp (`ruff==0.16.5` published 2026-08-27; `mypy==2.3.1` published 2026-08-15 — both released within the last two weeks, per this session's own live PyPI query), not the project's actual multi-year age. Both are extremely well-known, high-reputation tools already pinned in `research/STACK.md` and used unmodified across every prior phase of this project. Independently re-verified via a direct PyPI JSON API query this session: `ruff==0.16.5` and `mypy==2.3.1` are each package's exact current latest version [VERIFIED: pypi.org/pypi/{ruff,mypy}/json, queried 2026-08-29] — same versions STACK.md pinned a day earlier, zero drift. Treat as `OK` for planning purposes; no `checkpoint:human-verify` needed given this cross-check, but the planner may still add one if it wants a second pair of eyes on the exact pin before `uv add --dev`.

**Packages removed due to `SLOP` verdict:** none.
**Packages flagged as suspicious `[SUS]`:** `ruff`, `mypy` — both resolved to false positives per the note above (fast-release-cadence artifact of the legitimacy heuristic, not a real risk signal); no action needed beyond the version cross-check already performed.

GitHub Actions used this phase (`astral-sh/setup-uv`, `actions/checkout`, `stefanzweifel/git-auto-commit-action`) are not PyPI/npm packages and are outside the npm/PyPI legitimacy-check seam's scope — each was independently confirmed live against GitHub's own Releases API this session (see Standard Stack table), which is the equivalent authoritative-registry check for a GitHub Action.

## Architecture Patterns

### System Architecture Diagram

```
                    ┌─────────────────────────────────────────────────────┐
                    │  GitHub Actions: pull_request (required checks)      │
                    │                                                       │
  PR opened/updated │  ┌───────────────┐        ┌──────────────────────┐  │
  ─────────────────►│  │ lint-type-unit│        │   oracle-e2e          │  │
                    │  │ (fast, no      │        │  docker compose up   │  │
                    │  │  containers)   │        │  --wait (reuses      │  │
                    │  │  ruff/mypy/    │        │  docker-compose.yml  │  │
                    │  │  pytest tests/ │        │  unmodified)         │  │
                    │  │  unit/         │        │       │              │  │
                    │  └───────┬───────┘        │       ▼              │  │
                    │          │                 │  scripts/trigger_    │  │
                    │          │                 │  dag.sh (reused) ──► │  │
                    │          │                 │  poll deferred state │  │
                    │          │                 │  poll to completion  │  │
                    │          │                 │       │              │  │
                    │          │                 │       ▼              │  │
                    │          │                 │  assert Oracle       │  │
                    │          │                 │  VALID/INVALID rows  │  │
                    │          │                 │  (real SELECT, not   │  │
                    │          │                 │   Airflow state only)│  │
                    │          ▼                 └──────────┬───────────┘  │
                    │   both required → merge blocked until BOTH green     │
                    └──────────────────────────────────────┬────────────────┘
                                                             │ merge to main
                                                             ▼
                    ┌─────────────────────────────────────────────────────┐
                    │  GitHub Actions: push to main only                   │
                    │                                                       │
                    │  docker compose up --wait → trigger real ingestion  │
                    │  runs → scripts/verify_evidence.sql (customers⋈      │
                    │  orders report + counts + deferred-wake proof) →     │
                    │  regenerate README.md's Executive Summary section →  │
                    │  git-auto-commit-action (default GITHUB_TOKEN,       │
                    │  [skip ci] in message, no further CI triggered)      │
                    └─────────────────────────────────────────────────────┘

Local (no Airflow, no CI) — benchmark/run_benchmark.py:
  generator/generate_csv.py --dataset customers --rows 100000
              │
              ▼
  csv_processor.engine.process_chunks(file_path, config)  ◄── SAME call, both runs
              │                                    (parse/validate only ONCE per run)
      ┌───────┴────────┐
      ▼                ▼
  benchmark/        csv_processor.load.insert_rows()
  naive_loader.py   (existing executemany() path)
  (new: one              │
  cursor.execute()       ▼
  per row)          docs/benchmark.md (rows/sec, peak RSS via
      │              resource.getrusage, Oracle load time,
      ▼              per-chunk timing breakdown, speedup ratio)
  same docs/benchmark.md
```

### Recommended Project Structure

```
benchmark/
├── naive_loader.py       # D-04: single cursor.execute() per row, throwaway, never imported by csv_processor
└── run_benchmark.py      # D-03/D-04: orchestrates generate → process_chunks() → {naive|bulk} write, writes docs/benchmark.md
tests/
├── e2e/                  # NEW — TEST-03, kept separate from tests/unit/ and tests/integration/
│   ├── conftest.py       # docker-compose-stack-is-up precondition fixtures, reuses scripts/trigger_dag.sh's auth flow
│   └── test_csv_ingest_e2e.py
scripts/
└── verify_evidence.sql   # D-09: committed SQL, run via `make verify-evidence`
.github/
└── workflows/
    ├── ci.yml             # lint-type-unit + oracle-e2e jobs, triggered on pull_request (both required checks)
    └── readme-summary.yml # push-to-main-only job: regenerate + auto-commit Executive Summary
docs/
├── architecture.md        # D-15
├── configuration.md        # D-15
├── csv-engine.md           # D-15
├── oracle.md                # D-15
├── development.md          # D-15/D-17
├── benchmark.md             # D-05 — committed benchmark results
├── airflow-dag.md           # already exists (Phase 5)
└── environment.md           # already exists (Phase 1)
```

### Pattern 1: E2E test reuses `trigger_dag.sh`'s exact REST flow, asserts real Oracle rows

**What:** The e2e test is a `pytest` test (or a thin script `pytest` wraps) that: (1) ensures a fresh fixture file for a chosen dataset does NOT yet exist, (2) triggers `csv_ingest` via the same `/auth/token` → `Bearer` → `POST /api/v2/dags/csv_ingest/dagRuns` flow already proven working in `scripts/trigger_dag.sh` [VERIFIED: scripts/trigger_dag.sh:29-53], (3) polls `GET .../taskInstances/wait_for_file` and asserts `state == "deferred"` before the file is dropped — reproducing exactly what `docs/airflow-dag.md`'s "Live Verification Evidence" section already proved manually [VERIFIED: docs/airflow-dag.md:69-105], (4) drops the fixture file, (5) polls `GET .../wait?result=load_results_task&interval=1` to completion, (6) opens a **real Oracle connection** (`csv_processor.load.get_connection()`) and asserts row counts in `<dataset>_valid`/`<dataset>_invalid` directly — not just the DagRun's own `state == "success"`.

**When to use:** This is the only pattern that satisfies D-08's literal text ("must be evidenced with real Oracle query results, not just Airflow task state") — a test that only checks `state == "success"` would pass even if `load_results_task`'s XCom lied about counts.

**Example (based on the already-proven live flow, adapted to assertions):**
```python
# Source: adapted from scripts/trigger_dag.sh (this repo, Phase 5) and
# docs/airflow-dag.md's "Live Verification Evidence" section — not a new mechanism.
import subprocess

def trigger(dataset: str, config_path: str) -> str:
    run_id = subprocess.run(
        ["scripts/trigger_dag.sh", dataset, config_path],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    return run_id

# ... poll taskInstances/wait_for_file for state == "deferred" BEFORE dropping the file ...
# ... drop the file, poll .../wait?result=load_results_task&interval=1 to completion ...

def test_customers_e2e_lands_in_oracle(oracle_cursor):
    run_id = trigger("customers", "configs/datasets/customers.json")
    # ... assert deferred, then drop file, then poll to completion ...
    oracle_cursor.execute("SELECT COUNT(*) FROM customers_valid")
    assert oracle_cursor.fetchone()[0] > 0
```

### Pattern 2: Benchmark harness — one parse pass, two write strategies

**What:** `benchmark/run_benchmark.py` calls `csv_processor.engine.process_chunks(file_path, config)` [VERIFIED: packages/csv-processor/src/csv_processor/engine.py:33-35, signature: `def process_chunks(file_path: Path, config: DatasetConfig) -> Iterator[tuple[list[dict[str, object]], list[dict[str, object]]]]`] to get `(valid_rows, invalid_rows)` per chunk, identically for both the naive and chunked/bulk runs. For the chunked/bulk run, each chunk's `valid_rows`/`invalid_rows` are passed straight into the existing `csv_processor.load.insert_rows()` [VERIFIED: packages/csv-processor/src/csv_processor/load.py:85-139, uses `cursor.executemany(sql, rows)` at line 139]. For the naive run, `benchmark/naive_loader.py` iterates each row in the chunk and calls a single-row `cursor.execute(sql, row)` in a Python loop — never `executemany()`, matching D-01's "genuine ... single `cursor.execute()` per row" requirement precisely (not `executemany()` with `chunk_size=1`, which would still batch-bind internally).

**When to use:** Always, for this benchmark — reusing `process_chunks()` means the only variable under test between the two runs is the Oracle write strategy, which is the entire point of TEST-04's comparison. Building a second, separate CSV-parsing path for the naive run would confound the benchmark with an unrelated implementation difference.

**Peak memory measurement:**
```python
# Source: Python stdlib `resource` module — Claude's discretion per CONTEXT.md,
# recommended over tracemalloc (see Standard Stack "Supporting" table rationale).
import resource

def run_and_measure(fn):
    start_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # KB, monotonic peak
    result = fn()
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return result, peak_rss_kb
```
Note: `ru_maxrss` is a **process-lifetime peak**, not resettable mid-process — run the naive and chunked benchmarks as two **separate subprocess invocations** (e.g. `python -m benchmark.run_benchmark --mode naive` / `--mode bulk`, each its own OS process), not two calls inside one long-running script, or the second run's "peak" will be contaminated by the first run's already-freed-but-counted memory.

### Pattern 3: Oracle+Airflow via `docker compose up` as a CI step, not GH Actions' `services:` key

**What:** The `oracle-e2e` job's steps are: `actions/checkout`, recreate the two gitignored first-clone files (`.env` from `.env.example`, `docker/airflow/simple_auth_manager_passwords.json.generated` — both already documented in `docs/environment.md`'s "First-Clone Setup Gaps" [VERIFIED: docs/environment.md:138-155]), then `docker compose up -d --wait` using the project's own `docker-compose.yml` completely unmodified, then run the e2e test suite against `http://localhost:8080` / `localhost:1521` exactly as local dev already does.

**When to use:** Whenever the CI job needs more than one interdependent container with startup ordering (this project's `airflow-init` → `postgres`/`oracle` healthy → `airflow-apiserver`/`scheduler`/`dag-processor`/`triggerer` chain, all wired via `depends_on: condition: service_healthy` in `docker-compose.yml` [VERIFIED: docker-compose.yml:65-69, 104-153]) — GitHub Actions' native `services:` key has no `depends_on`/ordering primitive between service containers and starts every service container before `actions/checkout` even runs [CITED: community discussion, cross-checked against GitHub's official "About service containers" docs], so it cannot express this project's dependency chain or bind-mount `docker/oracle/init/*.sql`/`airflow/dags/` from the checked-out repo at container-start time.

**Example:**
```yaml
# Source: derived from this project's own docker-compose.yml + docs/environment.md's
# already-documented first-clone steps — not a new mechanism, just moved into CI.
jobs:
  oracle-e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7.0.1
      - name: Recreate first-clone-only local files
        run: |
          cp .env.example .env
          mkdir -p docker/airflow
          echo '{"admin": "admin"}' > docker/airflow/simple_auth_manager_passwords.json.generated
      - name: Bring up the full stack
        run: docker compose up -d --wait
      - uses: astral-sh/setup-uv@v10.0.1
      - run: uv sync --locked
      - name: Run the e2e suite against the live stack
        run: uv run pytest tests/e2e/ -x
      - name: Tear down
        if: always()
        run: docker compose down -v
```

### Pattern 4: README auto-commit — default `GITHUB_TOKEN`, not a PAT

**What:** A separate job/workflow, triggered on `push` to `main` only (i.e. after a PR's required checks already passed and it was merged), that re-runs a real ingestion (via the same `docker compose up` + trigger pattern as Pattern 3), runs `scripts/verify_evidence.sql`, regenerates the Executive Summary section of `README.md` between two HTML-comment markers, and commits it back using `stefanzweifel/git-auto-commit-action@v7.2.0` with the job's `permissions: contents: write` and the **default `GITHUB_TOKEN`** (no PAT).

**When to use:** Always for this use case — a commit authored by the default `GITHUB_TOKEN` does not trigger further `push`-based workflow runs at all [CITED: stefanzweifel/git-auto-commit-action README, "will not trigger another GitHub Actions Workflow run"], which structurally prevents D-13's feared infinite loop without needing `[skip ci]` to do the actual work. Keep `[skip ci]` in the commit message anyway (costs nothing, documents intent, and is real GitHub syntax that would matter if the token type ever changes later).

**Example:**
```yaml
# Source: stefanzweifel/git-auto-commit-action's own documented usage (verified
# live this session against its README) + this repo's docker-compose.yml pattern.
name: readme-summary
on:
  push:
    branches: [main]
jobs:
  regenerate-executive-summary:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v7.0.1
      # ... bring up the stack, run a real ingestion, run scripts/verify_evidence.sql,
      #     regenerate the marked section of README.md ...
      - uses: stefanzweifel/git-auto-commit-action@v7.2.0
        with:
          commit_message: "docs(readme): regenerate Executive Summary [skip ci]"
          file_pattern: README.md
```

### Pattern 5: `docs/development.md` as the single CI-troubleshooting surface

**What:** Per D-17, one file covers local dev workflow + architecture notes + "what CI actually runs, how to debug a failing check locally" — meaning it must explicitly document how to reproduce each CI job's exact commands locally (`uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy .`, `uv run pytest tests/unit/ -x`, and the full `docker compose up -d --wait && uv run pytest tests/e2e/ -x` sequence for the Oracle+e2e job), so a contributor never has to reverse-engineer `.github/workflows/ci.yml` to find out what a failing check ran.

**When to use:** Write this section by literally copying the exact `run:` step commands out of `.github/workflows/ci.yml` once it exists — don't paraphrase them, since a paraphrased command that silently differs from the real CI command (e.g. missing a flag) is exactly the kind of drift DOC-01's "no undocumented manual steps" success criterion is designed to catch.

### Anti-Patterns to Avoid

- **Simulating the naive baseline with `executemany()` at `chunk_size=1`:** D-01 explicitly rejects this — it still batch-binds internally and does not reproduce the real per-round-trip cost pattern the benchmark exists to demonstrate.
- **Re-implementing CSV parsing for the naive benchmark path:** confounds the comparison; always drive both paths from the same `process_chunks()` call.
- **Checking only `DagRun.state == "success"` in the e2e test:** does not satisfy D-08 — must query Oracle directly for row counts.
- **Reaching for a PAT "to be safe" on the auto-commit job:** reintroduces the exact infinite-loop risk the design is trying to avoid; the default token is both simpler and structurally safer here.
- **Hand-translating `docker-compose.yml` into GitHub Actions' native `services:` blocks:** loses `depends_on` ordering and the pre-checkout volume-mount capability this project's Oracle init scripts and Airflow DAG-folder mount both depend on.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| README write-back with no-op-diff detection + bot commit identity | Custom `git config`/`git add`/`git commit --allow-empty-message`/`git push` shell steps | `stefanzweifel/git-auto-commit-action@v7.2.0` | Already handles diff detection (skips committing when nothing changed — important since the Executive Summary may regenerate to byte-identical content on a run with no new data), default bot identity, and is independently version-pinned/verified this session |
| Airflow REST API auth flow | A new auth helper for the e2e test | `scripts/trigger_dag.sh`'s already-proven `/auth/token` → `Bearer` flow [VERIFIED: scripts/trigger_dag.sh:29-53] | Re-deriving this risks re-discovering the same `logical_date: null`/`interval` query-param gotchas Phase 5 already found and documented (`docs/airflow-dag.md`) |
| Oracle container readiness detection in CI | A custom polling loop against port 1521 | `docker compose up -d --wait` (already configured against the real `healthcheck.sh`-based healthcheck in `docker-compose.yml` [VERIFIED: docker-compose.yml:97-102]) | The healthcheck is already tuned (12 retries, 30s start period) from real Phase 1 experience; a hand-rolled poll would need to rediscover the same tuning |

**Key insight:** Every "don't hand-roll" item in this phase has an already-working, already-debugged equivalent somewhere in this project's own Phase 1/5 history — the risk in this phase is specifically re-deriving infrastructure gotchas (JWT secrets, execution-API URLs, container hostnames, healthcheck timing) that were already found and fixed the hard way (live-trigger debugging) in earlier phases. Reuse, don't reinvent.

## Common Pitfalls

### Pitfall 1: GitHub Actions' `services:` key silently can't do what this project needs
**What goes wrong:** A first draft of the CI workflow tries to express Oracle + all 5 Airflow processes as `services:` entries, discovers mid-implementation that there's no `depends_on` between them and that `docker/oracle/init/*.sql`/`airflow/dags/` aren't present at container-start time (services start before `actions/checkout`), and has to re-derive Phase 5's already-solved container-DNS/JWT-secret/execution-API-URL fixes a second time in a different form.
**Why it happens:** GitHub's own examples for service containers (Postgres, Redis, MySQL) are all single-container, no-volume-mount cases — they don't surface this limitation until a multi-container, volume-dependent stack like this project's is attempted.
**How to avoid:** Use `docker compose up -d --wait` with the project's own unmodified `docker-compose.yml` as a CI step (Pattern 3) — sidesteps the limitation entirely by never using the native `services:` key for Oracle/Airflow.
**Warning signs:** Any GH Actions YAML draft with more than one `services:` entry that also needs volume mounts or has a real (not accidental) startup-order dependency.

### Pitfall 2: A PAT-based auto-commit re-triggers CI, defeating D-13's whole point
**What goes wrong:** A developer reaches for a PAT (stored as a repo secret) for the README auto-commit "to be safe" or out of habit from other projects, and the resulting commit — because it's authored with real user credentials, not `GITHUB_TOKEN` — triggers the `pull_request`/`push` workflows again, which (if the workflow also runs on `push` to `main`) can spiral.
**Why it happens:** Many popular auto-commit tutorials default to PAT usage because it's needed for cases where the commit itself must re-trigger downstream automation (e.g. a deploy pipeline) — this project has no such need.
**How to avoid:** Use the default `GITHUB_TOKEN` with `permissions: contents: write` scoped to just the `readme-summary` job (Pattern 4) — structurally cannot re-trigger a `push`-based workflow.
**Warning signs:** The Executive Summary commit shows up authored by a real GitHub username instead of `github-actions[bot]`, or a second CI run starts immediately after the auto-commit lands.

### Pitfall 3: mypy under whole-repo strict settings flags bare, unparameterized `dict`/`list` return types already present in `airflow/dags/`
**What goes wrong:** `csv_ingest.py`'s existing task functions are annotated `-> dict`/take `config_dict: dict` [VERIFIED: airflow/dags/csv_ingest.py:42, 75, 99, 121 — literal `-> dict` and `config_dict: dict` annotations, no type parameters] rather than this codebase's own established `dict[str, object]` convention already used throughout `csv_processor` (e.g. `engine.py`'s `process_chunks()` signature uses `list[dict[str, object]]` [VERIFIED: packages/csv-processor/src/csv_processor/engine.py:35]). Under `disallow_any_generics = True` (a common strict-mode flag, and one the Pydantic mypy plugin's own recommended config enables), every bare `dict`/`list` in `airflow/dags/` will fail mypy once D-14 puts that directory under whole-repo type-checking.
**Why it happens:** `airflow/dags/csv_ingest.py` was written in Phase 5, before this phase's mypy adoption — it was never checked against any type checker.
**How to avoid:** Either (a) don't enable `disallow_any_generics` repo-wide (looser strict profile), or (b) fix the DAG file's own annotations to match the rest of the codebase's `dict[str, object]` convention as part of this phase's mypy-adoption work — recommend (b) for consistency, since it's a small, mechanical, low-risk fix and keeps one type-hygiene standard across the whole repo rather than two.
**Warning signs:** `mypy .` fails immediately on `airflow/dags/csv_ingest.py` with `Missing type parameters for generic type "dict"` the first time it's run against this file.

### Pitfall 4: The e2e test's "deferred" assertion is timing-sensitive — the file-drop must genuinely happen after the poll observes `deferred`, not before
**What goes wrong:** A test that triggers the DAG and immediately drops the fixture file (without first confirming `wait_for_file` reached `deferred` state) may race the scheduler — if the file already exists by the time the sensor first pokes, the sensor short-circuits without ever deferring, and the test's "deferred-wake" assertion becomes meaningless (it never actually happened) while still technically passing if written loosely (e.g. `state in {"deferred", "success"}`).
**Why it happens:** Phase 5's manual proof (`docs/airflow-dag.md`) got this right by construction (a human naturally checks state before manually creating the file) — an automated test must reproduce that same ordering deliberately, with an explicit poll-then-assert-then-act sequence, not implicit timing luck.
**How to avoid:** The e2e test must (1) trigger with the target file confirmed absent, (2) poll `taskInstances/wait_for_file` until `state == "deferred"` (with a bounded timeout, failing loudly if it's never observed), (3) only then drop the fixture file, (4) poll to completion. Never write the file before step 2 completes.
**Warning signs:** The e2e test passes even when run against a fixture file that was already present before the test started (a strong sign the deferred-state assertion isn't actually gating anything).

### Pitfall 5: CI disk pressure from this project's own already-measured ~14.8GB image+volume footprint
**What goes wrong:** `docs/environment.md`'s own Phase 1 measurement recorded ~14.8GB total (images + volumes) for this project's stack locally [VERIFIED: docs/environment.md:50-59, table showing 11.6GB images + 3.2GB volumes]. GitHub-hosted standard runners provide 4 vCPU/16GB RAM for public repos [CITED: GitHub Docs, cross-checked via WebSearch], comfortably meeting the RAM/CPU floor `docs/environment.md` already documents, but free disk headroom on hosted runners is a much tighter, less-publicized number that varies by runner image version and pre-installed toolchain footprint — a `docker compose build`/`pull` step can fail with `no space left on device` well before RAM becomes the constraint.
**Why it happens:** Local dev machines have far more free disk than a CI runner's ephemeral disk after its own pre-installed SDKs (`.NET`, `Android`, `Haskell`, etc.) already consume a large fraction of it.
**How to avoid:** Prefer the smaller `slim-faststart` Oracle tag for CI specifically if disk pressure appears (`1.24GB` vs. the standard `1.55GB` `23.26.2-faststart` tag — both confirmed to exist on Docker Hub this session [VERIFIED: hub.docker.com/r/gvenzl/oracle-free/tags, queried 2026-08-29]), and/or add a disk-cleanup step (removing unrelated pre-installed toolchains) before `docker compose up` if `no space left on device` is observed. This is a "watch for it, don't pre-solve it speculatively" pitfall — don't add cleanup steps unless the failure is actually observed, since they add job time for no benefit if disk was never actually tight.
**Warning signs:** `docker compose up`/`pull` fails with `no space left on device` specifically in the `oracle-e2e` CI job (not locally).

### Pitfall 6: A required status check must be configured in repo Branch Protection settings, not just in the workflow YAML
**What goes wrong:** A workflow with two jobs (`lint-type-unit`, `oracle-e2e`) that both run `on: pull_request` does NOT, by itself, block a PR merge if either job fails — "required" status checks are a **repository setting** (Settings → Branches → Branch protection rules → "Require status checks to pass before merging"), separate from the workflow file itself [CITED: GitHub Docs / community discussion on required status checks].
**Why it happens:** It's easy to read D-07 ("required status check ... blocks merge") as something the workflow YAML alone accomplishes — the YAML only makes the checks *exist and run*; someone with repo admin access must additionally enable branch protection naming those exact job names as required.
**How to avoid:** Treat "configure branch protection to require the `lint-type-unit` and `oracle-e2e` job names" as an explicit, separate, one-time repo-setup step — document it in `docs/development.md` (per D-17's "CI/troubleshooting" coverage) with the exact job names to select, and flag it in the plan as a manual/human step (not something a task can verify via `pytest`).
**Warning signs:** A PR merges successfully despite a red CI check — the single clearest sign branch protection was never actually configured.

### Pitfall 7: `docs/development.md` documenting stale/paraphrased CI commands
**What goes wrong:** `docs/development.md` (D-17) is written with prose approximations of what CI runs ("we lint and type-check the code") instead of the literal commands from `.github/workflows/ci.yml`, and later drifts when the workflow file changes but the doc doesn't.
**Why it happens:** Prose is faster to write than keeping two files byte-consistent, and nothing forces them to match.
**How to avoid:** Copy the exact `run:` step commands verbatim into `docs/development.md`'s "reproduce CI locally" section (Pattern 5) — a contributor should be able to `Ctrl+C`/`Ctrl+V` from the doc and get the identical command CI ran.
**Warning signs:** A contributor reports "it passed locally but failed in CI" and the root cause turns out to be a documented command that doesn't match the real one (a missing flag, a different test path).

## Code Examples

Verified patterns from this project's own already-working code and official sources:

### 1. The already-proven REST trigger flow (reuse verbatim in the e2e test)
```bash
# Source: scripts/trigger_dag.sh (this repo, Phase 5) — verified working live
# for both customers and orders datasets (05-02-SUMMARY.md)
JWT_TOKEN=$(curl -s -X POST "${AIRFLOW_BASE_URL}/auth/token" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"admin\", \"password\": \"admin\"}" | jq -r '.access_token')

DAG_RUN_ID=$(curl -s -X POST "${AIRFLOW_BASE_URL}/api/v2/dags/csv_ingest/dagRuns" \
  -H "Authorization: Bearer ${JWT_TOKEN}" -H "Content-Type: application/json" \
  -d "{\"conf\": {\"dataset\": \"customers\", \"config_path\": \"configs/datasets/customers.json\"}, \"logical_date\": null}" \
  | jq -r '.dag_run_id')
```

### 2. Polling for genuine deferral before dropping the fixture file
```bash
# Source: docs/airflow-dag.md's "Live Verification Evidence" section (Phase 5) —
# the exact curl pattern already proven to observe state == "deferred" live.
curl -s -H "Authorization: Bearer ${JWT_TOKEN}" \
  "${AIRFLOW_BASE_URL}/api/v2/dags/csv_ingest/dagRuns/${DAG_RUN_ID}/taskInstances/wait_for_file" \
  | jq -r '.state'
# Poll until this returns "deferred" (bounded timeout) BEFORE writing the fixture file.
```

### 3. Bulk vs. naive Oracle write, sharing one parse pass
```python
# Source: adapted directly from csv_processor.engine.process_chunks()'s existing
# contract (packages/csv-processor/src/csv_processor/engine.py:33-128) and
# csv_processor.load.insert_rows() (packages/csv-processor/src/csv_processor/load.py:85-139)
from csv_processor.engine import process_chunks
from csv_processor import load

def run_bulk(file_path, config, cursor):
    for valid_rows, invalid_rows in process_chunks(file_path, config):
        load.insert_rows(cursor, table=config.oracle.valid_table,
                          columns=[c.name for c in config.columns], rows=valid_rows)

def run_naive(file_path, config, cursor):  # benchmark/naive_loader.py
    columns = [c.name for c in config.columns]
    sql = f"INSERT INTO {config.oracle.valid_table} ({', '.join(columns)}) " \
          f"VALUES ({', '.join(f':{c}' for c in columns)})"
    for valid_rows, invalid_rows in process_chunks(file_path, config):
        for row in valid_rows:  # D-01: genuinely one execute() per row, no executemany()
            cursor.execute(sql, row)
```

### 4. `pyproject.toml` additions for D-14 (mypy + Pydantic plugin)
```toml
# Source: Context7 /pydantic/pydantic docs/integrations/mypy.md — verified this session
[tool.mypy]
plugins = ["pydantic.mypy"]
# Choose a strict profile deliberately (see Pitfall 3) rather than mypy's bare
# `strict = true` default, given the existing airflow/dags/ annotation gaps found.
```

### 5. SYSDBA connection pattern (only needed if the Oracle DDL must be (re-)applied inside a CI step rather than relying on docker-compose's own init-script mount — kept here for completeness, not required if Pattern 3 is followed, since `docker-compose.yml`'s existing `./docker/oracle/init:/container-entrypoint-initdb.d` mount already runs the DDL on first boot inside CI exactly as it does locally)
```python
# Source: Context7 /oracle/python-oracledb, doc/src/user_guide/connection_handling.md
import oracledb
connection = oracledb.connect(user="sys", password=oracle_password,
                               dsn="localhost:1521/FREEPDB1",
                               mode=oracledb.AuthMode.SYSDBA)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| Floating GitHub Action version tags (`@v8`, `@v10`) | Pinned immutable per-version tags (`@v10.0.1`) | `setup-uv` v8+ (per STACK.md, still current) | Floating tags on `setup-uv` specifically no longer resolve the old way; always pin the exact patch tag, confirmed still true this session |
| PAT-based CI write-back commits | Default `GITHUB_TOKEN` + `permissions: contents: write`, scoped per-job | Standard practice for several years now, still current | Removes secret-management overhead and structurally avoids the CI-triggers-itself failure mode for the common "write results back" case |

**Deprecated/outdated:** none newly identified this session beyond what STACK.md already flags (`cx_Oracle`, Airflow's legacy `/api/v1` REST endpoints — both already excluded, unaffected by this phase).

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `ubuntu-latest` GitHub-hosted runners ship Docker Engine + the `docker compose` v2 CLI plugin pre-installed | Standard Stack (Core table) | If wrong, the `oracle-e2e` job fails immediately on `docker compose up`; low risk (extremely well-established GH-hosted runner behavior), and the plan should add a cheap `docker compose version` first-step check regardless, which fails fast and visibly if this assumption is ever wrong |
| A2 | GitHub-hosted standard runner free disk headroom is tight enough to warrant proactive monitoring (exact free-GB figure not confirmed live this session) | Pitfalls §5 | If the real free-disk figure is generous, Pitfall 5's mitigation (slim image tag / cleanup step) is unnecessary extra CI time; if genuinely tight, the `oracle-e2e` job could fail with `no space left on device` — recommend treating this as "watch for it" rather than pre-emptively adding cleanup steps, per the pitfall's own guidance |

## Open Questions

1. **Exact HTML-comment marker syntax for the Executive Summary's regenerated section in README.md**
   - What we know: D-11/D-12 require a live-regenerated top section; Claude's Discretion explicitly leaves the exact marker syntax open.
   - What's unclear: Whether to use a simple `<!-- EXEC-SUMMARY:START -->`/`<!-- EXEC-SUMMARY:END --> ` pair or a more structured convention.
   - Recommendation: Use a plain HTML-comment marker pair — simplest possible mechanism, trivially `sed`/regex-replaceable by the regeneration script, and invisible when README.md renders on GitHub.

2. **Whether the deferred-wake proof (D-11b) is captured by the same e2e test run or the separate `readme-summary` job's own ingestion run**
   - What we know: Both jobs independently trigger a real ingestion against a real stack; either could observe and record the `deferred` state.
   - What's unclear: CONTEXT.md leaves this to Claude's discretion explicitly.
   - Recommendation: Capture it in the `readme-summary` job's own run (Pattern 4), not the PR's `oracle-e2e` job — the Executive Summary's evidence should come from the same run that produces the row counts it reports, keeping all of README's Executive Summary numbers internally consistent (same run, same moment), rather than splicing together evidence from two different job runs at different times.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `docker compose` v2 CLI plugin on GH-hosted runners | `oracle-e2e` and `readme-summary` CI jobs | Assumed ✓ (A1) | pre-installed on `ubuntu-latest` | Add a `docker compose version` first-step check; if genuinely missing, `docker/setup-compose-action` or a manual plugin install step |
| Sufficient CI runner free disk for this project's ~14.8GB image+volume footprint | `oracle-e2e`/`readme-summary` jobs | Unconfirmed (A2) | n/a | Switch to `gvenzl/oracle-free:23.26.2-slim-faststart` (confirmed to exist, smaller) if `no space left on device` is observed |
| Repo admin access to configure required Branch Protection status checks | D-07's "required status check" enforcement | Depends on repo owner's access, not this session | n/a | None — this is a manual, one-time human step outside what any workflow YAML or task can automate; document clearly (Pitfall 6) |

**Missing dependencies with no fallback:** Branch protection configuration (Pitfall 6) — inherently a manual GitHub repo-settings step, cannot be scripted from inside the workflow YAML itself.

**Missing dependencies with fallback:** `docker compose` availability (A1) and CI disk headroom (A2) both have documented fallbacks above.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | `pytest` 9.1.1 [VERIFIED: pyproject.toml `[dependency-groups] dev = ["pytest==9.1.1"]`] |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths = ["tests"]`, `pythonpath = ["."]`) — already exists, recurses into any new `tests/e2e/` directory automatically |
| Quick run command | `uv run pytest tests/unit/ -x` |
| Full suite command | `uv run pytest tests/unit/ tests/integration/ tests/e2e/ -x` (requires `make up` first for the latter two) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|--------------------|--------------|
| TEST-03 | Full HTTP-trigger → DAG → Oracle path, with genuine deferred-state proof and real row-count assertions | e2e | `uv run pytest tests/e2e/test_csv_ingest_e2e.py -x` | ❌ Wave 0 |
| TEST-04 | Naive vs. chunked/bulk benchmark at ~100K rows, recorded to `docs/benchmark.md` | manual/script (not a pass/fail pytest assertion — a data-producing script) | `uv run python -m benchmark.run_benchmark` | ❌ Wave 0 |
| CI-01 | Lint/type/unit run on every PR; Oracle+e2e as an additional required check | infra (workflow YAML, not pytest) | Verified by observing a real GitHub Actions run on a real PR, plus Branch Protection configuration (Pitfall 6) | ❌ Wave 0 |
| DOC-01 | New-developer clone-to-ingestion walkthrough with no undocumented manual steps | manual/human verification (docs cannot self-test) | Human follow-along of README.md + linked docs on a genuinely fresh clone/environment | ❌ Wave 0 (docs files) |

### Sampling Rate
- **Per task commit:** `uv run pytest tests/unit/ -x` (fast, no containers)
- **Per wave merge:** `make verify-phase6` (composition is Claude's discretion per CONTEXT.md — recommend: unit suite + `docker compose up` + e2e suite + `make lint`/`make typecheck`, mirroring `verify-phase4`/`verify-phase5`'s "requires `make up` first" pattern)
- **Phase gate:** Full suite green (`verify-phase6`) before `/gsd-verify-work`, plus a genuinely observed real GitHub Actions run on an actual PR (the workflow YAML itself cannot be "unit tested" — it must be observed running for real)

### Wave 0 Gaps
- [ ] `tests/e2e/__init__.py`, `tests/e2e/conftest.py` — new test package, needs its own fixtures (stack-is-up precondition, auth-token helper reused/adapted from `scripts/verify_environment.py`'s pattern)
- [ ] `tests/e2e/test_csv_ingest_e2e.py` — covers TEST-03
- [ ] `benchmark/__init__.py` (if made a package), `benchmark/naive_loader.py`, `benchmark/run_benchmark.py` — covers TEST-04
- [ ] `.github/workflows/ci.yml`, `.github/workflows/readme-summary.yml` — covers CI-01
- [ ] `scripts/verify_evidence.sql` — covers D-09/D-10/D-11
- [ ] Framework install: `uv add --dev "ruff==0.16.5" "mypy==2.3.1"` — mypy/ruff not yet dev dependencies (only `pytest==9.1.1` currently present in `pyproject.toml`)
- [ ] `[tool.mypy]`/`[tool.ruff]` config sections — neither exists yet in `pyproject.toml` (confirmed absent this session — no `[tool.ruff]`/`[tool.mypy]` in the current root `pyproject.toml`, no `ruff.toml`/`mypy.ini` files in the repo)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-------------------|
| V1 Architecture (CI/CD pipeline integrity) | yes | Pinned exact Action tags (not floating `@v10`/`@main`), `permissions:` scoped per-job (not workflow-wide), default `GITHUB_TOKEN` over a PAT wherever sufficient |
| V6 Cryptography / secrets handling | yes (narrow) | No new secrets introduced this phase — `admin`/`admin` local-dev credentials remain the only credential in play, already documented as acceptable per INFRA-03's local-dev-only scope; the auto-commit job needs zero new secrets (default token) |
| V14 Configuration | yes | `permissions: contents: write` must be scoped to only the `readme-summary` job, never set workflow-wide, so the `oracle-e2e`/`lint-type-unit` jobs (which run on untrusted PR branches, including forks) never hold write access |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|----------------------|
| A malicious/compromised third-party GitHub Action (`stefanzweifel/git-auto-commit-action`, `astral-sh/setup-uv`) | Tampering | Pin the exact verified tag (already done in Standard Stack table); consider pinning to a commit SHA instead of a tag for maximum supply-chain hardening if the project's risk tolerance later warrants it (not required for this local-dev-scale project, but worth a one-line note in `docs/development.md`) |
| A PR from a fork with write-scoped `GITHUB_TOKEN` permissions | Elevation of Privilege | `pull_request` (not `pull_request_target`) triggers for the `lint-type-unit`/`oracle-e2e` jobs — forked-repo PRs never get write access or repo secrets by GitHub's own default `pull_request` trigger semantics; do not switch to `pull_request_target` for this workflow, since that trigger intentionally grants base-repo permissions/secrets to a forked PR's code — a real risk this project does not need to take on |
| Secrets/credentials leaking into CI logs | Information Disclosure | The e2e test's JWT bearer token and Oracle `admin`/`admin` credential are both local-dev-only, non-sensitive values already accepted per INFRA-03 — but avoid printing the raw JWT token itself to CI logs regardless (matches PITFALLS.md's existing "don't log full row content" discipline, extended here to auth tokens) |

## Sources

### Primary (HIGH confidence)
- `api.github.com/repos/astral-sh/setup-uv/releases`, `api.github.com/repos/actions/checkout/releases`, `api.github.com/repos/stefanzweifel/git-auto-commit-action/releases` — queried live 2026-08-29, GitHub's own Releases API, canonical source for exact current tags
- `pypi.org/pypi/{ruff,mypy}/json` — queried live 2026-08-29, canonical PyPI registry, confirms exact current versions match STACK.md's existing pins
- `hub.docker.com/v2/repositories/gvenzl/oracle-free/tags` — queried live 2026-08-29 (via WebFetch), confirms `23.26.2-faststart` and `slim-faststart` tags both exist
- Context7 `/pydantic/pydantic` — `docs/integrations/mypy.md`, mypy plugin configuration
- Context7 `/oracle/python-oracledb` — `doc/src/user_guide/connection_handling.md`, SYSDBA thin-mode connection support
- This project's own source, read directly this session: `scripts/trigger_dag.sh`, `docs/airflow-dag.md`, `docker-compose.yml`, `packages/csv-processor/src/csv_processor/{engine,load}.py`, `airflow/dags/csv_ingest.py`, `airflow/dags/_common/paths.py`, `docs/environment.md`, `docker/oracle/init/*.sql`, `packages/csv-processor/src/csv_processor/{models,config/models}.py`, `.planning/config.json`, `pyproject.toml`
- Installed `airflow.sdk` package inside the running `airflow-scheduler` container this session — confirmed `py.typed` markers present at `airflow/py.typed` and `airflow/sdk/py.typed`, and a real `.pyi` stub with `@overload` definitions for `task`/`dag`/`task_group` decorators at `airflow/sdk/definitions/decorators/__init__.pyi` [VERIFIED: read directly via `docker compose exec airflow-scheduler cat ...`, 2026-08-29]

### Secondary (MEDIUM confidence)
- GitHub Docs "About service containers" + multiple cross-checked community sources (dev.to, Medium, GitHub community discussions) — service containers start before any step including checkout, no `depends_on` support between service containers
- WebSearch, cross-checked across 2+ independent sources — GitHub-hosted standard runner specs (4 vCPU/16GB RAM for public repos), `stefanzweifel/git-auto-commit-action`'s documented "will not trigger another Workflow run" behavior for `GITHUB_TOKEN`-authored commits

### Tertiary (LOW confidence)
- A1 (docker compose pre-installed on `ubuntu-latest`) — well-established GH-hosted-runner behavior, not independently re-verified against a live runner this session; mitigated by recommending a cheap first-step version check
- A2 (CI disk headroom) — not independently confirmed against a live runner's actual free-disk figure this session; treated as a "watch for it" pitfall rather than a pre-emptive fix

## Metadata

**Confidence breakdown:**
- Standard stack (Action/package versions): HIGH — every pinned version re-verified live against its canonical registry/API this session, not carried over from cached STACK.md research alone
- Architecture (CI mechanism choice, benchmark harness design): HIGH — derived by directly reading this project's own already-working code (`engine.py`, `load.py`, `trigger_dag.sh`, `docker-compose.yml`) rather than assumed from generic patterns
- Pitfalls: MEDIUM-HIGH — GitHub Actions service-container limitations and auto-commit token behavior are well-corroborated across multiple independent sources; exact CI runner disk-space figures remain unconfirmed (flagged as Assumption A2)

**Research date:** 2026-08-29
**Valid until:** 7 days for the GitHub Action tag pins specifically (STACK.md's own note: "action releases move faster than this research's cache TTL" — re-verify `astral-sh/setup-uv`/`actions/checkout`/`stefanzweifel/git-auto-commit-action` tags again if planning is delayed past ~1 week); 30 days for the architectural recommendations (CI mechanism choice, benchmark harness design) — these are unlikely to shift quickly

---
*Phase: 6-End-to-End Verification, Benchmark, CI & Docs*
*Research completed: 2026-08-29*
