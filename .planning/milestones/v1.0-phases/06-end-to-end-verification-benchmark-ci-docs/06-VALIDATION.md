---
phase: 6
slug: end-to-end-verification-benchmark-ci-docs
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-29
---

# Phase 6 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths = ["tests"]`, `pythonpath = ["."]`) — already exists, recurses into any new `tests/e2e/` directory automatically |
| **Quick run command** | `uv run pytest tests/unit/ -x` |
| **Full suite command** | `uv run pytest tests/unit/ tests/integration/ tests/e2e/ -x` (requires `make up` first for integration/e2e) |
| **Estimated runtime** | ~5s unit-only, ~120s full suite with `make up` |

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/unit/ -x`
- **After every plan wave:** Run `make verify-phase6` (unit suite + `docker compose up` + e2e suite + `make lint`/`make typecheck`; composition follows `verify-phase4`/`verify-phase5`'s "requires `make up` first" pattern)
- **Before `/gsd-verify-work`:** Full suite must be green, plus a genuinely observed real GitHub Actions run on an actual PR (workflow YAML cannot be unit-tested — it must be observed running for real)
- **Max feedback latency:** 120 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 06-01-01 | 01 | 0 | TEST-03 | T-06-01 | e2e test package scaffolded (`tests/e2e/__init__.py`, `conftest.py`) | unit | `uv run pytest tests/e2e/ --collect-only` | ❌ W0 | ⬜ pending |
| 06-01-02 | 01 | 1 | TEST-03 | T-06-01 | HTTP trigger → deferred `wait_for_file` genuinely wakes → `process_csv` runs → correct/incorrect rows land in Oracle `VALID`/`INVALID` | e2e | `uv run pytest tests/e2e/test_csv_ingest_e2e.py -x` | ❌ W0 | ⬜ pending |
| 06-02-01 | 02 | 1 | TEST-04 | — / N/A | Naive-loop baseline isolates only the Oracle-write strategy (single `cursor.execute()` per row vs. `executemany()`), both call `csv_processor.engine.process_chunks()` for parsing | script | `uv run python -m benchmark.run_benchmark --rows 1000` (smoke-scale dry run) | ❌ W0 | ⬜ pending |
| 06-02-02 | 02 | 1 | TEST-04 | — / N/A | Full ~100K-row benchmark run records rows/sec, peak memory, Oracle load time for both approaches into `docs/benchmark.md` | manual/script | `uv run python -m benchmark.run_benchmark --rows 100000` | ❌ W0 | ⬜ pending |
| 06-03-01 | 03 | 1 | CI-01 | T-06-02 | `permissions: contents: write` scoped only to the `readme-summary` job, never workflow-wide; `pull_request` (not `pull_request_target`) trigger for lint/type/unit and Oracle+e2e jobs | infra | `actionlint .github/workflows/*.yml` | ❌ W0 | ⬜ pending |
| 06-03-02 | 03 | 2 | CI-01 | T-06-02 | Lint/type-check/unit run on every PR; Oracle+e2e via `docker compose up -d --wait` as a required check | infra | Observed real GitHub Actions run on an actual PR (not locally reproducible as a pytest assertion) | ❌ W0 | ⬜ pending |
| 06-04-01 | 04 | 2 | DOC-01 | — / N/A | README + `docs/*.md` let a new developer go from `git clone` to a completed HTTP-triggered ingestion with no undocumented manual steps | manual | Human follow-along of README.md + linked docs on a genuinely fresh clone/environment | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/e2e/__init__.py`, `tests/e2e/conftest.py` — new test package; needs stack-is-up precondition fixture and auth-token helper (adapt from `scripts/verify_environment.py`'s pattern)
- [ ] `tests/e2e/test_csv_ingest_e2e.py` — stub for TEST-03
- [ ] `benchmark/__init__.py`, `benchmark/naive_loader.py`, `benchmark/run_benchmark.py` — stubs for TEST-04
- [ ] `.github/workflows/ci.yml`, `.github/workflows/readme-summary.yml` — stubs for CI-01
- [ ] `scripts/verify_evidence.sql` — stub for D-09/D-10/D-11 evidence capture
- [ ] `uv add --dev "ruff==0.16.5" "mypy==2.3.1"` — neither is a dev dependency yet (only `pytest==9.1.1` currently present)
- [ ] `[tool.mypy]`/`[tool.ruff]` sections in `pyproject.toml` — neither exists yet (confirmed absent this session)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| GitHub Actions PR run shows lint/type/unit + Oracle+e2e pass/fail on a real PR | CI-01 | Workflow YAML correctness cannot be unit-tested locally — GitHub Actions runner behavior (service container timing, `docker compose up -d --wait`, secrets scoping) must be observed on a real run | Open a real PR against the repo, observe the Checks tab shows all jobs green (or a deliberately broken PR shows them red) |
| Branch Protection marks the Oracle+e2e job as a required status check | CI-01 (D-07) | Not scriptable from workflow YAML — a GitHub repo Settings action | In repo Settings → Branches → branch protection rule, add the `oracle-e2e` job as a required check; confirm a PR cannot merge until it passes |
| New developer can clone-to-ingestion using only README + docs/ | DOC-01 | Docs cannot self-test; requires a human unfamiliar with the shortcuts already in the author's head | Have a second person (or simulate a fresh environment) follow README.md + linked docs verbatim from `git clone` to a completed HTTP-triggered ingestion, noting any undocumented step |
| CI auto-commit of the regenerated Executive Summary does not create an infinite trigger loop | CI-01 (D-12/D-13) | Requires observing two consecutive real merges to `main` to confirm the default-`GITHUB_TOKEN`-authored commit does not re-trigger Actions | Merge a PR to `main`, confirm the readme-summary job runs once, commits, and does not re-trigger itself |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 120s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
