---
phase: 06-end-to-end-verification-benchmark-ci-docs
plan: 03
subsystem: infra
tags: [ruff, mypy, github-actions, ci, docker-compose, pydantic-mypy, oracle, airflow]

# Dependency graph
requires:
  - phase: 06-01
    provides: tests/e2e/test_csv_ingest_e2e.py (the suite oracle-e2e's CI job runs)
  - phase: 01
    provides: docker-compose.yml (the stack oracle-e2e reuses unmodified) and docs/environment.md's first-clone bootstrap steps
provides:
  - Whole-repo ruff==0.16.5/mypy==2.3.1 configuration (D-14), including airflow/dags/
  - .github/workflows/ci.yml with lint-type-unit + oracle-e2e as two independent PR-triggered jobs
  - The Pitfall-3 dict -> dict[str, object] annotation fix in csv_ingest.py's task functions
affects: [06-04, 06-05, any future phase adding a new CI job or module to this repo]

# Actuals (#2632)
actuals:
  tokens: 32830
  tasks: 2
  commits: 2

# Tech tracking
tech-stack:
  added: ["ruff==0.16.5 (dev)", "mypy==2.3.1 (dev)", "pydantic.mypy plugin", "actions/checkout@v7.0.1", "astral-sh/setup-uv@v10.0.1"]
  patterns:
    - "mypy per-module overrides scope disallow_any_generics/disallow_untyped_defs/check_untyped_defs to this project's own source (csv_processor/generator/benchmark/scripts/_common/csv_ingest), never tests/ -- avoids a repo-wide 'nuclear strict=true' flood on first adoption"
    - "docker-compose.yml reused unmodified as a CI step (docker compose up -d --wait), never GitHub Actions' native services: key"

key-files:
  created:
    - .github/workflows/ci.yml
  modified:
    - pyproject.toml (new [tool.ruff]/[tool.mypy] sections, ruff/mypy dev deps)
    - airflow/dags/csv_ingest.py (bare dict -> dict[str, object])
    - packages/csv-processor/src/csv_processor/engine.py (zip strict=True, _result_from_existing helper)
    - packages/csv-processor/src/csv_processor/errors.py, config/errors.py (dict -> dict[str, object])
    - airflow/dags/_common/reporting.py, scripts/dag_polling.py (dict -> dict[str, object])
    - ~30 test files (ruff import-sort/format normalization + a handful of genuine mypy-surfaced fixes)

key-decisions:
  - "Excluded .planning/ from ruff's scope (extend-exclude) after ruff format's Markdown fenced-code-block formatting was found to reformat committed Python snippets inside research/pattern docs -- reverted those unintended changes and scoped D-14's 'whole repo' to code, not historical planning artifacts"
  - "mypy's disallow_any_generics/disallow_untyped_defs/check_untyped_defs applied only via [[tool.mypy.overrides]] to this project's own source modules, never tests/ -- keeps the profile 'targeted', matching the plan's explicit non-strict=true mandate, while still catching the Pitfall-3 bare-dict class of bug"
  - "airflow.* given ignore_missing_imports=true in mypy config -- Airflow is deliberately not a root pyproject.toml dependency (Phase 5 decision: installed only inside docker/airflow/Dockerfile), so mypy can never resolve it locally or in CI either"

patterns-established:
  - "Whole-repo ruff/mypy adoption on an already-large codebase: fix everything the new lint/type gate finds as part of the adopting task's own commit, don't defer to a follow-up phase"

requirements-completed: [CI-01]

coverage:
  - id: D1
    description: "Whole-repo ruff (including airflow/dags/) exits 0"
    requirement: CI-01
    verification:
      - kind: other
        ref: "uv run ruff check ."
        status: pass
    human_judgment: false
  - id: D2
    description: "Whole-repo mypy (including airflow/dags/) exits 0, with the Pitfall-3 dict[str, object] annotation fix applied"
    requirement: CI-01
    verification:
      - kind: other
        ref: "uv run mypy ."
        status: pass
    human_judgment: false
  - id: D3
    description: ".github/workflows/ci.yml exists with lint-type-unit + oracle-e2e, pull_request trigger only, no permissions: key anywhere"
    requirement: CI-01
    verification:
      - kind: other
        ref: "python3 -c \"import yaml; d = yaml.safe_load(open('.github/workflows/ci.yml')); assert set(d['jobs']) == {'lint-type-unit', 'oracle-e2e'}; assert 'permissions' not in d and all('permissions' not in j for j in d['jobs'].values())\""
        status: pass
    human_judgment: false
  - id: D4
    description: "Branch Protection actually enforcing lint-type-unit/oracle-e2e as required status checks (Pitfall 6) -- a repo-admin-only manual GitHub setting outside any workflow YAML"
    human_judgment: true
    rationale: "GitHub repo Settings -> Branches -> branch protection cannot be configured or verified from this repo's own tooling; must be done by a human with repo admin access, and documented as outstanding in docs/development.md (Plan 05)"

duration: ~12min (commit-timestamp delta from prior plan's completion; wall-clock work time was longer)
completed: 2026-08-30
status: complete
---

# Phase 6 Plan 3: Whole-Repo CI (ruff/mypy/pytest + real Oracle+Airflow) Summary

**Whole-repo ruff==0.16.5/mypy==2.3.1 (D-14, including `airflow/dags/`) plus `.github/workflows/ci.yml`'s two required PR checks -- `lint-type-unit` (fast, no containers) and `oracle-e2e` (reuses this project's own unmodified `docker-compose.yml` via `docker compose up -d --wait`, D-06/D-07).**

## Performance

- **Tasks:** 2/2 completed
- **Files modified:** 40 (Task 1) + 1 new (Task 2)
- **Commits:** 2 task commits (this SUMMARY commit is separate)

## Accomplishments

- `pyproject.toml` gained pinned `ruff==0.16.5`/`mypy==2.3.1` dev dependencies and non-nuclear `[tool.ruff]`/`[tool.mypy]` configuration sections covering the whole repo, `airflow/dags/` included (D-14) -- deliberately not a bare `strict = true`, per the plan's own explicit warning against flooding the pre-existing test suite.
- `uv run ruff check .` and `uv run mypy .` both exit 0 across the whole repo, first time either tool has ever been run against this codebase.
- `airflow/dags/csv_ingest.py`'s four bare `dict`/`config_dict: dict` task-function annotations became `dict[str, object]`, matching `csv_processor.engine.process_chunks()`'s already-established convention (06-RESEARCH.md Pitfall 3).
- `.github/workflows/ci.yml` created with two independent, `pull_request`-triggered jobs -- `lint-type-unit` (ruff check/format --check, mypy, unit suite) and `oracle-e2e` (recreates the two gitignored first-clone files, `docker compose up -d --wait` against the project's own unmodified stack, runs `tests/e2e/`, tears down with `if: always()`) -- neither ever holding write permissions or running under `pull_request_target` (T-06-03).

## Task Commits

1. **Task 1: Whole-repo ruff/mypy config (D-14) + DAG type-annotation fix** - `dcc239c` (feat)
2. **Task 2: `.github/workflows/ci.yml` -- lint-type-unit + oracle-e2e** - `8085b4a` (feat)

**Plan metadata:** (this commit, docs: complete plan)

## Files Created/Modified

- `.github/workflows/ci.yml` - the two required CI jobs (new file)
- `pyproject.toml` - `[tool.ruff]`/`[tool.mypy]` sections, `ruff`/`mypy` dev deps, `uv.lock` updated
- `airflow/dags/csv_ingest.py` - Pitfall-3 `dict[str, object]` annotation fix
- `airflow/dags/_common/reporting.py`, `scripts/dag_polling.py`, `packages/csv-processor/src/csv_processor/errors.py`, `packages/csv-processor/src/csv_processor/config/errors.py` - bare `dict` -> `dict[str, object]` (mypy `disallow_any_generics` findings in this project's own source)
- `packages/csv-processor/src/csv_processor/engine.py` - `zip(..., strict=True)` (provably safe post the length-mismatch `continue`), new `_result_from_existing()` helper de-duplicating the two `find_existing_ingestion` result-building call sites and resolving their `object` vs `int`/`str` mypy mismatches via documented `cast()`s
- ~30 test files across `tests/unit/`, `tests/e2e/`, `tests/integration/`, `tools/corpus/` - `ruff --fix`/`ruff format` import-sort and quote-style normalization (mechanical, zero behavior change, confirmed by 214/214 unit tests still passing), plus a handful of genuine mypy-surfaced fixes (missing `assert spec is not None` before `module_from_spec`, a reused `row` variable of two different types in one test function, `cast()`/`str()` narrowing on `object`-typed dict values)

## Decisions Made

- Excluded `.planning/` from ruff's scope entirely (`extend-exclude`) after discovering `ruff format .` reformats Python code fences embedded in Markdown -- it had rewritten committed research/pattern docs (`.planning/phases/*/  *-PATTERNS.md`, `*-RESEARCH.md`, `.planning/research/ARCHITECTURE.md`, `lightweight-spec.md`) before this was caught; those unintended changes were reverted (`git checkout --`) and the exclusion added so it can't recur. This is squarely a "code, not historical planning docs" reading of D-14's "whole repo," not a scope reduction on the code itself.
- Scoped mypy's `disallow_any_generics`/`disallow_untyped_defs`/`check_untyped_defs` to this project's own source modules only (`csv_processor.*`, `generator.*`, `benchmark.*`, `scripts.*`, `csv_ingest`, `_common.*`) via `[[tool.mypy.overrides]]`, leaving `tests/` under the lighter repo-wide defaults -- enabling any of the three repo-wide immediately flooded the pre-existing test suite (written across Phases 1-5, before this phase's mypy adoption) with dozens of unrelated findings, which is exactly the "nuclear `strict = true`" outcome the plan's own acceptance criteria explicitly warns against.
- `airflow.*` given `ignore_missing_imports = true` -- Airflow is deliberately not installed in this root project's own environment (Phase 5 decision: only `docker/airflow/Dockerfile`'s separate image installs it), so mypy can never resolve it locally or in CI's `lint-type-unit` job either; this follows directly from that already-recorded architectural boundary.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Whole-repo lint/type findings beyond the plan's own declared files**
- **Found during:** Task 1
- **Issue:** Task 1's declared `files_modified` list only names `pyproject.toml`/`airflow/dags/csv_ingest.py`, but its own acceptance criteria requires `uv run ruff check .` and `uv run mypy .` to exit 0 across the *whole* repo. First-time adoption of both tools surfaced 61 ruff findings and 65 mypy findings across ~20 pre-existing files (mostly `tests/`, plus a few real `csv_processor`/`airflow/dags` source bugs: a `zip()` without `strict=`, several bare `dict` annotations, and two `object`-typed `_build_result` argument mismatches revealed by `find_existing_ingestion`'s untyped-dict return).
- **Fix:** Ran `ruff check --fix` + `ruff format` for the mechanical majority (import sorting, quote/line-wrap style); hand-fixed the remainder (E501 wraps, `strict=True` on two provably-safe `zip()` calls, `dict` -> `dict[str, object]`, a `_result_from_existing()` helper with documented `cast()`s, `assert spec is not None` before `module_from_spec()` in three test files, one reused-variable-name-different-type bug in `test_structural_validation.py`, and a few `cast()`/`str()` narrowings on `object`-typed dict lookups in tests).
- **Files modified:** see "Files Created/Modified" above (~40 files total in Task 1's commit).
- **Verification:** `uv run ruff check .`, `uv run ruff format --check .`, and `uv run mypy .` all exit 0; `uv run pytest tests/unit/ -x` still 214/214 passing; full suite (`tests/e2e/`, `tests/integration/` included) still collects cleanly with `pytest tests/ --collect-only`.
- **Committed in:** `dcc239c` (Task 1 commit).

**2. [Rule 1 - Bug] `ruff format .` reformatted Python code embedded in committed planning Markdown docs**
- **Found during:** Task 1 (immediately after the first `ruff format .` run)
- **Issue:** Ruff's Markdown fenced-code-block formatting silently rewrote Python snippets inside `.planning/phases/*/*-PATTERNS.md`, `*-RESEARCH.md`, `.planning/research/ARCHITECTURE.md`, and `.planning/research/lightweight-spec.md` -- historical research/pattern-mapping artifacts from every prior phase, never intended targets of D-14's "whole repo" scope (which is about code, `airflow/dags/` included, not planning history).
- **Fix:** `git checkout --` on all 15 affected `.planning/` files to discard the unintended reformatting, then added `extend-exclude = [".planning"]` to `[tool.ruff]` so it cannot recur on any future `make lint`/CI run.
- **Files modified:** `pyproject.toml` (the exclude), plus the 15 reverted `.planning/` files (no net diff on those).
- **Verification:** `git status --short | grep .planning` shows no ruff-caused changes after the fix; `uv run ruff format --check .` reports 73 files checked (matches the actual `.py` + non-`.planning` `.md` file count), confirming `.planning/` is genuinely excluded.
- **Committed in:** `dcc239c` (Task 1 commit; the exclude and the reverts are both part of that one commit since the unintended changes were caught and fixed before any commit was made).

---

**Total deviations:** 2 auto-fixed (1 missing-critical-functionality, 1 bug/scope-boundary correction)
**Impact on plan:** Both directly required to satisfy Task 1's own literal acceptance criteria ("`ruff check .`/`mypy .` exit 0 across the whole repo"). No unrelated scope creep -- every touched file was either a genuine lint/type finding or (for the `.planning/` case) an unintended side effect caught and corrected within the same task, before commit.

## Issues Encountered

None beyond what's documented above as deviations.

## User Setup Required

None - no external service configuration required. GitHub repo Branch Protection (naming `lint-type-unit`/`oracle-e2e` as required status checks, Pitfall 6) remains an outstanding **manual, repo-admin-only** step -- explicitly not something any workflow YAML or automated command in this repo can configure or verify. It is documented as outstanding in `docs/development.md` per Plan 05's own scope (not yet written as of this plan).

## Next Phase Readiness

- `.github/workflows/ci.yml` is ready to run on the next real PR against this repo (not exercised live in this session -- no PR was opened as part of this plan; the YAML's own structural verification and the underlying `ruff`/`mypy`/`pytest tests/unit/` commands were all run and confirmed passing locally).
- `oracle-e2e`'s live behavior (real `docker compose up -d --wait` + `tests/e2e/` inside an actual GitHub Actions runner) is not proven by this plan -- Plan 01's `tests/e2e/test_csv_ingest_e2e.py` exists and was confirmed to *collect* cleanly, but running it inside CI for the first time is the natural next confirmation, either via a real PR or Plan 04/05's own verification work.
- Branch Protection configuration (Pitfall 6) is a blocking prerequisite for D-07's "required status check" guarantee to actually hold -- flagged, not silently assumed done.

---
*Phase: 06-end-to-end-verification-benchmark-ci-docs*
*Completed: 2026-08-30*

## Self-Check: PASSED

- FOUND: `.github/workflows/ci.yml`
- FOUND: `pyproject.toml`
- FOUND: `airflow/dags/csv_ingest.py`
- FOUND: `packages/csv-processor/src/csv_processor/engine.py`
- FOUND commit: `dcc239c`
- FOUND commit: `8085b4a`
