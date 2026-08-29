---
phase: 06-end-to-end-verification-benchmark-ci-docs
plan: 04
subsystem: testing
tags: [oracle, sqlplus, readme, github-actions, evidence, dag-polling, executive-summary]

# Dependency graph
requires:
  - phase: 06-end-to-end-verification-benchmark-ci-docs
    provides: "06-01's scripts/dag_polling.py (trigger_dag/wait_for_task_state/wait_for_dag_run_result) reused verbatim"
  - phase: 06-end-to-end-verification-benchmark-ci-docs
    provides: "06-03's .github/workflows/ci.yml oracle-e2e bring-up pattern (first-clone-file recreation + docker compose up -d --wait), mirrored in readme-summary.yml"
provides:
  - "scripts/verify_evidence.sql -- committed, reproducible SQL evidence capture (row counts + customers-JOIN-orders business report)"
  - "scripts/regenerate_readme_summary.py -- real trigger + Oracle evidence query + idempotent marker-based README.md splice"
  - "README.md's live Executive Summary section (seeded with real evidence from this plan's own run)"
  - ".github/workflows/readme-summary.yml -- push-to-main auto-commit using default GITHUB_TOKEN"
affects: [06-05, docs, ci]

# Actuals (#2632)
actuals:
  tokens: 6030
  tasks: 3
  commits: 3

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Marker-delimited idempotent file splice (<!-- EXEC-SUMMARY:START/END -->) for CI-regenerated content"
    - "Business-report SQL text mirrored verbatim between a committed .sql script and its Python-driven regeneration script, never re-authored independently"
    - "Default GITHUB_TOKEN + permissions: contents: write scoped to a single job for a CI auto-commit step (never a PAT)"

key-files:
  created:
    - scripts/verify_evidence.sql
    - scripts/regenerate_readme_summary.py
    - .github/workflows/readme-summary.yml
  modified:
    - README.md

key-decisions:
  - "Business-report SQL text lives once (verify_evidence.sql) and is mirrored verbatim as Python string constants in regenerate_readme_summary.py -- never re-derived independently, so the two can never silently diverge (this plan's own key_link requirement)"
  - "Deferred-wake proof (D-11b) captured from the readme-summary job's own customers ingestion run, not spliced from a separate job -- keeps all Executive Summary numbers internally consistent (same run, same moment), per 06-RESEARCH.md Open Question 2's recommendation"
  - "Executive Summary build is fully in-memory until every step (both ingestions, the Oracle query) has succeeded; README.md is written exactly once, at the very end -- guarantees the plan's own prohibition against a silent stale/misleading Executive Summary"

patterns-established:
  - "Any script that regenerates committed, evidence-bearing content must build the full replacement in memory and write it in a single final step, never incrementally -- so a mid-run failure can never leave partial/misleading output in place"

requirements-completed: [TEST-03, DOC-01]

coverage:
  - id: D1
    description: "scripts/verify_evidence.sql reproducibly returns real row-count and customers-JOIN-orders business-report evidence from the live Oracle container, read-only (no UPDATE/INSERT/DELETE)"
    requirement: "TEST-03"
    verification:
      - kind: manual_procedural
        ref: "docker compose exec -T oracle sqlplus -s admin/admin@//localhost:1521/FREEPDB1 < scripts/verify_evidence.sql"
        status: pass
    human_judgment: false
  - id: D2
    description: "scripts/regenerate_readme_summary.py runs a real ingestion for both datasets, captures the deferred-wake proof, and seeds README.md's Executive Summary with genuine evidence; idempotent on re-run"
    requirement: "DOC-01"
    verification:
      - kind: manual_procedural
        ref: "uv run python scripts/regenerate_readme_summary.py (run twice against the live stack; verified byte-identical content outside the marker pair on the second run)"
        status: pass
    human_judgment: false
  - id: D3
    description: ".github/workflows/readme-summary.yml automates the Executive Summary regeneration on push to main only, using the default GITHUB_TOKEN scoped to contents:write for just this job, no PAT anywhere"
    requirement: "DOC-01"
    verification:
      - kind: other
        ref: "python3 -c \"import yaml; d=yaml.safe_load(open('.github/workflows/readme-summary.yml')); assert d[True]=={'push':{'branches':['main']}}; j=d['jobs']['regenerate-executive-summary']; assert j['permissions']=={'contents':'write'}; assert 'permissions' not in d\""
        status: pass
    human_judgment: false

duration: ~25min
completed: 2026-08-29
status: complete
---

# Phase 6 Plan 4: Oracle Evidence Capture & Live README Executive Summary Summary

**A committed SQL script and a real-trigger Python regenerator together prove the pipeline works with live Oracle evidence (row counts + a customers-JOIN-orders business report), seeded into README.md's Executive Summary and kept fresh via a push-to-main GitHub Actions job under the default GITHUB_TOKEN.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-08-29T23:04:00Z
- **Tasks:** 3
- **Files modified:** 4 (3 created, 1 modified)

## Accomplishments
- `scripts/verify_evidence.sql`: two read-only `SELECT`s -- latest per-dataset `ingestion_metadata` row counts, and a `customers_valid JOIN orders_valid` business report grouped by `country` (the explicitly-flagged region proxy, no literal `region` column exists) x month-of-`order_date`, with order count + total/average amount. Verified against the live Oracle container: exits 0, both result sets print, zero `ORA-` errors.
- `scripts/regenerate_readme_summary.py`: triggers `csv_ingest` for both `customers` and `orders` via `scripts/dag_polling.py`'s already-proven REST flow, confirms `wait_for_file` genuinely reaches `deferred` BEFORE writing each run-unique fixture (Pitfall 4 ordering), captures the `customers` run's deferred-wake moment + `dag_run_id` as the README's proof line, then queries Oracle with the exact SELECTs mirrored from `verify_evidence.sql` (top-10 business report via `FETCH FIRST 10 ROWS ONLY`) to render and idempotently splice the Executive Summary into `README.md`.
- Ran the regeneration script for real against the live stack -- `README.md` is now seeded with genuine evidence (real row counts, a real `dag_run_id`/timestamp, a real business-report table), and a second run was verified idempotent: exactly one marker pair, and everything outside `<!-- EXEC-SUMMARY:START/END -->` is byte-identical between runs.
- `.github/workflows/readme-summary.yml`: `push: branches: [main]` only, mirrors `ci.yml`'s `oracle-e2e` bring-up sequence verbatim (first-clone-file recreation + `docker compose up -d --wait`), runs the regeneration script, then commits the refreshed `README.md` via `stefanzweifel/git-auto-commit-action@v7.2.0` under `permissions: contents: write` scoped to only this job -- default `GITHUB_TOKEN`, never a PAT, so the resulting commit structurally cannot re-trigger a `push`-based workflow run (Pitfall 2); `[skip ci]` kept in the commit message as defense-in-depth.

## Task Commits

Each task was committed atomically:

1. **Task 1: scripts/verify_evidence.sql -- reproducible row-count + business-report evidence (D-09/D-10)** - `baaf068` (feat)
2. **Task 2: scripts/regenerate_readme_summary.py -- real trigger + evidence capture, seeds README's Executive Summary now** - `378851c` (feat)
3. **Task 3: .github/workflows/readme-summary.yml -- push-to-main auto-commit, default GITHUB_TOKEN** - `769c308` (feat)

_No TDD tasks in this plan -- all three are `type="auto"`._

## Files Created/Modified
- `scripts/verify_evidence.sql` - Committed, reproducible SQL evidence-capture script (D-09/D-10)
- `scripts/regenerate_readme_summary.py` - Real-ingestion trigger + Oracle evidence query + idempotent README.md splice (D-11/D-12)
- `README.md` - Executive Summary section seeded with real evidence, at the top of the file
- `.github/workflows/readme-summary.yml` - Push-to-main auto-commit workflow, default GITHUB_TOKEN (D-12/D-13)

## Decisions Made
- Row-count table columns kept to dataset/file_name/total/valid/invalid/status/processed_at (dropped `checksum` from the README's rendered table -- present in the SQL evidence script's own output, but a 64-char hex string adds no readability value to a proof-of-life summary table; the full value remains visible via `make verify-evidence`/`scripts/verify_evidence.sql` directly).
- Business-report table renders a "no rows yet" placeholder row if the JOIN ever returns zero rows (defensive; did not trigger in this run since accumulated prior-phase Oracle data already produces thousands of matching rows).

## Deviations from Plan

None — plan executed as written. Two implementation-detail refinements surfaced while verifying, both resolved without deviating from the plan's own acceptance criteria:

1. **[Rule 1 - Bug] Stale `.mypy_cache` produced a false `Module "csv_processor" has no attribute "load"` error** for the new script in isolation. Root-caused to a stale cache entry (not a real type error) -- `rm -rf .mypy_cache && uv run mypy .` came back clean (`Success: no issues found in 69 source files`), matching the same `from csv_processor import load` pattern already used identically in `benchmark/run_benchmark.py`/`tests/e2e/conftest.py`. No code change required.
2. **[Rule 1 - Bug] Task 3's literal verify command (`d['on']`) fails under plain `yaml.safe_load`** because PyYAML's default (YAML 1.1) resolver treats the bare scalar `on` as boolean `True`, not the string key `'on'` -- the same quirk `.github/workflows/ci.yml` already has (confirmed: `yaml.safe_load(open('ci.yml'))` also returns `True` as a dict key, not `'on'`). 06-03-SUMMARY.md's own verification worked around this by never touching `d['on']` at all. Verified this file's actual acceptance criteria (exact `push: branches: [main]` trigger, `contents: write` scoped only to the one job, no PAT/secret reference anywhere) using `d[True]` instead of `d['on']` -- all three checks pass. No file content changed; this is a verification-tooling quirk, not a defect in `readme-summary.yml`.

---

**Total deviations:** 0 plan changes; 2 verification-method notes (both pre-existing tooling quirks, not caused by this plan's code).
**Impact on plan:** None -- all three tasks' actual acceptance criteria are met exactly as specified.

## Issues Encountered
None beyond the two verification-tooling notes above (both resolved by using the correct check, not by changing any committed file).

## User Setup Required

None for this plan's own tasks. Note (informative only, not this plan's job): Pitfall 6 (06-RESEARCH.md) still applies from Plan 03 -- branch protection requiring `lint-type-unit`/`oracle-e2e` as required status checks is a separate, one-time GitHub repo-admin setting, not something any workflow YAML alone accomplishes. `readme-summary.yml` itself needs no branch-protection configuration (it isn't a required check).

## Next Phase Readiness

- `scripts/verify_evidence.sql` and `scripts/regenerate_readme_summary.py` are both real, committed, reproducible artifacts -- ready for Plan 05 to wire `make verify-evidence` (Makefile target) and `docs/development.md`'s "reproduce CI locally" section around them.
- README.md already carries a live, real Executive Summary -- Plan 05's README rewrite (D-16, "summary + links" below the Executive Summary) can proceed without needing to seed placeholder evidence first.
- No blockers for Plan 05.

---
*Phase: 06-end-to-end-verification-benchmark-ci-docs*
*Completed: 2026-08-29*

## Self-Check: PASSED

All created files confirmed present on disk; all task commit hashes (`baaf068`, `378851c`, `769c308`) confirmed present in `git log`.
