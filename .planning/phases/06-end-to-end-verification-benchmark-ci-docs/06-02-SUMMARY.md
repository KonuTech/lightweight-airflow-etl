---
phase: 06-end-to-end-verification-benchmark-ci-docs
plan: 02
subsystem: testing
tags: [oracledb, benchmark, performance, executemany, csv_processor]

# Dependency graph
requires:
  - phase: 04-oracle-bulk-load-idempotency-engine-entrypoint
    provides: "csv_processor.load.insert_rows() (the real executemany() bulk-write path)"
  - phase: 03-csv-parsing-detect-validate-normalize-chunk
    provides: "csv_processor.engine.process_chunks() (the shared parse/validate generator)"
  - phase: 02-config-contract-csv-generator
    provides: "generator/generate_csv.py --dataset/--rows CLI for deterministic fixtures"
provides:
  - "benchmark/naive_loader.py -- genuine per-row cursor.execute() Oracle write baseline (D-01)"
  - "benchmark/run_benchmark.py -- CLI harness driving naive/bulk modes off one shared process_chunks() parse pass"
  - "docs/benchmark.md -- real ~100K-row TEST-04 results: rows/sec, peak memory, Oracle load time, 182.85x speedup ratio, per-chunk timing"
affects: [06-05-docs-and-ci, TEST-04]

actuals:
  tokens: 3907
  tasks: 2
  commits: 2

tech-stack:
  added: []
  patterns:
    - "Two subprocess invocations (never two calls in one process) to keep resource.getrusage() peak-RSS isolated between naive/bulk runs"
    - "Machine-parseable BENCHMARK_JSON <json> stdout line for a downstream doc-generation step to grep/parse"

key-files:
  created:
    - benchmark/__init__.py
    - benchmark/naive_loader.py
    - benchmark/run_benchmark.py
    - docs/benchmark.md
  modified: []

key-decisions:
  - "Naive path re-validates table/column identifiers via is_safe_identifier() before interpolating the INSERT string, mirroring load.insert_rows()'s own defense-in-depth check (T-06-04)"
  - "Docstrings/comments in naive_loader.py avoid the literal substring 'executemany' entirely (rephrased to 'array-bind bulk-insert call') so grep -c executemany benchmark/naive_loader.py is genuinely 0, not just semantically true"
  - "Both benchmark modes write only chunk_valid rows (never chunk_invalid) -- isolates exactly the Oracle write-strategy variable under test, matching the plan's literal action text"
  - "Fixture written to a benchmark-only data/benchmark/<dataset>_<rows>_<seed>.csv path, distinct from generate_csv.py's own day-stamped data/<dataset>/ convention -- never clobbers a real generated fixture, byte-identical across the two subprocess invocations for the same seed"

patterns-established:
  - "benchmark/ as a throwaway, non-tests, non-package-engine top-level directory for one-off performance proofs (D-04)"

requirements-completed: [TEST-04]

coverage:
  - id: D1
    description: "A genuine naive-loop Oracle write path (one cursor.execute() per row) exists, isolated from the real chunked/bulk executemany() path"
    requirement: TEST-04
    verification:
      - kind: other
        ref: "grep -c executemany benchmark/naive_loader.py == 0"
        status: pass
      - kind: e2e
        ref: "uv run python -m benchmark.run_benchmark --mode naive --rows 1000 --seed 1 (exit 0, live Oracle)"
        status: pass
    human_judgment: false
  - id: D2
    description: "Both naive and chunked/bulk runs consume the identical process_chunks() generator -- only the Oracle write strategy differs"
    requirement: TEST-04
    verification:
      - kind: e2e
        ref: "naive and bulk --rows 1000 --seed 1 runs report identical total_rows=1000/valid_rows=900/invalid_rows=100"
        status: pass
      - kind: e2e
        ref: "naive and bulk --rows 100000 --seed 20260101 runs report identical total_rows=100000/valid_rows=90000/invalid_rows=10000"
        status: pass
    human_judgment: false
  - id: D3
    description: "A ~100K-row customers run records rows/sec, peak memory, and Oracle load time for both approaches, committed to docs/benchmark.md with speedup ratio, run metadata, and per-chunk timing"
    requirement: TEST-04
    verification:
      - kind: other
        ref: "docs/benchmark.md contains Run Metadata / Comparison Table / Speedup Ratio / Per-Chunk Timing Breakdown sections, all populated from the real 100000-row run"
        status: pass
    human_judgment: false

duration: 25min
completed: 2026-08-29
status: complete
---

# Phase 6 Plan 2: Naive vs. Chunked/Bulk Oracle Benchmark Summary

**A genuine per-row Oracle insert loop measured 182.85x slower than the real `executemany()` bulk path at ~100K `customers` rows, both driven by the identical `process_chunks()` parse pass, committed to `docs/benchmark.md`.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-08-29T22:32:53Z
- **Completed:** 2026-08-29T22:58:00Z
- **Tasks:** 2
- **Files modified:** 4 (all created)

## Accomplishments
- `benchmark/naive_loader.py`: a genuinely isolated naive-loop Oracle write baseline (D-01) — one `cursor.execute()` call per row, verified via `grep -c executemany` returning `0`, never `executemany()` at any chunk size.
- `benchmark/run_benchmark.py`: a CLI harness (`--mode {naive,bulk}`) that drives both write strategies off the exact same `csv_processor.engine.process_chunks()` generator (D-03) — proven at both 1,000-row smoke scale and the real ~100,000-row run that the row-split counts are byte-identical regardless of write mode.
- `docs/benchmark.md`: real measured results at ~100K rows — naive 4,268.08 rows/sec vs. bulk 780,429.15 rows/sec (a 182.85x / 18,285% speedup), near-identical peak RSS (~130 MB both, confirming memory isn't the differentiator here), and a 20-chunk per-chunk timing table for the bulk run showing flat ~5.8ms mean write latency with no growth trend across chunks.

## Task Commits

Each task was committed atomically:

1. **Task 1: Naive vs. bulk write paths, wired to the shared parse pass — smoke-scale proof of isolation** - `4635637` (feat)
2. **Task 2: Full ~100K-row benchmark run, results committed to docs/benchmark.md** - `10b3cba` (docs)

_Note: this plan's tasks were both `type="auto"`, no TDD RED/GREEN split._

## Files Created/Modified
- `benchmark/__init__.py` - throwaway package docstring/marker (D-04)
- `benchmark/naive_loader.py` - `run_naive()`, the genuine per-row Oracle write baseline
- `benchmark/run_benchmark.py` - CLI orchestrator: generates a fixture, deletes target tables, iterates `process_chunks()`, dispatches to naive/bulk write, measures peak RSS + per-chunk timing, prints a `BENCHMARK_JSON` summary line
- `docs/benchmark.md` - the committed TEST-04 evidence: run metadata, comparison table, speedup ratio, per-chunk timing breakdown

## Decisions Made
- Reused `is_safe_identifier()` in both `naive_loader.py` and `run_benchmark.py`'s table-cleanup step, mirroring `load.insert_rows()`'s defense-in-depth pattern (T-06-04) rather than trusting `DatasetConfig`'s config-load-time validation alone.
- Rephrased all `naive_loader.py` docstrings/comments to avoid the literal substring `executemany` (used "array-bind bulk-insert call" instead) after discovering the plan's own acceptance criteria (`grep -c executemany benchmark/naive_loader.py` must be `0`) is a literal string check that would otherwise fail on prose explaining what the naive path deliberately does NOT do.
- Both benchmark write paths write only `chunk_valid` rows (never `chunk_invalid`), per the plan's literal action text — the benchmark isolates the Oracle write-strategy variable, not the full `process()` entrypoint's dual valid/invalid write behavior.
- Benchmark fixtures are written to a dedicated `data/benchmark/<dataset>_<rows>_<seed>.csv` path (gitignored via the existing `/data/` rule) rather than `generate_csv.py`'s own day-stamped `data/<dataset>/` convention, so repeated benchmark runs never clobber a real generated fixture and both subprocess invocations for the same `(dataset, rows, seed)` read byte-identical input.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `naive_loader.py`'s docstrings failed the plan's own literal `grep -c executemany` acceptance criterion**
- **Found during:** Task 1 (verifying acceptance criteria after initial implementation)
- **Issue:** The file's module/function docstrings and inline comments explained the naive path's intent by contrasting it against `executemany()` (e.g. "NEVER `cursor.executemany()`"), which is semantically correct but made `grep -c executemany benchmark/naive_loader.py` return `4` instead of the plan's required `0`.
- **Fix:** Rephrased every mention to avoid the literal substring `executemany` (e.g. "the array-bind bulk-insert call", "the bulk call run at `chunk_size=1`") while preserving the exact same explanatory intent.
- **Files modified:** `benchmark/naive_loader.py`
- **Verification:** `grep -c executemany benchmark/naive_loader.py` now returns `0`; both smoke-scale (`--rows 1000`) and full-scale (`--rows 100000`) runs re-verified passing after the edit.
- **Committed in:** `4635637` (part of Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Cosmetic-only fix to satisfy a literal acceptance-criteria grep; no behavior change, no scope creep.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required. Both benchmark runs executed against the already-running local Oracle container (`make up` already up from Plan 06-01).

## Next Phase Readiness
- TEST-04 is proven and documented; `docs/benchmark.md` is ready for `docs/development.md`/README linking in a later Phase 6 plan (D-15/D-16).
- `benchmark/run_benchmark.py`'s `BENCHMARK_JSON` output convention is available if a future plan wants to re-run/re-verify the benchmark (e.g. in CI, though CI-01/D-06 scope Oracle+e2e only, not the benchmark itself).
- No blockers for Plan 06-03/06-04/06-05.

---
*Phase: 06-end-to-end-verification-benchmark-ci-docs*
*Completed: 2026-08-29*

## Self-Check: PASSED

All created files (`benchmark/__init__.py`, `benchmark/naive_loader.py`, `benchmark/run_benchmark.py`, `docs/benchmark.md`) confirmed present on disk. Both task commits (`4635637`, `10b3cba`) confirmed present in `git log --oneline --all`.
