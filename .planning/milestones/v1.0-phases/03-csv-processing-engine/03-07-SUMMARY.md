---
phase: 03-csv-processing-engine
plan: 07
subsystem: database
tags: [csv-processing, validation, gap-closure, data-integrity]

# Dependency graph
requires:
  - phase: 03-csv-processing-engine (plan 06)
    provides: "source.py's _filtered_rows() PASS-2 exclusion generator (CR-02) that wired detect_header()'s footer_row_indices/repeated_header_row_indices into the real read for the first time -- this plan fixes the Critical silent-data-loss regression 03-REVIEW.md found in that fix"
provides:
  - "csv_processor.source._filtered_rows(paired_rows, *, start_index, excluded_indices, header_field_count, raw_header) -- rewritten to re-validate every sample-derived candidate exclusion against the REAL, full-file row content before excluding it, never on sample-derived index membership alone"
  - "csv_processor.source.prepare_source()'s PASS-2 call site threading header_field_count=len(header_detection.raw_header) and raw_header=header_detection.raw_header into the rewritten _filtered_rows()"
  - "Two new regression tests proving both the fix (>64 KiB well-formed file loses zero rows) and no regression of G-03-2 (in-sample repeated-header row still excluded, even inside a >64 KiB file)"
affects: ["04-oracle-bulk-load"]

# Actuals (#2632)
actuals:
  tokens: 2650
  tasks: 2
  commits: 2

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "_filtered_rows() re-validation pattern: a sample-derived candidate exclusion is only ever acted upon after independently re-deriving the SAME criterion detect_header() would apply (field-count mismatch or exact raw_header equality) against the row's own REAL content -- sample-derived indices are treated as candidates to verify, never as ground truth to trust"

key-files:
  created: []
  modified:
    - packages/csv-processor/src/csv_processor/source.py
    - tests/unit/test_structural_validation.py

key-decisions:
  - "Task 1 (tdd) and Task 2 (auto)'s new tests were authored and committed together in a single test-only RED commit, since both tests exercise the same rewritten _filtered_rows() and Task 2's test cannot independently pass until Task 1's fix lands (Task 2 is type=auto, not tdd -- it has no RED/GREEN cycle of its own, it proves a non-regression of an already-fixed behavior). Splitting them into two separate test-authoring commits would have required an artificial partial-file edit with no independent verification value."

patterns-established: []

requirements-completed: [ENGINE-01, ENGINE-05, TEST-01]

coverage:
  - id: D1
    description: "A well-formed CSV file larger than SAMPLE_BYTES (64 KiB) loses zero rows during process_chunks() -- every real data row appears in valid_rows, never silently absent, even when a row's bytes straddle the detection sample's byte cutoff"
    requirement: ENGINE-05
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_large_well_formed_file_loses_zero_rows_across_sample_boundary"
        status: pass
    human_judgment: false
  - id: D2
    description: "A genuine metadata preamble line, a trailing footer line, and an interior repeated-header row within the 64 KiB detection sample are still excluded from both valid_rows and invalid_rows, unchanged from G-03-2's original fix (03-06), including when the file's total size exceeds the sample"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_preamble_footer_and_repeated_header_rows_excluded_from_processing"
        status: pass
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_repeated_header_row_excluded_even_when_file_exceeds_sample_size"
        status: pass
    human_judgment: false
  - id: D3
    description: "Full unit suite (179 pre-existing + 2 new) passes with zero regressions"
    requirement: TEST-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/ -q (181 passed)"
        status: pass
    human_judgment: false

# Metrics
duration: 15min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 7: Gap Closure — Sample-Boundary Data-Loss Regression (CR-03) Summary

**Closed the Critical silent-data-loss regression 03-REVIEW.md found in 03-06's own CR-02 fix: `_filtered_rows()` no longer trusts `detect_header()`'s sample-derived footer/repeated-header indices unconditionally — it re-validates each candidate against the row's real, full-file content before excluding it.**

## Performance

- **Duration:** ~15 min
- **Completed:** 2026-08-29
- **Tasks:** 2/2 completed
- **Files modified:** 2

## Accomplishments

- Rewrote `source.py`'s `_filtered_rows()` to accept two new required keyword-only parameters (`header_field_count`, `raw_header`) and re-validate every sample-derived candidate exclusion against the REAL row at that index — a row is only excluded when its real field count differs from the header's or its real values exactly equal `raw_header`, never on sample-derived index membership alone
- Threaded `header_field_count=len(header_detection.raw_header)` and `raw_header=header_detection.raw_header` into the one PASS-2 call site; left `excluded_indices`'s construction, the header-skip loop, and all of PASS 1 completely untouched, exactly as the plan scoped
- Reproduced 03-REVIEW.md's exact bug live (RED): a 6,000-row, ~117 KiB well-formed `id,name` CSV lost exactly 1 row (`5999` valid, `0` invalid) against the pre-fix code
- Added `test_large_well_formed_file_loses_zero_rows_across_sample_boundary` (proves the Critical fix — GREEN after the rewrite: `6000` valid, `0` invalid, every `id` present via a set comparison) and `test_repeated_header_row_excluded_even_when_file_exceeds_sample_size` (proves no regression of G-03-2 — an interior repeated-header row within the sample is still excluded even when the file's 3,000 trailing filler rows push total size past `SAMPLE_BYTES`)
- Renamed the docstring's `(CR-02)` tag to `(CR-03)` since this rewrite supersedes 03-06's unconditional-trust version
- Independently re-ran 03-REVIEW.md's own live repro script directly against the fixed `process_chunks()`: `valid: 6000 invalid: 0`, zero missing IDs — matches the plan's own automated test exactly
- Full unit suite grew from 179 to 181 tests, zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1 (RED): Add failing regression tests for sample-boundary data loss** - `44c578b` (test) — includes both this task's own test and Task 2's regression test (see Decisions Made)
2. **Task 1 (GREEN) / Task 2: Re-validate footer/repeated-header candidates against real row content** - `875bfa9` (fix)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/source.py` - `_filtered_rows()` rewritten to re-validate every candidate exclusion against real row content; PASS-2 call site threads `header_field_count`/`raw_header`
- `tests/unit/test_structural_validation.py` - `_large_id_name_config()` helper, `test_large_well_formed_file_loses_zero_rows_across_sample_boundary`, `test_repeated_header_row_excluded_even_when_file_exceeds_sample_size`, `from csv_processor import source` import added

## Decisions Made

- Task 1's RED commit included Task 2's new test alongside Task 1's own — both tests exercise the same rewritten function and Task 2 (type="auto") has no independent RED/GREEN cycle of its own; it proves a non-regression that can only genuinely pass once Task 1's fix lands. Splitting them into two separate test-authoring commits would have added no independent verification value.

## Deviations from Plan

None beyond the commit-grouping decision documented above — both tasks' `<action>` blocks were implemented literally; all `<acceptance_criteria>` and the plan's own `<verification>` block were independently re-run and confirmed:

- `uv run pytest tests/unit/test_structural_validation.py -x -q` → 12 passed (10 pre-existing + 2 new)
- `uv run pytest tests/unit/ -q` → 181 passed (179 pre-existing + 2 new), zero failures, zero regressions
- Live repro (executed directly against `process_chunks()`, not just via pytest): a 6,000-row well-formed `id,name` CSV over `SAMPLE_BYTES` now reports `valid: 6000 invalid: 0`, zero missing IDs

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None. The fix is fully wired and exercised end-to-end with live regression tests; no placeholder/hardcoded-empty data paths introduced.

## Next Phase Readiness

`03-REVIEW.md`'s single Critical finding is now closed and independently re-verified. Phase 3's `csv_processor.engine.process_chunks(file_path, config)` public surface is unchanged in shape — Phase 4's Oracle bulk-load work builds on the same function signature, now with the row-count-accuracy guarantee (ENGINE-05) actually holding on files larger than the 64 KiB detection sample. `03-REVIEW.md`'s remaining Warning/Info findings (WR-01 injected-key collision, WR-02 file-handle leak on PASS-2 setup exception, WR-03 `required:false`+`nullable:false` contradiction, IN-01/IN-02) were not in this gap-closure plan's scope (it targeted only the one Critical finding) and remain open for a future pass if prioritized. No blockers for Phase 4.

---
*Phase: 03-csv-processing-engine*
*Completed: 2026-08-29*

## Self-Check: PASSED

- FOUND: packages/csv-processor/src/csv_processor/source.py
- FOUND: tests/unit/test_structural_validation.py
- FOUND: .planning/phases/03-csv-processing-engine/03-07-SUMMARY.md
- FOUND commit: 44c578b
- FOUND commit: 875bfa9
