---
phase: 03-csv-processing-engine
plan: 06
subsystem: database
tags: [csv-processing, validation, gap-closure, dataplat-isolation]

# Dependency graph
requires:
  - phase: 03-csv-processing-engine (plans 03-05)
    provides: "source.py's PASS-1/PASS-2 detect-once orchestrator, engine.py's process_chunks() chunked generator, detect/filename.py's vendored mask compiler -- all already shipped, this plan fixes two Critical bugs and one Warning found against them by 03-VERIFICATION.md/03-REVIEW.md"
provides:
  - "csv_processor.source.prepare_source()'s required_names-filtered missing-column check (only column.required: true can trigger MISSING_REQUIRED_COLUMN)"
  - "csv_processor.source._filtered_rows(paired_rows, *, start_index, excluded_indices) -- new private generator excluding footer/repeated-header rows from PASS 2's real read"
  - "csv_processor.engine.process_chunks()'s row_dict backfill preventing KeyError on a legitimately-absent optional column"
  - "csv_processor.detect.filename.FilenameMaskConfig -- new local dataclass, zero dataplat dependency"
affects: ["04-oracle-bulk-load"]

# Actuals (#2632)
actuals:
  tokens: 2941
  tasks: 3
  commits: 3

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "source.py's missing-column check now derives required_names = {c.name for c in config.columns if c.required} as a subset of declared_names -- the EXTRA_UNEXPECTED_COLUMN check deliberately keeps comparing against the full declared_names set, so a present optional column is never misflagged as extra"
    - "engine.py's row_dict backfill (row_dict.setdefault-equivalent for-loop) is the ONLY place a config-declared, header-absent column gets a value -- validate.check_row()/normalize_row()'s row[column.name] lookups stay completely unmodified, treating a never-in-file value identically to a present-but-blank one, governed by the same nullable flag"
    - "source.py's PASS 2 now consumes detect_header()'s own header_row_index/footer_row_indices/repeated_header_row_indices for the first time via a skip-count loop plus the new _filtered_rows() absolute-index exclusion generator -- both operate on the SAME absolute-row-index convention detect_header() already established, since PASS 2 reopens and re-reads the identical file from byte 0"

key-files:
  created:
    - tests/unit/test_filename_no_dataplat_import.py
  modified:
    - packages/csv-processor/src/csv_processor/source.py
    - packages/csv-processor/src/csv_processor/engine.py
    - packages/csv-processor/src/csv_processor/detect/filename.py
    - tests/unit/test_structural_validation.py

key-decisions:
  - "engine.py's KeyError-prevention backfill was folded into Task 1 (not a separate task) since fixing source.py's required-column filter alone would immediately reintroduce a crash the moment a required:false column is genuinely absent -- the two changes are one indivisible correctness fix, matching the plan's own <objective> framing"
  - "detect/filename.py's WR-01 fix (dataplat import removal) was folded into this same gap-closure plan as a third, lower-risk task, per the plan's own explicit rationale (one-line-cause fix with a real, now-tested consumer function)"

patterns-established: []

requirements-completed: [ENGINE-01, TEST-01]

coverage:
  - id: D1
    description: "A CSV that omits a column declared required: false (e.g. customers.json's own signup_country) processes successfully instead of raising MISSING_REQUIRED_COLUMN, with the absent column typed as None on every valid row"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_optional_column_absent_from_header_processes_successfully"
        status: pass
    human_judgment: false
  - id: D2
    description: "A CSV omitting a required: true column still raises MISSING_REQUIRED_COLUMN unchanged -- the fix narrows the check, it does not weaken it"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_09_missing_column"
        status: pass
    human_judgment: false
  - id: D3
    description: "A genuine metadata preamble line, a trailing footer line, and a repeated interior header row are all excluded from both valid_rows and invalid_rows -- only real data rows reach validation"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_preamble_footer_and_repeated_header_rows_excluded_from_processing"
        status: pass
    human_judgment: false
  - id: D4
    description: "detect/filename.py has zero dataplat import anywhere in executable code; its only prior consumer-facing type (FilenameMaskConfig) is now a real, local, project-owned class, proven via a live parse_filename() call"
    requirement: TEST-01
    verification:
      - kind: unit
        ref: "tests/unit/test_filename_no_dataplat_import.py::test_no_dataplat_import_statement"
        status: pass
      - kind: unit
        ref: "tests/unit/test_filename_no_dataplat_import.py::test_filename_mask_config_is_real_and_importable"
        status: pass
    human_judgment: false
  - id: D5
    description: "Full unit suite (175 pre-existing + 4 new) passes with zero regressions"
    requirement: TEST-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/ -q (179 passed)"
        status: pass
    human_judgment: false

# Metrics
duration: 25min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 6: Gap Closure — MISSING_REQUIRED_COLUMN Filtering, Preamble/Footer Exclusion, dataplat Import Removal Summary

**Closed both Critical structural-validation bugs 03-VERIFICATION.md found against the project's own shipped `customers.json` (a `required: false` column can now be genuinely absent; a metadata preamble/footer/repeated-header row can no longer corrupt row counts), plus the one residual `dataplat` import CLAUDE.md's never-import rule flagged.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-08-29
- **Tasks:** 3/3 completed
- **Files modified:** 4 (1 created, 3 modified)

## Accomplishments

- Fixed `source.py`'s `prepare_source()` missing-column check to filter by `column.required` (`required_names = {c.name for c in config.columns if c.required}`) instead of treating every declared column as mandatory — `customers.json`'s own `signup_country` (`required: false`) can now be genuinely absent from a file without a false whole-file reject, while `country` (`required: true`) still correctly triggers `MISSING_REQUIRED_COLUMN` when missing
- Fixed the resulting `KeyError` risk in `engine.py`'s `process_chunks()` by backfilling any config-declared column the detected header omits with an empty-string default — `validate.check_row()`/`normalize_row()`'s existing `row[column.name]` lookups needed zero changes; a never-in-file value is now treated identically to a present-but-blank one, governed by the column's own `nullable` flag
- Wired `detect_header()`'s already-computed `header_row_index`/`footer_row_indices`/`repeated_header_row_indices` into `source.py`'s PASS-2 real read for the first time: a skip-count loop (`header_row_index + 1` rows, not a hardcoded 1) covers a genuine metadata preamble, and the new `_filtered_rows()` generator excludes footer/repeated-header rows by absolute index — a preamble line, a footer line, and a repeated interior header row no longer pollute either `valid_rows` or `invalid_rows`
- Removed `detect/filename.py`'s residual `TYPE_CHECKING`-guarded `from dataplat.config.model import FilenameMaskConfig` import, replacing it with a real local `@dataclass(frozen=True, slots=True)` carrying only the single `.mask` attribute `parse_filename()` actually reads — closes the last `dataplat` coupling this phase's Tier-A vendoring left behind (only the runtime `dataplat.errors` import was fixed in 03-02; this type-only one was missed)
- Added 4 new regression tests (2 in `test_structural_validation.py`, 2 in `test_filename_no_dataplat_import.py`) proving each fix live; full unit suite grew from 175 to 179 tests, zero regressions
- Re-ran `03-VERIFICATION.md`'s own live repro for both failed truths directly against the fixed code — both now pass (see Deviations/verification evidence below)

## Task Commits

Each task was committed atomically:

1. **Task 1: Filter MISSING_REQUIRED_COLUMN by `column.required` + prevent the resulting row-dict KeyError (CR-01)** - `627896f` (fix)
2. **Task 2: Wire preamble/footer/repeated-header row exclusion into PASS 2's real read (CR-02)** - `14040d9` (fix)
3. **Task 3: Remove `detect/filename.py`'s residual `dataplat` import (WR-01)** - `b47aef5` (fix)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/source.py` - `required_names`-filtered missing-column check; new `_filtered_rows()` generator; PASS-2 skip-count and row-exclusion wired to `detect_header()`'s output
- `packages/csv-processor/src/csv_processor/engine.py` - `process_chunks()`'s row_dict backfill for a config-declared, header-absent optional column
- `packages/csv-processor/src/csv_processor/detect/filename.py` - local `FilenameMaskConfig` dataclass replacing the removed `dataplat.config.model` import
- `tests/unit/test_structural_validation.py` - `test_optional_column_absent_from_header_processes_successfully`, `test_preamble_footer_and_repeated_header_rows_excluded_from_processing` (+ fixture-local `_preamble_footer_config()` helper)
- `tests/unit/test_filename_no_dataplat_import.py` - new file, 2 tests

## Decisions Made

- Task 1 folded the `engine.py` KeyError-prevention backfill into the same commit as `source.py`'s `required_names` filter, per the plan's own framing — the two changes are one indivisible correctness fix; shipping the `source.py` change alone would immediately reintroduce a crash
- Task 3 (WR-01, a Warning-severity finding) was folded into this Critical-bug gap-closure plan per the plan's own stated rationale — a one-line-cause fix with a real, now-tested consumer

## Deviations from Plan

None — plan executed exactly as written. All three tasks' `<action>` blocks were implemented literally; all `<acceptance_criteria>` and the plan's own `<verification>` block were independently re-run and confirmed:

- `uv run pytest tests/unit/ -q` → 179 passed (175 pre-existing + 4 new), zero failures, zero regressions
- Live repro (executed directly, not just via pytest): a customers CSV omitting `signup_country` now processes successfully with `signup_country` typed as `None`; a preamble/footer/repeated-header CSV now yields exactly 3 valid rows (`CUST001`, `CUST002`, `CUST003`) with 0 invalid rows and none of the 3 non-data row kinds present in either output stream
- `grep -rn '^\s*\(from dataplat\|import dataplat\)' packages/csv-processor/src/csv_processor/detect/filename.py` → zero matches

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None. All three fixes are fully wired and exercised end-to-end with live regression tests; no placeholder/hardcoded-empty data paths introduced.

## Next Phase Readiness

`03-VERIFICATION.md`'s two failed truths (Truth 6: optional-column handling, Truth 7: preamble/footer/repeated-header exclusion) are now both verified true against the project's own shipped `customers.json`. Phase 3's `csv_processor.engine.process_chunks(file_path, config)` public surface is unchanged in shape — Phase 4's Oracle bulk-load work builds on the same function signature, now with both structural-validation guarantees actually holding. No blockers for Phase 4.

## Self-Check: PASSED
