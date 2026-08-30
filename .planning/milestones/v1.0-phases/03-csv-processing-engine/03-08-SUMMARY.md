---
phase: 03-csv-processing-engine
plan: 08
subsystem: database
tags: [csv-processing, validation, gap-closure, data-integrity]

# Dependency graph
requires:
  - phase: 03-csv-processing-engine (plan 07)
    provides: "source.py's _filtered_rows() CR-03 content re-validation of sample-derived footer/repeated-header candidates -- this plan layers a structural coverage-eligibility gate in front of it, closing CR-04's own residual manifestation of the same root cause"
provides:
  - "csv_processor.source.prepare_source()'s sample_was_truncated/sample_covered_row_count computation, threaded into _filtered_rows() as a new required keyword-only parameter"
  - "csv_processor.source._filtered_rows(paired_rows, *, start_index, excluded_indices, header_field_count, raw_header, sample_covered_row_count) -- gains a provable-sample-coverage eligibility gate (absolute_index < sample_covered_row_count) checked BEFORE CR-03's existing content re-validation, never replacing it"
  - "Two new regression tests: the CR-04 reproduction made permanent, and a same-file coexistence proof of G-03-2's exclusion guarantee alongside the CR-04 fix"
affects: ["04-oracle-bulk-load"]

# Actuals (#2632)
actuals:
  tokens: 3068
  tasks: 2
  commits: 3

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Provable-sample-coverage eligibility gate: a sample-derived candidate exclusion index is only ever ELIGIBLE for exclusion consideration when it falls strictly within sample_covered_row_count (rows the bounded detection sample's own bytes provably captured in full) -- this structural precondition runs BEFORE CR-03's content re-validation (is_footer_shaped/is_repeated_header), never replacing it. Distinguishes 'this row's absolute index cannot even be trusted as sample-derived evidence' from 'this row's real content independently confirms the sample-derived flag', closing a gap content re-validation alone cannot resolve."

key-files:
  created: []
  modified:
    - packages/csv-processor/src/csv_processor/source.py
    - tests/unit/test_structural_validation.py

key-decisions:
  - "Task 1's RED/GREEN cycle followed the standard TDD flow with two separate commits (test-only RED, then fix-only GREEN) rather than 03-07's precedent of bundling both plan tasks' tests into one RED commit -- this plan's Task 2 test could only be authored, verified, and committed independently after Task 1's fix landed, since Task 2's own coverage-gate-plus-content-reval scenario needed the real fix present to construct meaningfully; no artificial value in pre-writing it before GREEN."
  - "Task 2's malformed-row literal deviates from the plan's specified BADROW_ONLY_ONE_FIELD to BADROWONLYONEFIELD (no underscores) -- empirically, the underscore-containing literal at that exact sample-boundary position tips detect/encoding.py's charset_normalizer-vs-chardet corroboration for this specific byte sample from 'ascii' to 'utf_8' with no ascii candidate surviving corroboration, causing prepare_source() to raise an unrelated LookupError before CR-04's own logic is ever reached. This is a pre-existing detect/encoding.py sensitivity to the exact byte sample, unrelated to and out of scope for this plan's coverage-gate fix; the semantically-equivalent single-field literal avoids it without touching detect/encoding.py. (Rule 3 - auto-fix blocking issue, applied to test construction, not production code.)"

patterns-established: []

requirements-completed: [ENGINE-01, ENGINE-05, TEST-01]

coverage:
  - id: D1
    description: "A file larger than source.SAMPLE_BYTES containing one genuinely malformed data row whose position coincides with the bounded detection sample's own last parsed row is surfaced as a WRONG_COLUMN_COUNT invalid row by process_chunks() -- never silently dropped from both valid_rows and invalid_rows"
    requirement: ENGINE-05
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped"
        status: pass
      - kind: manual
        ref: "Independently re-ran 03-REVIEW.md's own live CR-04 repro (~125 KB, 2-column file, malformed row at data row 3277) directly against process_chunks(): valid=6278, invalid=1 (WRONG_COLUMN_COUNT, row_number=3277), BADROW value present in invalid_rows"
        status: pass
    human_judgment: false
  - id: D2
    description: "A well-formed CSV file larger than source.SAMPLE_BYTES with no genuine footer still loses zero rows (03-07's own CR-03 guarantee, unweakened) -- the row whose real bytes straddle the sample's byte cutoff is categorically ineligible for footer/repeated-header exclusion candidacy"
    requirement: ENGINE-05
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_large_well_formed_file_loses_zero_rows_across_sample_boundary"
        status: pass
    human_judgment: false
  - id: D3
    description: "A genuine metadata preamble line, trailing footer line, and interior repeated-header row -- all positioned strictly within sample_covered_row_count's provably-covered range -- are still excluded from both valid_rows and invalid_rows, unchanged from G-03-2 (03-06) and CR-03 (03-07), including in the same file as an out-of-coverage malformed row that must NOT be excluded"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_preamble_footer_and_repeated_header_rows_excluded_from_processing"
        status: pass
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_repeated_header_row_excluded_even_when_file_exceeds_sample_size"
        status: pass
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_repeated_header_excluded_and_out_of_coverage_malformed_row_surfaced_together"
        status: pass
    human_judgment: false
  - id: D4
    description: "Full unit suite (181 pre-existing + 2 new) passes with zero regressions"
    requirement: TEST-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/ -q (183 passed)"
        status: pass
    human_judgment: false

# Metrics
duration: 20min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 8: Gap Closure — Structural Fix for CR-04 Sample-Tail Malformed-Row Loss Summary

**Closed 03-REVIEW.md's new Critical finding (CR-04): `_filtered_rows()` now gates footer/repeated-header exclusion eligibility on provable sample byte/row coverage (`sample_covered_row_count`), checked BEFORE CR-03's existing content re-validation — a genuinely malformed row at the sample's own tail-adjacent position is now categorically ineligible for exclusion and surfaces as an ordinary `WRONG_COLUMN_COUNT` invalid row.**

## Performance

- **Duration:** ~20 min
- **Completed:** 2026-08-29
- **Tasks:** 2/2 completed
- **Files modified:** 2

## Accomplishments

- Added `prepare_source()`'s `sample_was_truncated`/`sample_covered_row_count` computation immediately after `sample_rows` is built: `sample_was_truncated = len(sample) == SAMPLE_BYTES`; `sample_covered_row_count = len(sample_rows) - 1 if sample_was_truncated and sample_rows else len(sample_rows)` — the count of rows the sample's own bytes provably read in full.
- Rewrote `_filtered_rows()`'s gate condition from `if absolute_index in excluded_indices:` to `if absolute_index in excluded_indices and absolute_index < sample_covered_row_count:` — the new coverage-eligibility check runs strictly BEFORE the existing (CR-03) `is_footer_shaped`/`is_repeated_header` content re-validation, which is otherwise completely unchanged.
- Threaded `sample_covered_row_count=sample_covered_row_count` into the one PASS-2 `_filtered_rows(...)` call site; left `excluded_indices`'s construction, the header-skip loop, and all of PASS 1 untouched, exactly as the plan scoped.
- Reproduced 03-REVIEW.md's CR-04 bug live (RED): a genuinely malformed row at the sample's own last-parsed-row position vanished from both `valid_rows` and `invalid_rows` (0 invalid rows) against the pre-fix code.
- Added `test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped` (GREEN after the fix: exactly 1 `WRONG_COLUMN_COUNT` invalid row with the malformed literal and `name=None`, every real row present via a set comparison against `good_ids`) and `test_repeated_header_excluded_and_out_of_coverage_malformed_row_surfaced_together` (proves an in-sample repeated-header row is still excluded AND an out-of-coverage malformed row still surfaces as invalid, in the same file).
- Independently re-ran 03-REVIEW.md's own exact live repro script (~125 KB, 2-column file, malformed row at data row 3277) directly against the fixed `process_chunks()`: `valid=6278, invalid=1` (`WRONG_COLUMN_COUNT`, `row_number=3277`), matching this plan's own automated test.
- Full unit suite grew from 181 to 183 tests, zero regressions.

## Task Commits

Each task was committed atomically:

1. **Task 1 (RED): Add failing regression test for CR-04** - `c0264eb` (test)
2. **Task 1 (GREEN): Gate footer/repeated-header exclusion by provable sample coverage** - `79a599c` (fix)
3. **Task 2: Prove G-03-2 exclusion and CR-04 fix coexist in one file** - `16ba830` (test)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/source.py` - `prepare_source()` computes `sample_was_truncated`/`sample_covered_row_count`; `_filtered_rows()` gains the required `sample_covered_row_count` keyword-only parameter and the new coverage-eligibility gate ahead of its existing content re-validation; docstrings updated with the CR-04 root-cause/fix narrative.
- `tests/unit/test_structural_validation.py` - `test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped`, `test_repeated_header_excluded_and_out_of_coverage_malformed_row_surfaced_together`.

## Decisions Made

- Standard TDD RED/GREEN commit split for Task 1 (unlike 03-07's precedent of bundling both plan tasks' tests into one RED commit) — Task 2's test needed the real fix present to construct and verify meaningfully, so authoring it before GREEN would have added no independent verification value.
- Task 2's malformed-row literal changed from the plan's specified `BADROW_ONLY_ONE_FIELD` to `BADROWONLYONEFIELD` (no underscores) — see Deviations below.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking issue in test construction] `BADROW_ONLY_ONE_FIELD` literal triggers an unrelated pre-existing `detect/encoding.py` ambiguity in Task 2's specific test**
- **Found during:** Task 2, initial test run (test failed with `LookupError: unknown encoding: undetermined`, not an assertion failure)
- **Issue:** At the exact sample-boundary byte position Task 2's test constructs, the underscore-containing literal `BADROW_ONLY_ONE_FIELD` tips `charset_normalizer`'s candidate ranking so its only surviving candidate is `utf_8` (chaos 0.0) with no `ascii` candidate present at all to corroborate `chardet`'s independent `ascii` verdict. `_best_corroborating_match()` then returns `None`, `detect_encoding()` reports `source="undetermined", encoding="undetermined"` (per its own documented contract — never raises), but `source.py`'s `codecs.lookup(enc_detection.encoding).name` call runs unconditionally on that string and raises `LookupError` before CR-04's own gate logic is ever reached. Verified empirically: removing only the underscores (same length, same field-count violation, same position) restores `ascii` detection cleanly. Confirmed this is data-content-dependent, not something a coverage-gate/footer-exclusion fix could or should influence.
- **Fix:** Changed Task 2's malformed-row literal to `BADROWONLYONEFIELD` (still a single field, no comma — the same real structural defect) which does not trigger this pre-existing `detect/encoding.py` sensitivity. Task 1's test keeps the plan's originally-specified `BADROW_ONLY_ONE_FIELD` literal unchanged since it does not hit this issue.
- **Scope note:** This is a pre-existing quirk in the vendored Tier-A `detect/encoding.py` module's `charset_normalizer`/`chardet` corroboration logic (and arguably a latent `source.py` bug — `detect_encoding()`'s own documented contract says it "never raises," yet an `"undetermined"` result is not a valid codec name for the unconditional `codecs.lookup()` call three lines later in `prepare_source()`). Out of scope for this plan (CR-04's coverage-gate fix); not modified. Flagged to `.planning/WINDOWS.md` below for future prioritization.
- **Files modified:** `tests/unit/test_structural_validation.py` (test literal only, no production code)
- **Commit:** `16ba830`

## Issues Encountered

None beyond the documented test-construction deviation above.

## User Setup Required

None — no external service configuration required.

## Known Stubs

None. The fix is fully wired and exercised end-to-end with live regression tests; no placeholder/hardcoded-empty data paths introduced.

## Threat Flags

None. This plan closes an existing threat register entry (T-03-14, already present in the plan's own `<threat_model>`) rather than introducing new security-relevant surface.

## Next Phase Readiness

03-REVIEW.md's CR-04 Critical finding is now closed and independently re-verified against its own live repro. Phase 3's `csv_processor.engine.process_chunks(file_path, config)` public surface is unchanged in shape — Phase 4's Oracle bulk-load work builds on the same function signature, now with ENGINE-05's row-count-accuracy guarantee holding against both prior manifestations of the sample-truncation root cause (CR-02/CR-03's well-formed-row-wrongly-excluded case, and CR-04's genuinely-malformed-row-silently-dropped case). This round's own documented residual (multiple consecutive genuinely-malformed rows exactly at the sample boundary — would still partially misclassify under CR-03's content check alone) and 03-REVIEW.md's remaining Warning/Info findings (WR-01/WR-02/WR-03/IN-01/IN-02) were not in this gap-closure plan's scope and remain open for a future pass if prioritized. No blockers for Phase 4.

---
*Phase: 03-csv-processing-engine*
*Completed: 2026-08-29*

## Self-Check: PASSED

- FOUND: packages/csv-processor/src/csv_processor/source.py
- FOUND: tests/unit/test_structural_validation.py
- FOUND: .planning/phases/03-csv-processing-engine/03-08-SUMMARY.md
- FOUND commit: c0264eb
- FOUND commit: 79a599c
- FOUND commit: 16ba830
