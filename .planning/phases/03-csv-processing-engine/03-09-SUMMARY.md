---
phase: 03-csv-processing-engine
plan: 09
subsystem: database
tags: [csv-processing, validation, gap-closure, data-integrity]

# Dependency graph
requires:
  - phase: 03-csv-processing-engine (plan 08)
    provides: "source.py's _filtered_rows() single-index coverage-eligibility gate (CR-04) -- this plan generalizes it from the single boundary index to the full contiguous run (CR-01), and fixes an independent off-by-one in the same file's sample_was_truncated computation (WR-01)"
provides:
  - "csv_processor.source._uncoverable_tail_indices(excluded_indices, sample_covered_row_count) -> set[int] -- new pure helper computing the maximal contiguous suffix of a SINGLE candidate-index set ending at sample_covered_row_count"
  - "csv_processor.source._filtered_rows(...) -- gate rewired to precompute uncoverable_tail once per call (union of two SEPARATE _uncoverable_tail_indices() calls, one per candidate source -- footer_row_indices and repeated_header_row_indices -- never a single call against their pre-merged union) and check absolute_index not in uncoverable_tail"
  - "csv_processor.source.prepare_source() -- reads SAMPLE_BYTES + 1 bytes and derives sample_was_truncated from the extra byte's actual presence, trimming sample back to SAMPLE_BYTES bytes when truncated (WR-01)"
  - "Four new regression tests: a table-driven direct proof of the helper's adversarial edge cases, the CR-01 contiguous-run reproduction, the checker-found cross-source-contamination guard, and the WR-01 exact-byte-size reproduction"
affects: ["04-oracle-bulk-load"]

# Actuals (#2632)
actuals:
  tokens: 5821
  tasks: 2
  commits: 4

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Per-source-then-union uncoverable-tail computation: a coverage-ineligibility helper that generalizes a single-index protection to a full contiguous run must be called SEPARATELY once per structurally-independent candidate source and have its RESULTS unioned afterward -- never by merging the input sources before walking. Two detectors over the same data (a contiguous backward walk vs. an unbounded full-scan) can produce numerically adjacent candidate indices by pure coincidence with no shared ordering guarantee, so union-then-walk lets one source's genuine boundary-touching run swallow the other source's unrelated interior candidate."

key-files:
  created: []
  modified:
    - packages/csv-processor/src/csv_processor/source.py
    - tests/unit/test_structural_validation.py

key-decisions:
  - "Corrected the plan's own Task 1 read_first/behavior prose for the third new test (test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run): the plan-checker's own review flagged that the contaminated row would surface as a spurious WRONG_COLUMN_COUNT invalid row with name=None, but tracing the actual RED-mode (naive union-then-walk) behavior shows the contaminated row's real content (id=\"id\", name=\"name\") satisfies every structural/type/nullability check for this config's plain string columns, so it leaks into all_valid as an ordinary VALID row instead -- not into all_invalid. The test's actual assertions (asserting the row is absent from BOTH streams) were already correct and still correctly distinguish RED from a hypothetical union-then-walk implementation from the actually-shipped per-source-then-union fix; only the docstring prose was corrected to match."
  - "test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run already passes against the pre-existing (pre-Task-1) single-index-gate code, not just against the shipped fix -- verified analytically: 03-08's own single-index gate leaves the interior repeated-header candidate eligible for CR-03's content re-validation, which correctly confirms it as a genuine repeated header and excludes it. The test is a permanent regression guard against a hypothetical incorrect union-then-walk refactor of CR-01 (the checker's own found risk), not a reproduction of a currently-shipping bug -- it was authored and committed in the plan's RED phase alongside the two genuinely-failing new tests per the plan's own task grouping, and continues to pass unchanged through GREEN."

patterns-established: []

requirements-completed: [ENGINE-01, ENGINE-05, TEST-01]

coverage:
  - id: D1
    description: "A contiguous run of two or more sample-derived footer/repeated-header candidate indices ending at the sample's own unprovable last row are ALL treated as coverage-ineligible for exclusion -- not just the single boundary index CR-04 protected -- so every genuinely malformed row in such a run surfaces as a WRONG_COLUMN_COUNT invalid row via process_chunks(), none silently dropped from both valid_rows and invalid_rows"
    requirement: ENGINE-05
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_two_contiguous_malformed_rows_at_sample_boundary_both_surface_as_invalid"
        status: pass
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_uncoverable_tail_indices_covers_adversarial_edge_cases"
        status: pass
    human_judgment: false
  - id: D2
    description: "The contiguous-run computation is performed SEPARATELY per candidate source (footer_row_indices, repeated_header_row_indices), each independently anchored at sample_covered_row_count, and the two resulting sets are unioned only after each walk completes -- a genuine, fully-covered interior repeated-header row is never stripped of its exclusion-eligibility merely because it sits next to an unrelated boundary-touching candidate from the other source"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run"
        status: pass
    human_judgment: false
  - id: D3
    description: "A file whose real (decompressed) byte size exactly equals source.SAMPLE_BYTES is never misclassified as truncated, so its genuinely complete last row is still correctly excluded when it is a real footer"
    requirement: ENGINE-05
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_file_exactly_sample_bytes_size_footer_still_correctly_excluded"
        status: pass
    human_judgment: false
  - id: D4
    description: "Every prior regression guarantee on this code path holds unchanged: G-03-2/03-06, CR-03/03-07, and CR-04/03-08's own named tests all continue to pass without modification, alongside this round's own four new tests, in one full unit suite run (183 pre-existing + 4 new = 187, zero regressions)"
    requirement: TEST-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/ -q (187 passed)"
        status: pass
    human_judgment: false

# Metrics
duration: 25min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 9: Gap Closure — Contiguous-Run Coverage Gate & Truncation Off-by-One Summary

**Closed 03-REVIEW.md's fourth-round Critical finding (CR-01) by generalizing `_filtered_rows()`'s single-index sample-coverage gate into a per-source `_uncoverable_tail_indices()` helper covering the full contiguous run, and fixed the WR-01 `sample_was_truncated` off-by-one by peeking one byte past `SAMPLE_BYTES`.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-08-29
- **Tasks:** 2/2 completed
- **Files modified:** 2

## Accomplishments

- Extracted `_uncoverable_tail_indices(excluded_indices, sample_covered_row_count) -> set[int]` — a pure helper walking backward from `sample_covered_row_count`, adding every contiguously-excluded index it finds, per 03-REVIEW.md's own suggested fix.
- Rewired `_filtered_rows()` to accept `footer_row_indices`/`repeated_header_row_indices` as two separate keyword-only parameters (alongside the unchanged, still-used `excluded_indices`), and to precompute `uncoverable_tail` once per call as the union of TWO separate `_uncoverable_tail_indices()` calls — one per candidate source, each independently anchored at `sample_covered_row_count` — never a single call against a pre-merged union. This closes both CR-01 (the contiguous-run gap) and a checker-found cross-source-contamination risk in the naive fix.
- Changed the gate condition from `absolute_index < sample_covered_row_count` to `absolute_index not in uncoverable_tail`.
- Updated `prepare_source()`'s PASS 2 `_filtered_rows(...)` call site to pass `footer_row_indices=set(header_detection.footer_row_indices)` and `repeated_header_row_indices=set(header_detection.repeated_header_row_indices)` as two new keyword arguments, alongside the unchanged `excluded_indices` argument.
- Fixed WR-01: `prepare_source()` now reads `SAMPLE_BYTES + 1` bytes and derives `sample_was_truncated = len(sample) > SAMPLE_BYTES` (the extra byte's actual presence), trimming `sample` back to `SAMPLE_BYTES` bytes when truncated — removed the redundant, incorrect `len(sample) == SAMPLE_BYTES` computation that previously sat after `sample_rows` construction.
- Added 4 new regression tests: a table-driven direct proof of `_uncoverable_tail_indices()` against 8 adversarial cases (empty set, single boundary index, contiguous run of 3, a boundary-touching run alongside a separate untouched run, a run touching absolute index 0, boundary at row 0 with/without index 0 itself as a candidate, a non-adjacent index), the CR-01 contiguous-run reproduction (two consecutive malformed rows at the sample boundary), the checker-found cross-source-contamination guard (an interior repeated-header row adjacent to a boundary-touching footer candidate), and the WR-01 exact-byte-size reproduction.
- Full unit suite grew from 183 to 187 tests, zero regressions.

## Task Commits

Each task was committed atomically, following standard TDD RED/GREEN cycles:

1. **Task 1 (RED): Add failing regression tests for CR-01 contiguous-run coverage gap** - `c57d256` (test)
2. **Task 1 (GREEN): Generalize CR-04 coverage gate to full contiguous run** - `364fb76` (fix)
3. **Task 2 (RED): Add failing regression test for WR-01 truncation off-by-one** - `e75060a` (test)
4. **Task 2 (GREEN): Correct sample_was_truncated off-by-one** - `04fe090` (fix)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/source.py` - New `_uncoverable_tail_indices()` helper; `_filtered_rows()` gains `footer_row_indices`/`repeated_header_row_indices` keyword-only parameters and the per-source-then-union gate; `prepare_source()` reads `SAMPLE_BYTES + 1` bytes and correctly derives `sample_was_truncated`; docstrings updated with the CR-01/WR-01 root-cause/fix narrative alongside existing CR-03/CR-04 history notes.
- `tests/unit/test_structural_validation.py` - `test_uncoverable_tail_indices_covers_adversarial_edge_cases`, `test_two_contiguous_malformed_rows_at_sample_boundary_both_surface_as_invalid`, `test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run`, `test_file_exactly_sample_bytes_size_footer_still_correctly_excluded`.

## Decisions Made

- Corrected the plan's own Task 1 prose for the third new test's RED-mode failure narrative (see key-decisions above) — the test's actual assertions were already correct, only the docstring's description of which stream the contaminated row would leak into was wrong (per the objective note flagging this as a known plan-checker finding).
- Confirmed `test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run` already passes against the pre-Task-1 code (not just the shipped fix) — it is a permanent guard against a hypothetical incorrect union-then-walk refactor, verified analytically rather than by literally implementing and testing the wrong version.

## Deviations from Plan

None — plan executed exactly as written. The one prose correction (see Decisions Made) was explicitly anticipated and pre-authorized by this plan's own objective note, not an unplanned deviation.

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required.

## Known Stubs

None. Both fixes are fully wired and exercised end-to-end with live regression tests; no placeholder/hardcoded-empty data paths introduced.

## Threat Flags

None. This plan closes two existing threat register entries (T-03-16, T-03-17, already present in the plan's own `<threat_model>`) rather than introducing new security-relevant surface.

## Next Phase Readiness

03-REVIEW.md's CR-01 Critical finding and WR-01 Warning finding are now both closed. `_filtered_rows()`'s coverage-eligibility gate now protects the full contiguous run of sample-derived candidates ending at the truncation boundary — computed per-source-then-unioned so the two structurally independent detectors (`_detect_footer_rows`'s contiguous walk, `_detect_repeated_header_rows`'s unbounded scan) can never cross-contaminate each other's boundary determination. `sample_was_truncated` is now derived from a genuine EOF check rather than an ambiguous byte-count equality. Every prior regression guarantee (G-03-2, CR-03, CR-04) holds unchanged, verified in the same full-suite run as this round's own four new tests. `csv_processor.engine.process_chunks(file_path, config)`'s public surface is unchanged in shape — Phase 4's Oracle bulk-load work builds on the same function signature, now with ENGINE-05's row-count-accuracy guarantee holding against every identified manifestation of the sample-truncation root cause across four consecutive review rounds. WR-02 (file-handle leak), WR-03/IN-01/IN-02 (from 03-REVIEW.md), and IN-03/IN-04/IN-05 remain out of this gap-closure plan's scope and open for a future pass if prioritized. No blockers for Phase 4.

---
*Phase: 03-csv-processing-engine*
*Completed: 2026-08-29*

## Self-Check: PASSED

- FOUND: packages/csv-processor/src/csv_processor/source.py
- FOUND: tests/unit/test_structural_validation.py
- FOUND: .planning/phases/03-csv-processing-engine/03-09-SUMMARY.md
- FOUND commit: c57d256
- FOUND commit: 364fb76
- FOUND commit: e75060a
- FOUND commit: 04fe090
