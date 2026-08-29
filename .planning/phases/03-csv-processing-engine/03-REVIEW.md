---
phase: 03-csv-processing-engine
reviewed: 2026-08-29T18:40:00Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - packages/csv-processor/src/csv_processor/config/models.py
  - packages/csv-processor/src/csv_processor/source.py
  - configs/defaults.json
  - tests/unit/test_config_models.py
  - tests/unit/test_structural_validation.py
findings:
  critical: 0
  warning: 1
  info: 2
  total: 3
status: issues_found
---

# Phase 3: Code Review Report (Gap-Closure Plan 03-10, FTR-01)

**Reviewed:** 2026-08-29T18:40:00Z
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found (no BLOCKERs; 1 WARNING, 2 INFO)

## Summary

This review is scoped strictly to plan 03-10's FTR-01 fix — the new
`CsvDialectConfig.has_footer` opt-in field and `prepare_source()`'s gating of
`footer_row_indices` consumption on it. The 03-06..03-09 sample-boundary-
truncation chain (`_uncoverable_tail_indices`, `_filtered_rows`'s CR-01/
CR-03/CR-04 logic) was explicitly out of scope and was not re-litigated
here except where FTR-01 touches it.

**Correctness verdict: the fix is sound.** I traced the full exclusion path
by hand rather than trusting the plan's own tests:

- `footer_row_indices = set(header_detection.footer_row_indices) if
  config.csv.has_footer else set()` (source.py:502-504) unconditionally
  empties the footer candidate set before `excluded_indices` is built, for
  every file size and every code path — there is no second place in
  `source.py` that reads `header_detection.footer_row_indices` directly, so
  there is no bypass.
- The one place that could theoretically leak a footer-shaped exclusion back
  in despite the gate is `_filtered_rows`'s per-row re-validation, which ORs
  `is_footer_shaped` (`len(row) != header_field_count`) with
  `is_repeated_header` (`tuple(row) == raw_header`) for any index present in
  `excluded_indices`. When `has_footer=False`, `excluded_indices` can only
  contain indices from `repeated_header_row_indices`. I verified this OR
  can never smuggle a footer-only exclusion through: `is_repeated_header`
  requires exact tuple equality with `raw_header`, which is only possible
  when `len(row) == len(raw_header) == header_field_count` — i.e.
  `is_repeated_header=True` structurally implies `is_footer_shaped=False`
  for the same row. The two predicates are mutually exclusive by
  construction, so a row can never be excluded by `has_footer=False`'s
  logic on field-count-mismatch grounds alone, regardless of what
  `repeated_header_row_indices` happens to also flag. This closes review
  question 1 with actual proof, not just by reading the plan's own tests.
- Opt-in path (`has_footer=True`) is byte-for-byte unchanged from pre-10
  behavior (the same `set(header_detection.footer_row_indices)` expression
  that ran unconditionally before now only runs conditionally) — the
  03-06..03-09 sample-boundary interaction is untouched, confirmed both by
  code inspection and by the existing tests in this file continuing to pass
  with `has_footer=True` explicitly threaded through
  `_preamble_footer_config`/`_large_id_name_config`.
- `repeated_header_row_indices` staying unconditional is safe for the
  reason above (exact-tuple-equality is structurally incapable of matching
  a field-count-mismatched row) — see IN-01 below for the one residual,
  explicitly-out-of-scope theoretical edge case worth a permanent record.
- Config plumbing checked end-to-end: `CsvDialectConfig.has_footer: bool =
  False` is a plain, frozen, unvalidated-further field; `configs/
  defaults.json` declares it explicitly as `false`; neither
  `customers.json` nor `orders.json` declares a `csv` block at all, so both
  inherit the default via `load_config`'s shallow `{**defaults, **dataset}`
  merge with zero ambiguity today (see IN-02 for a latent, pre-existing
  footgun this interacts with, not introduced by this plan).
- Ran the full suite (`uv run pytest`, 195 passed) and the two files' own
  62 tests in isolation — both green, no regressions.

I found no BLOCKER-level defect. The one WARNING is a test-coverage gap,
not a logic error — worth closing given this exact module's five prior
review rounds have all found real bugs hiding in untested file-size/
boundary combinations.

## Warnings

### WR-01: No regression test proves `has_footer=False` holds on a file exceeding `SAMPLE_BYTES`

**File:** `tests/unit/test_structural_validation.py:630-657`
**Issue:** `test_no_footer_optin_default_surfaces_malformed_last_row_as_invalid_not_dropped`
and `test_generator_driven_customers_seed11_wrong_column_count_last_row_not_dropped`
both explicitly construct files smaller than `source.SAMPLE_BYTES` (the
first asserts `csv_path.stat().st_size < source.SAMPLE_BYTES`; the second
uses only 50 generated rows). Every test in this file that exercises a file
*larger* than `SAMPLE_BYTES` (the entire 03-06..03-09 lineage:
`_preamble_footer_config`/`_large_id_name_config` callers) defaults
`has_footer=True`. There is no test combining `has_footer=False` with a
file that exceeds `SAMPLE_BYTES` and has a genuinely footer-shaped or
malformed trailing row near the sample-truncation boundary.

I hand-verified (see Summary) that the code is correct here regardless of
file size — the gate is applied before any size-dependent branching — but
this module's own history is that every one of the last five review rounds
found a real bug specifically in an untested boundary/file-size
combination. Leaving this exact combination untested is a self-inflicted
blind spot relative to this codebase's own established test-writing
convention (every algebraically-reasoned invariant in this file gets its
own literal adversarial regression test).

**Fix:** Add a test mirroring
`test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped`'s
construction (a file whose last sample-covered row is at the truncation
boundary) but with `_large_id_name_config(has_footer=False)`, asserting the
boundary row surfaces as `WRONG_COLUMN_COUNT` rather than being silently
excluded — proving the opt-out default holds even when the sample-
truncation machinery (`sample_covered_row_count`, `_uncoverable_tail_indices`)
is simultaneously in play.

## Info

### IN-01: `repeated_header_row_indices` unconditional exclusion has one narrow theoretical false-positive class

**File:** `packages/csv-processor/src/csv_processor/source.py:498-501`
**Issue:** Per this plan's own scope note, `repeated_header_row_indices`
stays unconditionally active because exact-tuple-equality against
`raw_header` is a much stronger signal than footer's field-count-mismatch
heuristic. That's correct, but it is not a *zero*-risk heuristic: a
genuine data row whose every field value happens to literally equal the
corresponding header column name (e.g. a row with values `("id", "name")`
in a 2-column `id,name` file) would still be silently excluded from both
`valid_rows` and `invalid_rows`, indistinguishable from a real repeated
header line. This is exceedingly unlikely in real data and explicitly out
of this plan's scope (correctly), but it is the one analogue to FTR-01's
original bug class that survives, and is worth recording rather than
re-discovering in a future gap-analysis pass.
**Fix:** No code change requested — purely a documentation note. Consider
adding one sentence to `CsvDialectConfig`'s or `_filtered_rows`'s docstring
recording this as a known, accepted residual risk (mirroring how
`_LineCapturingTextStream`'s multi-line-row limitation is already recorded
as an accepted limitation elsewhere in this file).

### IN-02: Shallow config merge silently discards `has_footer` (and any other `csv.*` field) the moment a dataset partially overrides the `csv` block

**File:** `packages/csv-processor/src/csv_processor/config/loader.py:74` (referenced from `configs/defaults.json`)
**Issue:** `load_config`'s merge is `merged = {**defaults, **dataset}` —
top-level-only, non-recursive, by explicit design (see the module
docstring). Today this is harmless for `has_footer` specifically because
neither shipped dataset config declares a `csv` block at all, and the
Pydantic model default (`False`) matches `defaults.json`'s explicit value
exactly either way. But the moment a *future* dataset config declares a
partial `csv` override (e.g. just `{"csv": {"delimiter": ";"}}` to change
one field), that whole dict replaces `defaults.json`'s `csv` block
wholesale — any dataset that had previously relied on inheriting
`has_footer: true` from a shared default would silently revert to
`has_footer: false` the moment it also needed to override an unrelated
csv field, with no validation error to catch it (both are valid
`CsvDialectConfig` instances).
**Fix:** Not a defect in the reviewed files (this merge behavior predates
plan 03-10 and is a recorded, deliberate design choice) — flagging only
because `has_footer` is now a second per-dataset-meaningful field living
inside the shallow-merged `csv` block whose accidental loss has real
silent-data-loss consequences (the exact failure mode FTR-01 was created
to close). If per-dataset `has_footer` overrides become common, consider
either a recursive merge scoped to `csv` alone, or requiring every dataset
that touches `csv` to repeat every field it cares about explicitly (already
true, just not enforced/documented as a footgun warning anywhere near the
`has_footer` field itself).

---

_Reviewed: 2026-08-29T18:40:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
