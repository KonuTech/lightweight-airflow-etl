---
phase: 03-csv-processing-engine
reviewed: 2026-08-29T00:00:00Z
depth: standard
files_reviewed: 2
files_reviewed_list:
  - packages/csv-processor/src/csv_processor/source.py
  - tests/unit/test_structural_validation.py
findings:
  critical: 1
  warning: 3
  info: 2
  total: 6
status: issues_found
---

# Phase 3: Code Review Report (focused re-review of gap-closure plan 03-07)

**Reviewed:** 2026-08-29T00:00:00Z
**Depth:** standard
**Files Reviewed:** 2
**Status:** issues_found

## Narrative Findings (AI reviewer)

### Summary

This is a focused re-review of gap-closure plan 03-07, which fixed the Critical silent-data-loss
bug from the prior 03-REVIEW.md (03-06 wired sample-derived `footer_row_indices`/
`repeated_header_row_indices` directly into `_filtered_rows()`, causing any file larger than the
64 KiB detection sample to silently drop the one real row whose bytes straddled the sample
cutoff).

**03-07's re-validation approach genuinely closes the reported reproduction and does not regress
the original G-03-2 guarantee.** `_filtered_rows()` now re-checks every sample-derived candidate
index against the REAL row read in PASS 2 (`len(row) != header_field_count` or
`tuple(row) == raw_header`) before excluding it. I re-ran the exact class of reproduction from the
prior review (a 6,000+-row well-formed CSV whose sample-boundary row is truncated mid-row inside
the 64 KiB sample only) and confirmed zero rows are lost — this matches
`test_large_well_formed_file_loses_zero_rows_across_sample_boundary`, which passes. I also
confirmed `test_repeated_header_row_excluded_even_when_file_exceeds_sample_size` and
`test_preamble_footer_and_repeated_header_rows_excluded_from_processing` still pass — a genuine,
small, within-sample preamble/footer/repeated-header row is still correctly excluded, both for
files that fit in the sample and files that don't. All 12 tests in
`tests/unit/test_structural_validation.py` pass.

**However, the fix only closes the specific "well-formed row wrongly excluded" manifestation of
the defect class. It does NOT close a second, still-open manifestation of the same root cause: a
genuinely malformed data row (wrong field count, unrelated to sample truncation) that happens to
land in the contiguous run `detect_header()`'s footer-scoring walk scans backward from the tail of
the 64 KiB sample is silently dropped — never surfaced as `WRONG_COLUMN_COUNT`, never counted as
valid.** This is a new Critical finding (empirically reproduced below), not a hypothetical. It
existed under 03-06 too (03-06 dropped it unconditionally), so 03-07 does not "reopen" the bug in
a new form so much as it leaves a narrower, previously-undocumented sub-case of the identical root
cause unaddressed. Given the project's own precedent for classifying "row vanishes from both
`valid_rows` and `invalid_rows` with zero trace" as Critical, this is reported at the same
severity.

The five findings carried forward from the prior 03-REVIEW.md (WR-01/WR-02/WR-03/IN-01/IN-02) were
explicitly out of scope for 03-07 and remain present, unchanged, verified by direct re-read of the
current file contents (and, for the two config-model-related findings, a targeted grep of
`config/models.py`'s validators, which still has no check for either).

### Critical Issues

#### CR-01: A genuinely malformed row coinciding with the sample-tail footer-scan window is still silently dropped instead of surfaced as `WRONG_COLUMN_COUNT`

**File:** `packages/csv-processor/src/csv_processor/source.py:118-184` (interacts with
`packages/csv-processor/src/csv_processor/detect/header.py:207-256`'s `_detect_footer_rows`, not
in this review's file list but necessary context, and
`packages/csv-processor/src/csv_processor/engine.py:69-89`'s `WRONG_COLUMN_COUNT` check, also not
in this review's file list)

**Issue:**

`detect_header()`'s footer-scoring walk (`header.py:_detect_footer_rows`) scans backward from the
LAST row of whatever `data_rows` it is given, including every contiguous row whose field count
differs from the header's, and stopping at the first well-formed row it hits. When `source.py`
calls `detect_header()` against only the bounded 64 KiB `sample_rows` (`source.py:284`), "the last
row of `data_rows`" is the tail of the SAMPLE, not the tail of the real file. For any file larger
than `SAMPLE_BYTES`, that tail is an essentially arbitrary interior position.

03-07's fix (`_filtered_rows()`, `source.py:178-184`) re-validates each candidate index using
**exactly the same field-count-mismatch predicate** (`len(row) != header_field_count`) that
`engine.py:69` uses to classify a row as `WRONG_COLUMN_COUNT`. This predicate cannot distinguish
"this row is footer-shaped because it's a truncation artifact of the sample cutoff" from "this row
is footer-shaped because it is a genuinely malformed data row that happens to sit at/near the
sample's tail." Both produce `is_footer_shaped = True` under re-validation, and both are silently
excluded by `_filtered_rows()` — the malformed row never reaches `engine.py`'s per-row
`WRONG_COLUMN_COUNT` check at all, because `_filtered_rows()` (called from `prepare_source()`,
upstream of `engine.py`'s row loop) has already removed it from the stream `engine.py` iterates.
The row disappears from both `valid_rows` and `invalid_rows` with zero trace — the identical
failure signature the prior 03-REVIEW.md classified as Critical.

**Reproduction (verified, not hypothetical):**

Built a ~125,570-byte, 2-column (`id,name`) CSV where data row 3,277 is genuinely malformed
(`BADROW_ONLY_ONE_FIELD`, one field instead of two) positioned so it falls within the last few
rows captured by the 65,536-byte detection sample, immediately followed by a row that straddles
the sample cutoff:

```
total bytes: 125570  SAMPLE_BYTES: 65536
malformed row is data row number: 3277
footer_row_indices: (3277,)          # detect_header() flags it via the sample-tail walk
expected data rows (excluding header): 6278
valid: 6277  invalid: 0
sum: 6277  MISSING: 1
WRONG_COLUMN_COUNT invalid rows found: 0
BADROW value present anywhere in valid/invalid: False
```

`process_chunks()` returns 6,277 valid rows and 0 invalid rows for a 6,278-data-row input. The
malformed row is not merely misclassified as a footer — a single-field row genuinely IS
footer-shaped by this module's own only-available criterion — but it is real data that a
config-driven ETL engine's own "collect-and-continue invalid-row model" (per this project's
CLAUDE.md) is specifically designed to surface as an invalid row, not discard invisibly.

**Fix:**

The field-count-mismatch predicate alone cannot resolve this ambiguity — it is inherent to
scoring footer-shape from a bounded head-sample instead of the real tail of the file (the same
architectural point the prior review's Critical finding's "more complete fix" section already
raised for the truncation case). Two options, in order of increasing completeness:

1. **Narrow the blast radius (partial mitigation):** Only ever treat a sample-derived candidate
   index as eligible for exclusion via `is_footer_shaped` when it is the LAST row of the sample
   (i.e. the one truncation can plausibly have produced) or immediately adjacent to it — never an
   entire contiguous run of "mismatched" sample-tail rows. A genuinely malformed row several
   positions before the true sample cutoff should never be treated as a footer candidate at all;
   only the single row whose byte range actually crosses `SAMPLE_BYTES` is a truncation-artifact
   suspect.

   ```python
   # In prepare_source(), before calling detect_header(): only the row whose bytes
   # actually straddle the sample cutoff is a truncation suspect -- never score an
   # earlier, fully-sample-contained row as a footer candidate on field-count alone.
   sample_was_truncated = len(sample) == SAMPLE_BYTES
   footer_candidate_rows = sample_rows[:-1] if sample_was_truncated and sample_rows else sample_rows
   ```

   (This is the same mitigation the prior 03-REVIEW.md proposed for CR-02/CR-03; 03-07 did not
   adopt it, choosing whole-candidate-set re-validation instead — which is necessary but not
   sufficient.)

2. **Complete fix (architectural, as the prior review also noted):** Footer detection is a
   "look at the true tail of the file" problem. Determining it correctly for a file larger than
   one head-sample requires buffering a real look-ahead window during the PASS 2 streaming read
   (only finalizing "these trailing N rows are footer" once EOF is actually reached) rather than
   trusting any bounded head-sample's own tail as a proxy for the file's true tail.

At minimum, add a regression test for this scenario (a genuinely malformed row positioned within
the sample's footer-scan window in a file exceeding `SAMPLE_BYTES`) alongside the existing
`test_large_well_formed_file_loses_zero_rows_across_sample_boundary` — no such test currently
exists, and this exact class of file (large, mostly clean, with a small number of malformed rows
scattered throughout) is a realistic production input for this project's stated use case.

### Warnings

#### WR-01: Injected per-row metadata keys can silently clobber a same-named declared column's value in `invalid_rows` output

**File:** `packages/csv-processor/src/csv_processor/engine.py:77-88, 106-115` (unchanged from
prior review — carried forward, not re-diagnosed; out of scope for 03-07)

**Issue:** Both invalid-row construction sites still build the output dict as `{**row_dict,
"error_code": ..., "error_message": ..., "source_file": ..., "row_number": ..., "raw_line":
...}`. A dataset config declaring a column literally named one of these reserved keys would have
that column's real value silently overwritten. `config/models.py` still has no validator
rejecting a column name in this reserved set (confirmed by re-reading its `model_validator`
list: `_check_type_specific_fields`, `_check_escapechar_present_when_doublequote_disabled`,
`_check_valid_and_invalid_tables_differ`, `_check_delimiter_does_not_collide_with_decimal_separator`,
`_check_column_names_are_unique` — none address this).

**Fix:** Unchanged from prior review — namespace the injected metadata keys (e.g. `"_error_code"`,
or nest under a single `"_meta"` key) or add a `DatasetConfig` validator rejecting a reserved
column name at config-load time.

#### WR-02: No `try`/`finally` around PASS 2 stream setup in `prepare_source` — file handle leak if the header-skip loop raises

**File:** `packages/csv-processor/src/csv_processor/source.py:330-363` (unchanged from prior
review — carried forward, not re-diagnosed; out of scope for 03-07)

**Issue:** `real_stream`/`text_stream` are still opened and returned with no `try`/`finally`
protecting the section between opening them and the `return` statement. `engine.py`'s own
`try`/`finally` (`engine.py:60-121`) only begins after `prepare_source()` has already returned, so
if `next(reader)` raises in the header-skip loop (`source.py:345-346`) — e.g. a TOCTOU race where
the file is truncated/rewritten between PASS 1's sampling and PASS 2's reopen — the handle is
never closed.

**Fix:** Unchanged from prior review — wrap the PASS 2 setup (from `real_stream = _open_raw_stream(...)`
through the `return` statement) in its own `try`/`except Exception: text_stream.close(); raise`.

#### WR-03: `ColumnSpec` permits a `required: false` + `nullable: false` combination that is guaranteed to fail every row where the column is absent, with no upfront config validation

**File:** `packages/csv-processor/src/csv_processor/config/models.py` (unchanged from prior
review — carried forward, not re-diagnosed; out of scope for 03-07; confirmed still absent by
re-checking `ColumnSpec`'s validator list, same as WR-01 above)

**Issue:** Unchanged — a column declared `required: false` but `nullable: false` will
deterministically fail `NULL_VIOLATION` on every row of a file that omits it, with no config-time
signal.

**Fix:** Unchanged from prior review — add a cross-field `model_validator` on `ColumnSpec`
rejecting `required=False` combined with `nullable=False` at config-load time.

### Info

#### IN-01: `# type: ignore[operator]` used to suppress a real `int | None` type mismatch instead of a runtime assertion

**File:** `packages/csv-processor/src/csv_processor/source.py:345, 357` (unchanged in substance
from prior review, line numbers shifted from 308/317 due to 03-07's added docstring/parameters —
carried forward, not re-diagnosed; out of scope for 03-07)

**Issue:** Unchanged — `header_detection.header_row_index` is relied on to be non-`None` at both
sites via the earlier `has_header` check, expressed only via `# type: ignore[operator]` rather
than a runtime-checkable assertion.

**Fix:** Unchanged from prior review — bind it once with `assert header_detection.header_row_index
is not None` immediately after the `has_header` check, and reuse the narrowed local in both call
sites instead of two separate `# type: ignore` comments.

#### IN-02: Module-level `csv.field_size_limit(1_048_576)` call mutates process-wide global state as an import side effect

**File:** `packages/csv-processor/src/csv_processor/source.py:40` (unchanged from prior review —
carried forward, not re-diagnosed; out of scope for 03-07)

**Issue:** Unchanged — importing `csv_processor.source` for any reason still silently changes the
process-wide `csv` module field-size limit for every other consumer in the same process.

**Fix:** Unchanged from prior review — move the call inside `prepare_source()`/`_open_raw_stream`,
or document the import-time side effect explicitly in the module docstring.

---

_Reviewed: 2026-08-29T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
