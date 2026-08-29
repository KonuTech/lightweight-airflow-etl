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
  info: 5
  total: 9
status: issues_found
---

# Phase 03: Code Review Report

**Reviewed:** 2026-08-29T00:00:00Z
**Depth:** standard
**Files Reviewed:** 2
**Status:** issues_found

## Summary

This is the fourth review pass on `source.py`'s footer/repeated-header row
exclusion logic. Round 03-08 (CR-04) added a `sample_covered_row_count`
coverage-eligibility gate in front of CR-03's content re-validation, so a
sample-derived exclusion candidate is now only actually excludable when its
absolute index is strictly less than the count of rows the 64 KiB detection
sample provably read in full.

**The gate correctly closes the single-row case** it was built for: a lone
malformed or well-formed row occupying the sample's own last (truncated)
parsed position is now categorically ineligible, and both
`test_large_well_formed_file_loses_zero_rows_across_sample_boundary` and
`test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped` pass
for the right reason: `sample_covered_row_count = len(sample_rows) - 1`
correctly identifies index `len(sample_rows) - 1` as the only unprovable row
when the sample was truncated.

**It does not close the general case, and the plan's own disclosed residual
risk is real, verified against `detect/header.py`'s actual implementation,
and broader than disclosed.** `_detect_footer_rows` (detect/header.py:240-255)
walks backward from the sample's last row and keeps appending to
`footer_offsets` for *every contiguous* row that fails the field-count/pattern
test, not just the final one. The new coverage gate only protects the single
index equal to `sample_covered_row_count`; every earlier row in the same
contiguous run remains eligible and is only checked by CR-03's content
re-validation — which cannot distinguish "genuine footer" from "genuinely
malformed data row that happens to also mismatch the header's field count."
A contiguous run of 2+ such rows ending at the sample's truncation boundary
therefore still silently drops all but the last one, with zero surfaced
diagnostic — the exact CR-02/CR-04 failure mode, recurring one row earlier.
This is not covered by any existing or added test.

A second, independent boundary defect was found in `sample_was_truncated`
itself: it is computed as `len(sample) == SAMPLE_BYTES`, which conflates "we
read exactly the sample-size worth of bytes" with "there is more file beyond
the sample." A file whose real (decompressed) size is exactly `SAMPLE_BYTES`
is misclassified as truncated even though nothing was cut off, incorrectly
stripping coverage-eligibility from that file's genuinely complete last row.

Previously-reported items were checked against the currently reviewed file
where possible; see the Info section for carried-forward status.

## Critical Issues

### CR-01: Contiguous run of ≥2 excluded-candidate rows at the sample boundary still silently drops all but the last

**File:** `packages/csv-processor/src/csv_processor/source.py:118-212` (gate: lines 206-212), cross-referenced against `packages/csv-processor/src/csv_processor/detect/header.py:240-255`

**Issue:**
`_filtered_rows()`'s coverage gate is:
```python
if absolute_index in excluded_indices and absolute_index < sample_covered_row_count:
```
This only removes eligibility from the single index equal to
`sample_covered_row_count` (the sample's last, unprovable, parsed row).
`_detect_footer_rows` in `detect/header.py`, however, builds
`footer_row_indices` by walking backward from that same last row and
appending *every consecutive* row that fails the field-count/pattern check
(`detect/header.py:247-254`, `break` only fires on the first row from the end
that passes):

```python
for offset in range(row_count - 1, -1, -1):
    row = data_rows[offset]
    field_count_differs = len(row) != header_field_count
    ...
    if not (field_count_differs or pattern_matches):
        break
    footer_offsets.append(offset)
```

So whenever the sample's truncated tail happens to contain **two or more
consecutive** rows that mismatch the header's field count — two genuinely
malformed data rows in a row, or (more realistically) an ordinary trailing
blank line immediately adjacent to the row that straddles the 64 KiB cutoff —
both land in `excluded_indices`. Only the very last index is protected by
`sample_covered_row_count`; the second-to-last (and any earlier row in the
same run) is fully covered by the sample (its bytes are not truncated, since
it's followed by more sample content), so it sails past the coverage gate
and is only checked by CR-03's content re-validation. Since that row is
*genuinely* malformed (its real content in the full file, not just the
sample, differs in field count from the header), CR-03's re-validation
"confirms" it as footer-shaped and it is silently excluded — vanishing from
both `valid_rows` and `invalid_rows` with no error surfaced anywhere. This is
precisely the class of bug this exact function has now shipped three times
(03-06 CR-02, 03-07 CR-03's own gap, 03-08 CR-04 fixed only the single-row
instance), now confirmed to still exist one row earlier in a contiguous run.

No test in `tests/unit/test_structural_validation.py` exercises this: every
boundary test (`test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped`,
`test_repeated_header_excluded_and_out_of_coverage_malformed_row_surfaced_together`)
constructs exactly one malformed/candidate row at the boundary, immediately
preceded by well-formed rows, which never exercises the backward-walk's
"keep appending while contiguous" behavior.

**Fix:**
The coverage-eligibility gate needs to protect the *entire contiguous run*
of candidate indices ending at the truncation boundary, not just the single
final index — e.g., compute the maximal suffix of `excluded_indices` that is
contiguous with (and includes) `sample_covered_row_count` itself, and treat
every index in that suffix as ineligible, not only the boundary index:

```python
def _uncoverable_tail_indices(
    excluded_indices: set[int], sample_covered_row_count: int
) -> set[int]:
    """Every excluded_indices member in the unbroken run ending at the
    sample's own unprovable last row is equally untrustworthy -- the
    backward footer/repeated-header walk that produced them cannot
    distinguish a genuine multi-row footer from a truncation artifact
    chained onto an adjacent genuinely malformed row."""
    uncoverable = set()
    idx = sample_covered_row_count  # the sample's own unprovable last row
    while idx in excluded_indices:
        uncoverable.add(idx)
        idx -= 1
    return uncoverable
```
and then check `absolute_index not in uncoverable_tail` (precomputed once
per call, not per row) instead of the current `< sample_covered_row_count`
comparison. Add a regression test with two consecutive malformed rows (and
one with a trailing blank line) landing at the sample boundary before
considering CR-04 closed.

## Warnings

### WR-01: `sample_was_truncated` misclassifies a file whose size exactly equals `SAMPLE_BYTES`

**File:** `packages/csv-processor/src/csv_processor/source.py:320-323`

**Issue:**
```python
sample_was_truncated = len(sample) == SAMPLE_BYTES
```
This treats "we read exactly `SAMPLE_BYTES` bytes" as proof more data
follows, but a file whose total (decompressed) size is exactly
`SAMPLE_BYTES` also reads exactly `SAMPLE_BYTES` bytes and hits EOF with
nothing left. In that case `sample_was_truncated` is wrongly `True`, so
`sample_covered_row_count = len(sample_rows) - 1` needlessly strips
eligibility from that file's genuinely complete, provably-covered last row.
If that last row is a real footer/repeated-header row, it now fails to be
excluded and instead surfaces as a spurious `WRONG_COLUMN_COUNT` invalid
row — a functional regression versus the module's own "provably captured in
full" contract, even though it fails loud rather than silently (lower
severity than CR-01, but the same root cause: inferring truncation from
byte count alone rather than confirming EOF).

**Fix:** Read one byte past the sample size and use its presence to
determine truncation, which works uniformly for compressed and
uncompressed streams:
```python
sample = raw_stream.read(SAMPLE_BYTES + 1)
sample_was_truncated = len(sample) > SAMPLE_BYTES
if sample_was_truncated:
    sample = sample[:SAMPLE_BYTES]
```

### WR-02: No test coverage for the plan's own disclosed multi-row residual risk

**File:** `tests/unit/test_structural_validation.py`

**Issue:** The 03-08 plan explicitly disclosed that a contiguous run of ≥2
genuinely malformed rows at the sample boundary would still be silently
dropped (all but the last), yet no test in this file constructs that
scenario — every boundary test uses exactly one candidate row. Given this
function's track record (three prior rounds, each missing an edge case the
previous round's own tests didn't cover), shipping a known, disclosed gap
without a red test guarding it is how this cycle repeats a fifth time.

**Fix:** Add a test mirroring
`test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped` but
with the last **two** appended rows both malformed (or one malformed row
immediately preceded by a blank line) before the cutoff, asserting both
surface as `WRONG_COLUMN_COUNT` invalid rows rather than one silently
vanishing.

### WR-03: File-handle leak in `prepare_source()`'s PASS 2 setup (carried forward, confirmed still present)

**File:** `packages/csv-processor/src/csv_processor/source.py:371-388`

**Issue:** Confirmed unchanged from the prior round's WR-02. `real_stream`
is opened and wrapped into `text_stream` at lines 372-375, then the
header-skip loop `for _ in range(...): next(reader)` (line 388) runs before
`prepare_source()` returns. If that loop raises (e.g. a `csv.Error` from the
1 MiB field-size limit, or a `StopIteration` if the real file somehow has
fewer physical rows than PASS 1's header/preamble count implied), the
exception propagates out of `prepare_source()` before the caller ever
receives `text_stream` to close it — the open file descriptor leaks. No
`try`/`except`+`close()` wraps this section.

**Fix:**
```python
real_stream = _open_raw_stream(file_path)
text_stream: TextIO = io.TextIOWrapper(
    real_stream, encoding=decode_codec, newline="", errors="strict"
)
wrapper = _LineCapturingTextStream(text_stream)
reader = csv.reader(wrapper, ...)
try:
    for _ in range(header_detection.header_row_index + 1):
        next(reader)
except Exception:
    text_stream.close()
    raise
```

## Info

### IN-01: `# type: ignore[operator]` still masking `int | None` arithmetic (carried forward, confirmed still present)

**File:** `packages/csv-processor/src/csv_processor/source.py:387,399`

**Issue:** Both sites do `header_detection.header_row_index + 1  # type: ignore[operator]` against a field typed `int | None`. The surrounding logic already guarantees non-`None` (the `has_header` check raises otherwise), so this is provably safe today, but a bare ignore comment gives no runtime protection if that invariant is ever broken by a future edit, and mypy silently stops checking this expression forever.

**Fix:** Replace with an explicit assertion once, right after the `has_header` check, which narrows the type for both use sites and adds a real runtime guard: `assert header_detection.header_row_index is not None`.

### IN-02: Module-level `csv.field_size_limit()` side effect (carried forward, confirmed still present)

**File:** `packages/csv-processor/src/csv_processor/source.py:40`

**Issue:** `csv.field_size_limit(1_048_576)` executes at import time, mutating global `csv` module state as a side effect of merely importing `csv_processor.source`. Any other code in the same process importing this module inherits this limit change even if it never calls into `source.py`'s functions, and test isolation across modules that also touch `csv.field_size_limit` can be affected by import order.

**Fix:** Move the call inside `prepare_source()` (idempotent to call repeatedly) or document explicitly in the module docstring that importing this module has this global side effect, so it isn't rediscovered by surprise later.

### IN-03: Header-row-itself truncation ambiguity is not covered by the new coverage gate

**File:** `packages/csv-processor/src/csv_processor/source.py:301-339`

**Issue:** `sample_covered_row_count`'s coverage concept is applied only to `excluded_indices` inside `_filtered_rows`; `header_detection.header_row_index`/`raw_header` themselves are used as ground truth for `MISSING_REQUIRED_COLUMN`/`EXTRA_UNEXPECTED_COLUMN` and for building every row's dict without any equivalent check. If a file's preamble exceeds `SAMPLE_BYTES` such that the header row itself is the sample's last (possibly truncated) parsed row, a truncated header could in principle be treated as ground truth. In practice `_row_is_header_shaped`'s modal-field-count gate (`detect/header.py:145-169`) usually rejects a truncated header outright (its field count won't match the modal count of following rows), producing `NO_HEADER_ROW` rather than silently accepting a wrong header — so this is a low-probability, largely self-mitigating case, not a confirmed reproducible bug like CR-01. Flagging because it's the same class of "sample-truncation ambiguity" this function has needed four rounds of fixes for, just applied to a different row.

**Fix:** No immediate action required given the self-mitigating gate, but worth a one-line comment in `prepare_source()` noting this is deliberately out of scope, and a targeted test (huge preamble > 64 KiB before a real header) to confirm the self-mitigation actually holds rather than relying on it implicitly.

### IN-04: WR-01 (reserved-metadata-key collision) and WR-03 (required/nullable contradiction) from prior rounds not independently reverifiable this round

**File:** N/A — outside this round's reviewed file scope

**Issue:** This review's file list is limited to `source.py` and `test_structural_validation.py`. The prior rounds' reserved-metadata-key collision (likely in `engine.py`, where per-row dicts are built) and the required/nullable contradiction (likely in `config/models.py`'s `ColumnSpec`) live in files not included in this pass's scope, so their current status could not be confirmed one way or the other here.

**Fix:** Include `engine.py` and `config/models.py` in the next review pass's file list to close out confirmation of these two items.

### IN-05: `prepare_source()` remains a large, densely-branching function

**File:** `packages/csv-processor/src/csv_processor/source.py:215-406`

**Issue:** Pre-existing, not introduced by this round's change, but every one of the four review rounds on this file has had to re-derive this same ~190-line function's full control flow (encoding cross-check, dialect cross-check, header cross-check, sample-coverage computation, PASS 2 setup) to reason about one isolated fix. The sample-truncation/coverage computation (lines 301-323) in particular is a self-contained, independently testable unit currently inlined in the middle of an unrelated detection sequence.

**Fix:** Extract lines ~301-323 into a small helper (e.g. `_sample_coverage(sample: bytes, sample_rows: list[list[str]]) -> int`) with its own focused unit tests for the boundary cases enumerated in this review (0 rows, 1 row, exactly `SAMPLE_BYTES` file size, truncated with >1 row) — would have caught WR-01 directly and makes the next round's review much faster.

---

_Reviewed: 2026-08-29T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
