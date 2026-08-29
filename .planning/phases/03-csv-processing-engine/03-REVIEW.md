---
phase: 03-csv-processing-engine
reviewed: 2026-08-29T13:59:20Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - packages/csv-processor/src/csv_processor/source.py
  - packages/csv-processor/src/csv_processor/engine.py
  - packages/csv-processor/src/csv_processor/detect/filename.py
  - tests/unit/test_structural_validation.py
  - tests/unit/test_filename_no_dataplat_import.py
findings:
  critical: 1
  warning: 3
  info: 2
  total: 6
status: issues_found
---

# Phase 3: Code Review Report (focused re-review of gap-closure plan 03-06)

**Reviewed:** 2026-08-29T13:59:20Z
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found

## Summary

This is a focused re-review of the 03-06 gap-closure plan, which claimed to fix two Critical
bugs (CR-01: `ColumnSpec.required` enforcement; CR-02: preamble/footer/repeated-header row
exclusion) and one Warning (WR-01: residual `dataplat` import in `detect/filename.py`).

- **WR-01 (residual `dataplat` import) is genuinely closed.** `detect/filename.py` has zero
  `dataplat` imports; the new regression test correctly anchors on line-start to avoid
  false-triggering on the docstring's prose mentions of `dataplat.diagnostics`/
  `dataplat.config.model`. Verified by direct read of the full file.

- **CR-01 (`required` enforcement) is genuinely and completely closed.** `source.py`'s header
  check now correctly compares only `required_names` (not all `declared_names`) against the
  header, and `engine.py`'s companion per-row backfill correctly prevents the `KeyError` that
  would otherwise fire in `validate.check_row`/`normalize_row` for an optional column absent
  from the detected header. Traced against `validate.py` and `customers.json`'s real config;
  confirmed correct by the existing regression test.

- **CR-02 (preamble/footer/repeated-header exclusion) is closed for the fixture-sized case the
  regression test covers, but is NOT complete — it has a new, silent, and empirically
  reproduced data-loss bug on any file larger than the 64 KiB detection sample.** See CR-02
  below; this is the Critical finding.

I built a minimal, out-of-band reproduction (a 6,000-row, ~117 KiB CSV with a clean header and
no footer at all) and ran it directly against `process_chunks()`: it silently drops exactly one
legitimate data row (`ID003276`, which happens to straddle the `SAMPLE_BYTES` = 65,536-byte
boundary) with **no** entry in either `valid_rows` or `invalid_rows` — the row simply vanishes.
This is worse than a false-positive footer misclassification alone would suggest, because the
row isn't reported as invalid either; it disappears from the pipeline's output entirely with no
trace.

## Critical Issues

### CR-01: Footer/repeated-header detection runs on a truncated 64 KiB sample, but exclusion is applied to the real full-file read — silently drops legitimate rows on any file > `SAMPLE_BYTES`

**File:** `packages/csv-processor/src/csv_processor/source.py:44, 149-321`
(root cause interacts with `packages/csv-processor/src/csv_processor/detect/header.py:207-256`,
not in this review's file list but necessary context)

**Issue:**

`prepare_source()` computes `header_detection` (including `footer_row_indices` and
`repeated_header_row_indices`) from `sample_rows`, which is built by parsing only the first
`SAMPLE_BYTES` (65,536) bytes of the file (`source.py:184, 235-246`). For any file larger than
that sample, the byte-65536 cutoff will, in the overwhelming majority of cases, land in the
middle of a physical row rather than exactly on a row boundary. The resulting truncated/partial
row (the last entry in `sample_rows`) is fed into `detect_header()`'s footer-detection walk
(`header.py:_detect_footer_rows`), which classifies a row as "footer" whenever its field count
differs from the header's field count — which a truncated row almost always will.

Before this gap-closure plan, `footer_row_indices`/`repeated_header_row_indices` were computed
but never consumed, so this sample-truncation artifact was harmless. CR-02's fix wires these
indices directly into `_filtered_rows()` (`source.py:118-146`) as absolute row indices to
*exclude from the real, complete PASS 2 read*. Since the absolute index of the truncated
sample-artifact row is a real index into the true file, PASS 2 excludes whatever genuine,
complete, well-formed row actually sits at that position — silently dropping it. It is not
reported as `invalid_rows` either; it is simply never yielded by `_filtered_rows` at all.

**Reproduction (verified, not hypothetical):**

```
$ .venv/bin/python3 repro_footer_bug.py
total bytes: 120008
valid: 5999 invalid: 0 expected: 6000
BUG CONFIRMED: rows lost or misclassified near sample boundary
missing ids sample: ['ID003276'] count: 1
invalid rows sample: []
```

A 6,000-row `id,name` CSV with a clean header and no footer whatsoever loses exactly row
`ID003276`, whose line begins at byte offset 65,528 — 8 bytes before the `SAMPLE_BYTES` = 65,536
cutoff, i.e. exactly the row the sample truncates mid-line. `process_chunks()` returns 5,999
valid rows and 0 invalid rows for a 6,000-row input, with zero indication anything went wrong.

This is a genuine, silent data-loss bug for realistic production file sizes (any CSV over
~64 KiB, which is a very small file for a bulk ETL target) — worse than the CR-02 bug it was
meant to fix, because a false *inclusion* of a footer row is at least visible in row counts,
whereas this is an invisible false *exclusion* of good data.

**Fix:**

The root problem is that footer/repeated-header detection is being done against a head-sample
that has no reliable knowledge of the true end of file. At minimum, guard against scoring a
sample-truncation artifact as a real row:

```python
# source.py, right after computing sample_rows, before calling detect_header:
sample_was_truncated = len(sample) == SAMPLE_BYTES  # more bytes may follow in the real file
if sample_was_truncated and sample_rows:
    # The last row in a truncated sample may be a partial/malformed artifact of the
    # cutoff, not a real row — never let it participate in footer/repeated-header
    # candidacy (it can still be safely used for encoding/dialect/header detection,
    # since those only look at the *start* of the file).
    footer_candidate_rows = sample_rows[:-1]
else:
    footer_candidate_rows = sample_rows
```

and pass `footer_candidate_rows` (not `sample_rows`) into whatever powers footer/repeated-header
detection specifically, while still using the full `sample_rows` for header detection itself.
This bounds the blast radius to "a footer that happens to fall exactly at the sample boundary is
missed" (a false negative, safe) rather than "an arbitrary real row near the boundary is
silently dropped" (a false positive with data loss).

The more complete fix is architectural: footer detection is inherently a "look at the tail of
the file" problem, not a "look at the head" problem, and a fixed 64 KiB head-sample can never
reliably see a real footer on a large file at all (a true footer on a multi-MB file is never in
this sample to begin with — a separate, pre-existing false-negative gap this same root cause
also produces, independent of the truncation-artifact bug above). Closing CR-02 completely would
require reading a real tail-window of the file (or scanning the actual PASS 2 stream and
buffering the last N rows before finalizing which are "footer"), not relying solely on the PASS
1 head-sample for this specific check.

## Warnings

### WR-01: Injected per-row metadata keys can silently clobber a same-named declared column's value in `invalid_rows` output

**File:** `packages/csv-processor/src/csv_processor/engine.py:77-88, 106-115`

**Issue:** Both invalid-row construction sites build the output dict as
`{**row_dict, "error_code": ..., "error_message": ..., "source_file": ..., "row_number": ...,
"raw_line": ...}`. If a dataset config ever declares a column literally named `error_code`,
`error_message`, `source_file`, `row_number`, or `raw_line`, that column's real parsed value is
silently overwritten by the injected metadata field — the caller has no way to recover the
original value, and there is no validation anywhere (`config/models.py`'s `ColumnSpec`/
`DatasetConfig`) that rejects a column name colliding with this reserved set. Neither shipped
config (`customers.json`, `orders.json`) currently triggers this, so it is latent rather than
active today, but this module is written as a general-purpose engine (its own docstrings
emphasize config-driven genericity), so this is a real correctness gap for any future dataset.

**Fix:** Either (a) namespace the injected metadata keys so they can never collide with a column
name (e.g. `"_error_code"`/`"_row_number"`, or nest them under a single `"_meta"` key rather than
splatting at the top level), or (b) add a `DatasetConfig` validator that rejects any column name
in a reserved-word set (`{"error_code", "error_message", "source_file", "row_number",
"raw_line"}`) at config-load time, matching the existing `_check_column_names_are_unique`
pattern in `config/models.py`.

### WR-02: No `try`/`finally` around PASS 2 stream setup in `prepare_source` — file handle leak if the header-skip loop raises

**File:** `packages/csv-processor/src/csv_processor/source.py:292-321`

**Issue:** `real_stream = _open_raw_stream(file_path)` opens a file handle, which is wrapped in
`text_stream = io.TextIOWrapper(real_stream, ...)` and returned to the caller for the caller to
close (per this function's own docstring: "the caller must close it once done, e.g. via
`try`/`finally`"). `engine.py`'s `try`/`finally` that closes `text_stream` only begins *after*
`source.prepare_source(...)` has already returned (`engine.py:59-61`). If anything between
opening `real_stream` and the `return` statement raises — e.g. `next(reader)` in the
header-skip loop at `source.py:308-309` (plausible under a TOCTOU race if an upstream process is
still writing/truncating the file between PASS 1's sampling and PASS 2's reopen, or if
`csv.field_size_limit` is exceeded on a row within the skipped preamble) — `real_stream`/
`text_stream` is never closed, because no reference to it exists yet in `engine.py`'s scope and
`prepare_source` itself has no `try`/`finally` protecting this section.

**Fix:** Wrap the PASS 2 setup in its own `try`/`except`, closing `real_stream`/`text_stream` and
re-raising on any exception:

```python
real_stream = _open_raw_stream(file_path)
text_stream = io.TextIOWrapper(real_stream, encoding=decode_codec, newline="", errors="strict")
try:
    wrapper = _LineCapturingTextStream(text_stream)
    reader = csv.reader(wrapper, ...)
    for _ in range(header_detection.header_row_index + 1):
        next(reader)
    ...
except Exception:
    text_stream.close()
    raise
```

### WR-03: `ColumnSpec` permits a `required: false` + `nullable: false` combination that is guaranteed to fail every row where the column is absent, with no upfront config validation

**File:** `packages/csv-processor/src/csv_processor/engine.py:92-103` (interaction with
`config/models.py`'s `ColumnSpec`, not in this review's file list)

**Issue:** CR-01's companion fix in `engine.py` backfills any declared column missing from the
detected header as an empty string (`row_dict[column.name] = ""`), so its `nullable` flag governs
it identically to a present-but-blank value (per the fix's own docstring, this is intentional).
However, if a column is declared `required: false` (legitimately absent from the file is allowed)
but also `nullable: false` (a present value can never be blank), every single row of a file that
omits that column will now deterministically fail `NULL_VIOLATION` — 100% of the time, with no
config-time signal that this contradiction exists. `ColumnSpec`'s own `_check_type_specific_fields`
validator (`config/models.py:46-64`) checks several other cross-field contradictions but not this
one.

**Fix:** Add a cross-field check to `ColumnSpec` (or `DatasetConfig`) rejecting `required=False`
combined with `nullable=False` at config-load time, e.g.:

```python
@model_validator(mode="after")
def _check_optional_column_is_nullable(self) -> ColumnSpec:
    if not self.required and not self.nullable:
        msg = (
            f"column {self.name!r}: 'required: false' with 'nullable: false' is "
            "contradictory — an absent optional column is always treated as blank, "
            "which 'nullable: false' would then always reject"
        )
        raise ValueError(msg)
    return self
```

## Info

### IN-01: `# type: ignore[operator]` used to suppress a real `int | None` type mismatch instead of a runtime assertion

**File:** `packages/csv-processor/src/csv_processor/source.py:308, 317`

**Issue:** `header_detection.header_row_index` is typed `int | None`. The code relies on the
earlier `if not header_detection.has_header: raise ...` check (line 253) to guarantee it is
non-`None` by this point, but expresses that guarantee only via two `# type: ignore[operator]`
comments rather than a runtime-checkable assertion. If a future refactor changes
`has_header`'s semantics without updating both mypy-suppression sites, this would silently
resurface as a `TypeError: unsupported operand type(s) for +: 'NoneType' and 'int'` at runtime
instead of a caught invariant violation.

**Fix:** Bind it once with an assertion mypy can also use as a type-narrowing signal:

```python
assert header_detection.header_row_index is not None  # has_header check above guarantees this
for _ in range(header_detection.header_row_index + 1):
    next(reader)
...
start_index=header_detection.header_row_index + 1,
```

### IN-02: Module-level `csv.field_size_limit(1_048_576)` call mutates process-wide global state as an import side effect

**File:** `packages/csv-processor/src/csv_processor/source.py:40`

**Issue:** `csv.field_size_limit()` is a process-global setting, not scoped to this module or to
`csv.reader` instances constructed here. Calling it unconditionally at import time means any
other code in the same process (a test suite, another package, a notebook) that imports
`csv_processor.source` for any reason silently has its own CSV field-size limit changed as a
side effect, which can be surprising and hard to trace if a different limit is expected
elsewhere in the same process. (Not part of this gap-closure plan's changes — pre-existing —
flagged here since the file was read in full for this review.)

**Fix:** Consider setting this inside `prepare_source()` (or `_open_raw_stream`) rather than at
import time, or documenting explicitly in the module docstring that importing this module has
this global side effect so future maintainers aren't surprised by it.

---

_Reviewed: 2026-08-29T13:59:20Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
