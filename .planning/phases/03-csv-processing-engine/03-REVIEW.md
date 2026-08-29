---
phase: 03-csv-processing-engine
reviewed: 2026-08-29T00:00:00Z
depth: standard
files_reviewed: 2
files_reviewed_list:
  - packages/csv-processor/src/csv_processor/source.py
  - tests/unit/test_structural_validation.py
findings:
  critical: 0
  warning: 2
  info: 4
  total: 6
status: issues_found
---

# Phase 03: Code Review Report (Round 5 — footer/repeated-header exclusion re-review)

**Reviewed:** 2026-08-29T00:00:00Z
**Depth:** standard
**Files Reviewed:** 2
**Status:** issues_found

## Summary

This is the fifth adversarial pass over `_uncoverable_tail_indices()` /
`_filtered_rows()` / `prepare_source()`'s footer/repeated-header row
exclusion logic in `source.py`, focused on round 09's two changes: (1)
computing the uncoverable-tail contiguous run **per source then unioning
the results** (`footer_row_indices` and `repeated_header_row_indices`
walked separately) instead of union-then-walk, and (2) reading
`SAMPLE_BYTES + 1` bytes to disambiguate "sample cut off because more file
follows" from "file's real size happens to equal `SAMPLE_BYTES` exactly."

I traced both changes against `detect/header.py`'s actual implementation
(`_detect_footer_rows`'s backward-walk-until-first-match algorithm,
`_detect_repeated_header_rows`'s unbounded full-scan) to establish ground
truth for what shapes of index sets each detector can actually produce,
then re-derived every adversarial case the prompt asked about by hand:
runs entirely within one source, runs split across both sources at the
same boundary, empty sets, index-0-touching runs,
`sample_covered_row_count == 0`, overlapping indices from both sources,
and 0/1-row files. The per-source-then-union design is **structurally
sound** for the specific bug class it was built to fix (round 09's own
regression test,
`test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run`,
correctly passes) — `_detect_footer_rows`'s output can only ever be a
single contiguous run ending at the sample's true last row (it breaks on
first mismatch, never resumes), so a naive union-then-walk was the only
way this bug could have previously "swallowed" a genuinely independent,
numerically-adjacent `repeated_header_row_indices` candidate, and the fix
correctly closes exactly that gap. The `SAMPLE_BYTES + 1` read is trimmed
back to `SAMPLE_BYTES` *before* it reaches encoding detection, dialect
detection, or `sample_rows` construction (lines 352–354), so it has no
observable side effect on any other sample-byte consumer, including
across `compression.py`'s `_DecompressionBombGuard.read()` (verified: its
`read(size)` loop guarantees a full read up to EOF, not just `read1`-style
short reads, so the +1-byte trick works identically for gzip/zip streams).

One real, still-unaddressed gap survives this round: the fix generalizes
CR-04's boundary-uncoverable protection **symmetrically** to
`repeated_header_row_indices`, but that source's exact-value-match
criterion does not have the same truncation-induced ambiguity that
justified the protection for `footer_row_indices`'s field-count-mismatch
criterion in the first place — see WR-01 below. No new critical/blocker
defect was found in this round's actual diff.

## Warnings

### WR-01: Repeated-header boundary protection has no real ambiguity to protect against, and silently under-excludes

**File:** `packages/csv-processor/src/csv_processor/source.py:290-292` (the two `_uncoverable_tail_indices()` calls), docstring rationale at lines 232-251

**Issue:** `_uncoverable_tail_indices()` is now called separately on
`repeated_header_row_indices` with the same "treat the contiguous run
ending at `sample_covered_row_count` as unprovable" logic used for
`footer_row_indices`. That logic is justified for `footer_row_indices`
because `_detect_footer_rows`'s criterion (field-count mismatch) is
*trivially* produced by mid-row truncation — any partial row cut at an
arbitrary byte offset almost certainly has the wrong field count, so the
sample's own truncated copy of the true last row cannot be trusted to
carry the same footer verdict as the real, complete row.

`_detect_repeated_header_rows`'s criterion is exact tuple equality against
`raw_header`. Truncating a row's bytes overwhelmingly *breaks* an
accidental match (a truncated fragment essentially never happens to equal
every one of the header's full field strings) — it does not *create*
false positives the way field-count mismatch does. There is no realistic
truncation-induced ambiguity for this source to protect against, so
extending the boundary gate to it only removes real detection power: a
genuine, interior-*or*-boundary repeated-header row whose real full
content (read fresh in PASS 2, never truncated at that point) exactly
equals `raw_header` is still marked "uncoverable" purely because its
sample-derived index happens to sit at/adjacent to
`sample_covered_row_count`, and is therefore never re-validated by CR-03
at all — it flows through unfiltered as an ordinary data row, contradicting
this module's own G-03-2 guarantee ("never loaded as records").

No test in `test_structural_validation.py` constructs a repeated-header
row that is *itself* the sample's last covered row (all three repeated-
header regression tests — lines 336, 370, 512 — place the repeated-header
candidate away from the boundary and only the *malformed/footer* candidate
at the boundary). The gap is real but narrow: it only manifests when a
genuine repeated-header line lands exactly at (or immediately before,
chained) the 64 KiB sample cutoff in a file exceeding `SAMPLE_BYTES`.

**Fix:** Either (a) don't apply `_uncoverable_tail_indices()` to
`repeated_header_row_indices` at all — let every repeated-header candidate
go straight to CR-03's exact-match re-validation regardless of boundary
position, since that criterion is self-verifying against real content; or
(b), if symmetry is still wanted for defense-in-depth, add the missing
regression test (repeated-header row *at* `sample_covered_row_count` in a
file > `SAMPLE_BYTES`) and explicitly document why the trade-off is
accepted despite the lack of a real ambiguity to protect against.

### WR-02 (carried forward, still present, out of scope for this plan): PASS 2 file-handle leak on skip-loop failure

**File:** `packages/csv-processor/src/csv_processor/source.py:474-490`

**Issue:** `real_stream`/`text_stream` are opened, then
`for _ in range(header_detection.header_row_index + 1): next(reader)`
consumes the preamble+header rows with no `try`/`finally` around it. If
`next(reader)` raises (e.g. a concurrent truncation between PASS 1 and
PASS 2, or a `csv.Error` from the 1 MiB field-size ceiling set at module
scope) before `prepare_source()` returns, `text_stream` is never handed to
the caller and is never closed — a leaked file descriptor. Confirmed still
present in this round; not part of round 09's diff and explicitly flagged
as out of scope by this review's brief, noted here only for continuity
with prior review IDs.

**Fix (unchanged from prior review):** wrap the open + skip-loop in a
`try/except` that closes `real_stream`/`text_stream` and re-raises on
failure.

## Info

### IN-01: `repeated_header_row_indices`/`footer_row_indices`/`excluded_indices` are recomputed as three separate `set()` calls over the same data

**File:** `packages/csv-processor/src/csv_processor/source.py:491-504`

**Issue:** `excluded_indices` (line 491-493) and the `footer_row_indices=`/
`repeated_header_row_indices=` keyword arguments (line 503-504) each
independently wrap `header_detection.footer_row_indices` /
`header_detection.repeated_header_row_indices` in `set(...)`, duplicating
the conversion work and creating three separate set objects from the same
two source tuples. Harmless (correctness is unaffected — verified the
three sets are always in agreement since they all derive from the same
`header_detection` instance), but avoidable duplication.

**Fix:** Compute `footer_set`/`repeated_header_set` once and reuse them for
both `excluded_indices = footer_set | repeated_header_set` and the
`_filtered_rows(...)` call's keyword arguments.

### IN-02: Unstructured `LookupError` possible from an invalid `config.csv.encoding`

**File:** `packages/csv-processor/src/csv_processor/source.py:356`

**Issue:** `codecs.lookup(config.csv.encoding).name` is not guarded — an
invalid/misspelled encoding name in `config.json` (if it somehow bypasses
upstream Pydantic validation, e.g. a valid-looking-but-unsupported codec
alias) raises a bare `LookupError`, not the module's own
`StructuralValidationError`, breaking the documented contract at the top
of the file ("Every whole-file structural reject raises the SAME
`errors.StructuralValidationError`... never a family of exception
subclasses"). Low severity since this is expected to be caught by config
validation before reaching this module, but this module has no defense of
its own if that assumption is ever violated.

**Fix:** Wrap the `codecs.lookup()` calls in a `try/except LookupError` and
re-raise as `StructuralValidationError` with a dedicated error code, or
document explicitly that this function trusts `config.csv.encoding` to
already be a valid codec name by construction.

### IN-03: Footer-detection's field-count-mismatch criterion cannot distinguish "genuine footer" from "genuinely malformed final row" for files that fit entirely within one sample — undocumented scope boundary

**File:** `packages/csv-processor/src/csv_processor/source.py:180-251` (module-level docstrings)

**Issue:** For a file ≤ `SAMPLE_BYTES` (no truncation, `sample_covered_row_count`
lands one past the last real index so the uncoverable-tail gate is never
engaged), a row that is genuinely the file's last physical row and happens
to have the wrong field count is *always* classified and excluded as a
footer by `_detect_footer_rows`/CR-03, with no way to tell "the file's
real footer" apart from "a data feed that happened to truncate its own
last write mid-row." This is a correct, intentional characteristic of
`detect/header.py`'s field-count-based footer criterion (documented there
as unconditional — "a row is footer when its field count differs...", no
further disambiguation attempted), not a defect introduced by this round's
changes or by `source.py`'s exclusion logic specifically. It is called out
here only because the surrounding docstrings in `source.py` (CR-02/03/04)
extensively document the *truncation-boundary* version of this ambiguity
but never mention that the identical ambiguity is unconditionally present
— and unmitigated by design — for small/medium files too.

**Fix:** No code change required; consider a one-line addition to
`_filtered_rows()`'s docstring noting this is a known, accepted scope
boundary (field-count-mismatch-based footer detection is inherently
unable to distinguish these two cases at any file size, not just at the
sample-truncation boundary) so a future reviewer doesn't mistake it for a
new gap.

### IN-04: Test suite fragility acknowledged in comments, not fixed

**File:** `tests/unit/test_structural_validation.py:398-401, 481-482`

**Issue:** Two tests explicitly avoid using underscores in malformed-row
literals because doing so "tips charset_normalizer's ascii-vs-utf-8 pick
for this specific sample's byte distribution" (an unrelated
`detect/encoding.py` quirk). This is a documented workaround, not a latent
bug in these two files, but it signals the test suite's malformed-row
fixtures are coupled to an external heuristic library's behavior in a way
that could silently regress if that library's version changes. Flagging
for awareness only; out of scope to fix here.

**Fix:** None required for this review; consider tracking the underlying
`detect/encoding.py` quirk separately so this workaround can eventually be
removed.

## Verification of the four specific adversarial scenarios requested

1. **Run entirely within one source** — verified correct: `footer_row_indices`
   can only ever be a single contiguous run ending at the sample's last row
   (by `_detect_footer_rows`'s break-on-first-match backward walk), and
   `_uncoverable_tail_indices()` captures the whole run in one call.
2. **Run split across both sources at the same boundary** — verified
   correct via `test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run`
   and hand-traced: each source's own internal contiguity, not shared
   ordering, determines its own contribution to the union; an isolated
   interior candidate from one source cannot be captured by the other
   source's boundary-touching run.
3. **Empty sets** — verified: `_uncoverable_tail_indices(set(), n)` returns
   `set()` immediately (idx not in an empty set), matches unit test line 433.
4. **Index-0-touching runs** — verified: no explicit bounds guard needed,
   since a negative index can never be a set member; matches unit tests at
   lines 439, 441.
5. **`sample_covered_row_count == 0`** — verified: only reachable when
   `sample_rows` has at most 1 row (header only), in which case
   `footer_row_indices`/`repeated_header_row_indices` are always empty
   (`data_rows` is empty), so no interaction occurs regardless of the
   anchor value.
6. **`SAMPLE_BYTES + 1` read side effects** — verified none: `sample` is
   trimmed back to exactly `SAMPLE_BYTES` bytes (lines 352-354) before any
   other consumer (encoding detection, dialect detection, `sample_rows`)
   sees it; confirmed this holds for compressed streams too by reading
   `compression.py`'s `_DecompressionBombGuard.read()`, which always loops
   to satisfy the full requested size (or true EOF), not just `read1`-style
   short reads.
7. **Multiple separate contiguous runs at different points in the file** —
   verified handled correctly: only the run touching
   `sample_covered_row_count` is swept into `uncoverable_tail`; any other,
   non-adjacent candidate index (from either source) is untouched and still
   goes through ordinary CR-03 content re-validation.
8. **`footer_row_indices` and `repeated_header_row_indices` overlapping on
   the same index** — verified harmless: both per-source walks
   independently include the shared index in their own result if it's part
   of their own contiguous run; the union is idempotent, no double-count or
   conflict.
9. **File with exactly 0 or 1 rows** — verified: 0-byte file raises
   `NO_HEADER_ROW` before this logic is reached (`sample_covered_row_count`
   is still computed defensively without going negative, guarded by the
   `and sample_rows` check at line 424); a 1-row (header-only) file has
   empty `data_rows`, so both candidate sets are empty and no exclusion
   logic engages, matching `test_14_header_only_no_rows_yields_zero_chunks`.
10. **See WR-01 above** for the one scenario where a real repeated-header
    row could still be silently mishandled (under-excluded, not dropped) —
    this is the one finding from this round's re-review that was not fully
    closed by round 09's fix.

---

_Reviewed: 2026-08-29T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
