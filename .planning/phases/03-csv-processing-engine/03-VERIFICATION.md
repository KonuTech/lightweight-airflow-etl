---
phase: 03-csv-processing-engine
verified: 2026-08-29T16:04:37Z
status: gaps_found
score: 4/5 roadmap success criteria verified (18/20 merged plan-level truths verified)
behavior_unverified: 0
overrides_applied: 0
re_verification:
  previous_status: gaps_found
  previous_score: 6/8
  gaps_closed:
    - "A CSV that is structurally valid per config (e.g. it omits a column declared `required: false`) is NOT whole-file-rejected — CLOSED by 03-06 (`required_names` filter in `source.py`), confirmed present in current code and covered by `test_optional_column_absent_from_header_processes_successfully`."
    - "Row counts and validation results remain accurate for a file containing a metadata preamble, a trailing footer, or a repeated interior header row — the ORIGINAL gap (PASS 2 skipping exactly 1 hardcoded row, never consulting `header_row_index`/`footer_row_indices`/`repeated_header_row_indices`) is CLOSED by 03-06, confirmed present in current `prepare_source()`/`_filtered_rows()` and covered by `test_preamble_footer_and_repeated_header_rows_excluded_from_processing`."
  gaps_remaining: []
  regressions: []
gaps:
  - truth: "Total row count (valid + invalid) reported by process_chunks() always equals the number of real, non-excluded data rows in the file — a bad row never silently vanishes without being counted (Roadmap SC #2 / ENGINE-05 / ENGINE-06 / 03-02-PLAN's explicit 'MUST NOT silently drop a structurally invalid row' prohibition)."
    status: failed
    reason: >
      Independently reproduced (not merely inferred from 03-REVIEW.md's IN-03, which downgrades
      this to an "Info … no code change required" note): `_detect_footer_rows()` in
      `detect/header.py` is invoked by `source.py`'s `prepare_source()` with
      `skip_footer_rows=0` and `footer_patterns=()` — i.e. footer detection is UNCONDITIONAL
      and UNCONFIGURABLE for every dataset (`DatasetConfig` has no field to disable or scope
      it). Its sole criterion is "does this row's field count differ from the header's field
      count." `_filtered_rows()`'s CR-03 content re-validation re-checks the SAME criterion
      (`is_footer_shaped = len(row) != header_field_count`) against the real row — so a row
      that is genuinely the file's last physical row AND genuinely malformed (wrong field
      count, e.g. a dropped trailing field) "confirms" its own exclusion under the identical
      test a real footer would pass, and is excluded via `continue` in `_filtered_rows()`
      BEFORE it ever reaches `engine.py`'s per-row `WRONG_COLUMN_COUNT` handling. The row is
      not counted in `valid_rows`, not counted in `invalid_rows`, and no exception is raised —
      it disappears with zero trace anywhere in `prepare_source()`/`process_chunks()`. This is
      NOT limited to files exceeding `SAMPLE_BYTES` (i.e. it is a different, broader defect
      than the CR-01/CR-04/WR-01 sample-truncation-boundary chain 03-06→03-09 fixed): for any
      file — small or large — whose real last data row happens to have the wrong number of
      fields, it is silently swallowed as a "footer," never surfaced as invalid.

      Verified three independent ways:
      1. Direct unit-level repro: a 4-line, 54-byte CSV (`id,name` header + 2 valid rows +
         one genuinely malformed final row `MALFORMEDLASTROWNOCOMMA`) — well within one 64 KiB
         sample, no truncation involved at all. `process_chunks()` returns 2 valid rows, 0
         invalid rows; the malformed 3rd data row is gone (total accounted = 2, expected = 3).
      2. WR-01's own scenario (a genuine repeated-header row placed exactly at the sample
         boundary) was independently re-derived and confirmed to be a DIFFERENT, lower-severity
         failure mode than this one: the row under-excludes (flows through as an ordinary row,
         landing in `valid_rows` since its literal content happens to satisfy this config's
         checks) — visible, not silent, matching 03-REVIEW.md's WR-01 characterization exactly.
      3. Realistic, generator-driven end-to-end repro against this project's OWN
         `generator/generate_csv.py` (Phase 2's GEN-01) and OWN shipped
         `configs/datasets/customers.json`: `generate_rows()`'s `wrong_column_count` invalid
         category (a real, always-applicable generator category — `row[:-1]`, one fewer field
         than the header) is placed at a uniformly random row index
         (`rng.sample(range(rows), num_invalid)`) with no exclusion of the last position. With
         seed=11 (50 rows, 30% invalid ratio, no hand-crafted fixture), the generator's own
         last generated row is a `wrong_column_count` row. The generator reports 35 valid / 15
         invalid / 50 total; `process_chunks()` over the generated file reports 35 valid / 14
         invalid / 49 total — one full row vanished with zero record, against the real
         production config and the real production generator, not a synthetic edge case.

      03-REVIEW.md's 5th-pass review (IN-03) identified the same root cause but classified it
      as "a correct, intentional characteristic … not a defect … no code change required,"
      reasoning that field-count-based footer detection is inherently unable to distinguish
      the two cases at any file size. That framing conflates "the ambiguity is inherent" (true)
      with "therefore silently discarding one interpretation without a trace is acceptable"
      (not supported by this phase's own must-haves) — 03-02-PLAN.md's must_haves explicitly
      states "MUST NOT silently drop a structurally invalid row without producing a
      corresponding invalid-row/whole-file-reject signal — every row must be accounted for in
      total/valid/invalid counts," and the roadmap's own Success Criterion #2 requires "both
      counts are accurate." Per this verifier's brief to independently confirm rather than
      trust the review's conclusion, this is scored FAILED, not accepted as an Info-level
      note.
    artifacts:
      - path: "packages/csv-processor/src/csv_processor/source.py"
        issue: "`_filtered_rows()` (lines 166-299) excludes a row via `continue` whenever it is both an `excluded_indices` candidate AND not part of `uncoverable_tail` AND its real content re-confirms `is_footer_shaped`/`is_repeated_header` — with no way to distinguish 'genuine footer' from 'genuinely malformed last row' for the common (non-sample-truncated) case, and no config knob to disable footer detection per dataset."
      - path: "packages/csv-processor/src/csv_processor/detect/header.py"
        issue: "`_detect_footer_rows()` (lines 207-255) is always invoked with `skip_footer_rows=0`/`footer_patterns=()` from `source.py`, so its unconditional field-count-mismatch heuristic runs on every file with no opt-out."
    missing:
      - "Decide the product intent: either (a) never treat a field-count-mismatched trailing row as an unconditional footer unless the dataset config explicitly opts in (thread `skip_footer_rows`/`footer_patterns` through `DatasetConfig` and only call `_detect_footer_rows` with real footer expectations), or (b) if a row is excluded as footer-shaped but was never independently corroborated as an INTENTIONAL footer (e.g. only exclude when `skip_footer_rows`/`footer_patterns` matched, and otherwise surface field-count mismatches as ordinary `WRONG_COLUMN_COUNT` invalid rows)."
      - "Whichever direction is chosen, add a regression test using the ACTUAL generator (not just a hand-crafted small fixture) proving a `wrong_column_count`-category row landing at the physical end of a file is never dropped from both `valid_rows` and `invalid_rows`."
      - "Re-classify 03-REVIEW.md's IN-03 from Info to at least Warning/Critical given the concrete generator-driven reproduction, or explicitly record an accepted-risk override with a named decision-maker if the team decides current behavior is intentional."
---

# Phase 3: CSV Processing Engine Verification Report

**Phase Goal:** Given a raw CSV file and a dataset config, the engine correctly separates valid, type-converted rows from invalid, error-tagged rows, processing in bounded-memory chunks, with zero Airflow dependency.
**Verified:** 2026-08-29T16:04:37Z
**Status:** gaps_found
**Re-verification:** Yes — after gap closure (03-06 through 03-09 closed the original 2 gaps; this pass independently discovered 1 new, related gap in the same feature area)

## Goal Achievement

### Roadmap Success Criteria

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A CSV with structural problems (wrong column count, missing/unexpected columns) is flagged before any type/nullability check runs | ✓ VERIFIED | `engine.py:69-89` short-circuits on `len(raw_row) != len(header)` before `validate.check_row()` is ever called (D-13); `source.py:443-471` rejects missing/extra/duplicate columns at the whole-file level before any row processing. Covered by `test_09..test_16` in `test_structural_validation.py`, all passing. |
| 2 | Processing a mixed valid/invalid fixture yields correctly typed valid rows and error-tagged invalid rows; one bad row never halts the rest; **both counts are accurate** | ✗ FAILED | See Gaps. A genuinely malformed row that happens to be the file's last physical row is silently excluded by the always-on footer-detection heuristic — never counted in either `valid_rows` or `invalid_rows`. Reproduced 3 independent ways, including against the real `generate_csv.py` generator and real `customers.json` config (seed=11: generator says 50 total, engine says 49). |
| 3 | Large-file processing runs in configurable chunks; memory stays bounded; detection runs once per file | ✓ VERIFIED | `engine.py:62` uses `itertools.batched(paired_rows, config.processing.chunk_size)`; `source.prepare_source()` (called once in `engine.py:59`) does all detection before chunking begins. `tests/unit/test_corpus_bounded_memory.py::test_streaming_read_survives_the_rlimit_as_cap` (passes) and its sibling `test_buffering_readlines_dies_under_the_identical_rlimit_as_cap` (a whole-file-accumulating variant, which DOES die under the same RLIMIT_AS cap) together empirically prove the bounded-memory claim, not just by code inspection. |
| 4 | `csv_processor` can be imported and its full test suite run with no Airflow installed | ✓ VERIFIED | `tests/unit/test_no_airflow_import.py` performs an AST-based scan (not grep) of every file under `packages/csv-processor/src/csv_processor/`, self-tested against synthetic `import airflow` / `from airflow.providers...` fixtures first to prove real detection power before trusting it against the real tree. `test_no_csv_processor_module_imports_airflow` passes. `pyproject.toml`/`packages/csv-processor/pyproject.toml` declare no airflow dependency for this package. |
| 5 | The unit test suite (config parsing, CSV parsing, type conversion, date validation, valid/invalid row handling, chunked processing) passes | ✓ VERIFIED | `uv run pytest tests/unit/ -q` → **187 passed** (fresh run performed by this verifier, not taken from SUMMARY.md claims). `make verify-phase3` target exists and runs the identical command. |

**Score:** 4/5 roadmap success criteria verified.

### Merged Plan-Level Truths (03-01 through 03-09 `must_haves.truths`)

Only truths bearing directly on this re-verification's focus (the footer/preamble/repeated-header exclusion chain, CR-01/CR-03/CR-04/WR-01, plus the two originally-failed truths) are detailed below; the remaining ~13 plan-level truths (dependency installation, corpus fixture detection, type-conversion tables, byte-level-hard fixtures, compression streaming, Oracle DDL widening) are covered cumulatively by the 187-test suite and were spot-checked (normalize.py's no-`float()`/explicit-Decimal contract, validate.py's check/normalize split, the AST-based no-airflow scanner) — all passing, no anti-patterns found.

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 6 | (03-06) A `required: false` column may be genuinely absent without whole-file rejection | ✓ VERIFIED | `source.py:444`: `required_names = {c.name for c in config.columns if c.required}`, used for the `missing` check; `extra` check still uses the full `declared_names` set (line 463). `test_optional_column_absent_from_header_processes_successfully` passes. |
| 7 | (03-06) A genuine preamble/footer/repeated-header row within the sample is excluded from both `valid_rows`/`invalid_rows` | ✓ VERIFIED | `test_preamble_footer_and_repeated_header_rows_excluded_from_processing` passes; independently re-traced `prepare_source()`'s skip of `header_row_index + 1` rows (line 489) and `_filtered_rows()`'s exclusion logic. |
| 8 | (03-07/CR-03) A well-formed file > `SAMPLE_BYTES` loses zero rows across the sample boundary | ✓ VERIFIED | `test_large_well_formed_file_loses_zero_rows_across_sample_boundary` passes; content re-validation (`is_footer_shaped`/`is_repeated_header` against the REAL row) confirmed present at `source.py:295-298`. |
| 9 | (03-08/CR-04) A single malformed row at the sample's tail-adjacent position surfaces as `WRONG_COLUMN_COUNT`, never silently dropped | ✓ VERIFIED | `test_malformed_row_at_sample_boundary_surfaces_as_invalid_not_dropped` passes; `sample_covered_row_count`-gated eligibility confirmed at `source.py:290-292`. |
| 10 | (03-09/CR-01) A contiguous run of 2+ candidates at the boundary are ALL coverage-ineligible, computed per-source-then-unioned | ✓ VERIFIED | `test_two_contiguous_malformed_rows_at_sample_boundary_both_surface_as_invalid` and `test_interior_repeated_header_row_not_contaminated_by_adjacent_boundary_footer_run` both pass; `_uncoverable_tail_indices()` called twice (once per source set) at `source.py:290-292`, confirmed by direct code read and by `test_uncoverable_tail_indices_covers_adversarial_edge_cases`' 8 hand-derived adversarial cases, all passing. |
| 11 | (03-09/WR-01) A file whose real size exactly equals `SAMPLE_BYTES` is never misclassified as truncated | ✓ VERIFIED | `test_file_exactly_sample_bytes_size_footer_still_correctly_excluded` passes; `prepare_source()` reads `SAMPLE_BYTES + 1` bytes and derives `sample_was_truncated = len(sample) > SAMPLE_BYTES` (line 352), independently re-traced and confirmed the extra byte is trimmed before any downstream detector sees it (lines 353-354). |
| 12 | (03-REVIEW.md WR-01, this round) A genuine repeated-header row exactly at the sample boundary is still excluded, never under-excluded | ⚠️ Confirmed as a REAL, narrow gap (matches 03-REVIEW.md's own classification) | Independently reproduced: a repeated-header row placed at `sample_covered_row_count`'s boundary is now protected by `_uncoverable_tail_indices()`'s boundary-eligibility gate (correctly, per CR-01's design), which means it is NEVER excluded regardless of its real content — it flows through to ordinary validation and, for this project's actual string-typed columns, lands in `valid_rows` as an ordinary (if odd-looking) row. Zero rows vanish (2000+1 rows in, 2000+1 rows out, all accounted for) — this is a **detection-precision gap** (a row that should structurally be excluded is not), not a **silent-data-loss** gap. Narrow: only manifests when a genuine repeated-header row lands exactly at/adjacent to the sample cutoff in a file > 64 KiB. Not scored as a phase-blocking FAILED truth per this verifier's own independent finding, since it does not violate ENGINE-05/06's counting-accuracy guarantee — but it does mean G-03-2's "always excluded" guarantee has a documented, narrow exception. Recorded here for visibility; 03-REVIEW.md's own suggested fix (a or b) remains open. |
| 13 | New finding (this verification round, not part of any prior review/plan) | ✗ FAILED | See top-level `gaps` — a genuinely malformed row at the TRUE end of ANY file (not gated by sample size/truncation at all) is silently dropped with zero record. See full detail above. |

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `packages/csv-processor/src/csv_processor/source.py` | `prepare_source()`/`_filtered_rows()`/`_uncoverable_tail_indices()` | ✓ VERIFIED (exists, substantive, wired) | Read in full; matches all 03-06→03-09 plan claims for the sample-boundary chain. New gap (above) is a pre-existing, always-on design characteristic of the footer-detection feature itself, not a wiring defect. |
| `packages/csv-processor/src/csv_processor/engine.py` | `process_chunks()` chunked valid/invalid split | ✓ VERIFIED | `itertools.batched`-driven, D-13 short-circuit present, `finally: text_stream.close()` present. |
| `packages/csv-processor/src/csv_processor/validate.py` | `check_row()`/`normalize_row()` | ✓ VERIFIED | Present, exported, imported by `engine.py`. |
| `packages/csv-processor/src/csv_processor/normalize.py` | `convert_value()`/`parse_decimal_strict()`/`parse_date_strict()` | ✓ VERIFIED | No `float()` used for decimal parsing (module docstring + grep confirms); `Decimal(raw)` used directly. |
| `packages/csv-processor/src/csv_processor/detect/header.py` | `detect_header()`/`_detect_footer_rows()`/`_detect_repeated_header_rows()` | ✓ VERIFIED (exists, substantive, wired) — behavior gap noted above is in the CALLER's (source.py's) unconditional invocation, not in this module's own documented, opt-in-capable contract | Confirmed `skip_footer_rows`/`footer_patterns` parameters exist and are honored inside `detect_header()`; `source.py` simply never passes non-default values. |
| `tests/unit/test_structural_validation.py` | Regression coverage for CR-01/CR-03/CR-04/WR-01/G-03-1/G-03-2 | ✓ VERIFIED | All 15 named tests present and passing (enumerated via `pytest --collect-only`). |
| `tests/unit/test_no_airflow_import.py` | AST-based ENGINE-09 scanner | ✓ VERIFIED | Self-tested against synthetic offenders before trusting the real scan. |
| `Makefile` (`verify-phase3` target) | Single-command phase gate | ✓ VERIFIED | `uv run pytest tests/unit/ -x`; ran the equivalent (`-q` instead of `-x`) directly: 187 passed. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `source.py`'s `prepare_source()` | `engine.py`'s `process_chunks()` | Sole caller, returns `(text_stream, paired_rows, header)` | ✓ WIRED | `engine.py:59`. |
| `detect.detect_header()`'s `footer_row_indices`/`repeated_header_row_indices` | `source.py`'s `_filtered_rows()` | Passed as separate keyword args at the `prepare_source()` call site (lines 503-504) | ✓ WIRED | Confirmed per-source-then-union design (not union-then-walk) matches 03-09's own documented intent, line-by-line. |
| `validate.py`'s `check_row()`/`normalize_row()` | `normalize.py`'s `convert_value()` | Shared type-dispatch table | ✓ WIRED | Confirmed via passing `test_type_validation.py`/`test_normalize.py`. |
| `DatasetConfig` | `detect.detect_header()`'s `skip_footer_rows`/`footer_patterns` opt-in | N/A — **NOT WIRED, by design gap** | ✗ NOT_WIRED | No config field exists to scope/disable footer detection per dataset; this is the root cause of the new gap above. |

### Behavioral Spot-Checks / Independent Reproductions

| Behavior | Command/Script | Result | Status |
|----------|-----------------|--------|--------|
| Full unit suite | `uv run pytest tests/unit/ -q` | 187 passed in 2.75s | ✓ PASS |
| All 15 named footer/repeated-header/uncoverable-tail regression tests exist | `pytest --collect-only -q \| grep ...` | All 15 found | ✓ PASS |
| WR-01 scenario (repeated header at boundary) — is it silent loss or under-exclusion? | Standalone repro script, `process_chunks()` over a 2000+-row file with a repeated-header row placed exactly at the sample boundary | 0 rows missing; the repeated-header row lands in `valid_rows` (under-excluded, not lost) | ✓ Confirms 03-REVIEW.md's own characterization |
| IN-03 scenario (malformed final row of a SMALL, non-truncated file) — is it silent loss? | Standalone repro script, 54-byte 4-line CSV | 1 row (of 3 data rows) silently vanishes: 2 accounted for, `MALFORMEDLASTROWNOCOMMA` never appears in either stream | ✗ FAIL — silent data loss confirmed |
| Same defect, realistic generator-driven scenario | `generator.generate_csv.generate_rows()` against real `configs/datasets/customers.json`, seed=11, 50 rows/30% invalid | Generator: 35 valid/15 invalid/50 total. Engine: 35 valid/14 invalid/49 total. 1 row vanished. | ✗ FAIL — confirmed against real production config/generator, not a synthetic edge case |

### Requirements Coverage

| Requirement | Source Plan(s) | Description | Status | Evidence |
|-------------|-----------------|--------------|--------|----------|
| ENGINE-01 | 02,03,04,05,06,07,08,09 | Structural validation before anything else | ✓ SATISFIED | SC #1 above; `test_09..test_16` pass. |
| ENGINE-02 | 03 | Per-column type validation | ✓ SATISFIED | `test_type_validation.py` passes (part of 187). |
| ENGINE-03 | 03 | Required-field non-empty validation | ✓ SATISFIED | `validate.check_row()` nullability check; covered by structural/type test suites. |
| ENGINE-04 | 03,05 | Explicit type conversion, no implicit Oracle casting/no `float()` for decimals | ✓ SATISFIED | `normalize.py` grep-confirmed; `test_normalize.py` passes. |
| ENGINE-05 | 03,05,07,08,09 | Invalid row doesn't stop processing; both counts accurate | ✗ **BLOCKED** | **Both-counts-accurate half of this requirement is violated** — see Gaps. The "doesn't stop processing" half is satisfied (generator engine never crashes on the malformed row; it simply omits it from any count). |
| ENGINE-06 | 01,03 | Each invalid row records error_code/message/source_file/row_number | ✗ **BLOCKED (partial)** | Satisfied for every row that DOES reach `engine.py`'s per-row loop (confirmed via `engine.py:77-88`); violated for the specific silently-excluded-as-footer row, which never gets an invalid-row record at all. |
| ENGINE-07 | 03,04,05 | Configurable chunked processing, bounded memory, detect-once | ✓ SATISFIED | SC #3 above; RLIMIT_AS proof test passes. |
| ENGINE-09 | 02,05 | No Airflow import, standalone-testable | ✓ SATISFIED | SC #4 above. |
| TEST-01 | 03,05,06,07,08,09 | Unit tests cover config/CSV/type/date/valid-invalid/chunking | ✓ SATISFIED | SC #5 above — 187 tests, no orphaned coverage gaps found. |

No orphaned requirements found — all 9 requested IDs (ENGINE-01..07, ENGINE-09, TEST-01) are declared across the 9 plans and map to REQUIREMENTS.md entries.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `packages/csv-processor/src/csv_processor/source.py` | 474-490 | No `try/finally` around PASS 2's `open()` + preamble/header skip-loop (`next(reader)`) | ℹ️ Info (carried forward from 03-REVIEW.md WR-02, pre-existing, unrelated to this fix chain) | A `next(reader)` exception before `prepare_source()` returns leaks `real_stream`/`text_stream`. Out of scope for this round's fix chain per the review's own brief; not re-litigated here as a blocker, but still open. |
| `packages/csv-processor/src/csv_processor/source.py` | 356 | Unguarded `codecs.lookup(config.csv.encoding)` can raise bare `LookupError` instead of `StructuralValidationError` | ℹ️ Info (carried forward from 03-REVIEW.md IN-02) | Low severity; expected to be caught upstream by config validation. |

No `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER` markers found anywhere under `packages/csv-processor/src/csv_processor/`.

### Human Verification Required

None. All findings in this report were resolved programmatically (via direct code reading, the existing test suite, and three independent standalone reproduction scripts run by this verifier).

### Gaps Summary

Both of the original 03-VERIFICATION.md's gaps are genuinely closed (`required_names` filtering, and `header_row_index`/`footer_row_indices`/`repeated_header_row_indices` consumption) and stayed closed through the 03-07→03-09 chain — confirmed by direct code reading and by all 15 named regression tests passing. The 03-REVIEW.md 5th-pass finding no Critical issues in that specific chain's own diff, which is an accurate, independently-corroborated conclusion for the sample-truncation-boundary scenarios it was scoped to test (CR-01/CR-03/CR-04/WR-01) — reproduced and confirmed by this verifier via direct code tracing and the WR-01 standalone repro (a real, but non-silent, under-exclusion edge case, exactly as characterized).

However, this verifier's independent, adversarial pass (per the explicit instruction not to just trust the review's conclusion) found that the review's own IN-03 finding — dismissed as "Info … no code change required … correct, intentional characteristic" — is in fact a violation of this phase's own explicit must-have prohibition ("MUST NOT silently drop a structurally invalid row … every row must be accounted for in total/valid/invalid counts") and of the roadmap's own Success Criterion #2 ("both counts are accurate"). This is NOT the same defect the 03-06→03-09 chain targeted (that chain is scoped entirely to files exceeding `SAMPLE_BYTES` and to ambiguity introduced by sample truncation); this is the BASELINE ambiguity present in the footer-detection feature for files of any size, and it was reproduced concretely against this project's own real generator and real production dataset config — not merely a theoretical corner case.

This phase cannot be marked fully passed until a decision is made and implemented: either scope footer detection behind an explicit config opt-in (never silently discard an unconfirmed field-count-mismatched trailing row from a dataset that never declared it expects footers), or otherwise ensure a row excluded on this basis is always accounted for in some count. An accepted-risk override is also a valid path forward if the team decides current behavior is intentional, but that decision should be made explicitly and recorded, not defaulted into by an Info-level review note.

---

_Verified: 2026-08-29T16:04:37Z_
_Verifier: Claude (gsd-verifier)_
