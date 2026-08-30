---
phase: 03-csv-processing-engine
verified: 2026-08-29T19:15:00Z
status: passed
score: 5/5 roadmap success criteria verified (23/23 merged plan-level truths verified)
behavior_unverified: 0
overrides_applied: 0
re_verification:
  previous_status: gaps_found
  previous_score: 4/5
  gaps_closed:
    - "FTR-01 — Total row count (valid + invalid) always equals the number of real, non-excluded data rows in the file — CLOSED by 03-10's `CsvDialectConfig.has_footer: bool = False` opt-in field, gating `prepare_source()`'s consumption of `detect/header.py`'s always-computed `footer_row_indices` output. Independently re-derived (not merely inferred from 03-10-SUMMARY.md or 03-REVIEW.md's conclusions): re-ran this verifier's own three original reproductions from scratch against the current code. (1) The 4-line/54-byte small-file repro now surfaces the malformed row as `WRONG_COLUMN_COUNT`, 3 of 3 rows accounted for. (2) The real generator/real `customers.json` seed=11 repro now reports 35 valid / 15 invalid / 50 total — exactly matching the generator's own report (was 35/14/49). (3) A newly constructed >SAMPLE_BYTES file (133,915 bytes, 5000 well-formed rows + 1 malformed last row, `has_footer=False`) — the exact combination 03-REVIEW.md's WR-01 flagged as untested — correctly surfaces the malformed row as invalid with zero loss (5001/5001 accounted for), confirming the review's own algebraic proof that the gate holds regardless of file size."
  gaps_remaining: []
  regressions: []
gaps: []
---

# Phase 3: CSV Processing Engine Verification Report

**Phase Goal:** Given a raw CSV file and a dataset config, the engine correctly separates valid, type-converted rows from invalid, error-tagged rows, processing in bounded-memory chunks, with zero Airflow dependency.
**Verified:** 2026-08-29T19:15:00Z
**Status:** passed
**Re-verification:** Yes — second re-verification, after 03-10's gap-closure plan for FTR-01 (this verifier's own prior-round finding)

## Goal Achievement

### Roadmap Success Criteria

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A CSV with structural problems (wrong column count, missing/unexpected columns) is flagged before any type/nullability check runs | ✓ VERIFIED | Unchanged from prior verification round — regression-checked: `engine.py:69` short-circuits on field-count mismatch before `validate.check_row()` runs; `source.py:443-471` rejects missing/extra columns at whole-file level. `tests/unit/test_structural_validation.py::test_09..test_16` all pass (re-ran, 21 passed). |
| 2 | Processing a mixed valid/invalid fixture yields correctly typed valid rows and error-tagged invalid rows; one bad row never halts the rest; **both counts are accurate** | ✓ VERIFIED | **This is the truth FTR-01 violated last round — independently re-verified as closed, not merely trusted.** Personally re-ran (not copied from 03-REVIEW.md or 03-10-SUMMARY.md) the exact seed=11/`customers.json` generator-driven reproduction from a fresh Python process: `generator: valid=35 invalid=15 total=50` / `engine: valid=35 invalid=15 total=50` — exact match. Also independently constructed a NEW >SAMPLE_BYTES (133,915-byte) scenario with `has_footer=False` and a malformed last row — the specific combination 03-REVIEW.md's WR-01 flagged as *never tested* (all existing 03-06..03-09 large-file tests default `has_footer=True`) — result: 5000 valid + 1 invalid = 5001/5001 accounted for, confirming the fix holds at any file size, not just the small-file case the plan's own regression tests cover. |
| 3 | Large-file processing runs in configurable chunks; memory stays bounded; detection runs once per file | ✓ VERIFIED | Unchanged from prior round — regression-checked: `itertools.batched` chunking, `RLIMIT_AS`-bounded-memory proof tests re-ran and pass (`test_process_chunks_streaming_survives_the_rlimit_as_cap`, `test_process_chunks_buffering_dies_under_the_identical_rlimit_as_cap`). |
| 4 | `csv_processor` can be imported and its full test suite run with no Airflow installed | ✓ VERIFIED | Unchanged — `tests/unit/test_no_airflow_import.py` re-ran, passes (AST-based scan, self-tested against synthetic offenders first). |
| 5 | The unit test suite (config parsing, CSV parsing, type conversion, date validation, valid/invalid row handling, chunked processing) passes | ✓ VERIFIED | Fresh run by this verifier: `uv run pytest -q` (full repo, not scoped to `tests/unit/`) → **195 passed** (192 unit tests for csv_processor + 3 unrelated Phase-1 environment tests). `uv run pytest tests/unit/ -q` alone → 192 passed. Matches 03-REVIEW.md's own claimed count exactly, independently reproduced, not copied. |

**Score:** 5/5 roadmap success criteria verified.

### Merged Plan-Level Truths (03-01 through 03-10 `must_haves.truths`)

All truths verified in the prior verification round (before FTR-01 was found) are regression-checked here via the still-passing 192-test suite and were not re-derived from scratch a third time, per re-verification-mode guidance (full detail already recorded in this file's own prior revision / superseded by this passed report). The truths added by 03-10 (FTR-01's fix) are verified in full detail below, since these are the items that previously failed.

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 14 | (03-10/FTR-01) A dataset whose config never declares `has_footer` never excludes a trailing row on field-count-mismatch grounds alone, at any file size | ✓ VERIFIED | Independently re-derived three ways (see SC #2 above): small 54-byte file, real generator/`customers.json` seed=11 file, and a new >SAMPLE_BYTES 5001-row file constructed by this verifier specifically to close 03-REVIEW.md's own WR-01 coverage gap. All three: zero rows lost. |
| 15 | (03-10/FTR-01) A dataset that opts in via `has_footer: true` still correctly excludes its genuine trailing footer row(s) | ✓ VERIFIED | `test_footer_optin_still_excludes_genuine_footer_row_within_sample` (small-file, no-truncation case) re-ran, passes. The 5 pre-existing 03-06..03-09 large-file tests (`_large_id_name_config()`/`_preamble_footer_config()` default `has_footer=True`) re-ran unmodified, all pass — proves the opted-in large-file path is byte-for-byte unchanged. |
| 16 | (03-10) `repeated_header_row_indices` consumption stays completely unconditional, unaffected by `has_footer` | ✓ VERIFIED | Directly re-read `source.py:505`: `repeated_header_row_indices = set(header_detection.repeated_header_row_indices)` — no `has_footer` gate. Confirmed by 03-REVIEW.md's own hand-proof (exact-tuple-equality with `raw_header` structurally implies field-count match, so it cannot smuggle a footer-shaped exclusion through the `has_footer=False` gate) and independently re-traced by this verifier: `is_repeated_header` requires `tuple(row) == raw_header`, which is only possible when `len(row) == len(raw_header)`, making it mutually exclusive with `is_footer_shaped`. |
| 17 | `configs/defaults.json`'s `has_footer: false` is genuinely inherited by every shipped dataset config | ✓ VERIFIED | Directly read `configs/datasets/customers.json` and `configs/datasets/orders.json` — neither declares a `csv` block at all. `loader.py`'s merge is `{**defaults, **dataset}` (top-level-only) — since `dataset` has no `csv` key to collide, the ENTIRE `csv` block (including `has_footer: false`) flows from `defaults.json` unchanged. Confirmed programmatically: `load_config(customers.json).csv.has_footer` → `False` (verified in a fresh Python process by this verifier, not copied from any prior report). |
| 18 | `detect/header.py` is completely unmodified by the FTR-01 fix (fix is scoped to consumption, not detection) | ✓ VERIFIED | `git log --oneline -- packages/csv-processor/src/csv_processor/detect/header.py` shows exactly one commit ever (`2361a17`, the original 03-02 vendoring) — zero commits from the 03-06 through 03-10 gap-closure chain touch this file. Read the file in full: still unconditionally computes `footer_row_indices` via the field-count-mismatch heuristic with no opt-out parameter wired from `source.py`. |

**No new silent-drop path found elsewhere in the pipeline** (per this verifier's explicit brief to scrutinize the whole row-count-reconciliation invariant, not just re-confirm the reported fix):
- `engine.py`'s per-row loop (`validate.check_row()`/`normalize_row()`) has no early-return/silent-skip path — every row that reaches it lands in exactly one of `valid_rows`/`invalid_rows` (read in full; no `continue` without an append exists in this loop).
- `validate.check_row()`/`normalize_row()` never drop a row — both always return a definite verdict or typed dict for every column.
- A blank physical line is not silently skipped by `csv.reader`/`_filtered_rows()` — it parses as `[]`, `len([]) != len(header)` is true, so it is counted as a `WRONG_COLUMN_COUNT` invalid row (Python's stdlib `csv` module does not skip blank lines).
- The one remaining unconditional heuristic in the pipeline, `repeated_header_row_indices` (item #16 above), was independently re-derived by this verifier (not just trusted from 03-REVIEW.md's IN-01) to be structurally incapable of matching a field-count-mismatched row, and its residual false-positive class (a genuine data row whose every field value is byte-identical to the corresponding header column name, e.g. a `country` column literally containing the string `"country"`) is accepted as an extremely low-probability, already-documented residual (03-REVIEW.md IN-01) — not a new gap, and out of FTR-01's scope by explicit plan decision.
- Chunk-boundary/row-number continuity across `itertools.batched()` batches was re-run via `test_chunk_boundaries_and_cross_chunk_row_number_continuity` (passes) — no row lost or double-counted at a chunk boundary.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `packages/csv-processor/src/csv_processor/config/models.py` | `CsvDialectConfig.has_footer: bool = False` | ✓ VERIFIED (exists, substantive, wired) | Read in full — 9th field, no cross-field validator needed, documented inline with FTR-01 rationale. |
| `packages/csv-processor/src/csv_processor/source.py` | `prepare_source()` gates `footer_row_indices` on `config.csv.has_footer` | ✓ VERIFIED (exists, substantive, wired) | Read in full — `footer_row_indices = set(header_detection.footer_row_indices) if config.csv.has_footer else set()` at line 502-504, computed before `excluded_indices` is built; `_filtered_rows()`'s own body/signature untouched. |
| `configs/defaults.json` | Explicit `"has_footer": false` | ✓ VERIFIED | Present as the 9th key in the `csv` block. |
| `configs/datasets/customers.json` / `orders.json` | Inherit `has_footer: false` via shallow merge | ✓ VERIFIED (data-flow traced) | Neither declares a `csv` block; `loader.py`'s `{**defaults, **dataset}` merge confirmed to flow the entire `defaults.json` `csv` block through unchanged. Independently confirmed via a fresh `load_config()` call, not just static reading. |
| `tests/unit/test_structural_validation.py` | 3 new FTR-01 regression tests + all 03-06..03-09 tests still passing unmodified | ✓ VERIFIED | All 3 new tests (`test_no_footer_optin_default_surfaces_malformed_last_row_as_invalid_not_dropped`, `test_generator_driven_customers_seed11_wrong_column_count_last_row_not_dropped`, `test_footer_optin_still_excludes_genuine_footer_row_within_sample`) individually re-run, all pass. |
| `Makefile` (`verify-phase3` target) | Single-command phase gate | ✓ VERIFIED | Equivalent full-suite run performed directly: 195 passed. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `DatasetConfig.csv.has_footer` | `source.py`'s `prepare_source()` `footer_row_indices` computation | Boolean short-circuit at the PASS-2 call site, before `excluded_indices` is built | ✓ WIRED | Confirmed by direct read and by all three independent reproductions above producing the expected gated behavior. |
| `configs/defaults.json` | `DatasetConfig.csv.has_footer` for every shipped dataset | `loader.load_config()`'s shallow top-level merge | ✓ WIRED | Confirmed programmatically — `load_config(customers.json).csv.has_footer is False`, `load_config(orders.json).csv.has_footer is False` (both independently checked). |
| `detect.detect_header()`'s `repeated_header_row_indices` | `source.py`'s `_filtered_rows()` | Unconditional pass-through, unaffected by `has_footer` | ✓ WIRED (accepted narrow residual, documented) | See truth #16/IN-01 above. |

### Behavioral Spot-Checks / Independent Reproductions

| Behavior | Command/Script | Result | Status |
|----------|-----------------|--------|--------|
| Full unit suite (csv_processor scope) | `uv run pytest tests/unit/ -q` | 192 passed | ✓ PASS |
| Full repo suite | `uv run pytest -q` | 195 passed | ✓ PASS |
| 3 named FTR-01 regression tests | `uv run pytest tests/unit/test_structural_validation.py -k "no_footer_optin_default or generator_driven_customers_seed11 or footer_optin_still_excludes" -v` | 3 passed | ✓ PASS |
| Seed=11 `customers.json` reproduction (this verifier's own primary finding last round) | Standalone script: `generate_rows(customers_config, rows=50, invalid_ratio=0.3, seed=11)` → `write_csv` → `process_chunks` | Generator: 35 valid/15 invalid/50 total. Engine: 35 valid/15 invalid/50 total. Exact match. | ✓ PASS (was FAIL last round: 35/14/49) |
| NEW: >SAMPLE_BYTES file, `has_footer=False`, malformed last row (closes 03-REVIEW.md's WR-01 coverage gap — never tested by any existing named test) | Standalone script: 5000-row well-formed file (133,915 bytes, > 65,536-byte `SAMPLE_BYTES`) + 1 malformed final row, `has_footer=False` | 5000 valid + 1 invalid (`WRONG_COLUMN_COUNT`, row_number 5001) = 5001/5001 accounted for | ✓ PASS — confirms the review's algebraic proof that the fix is size-independent, with an actual large-file execution rather than code reading alone |
| Config inheritance for both shipped datasets | Fresh Python process: `load_config(customers.json).csv.has_footer`, `load_config(orders.json).csv.has_footer` | Both `False` | ✓ PASS |
| Regression: ENGINE-02/03/04/07/09 test suites (spot-checked, not re-derived from scratch — no code changed here since last round) | `uv run pytest tests/unit/test_type_validation.py tests/unit/test_no_airflow_import.py tests/unit/test_corpus_bounded_memory.py -q` | 12 passed | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan(s) | Description | Status | Evidence |
|-------------|-----------------|--------------|--------|----------|
| ENGINE-01 | 02,03,04,05,06,07,08,09,10 | Structural validation before anything else | ✓ SATISFIED | SC #1; unchanged from prior round, regression-checked. |
| ENGINE-02 | 03 | Per-column type validation | ✓ SATISFIED | `test_type_validation.py` re-ran, passes. |
| ENGINE-03 | 03 | Required-field non-empty validation | ✓ SATISFIED | `validate.check_row()` nullability check, unchanged; covered by structural/type suites. |
| ENGINE-04 | 03,05 | Explicit type conversion, no implicit Oracle casting/no `float()` for decimals | ✓ SATISFIED | `normalize.py` unchanged since last round; `test_normalize.py` re-ran, passes. |
| ENGINE-05 | 03,05,07,08,09,10 | Invalid row doesn't stop processing; both counts accurate | ✓ SATISFIED — **previously BLOCKED, now closed** | Both halves confirmed: processing continues past a bad row (unchanged), and — the previously-failing half — both counts are now accurate at any file size, independently re-derived three ways above. |
| ENGINE-06 | 01,03,10 | Each invalid row records error_code/message/source_file/row_number | ✓ SATISFIED — **previously BLOCKED, now closed** | The specific row that was silently excluded-as-footer last round now reaches `engine.py`'s per-row loop and gets a full invalid-row record (`error_code`, `error_message`, `source_file`, `row_number` all present — confirmed in this verifier's own new >SAMPLE_BYTES reproduction). |
| ENGINE-07 | 03,04,05 | Configurable chunked processing, bounded memory, detect-once | ✓ SATISFIED | SC #3; unchanged, regression-checked. |
| ENGINE-09 | 02,05 | No Airflow import, standalone-testable | ✓ SATISFIED | SC #4; unchanged, regression-checked. |
| TEST-01 | 03,05,06,07,08,09,10 | Unit tests cover config/CSV/type/date/valid-invalid/chunking | ✓ SATISFIED | SC #5 — 192 csv_processor unit tests (195 full repo), including the 3 new FTR-01 regression tests, all passing. |

No orphaned requirements found — all 9 requested IDs (ENGINE-01..07, ENGINE-09, TEST-01) are declared across the 10 plans and map to REQUIREMENTS.md entries.

**Documentation-hygiene note (not a code gap, not blocking):** `.planning/REQUIREMENTS.md`'s own checklist/status table (lines 30-53, 178-199) still shows ENGINE-02, ENGINE-03, ENGINE-04, ENGINE-07, and ENGINE-09 as unchecked `[ ]` / "Gaps Found", and does not reflect either the prior verification round's ✓ SATISFIED conclusion or this round's. This is a stale tracking artifact — this verifier independently re-confirmed each of these five requirements is actually satisfied in the codebase (see table above and the fresh test runs), so it is recorded here as a documentation-freshness item for the team to reconcile, not as a phase-blocking gap.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `packages/csv-processor/src/csv_processor/source.py` | 474-490 | No `try/finally` around PASS 2's `open()` + preamble/header skip-loop (`next(reader)`) | ℹ️ Info (carried forward from 03-REVIEW.md WR-02, pre-existing, unrelated to FTR-01) | A `next(reader)` exception before `prepare_source()` returns leaks `real_stream`/`text_stream`. Out of scope for FTR-01; still open, not re-litigated as a blocker here. |
| `packages/csv-processor/src/csv_processor/source.py` | 356 | Unguarded `codecs.lookup(config.csv.encoding)` can raise bare `LookupError` instead of `StructuralValidationError` | ℹ️ Info (carried forward from 03-REVIEW.md IN-02) | Low severity; expected to be caught upstream by config validation. |
| `packages/csv-processor/src/csv_processor/config/loader.py` | 74 | Shallow, non-recursive config merge could silently discard a future dataset's partial `csv` override (e.g. `has_footer: true` lost the moment that dataset also overrides one unrelated `csv` field) | ℹ️ Info (03-REVIEW.md IN-02, latent — no current dataset config is affected; both shipped datasets omit `csv` entirely) | Not a defect today — flagged as a footgun for future config authors, per 03-REVIEW.md's own note. |
| `packages/csv-processor/src/csv_processor/source.py` | 498-501 | `repeated_header_row_indices` stays unconditionally active; a genuine data row whose every field byte-matches the header's column names would be silently excluded | ℹ️ Info (03-REVIEW.md IN-01, independently re-derived and confirmed structurally narrow by this verifier) | Astronomically unlikely for this project's actual schemas (`customer_id`/`name`/`country`/... columns would each need to literally contain their own header string); explicitly out of FTR-01's scope by plan decision. |

No `TBD`/`FIXME`/`XXX`/`HACK`/`PLACEHOLDER` markers found anywhere under `packages/csv-processor/src/csv_processor/`. (One incidental grep hit for the word "placeholder" in `detect/dialect.py`'s docstring is a legitimate design description of a well-formed sentinel value, not a debt marker — read in context and confirmed not a stub.)

### Human Verification Required

None. All findings in this report were resolved programmatically — via direct code reading, the existing 195-test suite (freshly run by this verifier, not taken from any SUMMARY/REVIEW claim), and four independent standalone reproduction scripts this verifier wrote and ran itself, including one new large-file (>SAMPLE_BYTES) scenario specifically constructed to close 03-REVIEW.md's own WR-01 coverage gap.

### Gaps Summary

None. FTR-01 — the sole remaining gap from the prior verification round — is closed. This verifier independently re-derived the exact failure this report itself found last round (not merely re-reading 03-REVIEW.md's or 03-10-SUMMARY.md's conclusions) and confirmed:

1. The primary reproduction (real generator + real `customers.json`, seed=11) now reports 35/15/50, an exact match to the generator's own count — was 35/14/49.
2. Config inheritance is genuine: neither shipped dataset config declares a `csv` block, so both get `has_footer: false` via `defaults.json`'s shallow-merged default with zero dataset-file edits.
3. No other unconditional-heuristic silent-drop path remains in `source.py`/`engine.py`'s row processing: `engine.py`'s per-row loop has no drop path, blank lines are counted (not skipped), chunk-boundary/row-number continuity holds, and the one other unconditional heuristic (`repeated_header_row_indices`) was independently re-derived (not just trusted) to be structurally incapable of the same failure class, with its own narrow residual risk already documented and accepted (03-REVIEW.md IN-01).
4. This verifier additionally closed the one open code-review WARNING (03-REVIEW.md WR-01 — no test proves `has_footer=False` holds on a file exceeding `SAMPLE_BYTES`) by constructing and running that exact scenario itself: 5001/5001 rows accounted for. This is not itself a phase-blocking gap (the review's own algebraic proof already established correctness independent of file size, and this verifier's execution now provides direct empirical confirmation) — recorded here as closed evidence, not as a new gap requiring a plan.

All 9 requirement IDs for this phase (ENGINE-01..07, ENGINE-09, TEST-01) are satisfied with direct evidence. `.planning/REQUIREMENTS.md`'s own checklist is stale (unrelated to this round's fix) and should be reconciled by the team, but this is a documentation-freshness item, not a code gap.

---

_Verified: 2026-08-29T19:15:00Z_
_Verifier: Claude (gsd-verifier)_
