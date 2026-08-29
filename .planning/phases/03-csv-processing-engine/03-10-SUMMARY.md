---
phase: 03-csv-processing-engine
plan: 10
subsystem: database
tags: [csv-processing, validation, gap-closure, data-integrity]

# Dependency graph
requires:
  - phase: 03-csv-processing-engine (plan 09)
    provides: "source.py's _uncoverable_tail_indices()/_filtered_rows() sample-boundary coverage-eligibility chain (CR-01/CR-03/CR-04/WR-01) -- this plan's fix sits BEFORE that chain (footer_row_indices is now forced empty at the call site for non-opted-in datasets) and leaves it completely unmodified for opted-in datasets"
provides:
  - "csv_processor.config.models.CsvDialectConfig.has_footer: bool = False (FTR-01) -- new per-dataset opt-in field for footer-shape exclusion, absent from every dataset config shipped today"
  - "csv_processor.source.prepare_source() -- PASS 2 call site now computes footer_row_indices = set(header_detection.footer_row_indices) if config.csv.has_footer else set() BEFORE building excluded_indices, so a non-opted-in dataset's excluded_indices never contains a footer candidate at all"
  - "Three new regression tests: default-off silent-drop reproduction (small-file, no truncation), real-generator/real-customers.json seed=11 reproduction (35/15/50), and opt-in-still-excludes-genuine-footer proof (no-truncation case)"
affects: ["04-oracle-bulk-load"]

# Actuals (#2632)
actuals:
  tokens: 9400
  tasks: 2
  commits: 4

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Config-gated feature CONSUMPTION at the call site, not at the detector: detect/header.py's _detect_footer_rows() stays unconditional and pure (unmodified); only whether source.py consumes its output is now conditional on a per-dataset opt-in. This keeps the detector's own contract (skip_footer_rows/footer_patterns already existed as unused parameters) fully intact while closing the silent-drop gap entirely at the one call site that matters."

key-files:
  created: []
  modified:
    - packages/csv-processor/src/csv_processor/config/models.py
    - packages/csv-processor/src/csv_processor/source.py
    - configs/defaults.json
    - tests/unit/test_config_models.py
    - tests/unit/test_config_loader.py
    - tests/unit/test_structural_validation.py

key-decisions:
  - "Corrected the plan's own cosmetic arithmetic note (Task 2 acceptance criteria said '190 total'; actual is 192 = 187 pre-existing + 5 new: 2 config tests from Task 1, 3 structural tests from Task 2) -- confirmed by running the full suite, which reports 192 passed. No functional impact; the plan's objective note pre-authorized this correction."
  - "repeated_header_row_indices consumption is left completely unconditional (never gated by has_footer) per the plan's own explicit scope boundary -- an exact-text match against the real header is a much stronger, lower-false-positive signal than field-count mismatch and needs no opt-in gate."

patterns-established: []

requirements-completed: [ENGINE-01, ENGINE-05, ENGINE-06, TEST-01]

coverage:
  - id: D1
    description: "A dataset whose config never declares has_footer (the default for every dataset config shipped today) never excludes a trailing row on shape/field-count-mismatch grounds alone, at any file size -- a genuinely malformed last row always surfaces as WRONG_COLUMN_COUNT, never silently dropped"
    requirement: ENGINE-05
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_no_footer_optin_default_surfaces_malformed_last_row_as_invalid_not_dropped"
        status: pass
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_generator_driven_customers_seed11_wrong_column_count_last_row_not_dropped"
        status: pass
    human_judgment: false
  - id: D2
    description: "A dataset that explicitly opts in via has_footer=true still correctly excludes its genuine trailing footer row(s), identically to the pre-existing unconditional-heuristic behavior, for the no-truncation case"
    requirement: ENGINE-06
    verification:
      - kind: unit
        ref: "tests/unit/test_structural_validation.py::test_footer_optin_still_excludes_genuine_footer_row_within_sample"
        status: pass
    human_judgment: false
  - id: D3
    description: "CsvDialectConfig.has_footer exists, defaults to False, round-trips to True when explicitly set, and every dataset config shipped today (customers.json, orders.json) resolves it to False through the real load_config() merge path with zero dataset-JSON edits"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_config_models.py::test_has_footer_defaults_to_false, ::test_has_footer_round_trips_with_explicit_true"
        status: pass
      - kind: unit
        ref: "tests/unit/test_config_loader.py::test_load_config_returns_validated_customers_dataset (extended assertion)"
        status: pass
    human_judgment: false
  - id: D4
    description: "Every one of the 15+ pre-existing regression tests from the prior four gap closure rounds (03-06 through 03-09) passes unmodified; detect/header.py and _filtered_rows()'s own body are untouched by this plan"
    requirement: TEST-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/ -q (192 passed: 187 pre-existing + 5 new)"
        status: pass
    human_judgment: false

# Metrics
duration: 20min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 10: Gap Closure — Per-Dataset Footer Opt-In (FTR-01) Summary

**Closed 03-VERIFICATION.md's FTR-01 gap by adding `CsvDialectConfig.has_footer: bool = False` and gating `prepare_source()`'s footer-row-indices consumption on it, so a genuinely malformed last row is never silently dropped for any dataset that never declares it expects a footer.**

## Performance

- **Duration:** ~20 min
- **Completed:** 2026-08-29
- **Tasks:** 2/2 completed
- **Files modified:** 6

## Accomplishments

- Added `CsvDialectConfig.has_footer: bool = False` as the 9th field on `CsvDialectConfig` (FTR-01), with an inline comment documenting its rationale and gap origin. `configs/defaults.json`'s `csv` block gains an explicit `"has_footer": false` entry per this file's own established convention.
- Rewrote `prepare_source()`'s PASS 2 call site so `footer_row_indices` is computed as `set(header_detection.footer_row_indices) if config.csv.has_footer else set()` BEFORE `excluded_indices` is built — a non-opted-in dataset's `excluded_indices` never contains a footer candidate at all. `repeated_header_row_indices` stays unconditionally active, unaffected. `detect/header.py` and `_filtered_rows()`'s own signature/body are completely unmodified — only the CALL SITE'S consumption of already-computed detector output is now conditional.
- Added `has_footer: bool = True` as a keyword-only parameter to both `_large_id_name_config()` and `_preamble_footer_config()` in `tests/unit/test_structural_validation.py` (default preserves all 8 existing zero-argument call sites' behavior byte-for-byte).
- Added three new regression tests: `test_no_footer_optin_default_surfaces_malformed_last_row_as_invalid_not_dropped` (small-file, no-truncation reproduction — RED against pre-fix code: 0 invalid rows instead of 1), `test_generator_driven_customers_seed11_wrong_column_count_last_row_not_dropped` (real generator + real `customers.json`, seed=11 — RED against pre-fix code: 14/49 instead of 15/50, matching 03-VERIFICATION.md's own reproduction exactly), and `test_footer_optin_still_excludes_genuine_footer_row_within_sample` (permanent regression proof, not a RED/GREEN pair — passes both before and after the fix).
- Added `test_has_footer_defaults_to_false`/`test_has_footer_round_trips_with_explicit_true` to `tests/unit/test_config_models.py`, and extended `test_load_config_returns_validated_customers_dataset` in `tests/unit/test_config_loader.py` with one new assertion proving the real `customers.json` resolves the new opt-in to `False` through the real `load_config()` merge path.
- Full unit suite grew from 187 to 192 tests (2 config tests + 3 structural tests), zero regressions. Independently re-ran the exact generator-driven reproduction (`generate_rows(customers_config, rows=50, invalid_ratio=0.3, seed=11)` → `write_csv` → `process_chunks`) outside the test suite: now reports `35 valid / 15 invalid / 50 total`, matching the generator's own report exactly (previously `35/14/49`).

## Task Commits

Each task was committed atomically, following standard TDD RED/GREEN cycles:

1. **Task 1 (RED): Add failing tests for `CsvDialectConfig.has_footer`** - `29095e5` (test)
2. **Task 1 (GREEN): Add `CsvDialectConfig.has_footer` opt-in field** - `1f0d73c` (feat)
3. **Task 2 (RED): Add failing regression tests for FTR-01 footer opt-in** - `1ad62cc` (test)
4. **Task 2 (GREEN): Gate footer-row exclusion on `has_footer` in `prepare_source()`** - `f251178` (fix)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/config/models.py` - `CsvDialectConfig` gains `has_footer: bool = False` (FTR-01) as its 9th field.
- `configs/defaults.json` - `csv` block gains `"has_footer": false` as the explicit 9th key.
- `packages/csv-processor/src/csv_processor/source.py` - `prepare_source()`'s PASS 2 call site gates `footer_row_indices` on `config.csv.has_footer`; `excluded_indices` computed from the gated local variable. No other line changed; `_filtered_rows()`'s own body untouched.
- `tests/unit/test_config_models.py` - `test_has_footer_defaults_to_false`, `test_has_footer_round_trips_with_explicit_true`.
- `tests/unit/test_config_loader.py` - `test_load_config_returns_validated_customers_dataset` extended with `assert config.csv.has_footer is False`.
- `tests/unit/test_structural_validation.py` - `_large_id_name_config()`/`_preamble_footer_config()` gain `has_footer: bool = True` keyword-only parameter; three new tests (see Accomplishments); new top-of-file import `from generator.generate_csv import generate_rows, write_csv`.

## Decisions Made

- Corrected the plan's own cosmetic arithmetic note in Task 2's acceptance criteria (stated "190 total"; actual is 192 = 187 + 5 new) — pre-authorized by this plan's objective note as a prose-only slip, no functional impact. Confirmed via `uv run pytest tests/unit/ -q` reporting 192 passed.
- Left `repeated_header_row_indices` consumption completely unconditional (never gated by `has_footer`), matching the plan's explicit scope boundary — an exact-text match against the real header is a stronger, lower-false-positive signal than field-count mismatch and needs no opt-in gate.

## Deviations from Plan

None — plan executed exactly as written, aside from the pre-authorized cosmetic arithmetic correction noted above (not a deviation from the plan's own rules, since the objective note explicitly flagged and pre-approved it).

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required.

## Known Stubs

None. Both the config field and the gated consumption are fully wired and exercised end-to-end with live regression tests; no placeholder/hardcoded-empty data paths introduced.

## Threat Flags

None. This plan closes an existing threat register entry (T-03-19, already present in the plan's own `<threat_model>`) rather than introducing new security-relevant surface. T-03-20 (documented, accepted residual for opted-in datasets, unchanged from 03-06..03-09) remains open by design.

## Next Phase Readiness

03-VERIFICATION.md's FTR-01 gap (the new finding scored FAILED against Roadmap Success Criterion #2 and 03-02-PLAN's "MUST NOT silently drop a structurally invalid row" prohibition) is now closed. `process_chunks()` over the real generator's real `customers.json` output (seed=11, 50 rows, 30% invalid) accounts for all 50 rows (35 valid + 15 invalid), matching the generator's own report exactly. `repeated_header_row_indices` consumption remains completely unaffected by this change, exactly as scoped. `detect/header.py` and `_filtered_rows()`'s own body are unmodified by this plan. Every one of the 15+ pre-existing regression tests from the prior four gap closure rounds (03-06 through 03-09) passes with zero modification to its own function body. `csv_processor.engine.process_chunks(file_path, config)`'s public surface is unchanged in shape — Phase 4's Oracle bulk-load work builds on the same function signature, now with ENGINE-05's row-count-accuracy guarantee holding against every identified silent-data-loss vector across five consecutive gap-closure rounds (03-06 through 03-10). No blockers for Phase 4.

---
*Phase: 03-csv-processing-engine*
*Completed: 2026-08-29*

## Self-Check: PASSED

- FOUND: packages/csv-processor/src/csv_processor/config/models.py
- FOUND: packages/csv-processor/src/csv_processor/source.py
- FOUND: configs/defaults.json
- FOUND: tests/unit/test_config_models.py
- FOUND: tests/unit/test_config_loader.py
- FOUND: tests/unit/test_structural_validation.py
- FOUND: .planning/phases/03-csv-processing-engine/03-10-SUMMARY.md
- FOUND commit: 29095e5
- FOUND commit: 1f0d73c
- FOUND commit: 1ad62cc
- FOUND commit: f251178
