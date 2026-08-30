---
phase: 02-config-contract-csv-generator
fixed_at: 2026-08-29T06:15:36Z
review_path: .planning/phases/02-config-contract-csv-generator/02-REVIEW.md
iteration: 1
findings_in_scope: 8
fixed: 7
skipped: 1
status: partial
---

# Phase 2: Code Review Fix Report

**Fixed at:** 2026-08-29T06:15:36Z
**Source review:** .planning/phases/02-config-contract-csv-generator/02-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 8 (CR-01, WR-01 through WR-07; `fix_scope=critical_warning` so IN-01..IN-04 were excluded)
- Fixed: 7
- Skipped: 1

## Fixed Issues

### CR-01: `CsvDialectConfig` permits `doublequote=false` without `escapechar`, crashing the CSV writer at runtime

**Files modified:** `packages/csv-processor/src/csv_processor/config/models.py`
**Commit:** e316dd8
**Applied fix:** Added a `model_validator(mode="after")` to `CsvDialectConfig` that raises `ValueError` when `doublequote` is `False` and `escapechar` is unset, mirroring the review's suggested fix exactly (matches `DatasetConfig`'s existing delimiter/decimal-separator collision validator pattern already in the file).

### WR-01: `OracleTargetSpec` does not reject `valid_table == invalid_table`

**Files modified:** `packages/csv-processor/src/csv_processor/config/models.py`
**Commit:** 43fe4b5
**Applied fix:** Added a `model_validator(mode="after")` to `OracleTargetSpec` rejecting a case-insensitive match between `valid_table` and `invalid_table`, as suggested.

### WR-02: `ColumnSpec` accepts `precision`/`scale` on non-decimal columns

**Files modified:** `packages/csv-processor/src/csv_processor/config/models.py`
**Commit:** 1561a1b
**Applied fix:** Extended `_check_type_specific_fields`'s existing `if self.type == "decimal": ... else:` structure with an `elif` branch that raises `ValueError` when `precision`/`scale` are set on a non-decimal column, as suggested. Verified indentation attaches the new branch to the correct `if`.

### WR-03: `DatasetConfig.columns` allows duplicate column names

**Files modified:** `packages/csv-processor/src/csv_processor/config/models.py`
**Commit:** 3877133
**Applied fix:** Added a `model_validator(mode="after")` to `DatasetConfig` (alongside the existing delimiter/decimal-separator collision validator) rejecting duplicate `ColumnSpec.name` values, as suggested.

### WR-05: `tools/corpus/generators.py` row-spec renderers raise raw `KeyError` instead of `GeneratorError`

**Files modified:** `tools/corpus/generators.py`
**Commit:** 10df782
**Applied fix:** Threaded `fixture_name`/`column` (already available at the `_renderer_for` call sites) into all four renderer builders (`_zero_padded_renderer`, `_pick_renderer`, `_decimal_renderer`, `_repeat_renderer`) and wrapped each builder's required-key dict lookups in `try`/`except KeyError`, re-raising as `GeneratorError` naming the fixture, column, and missing key — matching the module's existing error-naming discipline. Confirmed no other call sites (tests or otherwise) invoke these renderer builders directly with the old single-arg signature. Targeted test run (`tests/unit/test_corpus_generators.py`, 9 tests) passed after the change.

### WR-06: `_decimal_renderer` emits mathematically wrong digits for negative decimal ranges

**Files modified:** `tools/corpus/generators.py`
**Commit:** d662104
**Applied fix:** Replaced the floor-division/modulo formatting (which assumed `units >= 0`) with explicit sign/magnitude decomposition before formatting, as suggested. Verified live: `units=-439, power=100, scale=2` now renders `"-4.39"` (previously the buggy `"-5.61"`), matching the review's worked example exactly. Targeted test run passed.

### WR-07: `ColumnSpec.precision`/`scale` lack `Field(gt=0)`, crashing downstream with an unrelated `TypeError`

**Files modified:** `packages/csv-processor/src/csv_processor/config/models.py`
**Commit:** ec57f2a
**Applied fix:** Changed both `precision: int | None = None` and `scale: int | None = None` to `Field(default=None, gt=0)`, consistent with `ProcessingConfig.chunk_size`'s existing `Field(gt=0)` precedent cited by the review. Confirmed no existing fixture/config/test declares `scale: 0` (which `gt=0` would now reject) via repo-wide grep before applying — the constraint only affects negative/zero values, which had no legitimate prior use.

## Skipped Issues

### WR-04: `generate_csv.py`'s "missing_required" category never reads `ColumnSpec.required`

**File:** `generator/generate_csv.py:54-55`, `127-129`
**Reason:** Skipped — the review's characterization of this as a bug conflicts with an explicit, documented design decision found in `02-01-PLAN.md` (lines 347-350), which this fixer read as part of applying the fix and which the reviewer's pass apparently did not have in scope. The plan states verbatim: *"always include `\"missing_required\"` (target any `nullable=False` column — note this maps to `nullable`, not the separate `required` field, per REQUIREMENTS.md's own ENGINE-03 phrasing 'required (non-nullable)'; D-09's `required` field stays schema-declared for a future column-presence check Phase 3 does not yet implement)."* `REQUIREMENTS.md`'s `ENGINE-03` ("Engine validates required (non-nullable) fields are non-empty") is confirmed `Pending`/scoped to Phase 3, not this phase. In other words: the generator's current `nullable`-keyed behavior is intentional and matches this phase's own plan; `ColumnSpec.required` is deliberately inert scaffolding reserved for a future column-presence check that Phase 3 will implement, not dead code left by oversight. Applying either of the review's suggested fixes (renaming the category, or implementing a distinct required-column-presence-drop category) would contradict this phase's recorded design intent rather than fix a genuine defect. Flagging for human judgment: either the naming should be revisited in a future phase alongside Phase 3's actual `required` implementation, or the review's finding should be marked as intentional/by-design in a subsequent review pass.
**Original issue:** `applicable_categories()` and `_generate_invalid_row()` both compute "missing_required" eligibility/behavior from `not column.nullable`, never from `column.required`, making `column.required` effectively dead code outside its own model/test.

## Verification

`uv run pytest tests/unit/ -q` was run after all fixes were applied and committed: **89 passed**, 0 failed, 0 regressions. (Also ran a scoped `tests/unit/test_corpus_generators.py` pass — 9/9 — immediately after each `tools/corpus/generators.py` edit, before the full-suite confirmation.)

Verification ran inside the isolated git worktree created for this fix run (`workflow.use_worktrees` was not set to `false` in `.planning/config.json`, so the default isolated-worktree path was used); the worktree's commits are fast-forwarded onto the `master` branch as part of this run's cleanup, so the same 89/89 result is reproducible from the main checkout after that fast-forward completes.

---

_Fixed: 2026-08-29T06:15:36Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
