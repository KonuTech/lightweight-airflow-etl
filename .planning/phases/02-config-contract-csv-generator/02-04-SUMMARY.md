---
phase: 02-config-contract-csv-generator
plan: 04
subsystem: testing
tags: [fixture-corpus, digest-oracle, determinism]
requires:
  - phase: 02-config-contract-csv-generator
    provides: tools/corpus manifest/generator/digest mechanism (Plan 03)
provides:
  - tests/fixtures/corpus.yaml extended from 8 to 27 fixtures across four categories (dialect_encoding, structural, type_nullability, byte_level_hard) — Phase 3's real malformed-CSV byte fixtures for the Tier-A vendored detection modules to run against
  - tests/fixtures/CORPUS.sha256 committed digest oracle extended to 27 entries
affects: [phase 3 detection/parsing tests]
actuals:
  tokens: 3668
  tasks: 3
  commits: 3
tech-stack:
  added: []
  patterns:
    - "Deterministic-literal-value discipline for fixtures whose exact byte content is asserted in expect: use the repeat row_spec kind (a constant string, no randomness) instead of pick with multiple candidates -- pick's per-row .random() draw is not guaranteed to select every candidate across a small row count, so a fixture meant to encode one specific invalid value can silently generate only the 'good' candidate"
key-files:
  created: []
  modified: [tests/fixtures/corpus.yaml, tests/fixtures/CORPUS.sha256]
key-decisions:
  - "12_wrong_column_count_row authored as literal, not tabular as the plan's action text specified: the tabular generator renders every row through the same per-column renderers built once from the header, with no mechanism to vary a single row's field count -- there is no row_spec kind that can omit a field for one specific row. Fell back to literal with the ragged row hand-constructed, consistent with the plan's own authorized fallback pattern for Task 3."
  - "27_oversized_field_value authored as tabular (header + repeat row_spec, length: 10001) instead of literal as the plan's action text specified: the repeat row_spec kind already supports an arbitrary-length constant field, producing the exact 10,001-character value without embedding a 10K-character blob directly in the YAML manifest. No new GeneratorKind or RowSpecKind was needed -- repeat already existed from Plan 03."
  - "17-22 (type_nullability) and 23-25 (byte_level_hard) all use rows: 1 with the repeat row_spec kind for any column whose literal value is asserted in expect:, rather than rows: 2 with pick -- see Deviations for why the first (pick-based) attempt at 17-22 was replaced before committing."
patterns-established:
  - "Category comment banners in corpus.yaml mark where each plan's authored block starts, mirroring Plan 03's existing dialect_encoding banner."
requirements-completed: [GEN-01]
coverage:
  - id: D1
    description: "27 fixtures across dialect/encoding, structural, type/nullability, and byte-level-hard categories committed with a passing digest-oracle round-trip"
    verification:
      - kind: other
        ref: "make fixtures && make fixtures-verify (exits 0, 27 fixtures matched)"
        status: pass
      - kind: other
        ref: "sha256sum -c tests/fixtures/CORPUS.sha256 (independent check from repo root, all 27 OK)"
        status: pass
      - kind: other
        ref: "make fixtures && make fixtures-verify run a second time in a row (Task 3 acceptance criterion) -- CORPUS.sha256 byte-identical, only the 5 new digest lines appear in git diff against the prior commit"
        status: pass
    human_judgment: false
  - id: D2
    description: "22_empty_nullable_field_should_pass documents a should-PASS case, not a failure case, in its expect: text"
    verification:
      - kind: other
        ref: "grep -n 22_empty_nullable_field_should_pass -A20 tests/fixtures/corpus.yaml | grep -E pass|clean -- matches (validate cleanly)"
        status: pass
    human_judgment: false
duration: ~20min
completed: 2026-08-28
status: complete
---

# Phase 02 Plan 04: Structural, Type/Nullability, and Byte-Level-Hard Fixture Categories Summary

**Scaled the fixture corpus from 8 to 27 fixtures across three new categories (structural,
type/nullability, byte-level-hard), fixing a randomness-vs-content-guarantee bug in the first
draft of the type/nullability fixtures along the way — no new generator kinds were needed, both
generator-kind mismatches against the plan's literal action text were resolved by falling back to
an already-implemented kind (repeat/literal) rather than adding new machinery.**

## Performance
- **Duration:** ~20min
- **Completed:** 2026-08-28
- **Tasks:** 3
- **Files modified:** 2

## Accomplishments
- Task 1: 8 structural fixtures (`09_missing_column` through `16_ragged_rows_and_blank_lines`) —
  missing/extra/duplicate columns, a ragged data row, a zero-byte file, header-only-zero-rows, no
  header line at all, and mixed short/long rows with interspersed blank lines and no trailing
  newline. Manifest grew to 16 entries, `make fixtures && make fixtures-verify` exits 0.
- Task 2: 6 type/nullability fixtures (`17_invalid_integer_value` through
  `22_empty_nullable_field_should_pass`) — invalid integer, over-scale decimal (3 decimal places
  against `orders.amount`'s configured `scale=2`), invalid date/timestamp format (against
  `customers.json`'s `%Y-%m-%d`/`%Y-%m-%dT%H:%M:%S%z`), an empty non-nullable field, and the one
  explicit should-PASS case (an empty *nullable* field). Manifest grew to 22 entries.
- Task 3: 5 byte-level-hard fixtures (`23_embedded_newline_in_quoted_field` through
  `27_oversized_field_value`) — embedded newline and embedded delimiter inside quoted fields,
  RFC-4180 doubled-quote escaping, a raw embedded NUL byte, and a 10,001-character oversized
  field. Manifest grew to 27 entries — the full corpus except large/compressed (Plan 05's job).
  `make fixtures && make fixtures-verify` run twice in a row: byte-identical both times, and
  independently confirmed via `sha256sum -c tests/fixtures/CORPUS.sha256` (all 27 fixtures `OK`).
- Full test suite (`uv run pytest -q`) still passes at 81/81 — no regressions from any of the
  three tasks.

## Task Commits
1. **Task 1: structural fixtures** - `866814e` (feat)
2. **Task 2: type/nullability fixtures** - `8bec721` (feat)
3. **Task 3: byte-level-hard fixtures** - `cf0e283` (feat)

## Files Created/Modified
- `tests/fixtures/corpus.yaml` - extended from 8 to 27 fixture entries across three new categories
- `tests/fixtures/CORPUS.sha256` - extended from 8 to 27 committed SHA-256 digest lines

## Decisions Made
- `12_wrong_column_count_row` uses the `literal` generator kind, not `tabular` as the plan's Task 1
  action text named — `tools/corpus/generators.py`'s `_generate_tabular` builds one renderer per
  header column and applies it to every row uniformly; no `row_spec` kind can vary a single row's
  field count. Followed the plan's own explicitly authorized fallback pattern (stated for Task 3,
  applied here by the same underlying constraint) instead of adding a new capability.
- `27_oversized_field_value` uses `tabular` + the already-existing `repeat` row_spec kind
  (`{char: "x", length: 10001}`), not `literal` as the plan's Task 3 action text named — `repeat`
  already supports an arbitrary-length constant field, which produces the exact 10,001-character
  value cleanly without hand-writing a 10K-character string directly into the YAML manifest. No new
  `GeneratorKind` or `RowSpecKind` was added; this is a cleaner use of an existing one.
- Fixtures 17-22 and 23-25 use `rows: 1` with the `repeat` row_spec kind (a constant string, no
  randomness) for every column whose exact literal value is asserted in `expect:`, rather than
  `rows: 2` with `pick` selecting between a "bad" and "good" candidate. See Deviations below for
  why the initial `pick`-based draft of 17-22 was replaced before committing.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `pick`-based type/nullability fixtures did not reliably contain the invalid
value they were meant to demonstrate**
- **Found during:** Task 2, immediately after the first `make fixtures` run, while inspecting
  generated fixture content before committing.
- **Issue:** The first draft of fixtures 17-22 used `rows: 2` with a `pick` row_spec renderer
  choosing between a "bad" value (e.g. `amount: "100.999"`) and a "good" value (e.g.
  `"250.12"`) for the column under test. `pick`'s renderer selects an index via
  `int(rng.random() * count)` — a per-row draw with no guarantee that a 2-row fixture's two draws
  will cover both candidates. Generated output confirmed the failure directly:
  `18_invalid_decimal_too_many_places` rendered `250.12` (the valid candidate) in *both* rows,
  meaning the fixture never actually contained the over-scale decimal value its `expect:` block
  claimed to encode — the must-have truth "`18_invalid_decimal_too_many_places` encodes an
  amount-shaped value with more decimal places than orders.json's configured scale=2" would have
  been violated had this been committed.
- **Fix:** Rewrote fixtures 17-22 as `rows: 1` with the asserted-value column using the `repeat`
  row_spec kind (`{char: "<the exact literal string>", length: 1}`) — `repeat`'s renderer is a
  pure constant with no `.random()` call, so the exact intended value is guaranteed present on
  every regeneration. Non-asserted columns (e.g. `order_id`, `customer_id`, `name`) kept
  `zero_padded_int`/`repeat` for simplicity; no column in this category still uses `pick`.
  Applied the same discipline preemptively to fixtures 23-25 (byte-level-hard) before generating
  them for the first time, avoiding a repeat of the same bug.
- **Files modified:** `tests/fixtures/corpus.yaml` (fixtures 17-22 rewritten before the Task 2
  commit was made — the buggy draft was never committed).
- **Verification:** Re-ran `make fixtures && make fixtures-verify`; inspected every fixture's
  generated file content directly (`cat tests/fixtures/csv/1[7-9]*` etc.) and confirmed each
  asserted value is now present exactly as `expect:` describes.
- **Commit:** `8bec721` (the corrected fixtures were what was actually committed — no separate
  fix commit was needed since the bug was caught pre-commit).

### None beyond the above

No other deviations. All three tasks otherwise executed exactly per the plan's authored fixture
list, column shapes (drawn from `configs/datasets/customers.json` / `orders.json`), and `expect:`
wording style.

## Issues Encountered
None beyond the one auto-fixed deviation above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
Ready for Plan 05 (large/compressed category — the `wrapper` generator kind for gzip/zip — which
completes the corpus at ~30 fixtures). Phase 3's detection-module tests can now run against a real
27-fixture byte-level corpus spanning dialect/encoding, structural, type/nullability, and
byte-level-hard failure classes, plus the one explicit should-pass case documenting the
nullable=true happy path.

---
*Phase: 02-config-contract-csv-generator*
*Completed: 2026-08-28*

## Self-Check: PASSED

Both modified files (`tests/fixtures/corpus.yaml`, `tests/fixtures/CORPUS.sha256`) confirmed
present on disk. All 3 commits (`866814e`, `8bec721`, `cf0e283`) confirmed in `git log`.
