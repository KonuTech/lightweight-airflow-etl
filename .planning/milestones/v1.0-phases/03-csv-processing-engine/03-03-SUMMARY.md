---
phase: 03-csv-processing-engine
plan: 03
subsystem: database
tags: [csv-processing, validation, type-conversion, decimal, strptime, itertools-batched]

# Dependency graph
requires:
  - phase: 03-csv-processing-engine (plan 02)
    provides: "csv_processor.errors's local exception hierarchy + complete error_code vocabulary; csv_processor.detect's vendored detect_dialect/detect_encoding/detect_header (+ decode_strict/to_stdlib_dialect)"
provides:
  - "csv_processor.source.prepare_source(file_path, config) -- detect-once, cross-check against config.json (D-25/D-28), whole-file structural reject (D-16..D-20)"
  - "csv_processor.validate.check_row(row, config)/normalize_row(row, config) -- exhaustive nullability-then-type check with declared-column-order tie-break (D-13/D-14/D-15)"
  - "csv_processor.normalize.convert_value(raw, column) + parse_decimal_strict/parse_date_strict -- per-type string->Python conversion, Decimal.as_tuple() precision/scale check, strict strptime rejection"
  - "csv_processor.engine.process_chunks(file_path, config) -- the D-11 lazy chunked generator, this phase's public surface"
affects: ["03-04", "03-05", "04-oracle-bulk-load"]

# Actuals (#2632)
actuals:
  tokens: 13197
  tasks: 3
  commits: 3

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Two-pass source.prepare_source(): PASS 1 reads one 64 KiB sample and runs detect-once (encoding -> dialect -> header), cross-checking each against config.json; PASS 2 reopens the file fresh with the resolved codec/dialect for the real streaming read -- detection never re-runs per chunk"
    - "A 'detected ascii' encoding result is never flagged as a mismatch against any configured encoding (e.g. utf-8) -- ASCII bytes decode identically under any ASCII-superset codec, so this is never a real conflict, only a genuine non-ASCII-vs-declared disagreement (e.g. detected cp1250) raises DETECT_ENCODING_MISMATCH"
    - "check_row()/normalize_row() share one column-order iteration and one convert_value() dispatch table -- validation and conversion can never diverge on what counts as a valid value for a given column"
    - "engine.process_chunks() is the ONLY place a StructuralValidationError is allowed to propagate; every row-level failure (WRONG_COLUMN_COUNT, NULL_VIOLATION, TYPE_MISMATCH, etc.) becomes an invalid-row dict instead, keeping the generator crash-proof against any single crafted row (T-03-08)"
    - "Fixture-scoped ad hoc DatasetConfig for corpus fixtures whose declared header is a genuine subset/replacement of a real dataset's column set (17/19/20/21/22) -- carries over the real per-column type/nullable/format/precision/scale semantics from customers.json/orders.json rather than routing through the full real config, which would trip a whole-file structural reject before the row-level check under test ever ran (03-RESEARCH.md Pitfall 3's guidance, applied beyond its originally-scoped byte_level_hard category)"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/source.py
    - packages/csv-processor/src/csv_processor/validate.py
    - packages/csv-processor/src/csv_processor/normalize.py
    - packages/csv-processor/src/csv_processor/engine.py
    - tests/unit/test_engine_chunks.py
    - tests/unit/test_normalize.py
    - tests/unit/test_type_validation.py
    - tests/unit/test_structural_validation.py
  modified: []

key-decisions:
  - "A 'detected ascii' encoding never triggers DETECT_ENCODING_MISMATCH regardless of the configured encoding -- a real bug found while running Task 1's own tracer test (plain-ASCII tracer content detected as 'ascii', configured as 'utf-8', both canonically different codec names under codecs.lookup() yet never a genuine conflict). Fixed inline (Rule 1) rather than shipping a source.py that spuriously rejects every ASCII-only fixture/test file."
  - "Task 2's type_nullability fixtures 17/19/20/21/22 use fixture-scoped ad hoc DatasetConfig instances, not the real customers.json/orders.json loaded via load_config() -- each fixture's own declared header (e.g. fixture 17's order_id/customer_id/quantity/amount, where 'quantity' has no equivalent column in either real dataset at all) does not match either real dataset's full declared column set, so routing through the real config would trip a whole-file structural reject (MISSING_REQUIRED_COLUMN/EXTRA_UNEXPECTED_COLUMN) before the fixture's own row-level type/nullability check ever ran. Fixture 18 (order_id/customer_id/order_date/amount) is the one fixture whose header exactly matches orders.json's real columns, and uses the loaded real config directly."

patterns-established:
  - "_LineCapturingTextStream wraps the real-read text stream, remembering the last physical line __next__ yielded, so engine.py can pair each parsed row with its own raw_line (D-06) without csv.reader itself ever exposing the original line text"

requirements-completed: [ENGINE-01, ENGINE-02, ENGINE-03, ENGINE-04, ENGINE-05, ENGINE-06, ENGINE-07, TEST-01]

coverage:
  - id: D1
    description: "process_chunks() correctly detects, structurally validates, nullability/type-checks, normalizes, and splits one valid + one invalid customers row end-to-end, proven via a real pytest assertion (not just 'no exception raised')"
    requirement: ENGINE-05
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_chunks.py::test_one_valid_one_invalid_customers_row_end_to_end"
        status: pass
    human_judgment: false
  - id: D2
    description: "A structurally-broken (wrong field count) row never reaches check_row() -- its error_code is always WRONG_COLUMN_COUNT, never a type/nullability code (D-13 short-circuit)"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_chunks.py::test_structurally_broken_row_never_reaches_check_row"
        status: pass
    human_judgment: false
  - id: D3
    description: "All 6 column types (string/integer/decimal/date/timestamp/boolean) have an explicit convert_value() conversion path, each proven with both a passing and a failing case; Decimal.as_tuple() precision/scale and strict strptime rejection proven at their exact declared boundaries"
    requirement: ENGINE-04
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/test_normalize.py -q (24 passed)"
        status: pass
    human_judgment: false
  - id: D4
    description: "Corpus fixtures 17-22 (type_nullability category) each produce their exact documented error_code (or, for fixture 22, the exact None typed value) when run through process_chunks()"
    requirement: ENGINE-02
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/test_type_validation.py -q (6 passed)"
        status: pass
    human_judgment: false
  - id: D5
    description: "Every header-level whole-file-reject rule (missing/extra/duplicate declared column, no header row, empty file) and the row-level WRONG_COLUMN_COUNT rule are proven against all 8 structural corpus fixtures (9-16), including the header-only/zero-rows case yielding zero chunks (not one empty chunk)"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/test_structural_validation.py -q (8 passed)"
        status: pass
    human_judgment: false
  - id: D6
    description: "csv.field_size_limit is set explicitly to 1_048_576 at source.py's module scope, never left at Python's unstated 131072 default"
    requirement: ENGINE-07
    verification:
      - kind: unit
        ref: "uv run python3 -c \"import csv_processor.source; import csv; print(csv.field_size_limit())\" -> 1048576"
        status: pass
    human_judgment: false
  - id: D7
    description: "No csv_processor module added this plan imports anything from airflow -- source.py/validate.py/normalize.py/engine.py import only stdlib + csv_processor.{detect,errors,config}"
    requirement: TEST-01
    verification:
      - kind: unit
        ref: "grep -rn airflow packages/csv-processor/src/csv_processor/{source,validate,normalize,engine}.py -- zero hits"
        status: pass
    human_judgment: false

# Metrics
duration: 35min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 3: Detect -> Parse -> Validate -> Normalize -> Split Engine Summary

**Wired source.py/validate.py/normalize.py/engine.py end-to-end for the first time, proven against a real customers row and all 14 corpus fixtures (9-22) covering every structural/type/nullability rule this phase's requirements require.**

## Performance

- **Duration:** ~35 min
- **Completed:** 2026-08-29
- **Tasks:** 3/3 completed
- **Files modified:** 8 (all created, 0 modified)

## Accomplishments

- Built `source.py`'s two-pass `prepare_source()`: PASS 1 detects encoding/dialect/header from one 64 KiB sample and cross-checks each against `config.json` (D-25/D-28), raising `StructuralValidationError` on a high-confidence disagreement or a header-level structural problem (missing/extra/duplicate declared column, no header row); PASS 2 reopens the file fresh with the resolved codec/dialect for the real streaming read, pairing each parsed row with its own raw source line via a small line-capturing text-stream wrapper (D-06)
- Built `validate.py`'s `check_row()`/`normalize_row()`: exhaustive nullability-then-type check across every column (never stopping at the first violation), reporting the highest-priority violation (nullability beats type, D-14) with a deterministic declared-column-order tie-break (D-15)
- Built `normalize.py`'s `convert_value()` dispatch table for all 6 column types, plus the two Tier-B derived helpers this phase's own schema needed: `parse_decimal_strict()` (`Decimal.as_tuple()`-based precision/scale check, no reference-repo precedent) and `parse_date_strict()` (strict `strptime` rejection, adapted from `dataplat/normalize/dates.py`)
- Built `engine.py`'s `process_chunks()` generator (D-11): `itertools.batched` chunking, `WRONG_COLUMN_COUNT` row-level short-circuit (D-13), the complete D-09 invalid-row shape (original values + `error_code`/`error_message`/`source_file`/`row_number`/`raw_line`)
- Proved the complete pipeline end-to-end via Task 1's tracer test (one valid + one invalid `customers` row through detect -> structural -> nullability/type -> normalize -> split), then broadened coverage to all 6 column types (fixtures 17-22) and all 8 structural fixtures (9-16) -- 42 new tests, all passing; full unit suite (147 tests) green with zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: End-to-end "one valid, one invalid customers row" — detect through split** - `b318cce` (feat, tracer)
2. **Task 2: Full type/nullability coverage — integer/decimal/boolean, corpus fixtures 17-22** - `b5e3506` (test)
3. **Task 3: Full structural coverage — header-level and row-level, corpus fixtures 9-16** - `4bb8a03` (test)

_Tasks 2/3 required no production changes beyond Task 1's own inline ASCII-encoding fix (see Deviations) -- `normalize.py`/`validate.py`/`source.py`/`engine.py`'s Task-1 implementation already satisfied every fixture's expected behavior on first run._

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/source.py` - two-pass detect-once + cross-check + header-match orchestrator; `_open_raw_stream`/`SAMPLE_BYTES`/`csv.field_size_limit(1_048_576)`
- `packages/csv-processor/src/csv_processor/validate.py` - `check_row()`/`normalize_row()`, the D-13/D-14/D-15 check-priority/tie-break logic
- `packages/csv-processor/src/csv_processor/normalize.py` - `convert_value()` + `parse_decimal_strict()`/`parse_date_strict()`
- `packages/csv-processor/src/csv_processor/engine.py` - `process_chunks()` generator, D-11's public surface
- `tests/unit/test_engine_chunks.py` - Task 1's tracer + D-13 short-circuit proof
- `tests/unit/test_normalize.py` - all 6 `convert_value` branches + decimal/date boundary tests
- `tests/unit/test_type_validation.py` - corpus fixtures 17-22
- `tests/unit/test_structural_validation.py` - corpus fixtures 9-16

## Decisions Made

- Fixed a real ASCII-vs-UTF-8 false-positive bug inline during Task 1 (see Deviations) rather than hand-crafting non-ASCII test content to route around it
- Task 2's fixtures 17/19/20/21/22 use fixture-scoped ad hoc `DatasetConfig` instances rather than the real `customers.json`/`orders.json` (see Deviations) -- documented in `test_type_validation.py`'s own module docstring so the reasoning survives independently of this Summary

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `source.py`'s encoding cross-check falsely flagged plain-ASCII content as a DETECT_ENCODING_MISMATCH**
- **Found during:** Task 1, running the tracer test for the first time
- **Issue:** The plan's own action text compares `detect_encoding()`'s canonicalized result against `config.json`'s declared encoding via `codecs.lookup(...).name` equality. A tracer CSV file containing only ASCII bytes is correctly detected as `source="detected"`, `encoding="ascii"` -- but `codecs.lookup("ascii").name` (`"ascii"`) never equals `codecs.lookup("utf-8").name` (`"utf-8"`), so the literal plan text as written would raise `StructuralValidationError(DETECT_ENCODING_MISMATCH)` for every plain-ASCII file tested against a `utf-8`-configured dataset -- which is every fixture and every hand-written test in this phase, none of which use non-ASCII bytes deliberately.
- **Fix:** Added one additional condition to the mismatch check: a `detected_name == "ascii"` result is never treated as a mismatch, regardless of the configured encoding -- ASCII bytes decode identically under any ASCII-superset codec (`utf-8`, `latin-1`, `cp1252`, ...), so "detected ascii" is never a real conflict, only a genuine non-ASCII-vs-declared disagreement (e.g. a real `cp1250` detection against a `utf-8`-configured dataset) still raises.
- **Files modified:** `packages/csv-processor/src/csv_processor/source.py`
- **Verification:** `uv run pytest tests/unit/test_engine_chunks.py -x -q` (was failing on the mismatch before the fix, passes after); the same fix is exercised implicitly by every other test file in this plan (all fixtures are plain-ASCII).
- **Committed in:** `b318cce` (Task 1 commit)

**2. [Rule 1 - Plan-ambiguity resolution] Task 2's action text says to test fixtures 17-22 "against the REAL customers.json/orders.json config as appropriate", but 5 of those 6 fixtures' declared headers do not match either real dataset's full column set**
- **Found during:** Task 2, before writing `test_type_validation.py`
- **Issue:** Fixture 17's header is `order_id,customer_id,quantity,amount` -- `quantity` has no equivalent column in either `customers.json` or `orders.json` at all, and `order_date` (a real, required `orders.json` column) is absent. Fixtures 19/20/21/22 each declare only 2-3 of `customers.json`'s 6 real columns. Per this plan's own `source.py` structural check (D-21), a header must match a dataset's FULL declared column-name set (order-independent) to pass structurally -- routing any of these 5 fixtures through the real, fully-loaded `customers.json`/`orders.json` config would raise a whole-file `MISSING_REQUIRED_COLUMN`/`EXTRA_UNEXPECTED_COLUMN` structural reject before the fixture's own row-level type/nullability check (the actual thing under test) ever ran. This is the identical schema-mismatch trap 03-RESEARCH.md's Pitfall 3 documents for the `byte_level_hard` category (fixtures 23-27) -- the same trap independently applies to 5 of these 6 `type_nullability` fixtures too, which Pitfall 3's own text did not call out.
- **Fix:** Built a small, fixture-scoped `DatasetConfig` per affected fixture (17/19/20/21/22), matching that fixture's own declared header exactly, but carrying over the real per-column `type`/`nullable`/`format`/`precision`/`scale` semantics from `customers.json`/`orders.json` for every column name the fixture shares with a real dataset (e.g. fixture 19's `birth_date` column uses the exact `format: "%Y-%m-%d"` `customers.json` declares). Fixture 18 (`order_id,customer_id,order_date,amount`) is the one fixture whose header exactly matches `orders.json`'s real column set, and uses the loaded real config directly, per the plan's own literal instruction.
- **Files modified:** No production files changed -- this is a test-authoring decision, documented in `tests/unit/test_type_validation.py`'s own module docstring.
- **Verification:** `uv run pytest tests/unit/test_type_validation.py -x -q` -- all 6 fixtures pass with their exact documented `error_code` (or, for fixture 22, the exact `None` typed value).
- **Committed in:** `b5e3506` (Task 2 commit)

---

**Total deviations:** 2 auto-fixed (1 real bug fix, 1 plan-ambiguity resolution documented at the point of use)
**Impact on plan:** Deviation 1 is a correctness fix required for the pipeline to work at all against realistic ASCII-only CSV content -- without it, essentially every test in this phase (and every real customers/orders file, which are plain ASCII) would spuriously fail structural validation. Deviation 2 changes only test authorship (which config object is passed to `process_chunks()` in a test), not any production code path or the actual behavior under test -- the exact `error_code` assertions the plan's own `<behavior>` blocks specify are all satisfied unchanged.

## Issues Encountered

None beyond the two deviations documented above.

## User Setup Required

None - no external service configuration required.

## Known Stubs

None. `detect/filename.py`/`detect/schema.py` remain unwired per 03-02's own D-27 parity decision (pre-existing, not introduced by this plan) -- this plan's own four new modules (`source.py`/`validate.py`/`normalize.py`/`engine.py`) are fully wired and exercised end-to-end, with no placeholder/hardcoded-empty data paths.

## Next Phase Readiness

`csv_processor.engine.process_chunks(file_path, config)` is the complete, tested public surface Phase 3's remaining plans build on: 03-04 (compressed input) only needs to replace `source._open_raw_stream()`'s body with magic-byte-sniffed gzip/zip opening -- every other function in `source.py`/`engine.py` keeps calling it by name, so nothing else changes. 03-05 (bounded-memory/chunk-boundary/no-airflow-import proof) has a fully-built `process_chunks()` to test against. Phase 4's future `process()`/`ProcessingResult` wrapper (ENGINE-08) consumes `process_chunks()`'s `(valid_rows, invalid_rows)` chunk generator directly for `executemany()` binding. No blockers for 03-04 or subsequent phases.

## Self-Check: PASSED
