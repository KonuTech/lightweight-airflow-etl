---
phase: 02-config-contract-csv-generator
plan: 02
subsystem: config-contract-and-csv-generator
tags: [pydantic-v2, config-validation, csv-generation, tdd, pytest]
requires:
  - phase: 02-config-contract-csv-generator
    provides: config-contract package, loader, generate_csv.py CLI (Plan 01)
provides:
  - "configs/datasets/orders.json -- the second, DDL-verified dataset config (4 columns matching docker/oracle/init/03_orders.sql:11-17, decimal amount matching NUMBER(12,2))"
  - "Proof that generator/generate_csv.py and csv_processor.config.{models,loader} are dataset-agnostic: both customers and orders validate/generate through the identical, unmodified code path"
  - "tests/unit/test_config_models.py -- 39-test comprehensive Pydantic validation-rule suite (D-17) covering every rule in csv_processor.config.models: types, precision/scale, date/timestamp format requirement, nullable/required independence, CSV dialect extra fields, delimiter/decimal-separator collision, extra=forbid, frozen instances, and a credential-field-name mechanical scan"
affects: [phase-3-csv-processing-engine, phase-4-oracle-bulk-load, phase-5-dag-wiring]
actuals:
  tokens: 5166
  tasks: 2
  commits: 3
tech-stack:
  added: []
  patterns:
    - "Config-model unit tests split one-file-per-concern (D-17): test_config_models.py exercises Pydantic validation rules directly against model classes; test_config_loader.py exercises load_config()'s file-based success/failure/merge behavior -- neither file duplicates the other's assertions"
    - "Test-of-existing-behavior still gets a test(...) commit even with zero production code changes, when the behavior under test was already implemented by a prior plan"
key-files:
  created:
    - configs/datasets/orders.json
    - tests/unit/test_config_models.py
  modified:
    - tests/unit/test_config_loader.py
    - tests/unit/test_generate_csv.py
key-decisions:
  - "Task 1 followed a real RED/GREEN cycle: orders.json was set aside on disk while the orders-specific test assertions were written and run (confirmed ConfigurationError: No such file or directory), then restored for GREEN -- proving the tests would actually fail without the config, not just that they pass once everything exists together"
  - "Task 2 is a single test(...) commit with no paired feat(...) commit: every validation rule under test (types, precision/scale, format requirement, nullable/required, dialect extra fields, delimiter collision, extra=forbid, frozen) already exists in Plan 01's models.py -- all 39 assertions pass immediately, so there is no production code to make green, per the plan's own explicit TDD guidance for this case"
patterns-established: []
requirements-completed: [CONFIG-01, CONFIG-02, GEN-01]
coverage:
  - id: D1
    description: "configs/datasets/orders.json validates through load_config() with dataset=orders, 4 columns, oracle.valid_table/invalid_table=orders_valid/orders_invalid, and amount as decimal(precision=12,scale=2)"
    requirement: "CONFIG-01"
    verification:
      - kind: unit
        ref: "tests/unit/test_config_loader.py::test_load_config_returns_validated_orders_dataset"
        status: pass
    human_judgment: false
  - id: D2
    description: "generate_csv.py --dataset orders produces deterministic, byte-identical CSV output across repeated runs with the same seed, and exercises both wrong_type (amount) and invalid_date (order_date) D-15 categories -- richer coverage than customers alone can produce"
    requirement: "GEN-01"
    verification:
      - kind: unit
        ref: "tests/unit/test_generate_csv.py::test_cli_run_twice_produces_byte_identical_files_orders"
        status: pass
      - kind: unit
        ref: "tests/unit/test_generate_csv.py::test_orders_exercises_wrong_type_and_invalid_date_categories"
        status: pass
      - kind: unit
        ref: "tests/unit/test_generate_csv.py::test_orders_valid_amount_values_have_exactly_two_decimal_places"
        status: pass
      - kind: manual
        ref: "uv run python generator/generate_csv.py --dataset orders --rows 20 --invalid-ratio 0.25 --seed 7"
        status: pass
    human_judgment: false
  - id: D3
    description: "Every Pydantic validation rule declared in csv_processor.config.models has an explicit, passing unit test (D-17) -- 39 tests across 9 test classes in test_config_models.py"
    requirement: "CONFIG-02"
    verification:
      - kind: unit
        ref: "tests/unit/test_config_models.py (39 tests, all classes)"
        status: pass
    human_judgment: false
  - id: D4
    description: "Credential-field-name mechanical scan proves no field name across ColumnSpec/CsvDialectConfig/OracleTargetSpec/ProcessingConfig/DatasetConfig resembles a credential (password/secret/credential/connection/conn_str/dsn), backing T-02-02's privacy prohibition"
    requirement: null
    verification:
      - kind: unit
        ref: "tests/unit/test_config_models.py::TestNoCredentialFieldNames::test_no_model_field_name_resembles_a_credential"
        status: pass
    human_judgment: false
duration: ~2min (commit-to-commit span; excludes research/reading time)
completed: 2026-08-28
status: complete
---

# Phase 02 Plan 02: Orders Dataset + Config Model Validation Suite Summary

**Proved the config-contract + generator pipeline is genuinely dataset-agnostic by wiring `orders` through the identical, unmodified code path Plan 01 built for `customers`, and locked down every one of `csv_processor.config.models`'s Pydantic validation rules with an explicit 39-test suite.**

## Performance

- **Duration:** ~2 minutes (commit-to-commit span: `02736c6` at 23:17:19 to `7fd91a0` at 23:18:57; excludes the research/reading pass at session start)
- **Started:** 2026-08-28T23:17:19+02:00 (Task 1's RED commit)
- **Completed:** 2026-08-28T23:18:57+02:00 (Task 2's test-suite commit)
- **Tasks:** 2
- **Files modified:** 4 (2 created, 2 modified)

## Accomplishments

- Created `configs/datasets/orders.json` (D-04): `dataset: "orders"`, `file_pattern: "orders_*.csv"` (D-07), 4 columns (`order_id`, `customer_id`, `order_date`, `amount`) matching `docker/oracle/init/03_orders.sql:11-17` exactly, excluding the engine-managed `ingested_at` column. `amount` is `decimal(precision=12, scale=2)`, matching Oracle's `NUMBER(12,2)` exactly (D-10). `oracle.valid_table`/`invalid_table` = `orders_valid`/`orders_invalid`; `processing.chunk_size: 5000`.
- Ran a genuine RED/GREEN cycle: wrote the `orders`-specific load/generate/determinism assertions in `test_config_loader.py`/`test_generate_csv.py` while `orders.json` was temporarily set aside, confirmed they failed with `ConfigurationError: ... No such file or directory`, then restored the config file and confirmed all 31 tests in those two files pass (GREEN) — a concrete proof, not an assumption, that `generator/generate_csv.py` and `csv_processor.config.{models,loader}` needed zero code changes to support the second dataset.
- Proved `orders`' richer schema (a decimal column AND a date column) exercises both the `wrong_type` (targeting `amount`) and `invalid_date` (targeting `order_date`) D-15 categories — coverage `customers` alone cannot produce, since it has no decimal/integer/boolean column.
- Proved every valid `amount` value, parsed as `Decimal`, has an exponent of exactly `-2` (never more, never fewer decimal places) across a 200-row sample.
- Built `tests/unit/test_config_models.py` (D-17): 39 tests across 9 test classes, covering every Pydantic validation rule in `csv_processor.config.models` — full valid round-trip, all six `ColumnSpec.type` literals (plus unrecognized-type rejection), decimal precision/scale (valid, `scale > precision`, missing precision/scale individually and together), date/timestamp format requirement, all four `nullable`/`required` boolean combinations independently, each of `escapechar`/`doublequote`/`lineterminator`/`decimal_separator` round-tripping with a non-default value (D-02), the delimiter/decimal-separator collision validator (two distinct shared-char cases plus a differing-char pass case), `extra="forbid"` at the top level and nested in `csv`/`oracle`/`processing`/column blocks, `columns=[]` rejection, `frozen=True` enforcement on all five model classes, and a mechanical credential-field-name scan across every model's `model_fields` (T-02-02 backing evidence).
- Since every rule under test already existed in Plan 01's `models.py`, all 39 assertions passed immediately with zero production code changes — a single `test(...)` commit, per the plan's own explicit guidance for testing already-passing behavior.

## Task Commits

1. **Task 1 (RED): orders-specific failing tests** - `02736c6` (test) — `test_config_loader.py`/`test_generate_csv.py` extended with orders assertions; failed against a missing `configs/datasets/orders.json`
2. **Task 1 (GREEN): orders.json dataset config** - `dbdb628` (feat) — `configs/datasets/orders.json` created; all 31 tests in the two extended files pass
3. **Task 2 (test-of-existing-behavior): config model validation suite** - `7fd91a0` (test) — `tests/unit/test_config_models.py` created, 39 tests, all passing without any production code change

## Files Created/Modified

- `configs/datasets/orders.json` — the second, DDL-verified dataset config (D-04)
- `tests/unit/test_config_loader.py` — added `test_load_config_returns_validated_orders_dataset`
- `tests/unit/test_generate_csv.py` — added 6 orders-specific tests (determinism, byte-identity, header order, category coverage, decimal-scale, CLI end-to-end)
- `tests/unit/test_config_models.py` — new file, 39 tests across 9 classes covering every Pydantic validation rule in `models.py`

## Decisions Made

- **Genuine RED verification (Task 1):** rather than writing `orders.json` and the tests together and trusting they'd fail without it, `orders.json` was physically moved out of the repo tree while the tests were written and run, producing a real `ConfigurationError` failure before being restored for GREEN. This matches the plan's TDD requirement literally rather than performing the ritual without the verification.
- **Single `test(...)` commit for Task 2, no paired `feat(...)`:** every validation rule `test_config_models.py` exercises (types, precision/scale, format requirement, nullable/required, dialect extra fields, delimiter collision, `extra="forbid"`, frozen) was already implemented in Plan 01's `models.py`. There is no new production code to make green — all 39 tests pass on first run. This follows the plan's own explicit carve-out for "tests targeting already-passing existing model behavior."

## Deviations from Plan

None - plan executed exactly as written. No auto-fixes, no blocking issues, no architectural questions arose. `generator/generate_csv.py` and `csv_processor.config.{models,loader}` were not modified, per the plan's own explicit instruction — both proved dataset-agnostic on the first GREEN run.

**Total deviations:** 0
**Impact:** None — the plan's central hypothesis (Plan 01's config/generator pipeline needed zero changes to support a second dataset) held exactly as predicted.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required. No new dependencies were introduced (both tasks used only what Plan 01 already installed: Pydantic, Faker, pytest).

## Known Stubs

None. `orders.json` is a real, DDL-verified config; `test_config_models.py` exercises real model validation with no mocked/stubbed behavior.

## Next Phase Readiness

Ready for Plan 03 (fixture-corpus manifest + digest-oracle subsystem, D-16). `make generate`'s `orders` half is now runnable — both datasets validate and generate through the identical config/generator code path, and every Pydantic validation rule in `models.py` has explicit, passing test coverage per D-17.

---
*Phase: 02-config-contract-csv-generator*
*Completed: 2026-08-28*
