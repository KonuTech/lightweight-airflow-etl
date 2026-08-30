---
phase: 02-config-contract-csv-generator
plan: 01
subsystem: config-contract-and-csv-generator
tags: [pydantic-v2, config-validation, faker, csv-generation, tdd, pytest]
requires:
  - phase: 01-environment-oracle-foundation
    provides: "packages/csv-processor/ scaffold (empty package + pyproject.toml), docker/oracle/init/02_customers.sql DDL, Makefile-as-entrypoint convention"
provides:
  - "csv_processor.config package: ColumnSpec/CsvDialectConfig/OracleTargetSpec/ProcessingConfig/DatasetConfig (frozen, extra=forbid Pydantic v2 models) + load_config() merge/validate/re-raise loader + ConfigurationError"
  - "configs/defaults.json + configs/datasets/customers.json -- the first real, DDL-verified dataset config"
  - "generator/generate_csv.py -- deterministic Faker + random.Random(seed) CLI producing config-validated CSV fixtures"
  - "Project's first pytest infrastructure (pytest==9.1.1, tool.pytest.ini_options, tests/unit/)"
  - "uv workspace wiring making csv_processor locally importable from the project root"
  - "./data:/opt/airflow/data docker-compose mount + /data//tests/fixtures/csv/ gitignore entries"
  - "make generate target"
affects: [phase-2-plan-02-orders-dataset, phase-2-plans-03-05-fixture-corpus, phase-3-csv-processing-engine, phase-5-dag-wiring]
actuals:
  tokens: 18684
  tasks: 3
  commits: 4
tech-stack:
  added: [pydantic==2.13.4, pytest==9.1.1, Faker==40.37.0]
  patterns:
    - "Frozen, extra=forbid Pydantic v2 models on every config class (ported from dataplat/config/model.py's convention, verified across all 13 of its classes)"
    - "load->merge->validate->re-raise as ConfigurationError, never letting a raw pydantic.ValidationError/OSError/json.JSONDecodeError escape a caller"
    - "Two independent randomness streams in a generator: Faker.seed(seed) for realistic strings, a separate random.Random(seed) for structural/invalid-row decisions -- never interleaved"
    - "applicable_categories() pattern: restrict a fixed enum of behaviors to what a given schema can actually exercise, rather than assuming every category always applies"
    - "Explicit Decimal.scaleb()/f-string formatting for generated numeric values, never str(float)"
key-files:
  created:
    - packages/csv-processor/src/csv_processor/config/__init__.py
    - packages/csv-processor/src/csv_processor/config/models.py
    - packages/csv-processor/src/csv_processor/config/errors.py
    - packages/csv-processor/src/csv_processor/config/loader.py
    - configs/defaults.json
    - configs/datasets/customers.json
    - generator/generate_csv.py
    - tests/unit/test_config_loader.py
    - tests/unit/test_generate_csv.py
  modified:
    - pyproject.toml
    - packages/csv-processor/pyproject.toml
    - uv.lock
    - docker-compose.yml
    - .gitignore
    - Makefile
key-decisions:
  - "Added uv workspace wiring ([tool.uv.workspace]/[tool.uv.sources]) to root pyproject.toml so csv_processor installs editable locally -- Phase 1 only scaffolded the package, never made it importable outside the Airflow container's separate Dockerfile install"
  - "generate_csv.py exposes generate_rows()/write_csv()/applicable_categories()/format_decimal() as separately testable functions rather than one monolithic main(), so determinism/boundary/category tests don't need to shell out to the CLI"
  - "GeneratedCsv (a frozen dataclass) carries a parallel categories list (None for valid rows) alongside rows, so tests can assert on which D-15 category was used without re-parsing written CSV bytes"
  - "Dynamic-import test convention (importlib.util.spec_from_file_location) extended from the project's own test_verify_environment.py precedent to test_generate_csv.py, since generator/ is not an installed package; required registering the module in sys.modules before exec_module to work around a dataclasses forward-ref resolution quirk with postponed annotations"
patterns-established:
  - "Config-contract-then-generator pipeline: load_config() is a generator/CLI's ONLY dependency on csv_processor, never .detect/.validate/.normalize (D-14's zero-coupling constraint) -- this shape is what Plan 02 repeats for orders"
requirements-completed: [CONFIG-01, CONFIG-02, GEN-01]
coverage:
  - id: D1
    description: "Pydantic v2 config-contract model tree (ColumnSpec, CsvDialectConfig, OracleTargetSpec, ProcessingConfig, DatasetConfig) -- frozen, extra=forbid, validates the customers dataset end-to-end"
    requirement: "CONFIG-01"
    verification:
      - kind: unit
        ref: "tests/unit/test_config_loader.py"
        status: pass
    human_judgment: false
  - id: D2
    description: "load_config() merges configs/defaults.json with a per-dataset override and never lets a raw pydantic/OS/JSON exception escape -- always ConfigurationError carrying the complete pydantic.ValidationError.errors() list"
    requirement: "CONFIG-02"
    verification:
      - kind: unit
        ref: "tests/unit/test_config_loader.py::test_load_config_aggregates_multiple_field_errors_in_one_pass"
        status: pass
      - kind: unit
        ref: "tests/unit/test_config_loader.py::test_load_config_missing_file_raises_configuration_error"
        status: pass
      - kind: unit
        ref: "tests/unit/test_config_loader.py::test_load_config_empty_file_raises_configuration_error"
        status: pass
    human_judgment: false
  - id: D3
    description: "generator/generate_csv.py CLI generates a deterministic, config-matching CSV fixture for customers with a configurable ratio of D-15's four invalid-row categories (restricted to what the schema can produce)"
    requirement: "GEN-01"
    verification:
      - kind: unit
        ref: "tests/unit/test_generate_csv.py"
        status: pass
      - kind: manual
        ref: "uv run python generator/generate_csv.py --dataset customers --rows 20 --invalid-ratio 0.25 --seed 7"
        status: pass
    human_judgment: false
  - id: D4
    description: "Faker==40.37.0 package-legitimacy checkpoint (T-02-SC-01) -- verified against pypi.org/project/Faker before uv add"
    requirement: null
    verification:
      - kind: manual
        ref: "Task 2 checkpoint -- user responded 'approved'"
        status: pass
    human_judgment: true
    human_judgment_rationale: "Package legitimacy is inherently a human trust decision (gate=blocking-human); the executor cannot self-certify a third-party PyPI package's supply-chain safety."
duration: ~24min (this session, Task 2 approval through plan close-out; Task 1 alone took ~a prior session's time not separately timed)
completed: 2026-08-28
status: complete
---

# Phase 02 Plan 01: Config Contract + CSV Generator (Customers Tracer) Summary

**A frozen, `extra="forbid"` Pydantic v2 config-contract tree with a `load_config()` merge/validate loader, wired end-to-end to a deterministic Faker + `random.Random(seed)` CLI that writes a real, config-validated CSV fixture for `customers` to `./data/customers/`.**

## Performance

- **Duration:** ~24 minutes (this continuation session: Task 2 approval → Task 3 → plan close-out; commits span `22:45:38` to `23:09:15` local time across the whole plan)
- **Started:** 2026-08-28T22:45:38+02:00 (Task 1's RED commit)
- **Completed:** 2026-08-28T23:09:15+02:00 (Task 3's GREEN commit)
- **Tasks:** 3
- **Files modified:** 15 (9 created, 6 modified)

## Accomplishments

- Built `csv_processor.config`: five frozen, `extra="forbid"` Pydantic v2 model classes (`ColumnSpec`, `CsvDialectConfig`, `OracleTargetSpec`, `ProcessingConfig`, `DatasetConfig`) matching `docker/oracle/init/02_customers.sql`'s real column shape exactly, plus a `load_config()` that shallow-merges `configs/defaults.json` under a per-dataset override and re-raises every failure path (missing file, empty file, malformed JSON, multi-field validation errors, delimiter/decimal-separator collision, unrecognized keys) as one `ConfigurationError` carrying the complete error list.
- Confirmed Faker package legitimacy at the Task 2 checkpoint (PyPI project page, `github.com/joke2k/faker`, active release history) — user responded "approved".
- Built `generator/generate_csv.py`: an argparse CLI (`--dataset`/`--rows`/`--invalid-ratio`/`--seed`) that reads a dataset's validated `DatasetConfig` (its only dependency on `csv_processor`) and writes a deterministic CSV via `csv.writer` using the config's own dialect fields. Two independent randomness streams (`Faker.seed(seed)` for realistic strings, `random.Random(seed)` for structural decisions) keep the output fully reproducible for a fixed seed. `applicable_categories()` restricts D-15's four invalid-row categories to what a dataset's schema can actually produce — `customers` has no integer/decimal/boolean column, so `wrong_type` is correctly never generated for it.
- Wired the rest of the phase's shared infrastructure: `docker-compose.yml`'s new `./data:/opt/airflow/data` mount (D-06), `.gitignore`'s `/data/` and `/tests/fixtures/csv/` entries, and the Makefile's new `generate` target.
- Closed the project's Wave-0 pytest gap: `pytest==9.1.1` installed, `[tool.pytest.ini_options]` added, `tests/unit/` established as the project's first pytest-native test directory (24 tests total across both new files, all passing).

## Task Commits

1. **Task 1 (tracer): Config contract package + loader** - `a50a0f8` (test — RED), `4e58d63` (feat — GREEN)
2. **Task 2 (checkpoint): Confirm Faker package legitimacy** - approved by user ("approved"), no code commit
3. **Task 3 (expansion): Business-row generator CLI** - `84b67f0` (test — RED), `802718e` (feat — GREEN)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/config/models.py` — the five frozen, `extra="forbid"` Pydantic v2 model classes
- `packages/csv-processor/src/csv_processor/config/loader.py` — `load_config()` merge/validate/re-raise
- `packages/csv-processor/src/csv_processor/config/errors.py` — `ConfigurationError`
- `packages/csv-processor/src/csv_processor/config/__init__.py` — package re-exports
- `configs/defaults.json` — shared CSV dialect defaults
- `configs/datasets/customers.json` — the first real, DDL-verified dataset config
- `generator/generate_csv.py` — the business-row generator CLI
- `tests/unit/test_config_loader.py` — 12 tests covering CONFIG-01/CONFIG-02
- `tests/unit/test_generate_csv.py` — 12 tests covering GEN-01
- `pyproject.toml` — added `pydantic`, `faker`, `pytest` (dev), `[tool.pytest.ini_options]`, `[tool.uv.workspace]`/`[tool.uv.sources]`
- `packages/csv-processor/pyproject.toml` — added `pydantic==2.13.4` to `dependencies`
- `docker-compose.yml` — new `./data:/opt/airflow/data` volume mount
- `.gitignore` — `/data/`, `/tests/fixtures/csv/`
- `Makefile` — new `generate` target + `.PHONY` entry
- `uv.lock` — resolved lockfile for the above

## Decisions Made

- **uv workspace wiring (Task 1, deviation):** `csv_processor` was not locally importable from the project root at all — Phase 1 only scaffolded the package. Fixed by adding `[tool.uv.workspace]`/`[tool.uv.sources]` to root `pyproject.toml` and adding `csv-processor` to root `dependencies`, with zero effect on the Airflow container's own separate `pip install --no-deps` line.
- **Faker approved for install (Task 2):** RESEARCH.md's Package Legitimacy Audit override (14+ years of continuous PyPI releases, real `github.com/joke2k/faker` repo) was independently confirmed by the user against the live PyPI project page.
- **`GeneratedCsv` as a testable intermediate representation (Task 3):** `generate_rows()` returns a frozen dataclass (header/rows/categories) rather than writing directly to disk, so determinism/boundary/category-restriction tests operate on in-memory data without round-tripping through file I/O for every assertion. `write_csv()` is the separate, thin file-writing step.
- **`sys.modules` registration for dynamic dataclass import (Task 3, deviation):** loading `generate_csv.py` via `importlib.util.spec_from_file_location` (matching the project's own `test_verify_environment.py` convention for non-package script directories) raised `AttributeError: 'NoneType' object has no attribute '__dict__'` from `dataclasses`' forward-ref resolution, because `GeneratedCsv`'s postponed annotations need `sys.modules[cls.__module__]` to exist. Fixed by registering the module in `sys.modules` before calling `exec_module()`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added uv workspace wiring for csv_processor**
- **Found during:** Task 1
- **Issue:** `csv_processor` package was not locally installable from the root project (Phase 1 only scaffolded it) — this blocked `uv run pytest` from importing `csv_processor.config` at all.
- **Fix:** Added `[tool.uv.workspace]`/`[tool.uv.sources]` to root `pyproject.toml`, added `csv-processor` to root `dependencies`.
- **Files modified:** `pyproject.toml`, `uv.lock`
- **Verification:** `uv run pytest` successfully imports `csv_processor.config`; `uv run python -c "...load_config(...)..."` prints `customers 6 customers_valid`.
- **Committed in:** `a50a0f8`

**2. [Rule 3 - Blocking] sys.modules registration for dynamic-import test loading**
- **Found during:** Task 3, writing `tests/unit/test_generate_csv.py`
- **Issue:** Loading `generator/generate_csv.py` via `importlib.util.spec_from_file_location` (the project's established pattern for non-package script directories, per `test_verify_environment.py`) failed at collection time with `AttributeError: 'NoneType' object has no attribute '__dict__'` — `dataclasses`' postponed-annotation forward-ref resolution looks up `sys.modules[cls.__module__]`, which doesn't exist for a dynamically-loaded module not yet registered.
- **Fix:** Register the module object in `sys.modules["generate_csv"]` immediately before calling `exec_module()`.
- **Files modified:** `tests/unit/test_generate_csv.py`
- **Verification:** `uv run pytest tests/unit/test_generate_csv.py -x` collects and passes all 12 tests.
- **Committed in:** `84b67f0` (test file already carried the fix in its RED commit, since the fix was needed just to get the module to load/fail correctly rather than error at collection)

---

**Total deviations:** 2 auto-fixed (both Rule 3 — blocking issues preventing task completion)
**Impact on plan:** Neither deviation touched any planned behavior, config shape, or CLI surface — both were pure infrastructure/tooling fixes required to make the plan's own already-decided design actually runnable/testable. No architectural changes were needed (no Rule 4 triggers encountered).

## Issues Encountered

None beyond the two auto-fixed deviations above.

## User Setup Required

None - no external service configuration required. The Faker package-legitimacy checkpoint (Task 2) was the only human-input point in this plan, and it has already been resolved ("approved").

## Known Stubs

None. `generate_csv.py` produces real, config-validated CSV output with no hardcoded/placeholder data paths; `customer_id`/string-typed fields use `Faker`'s realistic word/name/country generators rather than empty placeholders.

## Next Phase Readiness

Ready for Plan 02 (extends this same config-contract + generator shape to the `orders` dataset, adds `test_config_models.py`'s D-17 coverage) and Plans 03-05 (the independent fixture-corpus/digest-oracle subsystem, D-16, which Phase 3's detection code will test against). The `make generate` target's `orders` half will become runnable once Plan 02 lands `configs/datasets/orders.json`.

---
*Phase: 02-config-contract-csv-generator*
*Completed: 2026-08-28*

## Self-Check: PASSED

All 9 created files confirmed present on disk; all 5 commits (`a50a0f8`, `4e58d63`, `84b67f0`, `802718e`, `1aac72f`) confirmed present in git history.
