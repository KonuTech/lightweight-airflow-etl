---
phase: 07-correlated-customer-order-business-report
plan: 02
subsystem: data-generation
tags: [python, cli, argparse, file-io, atomic-rename, makefile]

# Dependency graph
requires:
  - phase: 07-correlated-customer-order-business-report
    plan: 01
    provides: "generate_correlated_datasets()/CorrelatedDatasets -- the shared correlation function this plan wires into the CLI"
provides:
  - "staging_path()/write_staged(): the one staging+atomic-rename write path every production CSV write now uses"
  - "--correlated CLI mode: generates both customers and orders together via generate_correlated_datasets()"
  - "CLI-level enforcement that --dataset orders alone (no --correlated) is rejected (D-22)"
affects: [07-03-plan, 07-04-plan, 07-05-plan, scripts/regenerate_readme_summary.py, tests/e2e/test_correlated_report_e2e.py]

# Actuals (#2632)
actuals:
  tokens: 2454
  tasks: 2
  commits: 2

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Staging-path-then-atomic-rename (Path.rename() on the same filesystem) as the one write discipline every production CSV writer shares -- never a direct write into a watched directory"
    - "argparse parser.error() as the CLI-level enforcement point for a decision (D-22) that could otherwise only be enforced deep inside a function"

key-files:
  created: []
  modified:
    - generator/generate_csv.py
    - Makefile
    - tests/unit/test_generate_csv.py

key-decisions:
  - "--dataset changed from required=True to optional (default None) -- --correlated mode needs no --dataset at all, and main() validates the 'one of --correlated or --dataset' contract itself via parser.error()"
  - "write_staged() centralizes --compress's gzip-then-remove-plain-file transform (D-32) on the STAGED file, before the rename, rather than keeping a second post-write transform in main() -- one write path for both compressed and uncompressed output"
  - "main()'s single-dataset (--dataset customers) path also routes through write_staged() (not just the new --correlated path) -- D-24's staging discipline was never scoped only to the correlated case"

patterns-established:
  - "write_staged(generated, config, dataset, *, today=None, compress=False) -> Path as the shared write entrypoint later plans (07-05's regenerate_readme_summary.py adoption, the live e2e test) can call directly"

requirements-completed: [GEN-02, INFRA-04]

coverage:
  - id: T1
    description: "make generate produces both correlated CSVs via one combined invocation, never two independent per-dataset calls"
    requirement: "GEN-02"
    verification:
      - kind: manual
        ref: "make generate (live run) -- single `uv run python generator/generate_csv.py --correlated` subprocess call, confirmed via grep -c generate_csv.py Makefile"
        status: pass
    human_judgment: false
  - id: T2
    description: "A generated CSV is written to a staging path and atomically renamed into its watched directory (never written directly into the watched path)"
    requirement: "INFRA-04"
    verification:
      - kind: manual
        ref: "live --correlated run: data/<dataset>/.staging/ left empty after the run (files moved, not copied), confirmed via ls"
        status: pass
    human_judgment: false
  - id: T3
    description: "Running the CLI with --dataset orders alone (no --correlated) is rejected"
    requirement: "GEN-02"
    verification:
      - kind: unit
        ref: "tests/unit/test_generate_csv.py#test_bare_dataset_orders_cli_is_rejected"
        status: pass
      - kind: manual
        ref: "uv run python generator/generate_csv.py --dataset orders exits with status 2"
        status: pass
    human_judgment: false

duration: ~15min
completed: 2026-08-30
status: complete
---

# Phase 7 Plan 2: --correlated CLI Mode + Staging/Atomic-Rename Write Helper Summary

**`generate_csv.py` gains `--correlated` (wiring Plan 07-01's `generate_correlated_datasets()` into the CLI) and a `staging_path()`/`write_staged()` helper that every production write path now shares, making `make generate` a single combined invocation and rejecting standalone `--dataset orders`.**

## Performance

- **Duration:** ~15 min
- **Tasks:** 2
- **Files modified:** 3 (0 created, 3 modified)

## Accomplishments
- `staging_path()` / `write_staged()` added: every production CSV write now stages to `data/<dataset>/.staging/<filename>` then atomically `Path.rename()`s into the watched directory (D-24) -- replaces the old direct `write_csv()` call in `main()`, and folds the `--compress` gzip-then-remove-plain-file transform (D-32) into the staged write, before the rename
- `--correlated` CLI flag added: loads both `customers.json` and `orders.json`, calls `generate_correlated_datasets()`, writes both results via `write_staged()`, prints a combined row-count/path summary
- `--dataset` changed from `required=True` to optional -- `main()` enforces "either `--correlated` or a valid `--dataset`" itself, and rejects bare `--dataset orders` via `parser.error()` (D-22's CLI-level enforcement, exit code 2)
- `Makefile`'s `generate` target collapsed from two independent `generate_csv.py` subprocess invocations to one `--correlated` call (D-21/D-22)
- Two CLI-level tests that exercised standalone `--dataset orders` updated to `--correlated` mode; one new test (`test_bare_dataset_orders_cli_is_rejected`) proves the rejection

## Task Commits

Each task was committed atomically:

1. **Task 1: --correlated CLI mode + staging/atomic-rename write helper** - `a37ecd4` (feat)
2. **Task 2: Update the two CLI-level standalone-orders tests (D-22 remediation)** - `6a4b40b` (test)

## Files Created/Modified
- `generator/generate_csv.py` - Added `staging_path()`, `write_staged()`; `build_parser()` gains `--correlated` and makes `--dataset` optional; `main()` gains a correlated-mode branch, rejects bare `--dataset orders`, and routes the single-dataset path through `write_staged()` too
- `Makefile` - `generate` target body changed to a single `--correlated` invocation; trailing comment now cites D-21/D-22
- `tests/unit/test_generate_csv.py` - `test_cli_run_twice_produces_byte_identical_files_orders` and `test_cli_end_to_end_writes_real_csv_file_orders` now use `--correlated`; new `test_bare_dataset_orders_cli_is_rejected`

## Decisions Made
- `--dataset`'s `required=True` was changed to optional (default `None`) -- not explicitly called out in the plan's action text, but required for `--correlated`-only invocations (`--rows`/`--seed` with no `--dataset` at all) to parse successfully; `main()` calls `parser.error("--dataset is required unless --correlated is set")` if neither is satisfied
- `write_staged()` handles `--compress` internally (staged-file gzip, then rename to the `.gz` final target) rather than keeping a second gzip-then-unlink block in `main()` -- one write path for both compressed and uncompressed output, matching the plan's "every production write path shares one staging+rename discipline" intent
- The single-dataset `--dataset customers` path in `main()` also now writes via `write_staged()` (not just `--correlated`), per the plan's explicit instruction that D-24's staging discipline was never restricted to the correlated case

## Deviations from Plan

None - plan executed as written, with one clarifying addition (making `--dataset` optional) needed to make `--correlated`-only CLI invocations parse, which the plan's action text implied (the verify command runs `--correlated` with no `--dataset` at all) but didn't spell out explicitly. Logged here for traceability rather than as a Rule 1-4 deviation, since it's a direct, unambiguous consequence of the plan's own stated verification command.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `write_staged()` is ready for `scripts/regenerate_readme_summary.py`'s adoption (Plan 07-05) and the live e2e test's fixture setup
- `make generate` is now a single, combined `--correlated` invocation as D-21/D-22 require
- No blockers identified

---
*Phase: 07-correlated-customer-order-business-report*
*Completed: 2026-08-30*

## Self-Check: PASSED

All claimed files exist on disk and both task commit hashes (`a37ecd4`, `6a4b40b`) are present in git history.
