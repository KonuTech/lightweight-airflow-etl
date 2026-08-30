---
phase: 07-correlated-customer-order-business-report
plan: 01
subsystem: data-generation
tags: [python, faker, oracle, python-oracledb, zipf, correlation, csv-generator]

# Dependency graph
requires:
  - phase: 04-oracle-bulk-load
    provides: "csv_processor.engine.process() -- the proven detect->parse->validate->normalize->chunk->load entrypoint this plan's e2e test calls directly against real Oracle"
  - phase: 02-config-driven-generator
    provides: "generator/generate_csv.py's generate_rows()/GeneratedCsv/applicable_categories() base shape, extended (not replaced) by this plan"
provides:
  - "generate_correlated_datasets() / CorrelatedDatasets: a customers/orders pair where orders.customer_id is a real, Zipf-weighted, with-replacement sample from customers' own valid-row pool"
  - "seed_component() / structured_id(): deterministic, seed-derived CUST-.../ORD-... IDs, replacing random Faker words"
  - "generate_rows() keyword-only rng/fake/customer_id_pool extension points, fully backward compatible with every pre-existing 4-positional-arg caller"
  - "tests/e2e/test_correlated_report_e2e.py: first-ever live proof that customers JOIN orders returns real rows"
affects: [07-02-plan, 07-03-plan, generator/generate_csv.py callers, scripts/regenerate_readme_summary.py, benchmark re-verification]

# Actuals (#2632)
actuals:
  tokens: 5240
  tasks: 2
  commits: 2

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Keyword-only optional rng/fake overrides on an existing positional-arg function (PD-1) to enable object-identity RNG continuation across two calls without breaking any existing caller"
    - "id_overrides dict threaded into row-generation helpers, applied BEFORE invalid-category corruption so corruption semantics (e.g. missing_required blanking an ID) stay unchanged"

key-files:
  created:
    - tests/e2e/test_correlated_report_e2e.py
  modified:
    - generator/generate_csv.py
    - tests/unit/test_generate_csv.py

key-decisions:
  - "generate_rows() gains keyword-only rng/fake/customer_id_pool parameters (PD-1/PD-2) -- zero changes to any pre-existing 4-positional-arg call site"
  - "generate_correlated_datasets() constructs ONE fake/rng pair and passes the SAME live objects into both the customers and orders generate_rows() calls -- literal object-identity RNG continuation satisfying D-05"
  - "D-04's empty-pool check lives in generate_correlated_datasets() itself (checked before ever calling generate_rows() for orders), with generate_rows()'s own empty customer_id_pool check as a defense-in-depth mirror"
  - "REQUIREMENTS.md's Phase 7 traceability rows used a non-standard 'Planned' status instead of the requirements mark-complete tool's expected 'Pending' -- fixed the three rows this plan covers (DATA-01/DATA-02/TEST-05) to 'Pending' before marking them Complete"

patterns-established:
  - "Structured, seed-derived IDs via structured_id(prefix, seed, sequence) -- never a random Faker word for any ID column a schema declares"
  - "Zipf-weighted, with-replacement pool sampling via zipf_weighted_sample(rng, pool, k) -- weight ∝ 1/rank"

requirements-completed: [DATA-01, DATA-02, TEST-05]

coverage:
  - id: D1
    description: "orders.customer_id is a real, Zipf-weighted, with-replacement sample from the pool of customer_id values landing in customers_valid, never independently random"
    requirement: "DATA-01"
    verification:
      - kind: unit
        ref: "tests/unit/test_generate_csv.py#test_correlated_orders_customer_id_is_subset_of_valid_customer_pool"
        status: pass
      - kind: unit
        ref: "tests/unit/test_generate_csv.py#test_correlated_orders_customer_id_sampling_is_zipf_weighted"
        status: pass
      - kind: e2e
        ref: "tests/e2e/test_correlated_report_e2e.py#test_correlated_customers_orders_join_returns_at_least_one_row"
        status: pass
    human_judgment: false
  - id: D2
    description: "Same seed produces byte-identical CorrelatedDatasets (customers and orders) across two runs"
    requirement: "DATA-01"
    verification:
      - kind: unit
        ref: "tests/unit/test_generate_csv.py#test_generate_correlated_datasets_is_deterministic_for_same_seed"
        status: pass
    human_judgment: false
  - id: D3
    description: "customer_id/order_id are seed-derived structured IDs (CUST-.../ORD-...), never a random Faker word"
    requirement: "DATA-02"
    verification:
      - kind: unit
        ref: "tests/unit/test_generate_csv.py#test_correlated_ids_match_structured_id_format"
        status: pass
    human_judgment: false
  - id: D4
    description: "Generating orders against an empty valid-customer pool raises immediately instead of silently falling back to independent random IDs"
    requirement: "DATA-01"
    verification:
      - kind: unit
        ref: "tests/unit/test_generate_csv.py#test_generate_correlated_datasets_raises_on_empty_valid_customer_pool"
        status: pass
    human_judgment: false
  - id: D5
    description: "A real, live customers JOIN orders against freshly-ingested correlated data returns at least one row"
    requirement: "TEST-05"
    verification:
      - kind: e2e
        ref: "tests/e2e/test_correlated_report_e2e.py#test_correlated_customers_orders_join_returns_at_least_one_row"
        status: pass
    human_judgment: false

duration: 20min
completed: 2026-08-30
status: complete
---

# Phase 7 Plan 1: Correlated Customer-Order ID Generation Summary

**`generate_correlated_datasets()` makes `orders.customer_id` a real Zipf-weighted, with-replacement sample from the seed-derived `customer_id` pool that lands in `customers_valid`, proven by a live, un-mocked Oracle `customers JOIN orders` returning real rows for the first time in this project's history.**

## Performance

- **Duration:** ~20 min
- **Tasks:** 2
- **Files modified:** 3 (1 created, 2 modified)

## Accomplishments
- `generate_rows()` extended with keyword-only `rng`/`fake`/`customer_id_pool` parameters (PD-1/PD-2), fully backward compatible with every pre-existing 4-positional-arg call site
- `seed_component()`, `structured_id()`, `zipf_weighted_sample()` added: deterministic hash-derived structured IDs and Zipf-weighted (weight ∝ 1/rank) with-replacement pool sampling
- `generate_correlated_datasets()` / `CorrelatedDatasets`: generates customers, extracts the pool of `customer_id` values that will land in `customers_valid`, raises `ValueError` if that pool is empty (D-04), then generates orders sampling `customer_id` from that pool using the SAME live `rng`/`fake` objects (literal RNG-continuation, D-05)
- New `tests/e2e/test_correlated_report_e2e.py` proves, against the real running Oracle+Airflow stack, that a `customers_valid JOIN orders_valid` query (mirrored verbatim from `scripts/verify_evidence.sql`) returns ≥1 row
- 5 new unit tests cover D-01 (pool subset), D-03 (Zipf skew), D-04 (empty-pool error), D-05 (determinism), and D-06/D-08 (structured-ID format)

## Task Commits

Each task was committed atomically:

1. **Task 1: Correlated ID generation wired end-to-end to a real Oracle JOIN** - `07d0bce` (feat)
2. **Task 2: Unit coverage for D-09's correlation properties** - `e239b5b` (test)

_Note: Task 1 is `type="tracer"` — implemented and committed as a single production-quality slice, then its own `<verify>` (the e2e test) was re-run and confirmed passing before Task 2 began (autonomous tracer feedback gate)._

## Files Created/Modified
- `generator/generate_csv.py` - Added `seed_component()`, `structured_id()`, `zipf_weighted_sample()`, `CorrelatedDatasets`, `generate_correlated_datasets()`; extended `generate_rows()`/`_generate_valid_row()`/`_generate_invalid_row()` with `rng`/`fake`/`customer_id_pool`/`overrides` parameters
- `tests/e2e/test_correlated_report_e2e.py` - New file: live Oracle JOIN proof (D-09/D-10), with a local `clean_orders_tables` autouse fixture and the verbatim business-report SQL constant
- `tests/unit/test_generate_csv.py` - Added 5 tests covering the correlation function's pool-subset, Zipf-weighting, determinism, empty-pool-error, and structured-ID-format properties

## Decisions Made
- `generate_rows()`'s new parameters are keyword-only and default to `None`, preserving every existing call site's exact behavior (PD-1/PD-2) — no CLI/other caller needed changes in this plan
- `generate_correlated_datasets()` checks the empty-pool condition itself, before calling `generate_rows()` for orders, rather than relying solely on `generate_rows()`'s own defense-in-depth check
- Unit tests exercising the structured-ID-format and pool-subset properties use `invalid_ratio=0.0` deliberately — a `missing_required` invalid-row category can legitimately blank a pooled/structured ID (unchanged corruption semantics per PD-2), which is an orthogonal property to the one each of those tests targets

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed REQUIREMENTS.md's non-standard "Planned" status blocking `requirements mark-complete`**
- **Found during:** State-update step, after both tasks committed
- **Issue:** The Phase 7 traceability table used `Planned` for DATA-01/DATA-02/TEST-05's Status cell; `gsd-tools query requirements.mark-complete` only transitions rows whose current Status is `Pending` or `Gaps Found`, so all three IDs were reported `not_found` and neither the checkbox nor the row flipped
- **Fix:** Changed the three rows' Status cell from `Planned` to `Pending` in `.planning/REQUIREMENTS.md`, then re-ran `requirements mark-complete DATA-01 DATA-02 TEST-05`, which flipped both surfaces to Complete
- **Files modified:** `.planning/REQUIREMENTS.md`
- **Verification:** `requirements mark-complete` output showed `"updated": true`, all three in `marked_complete`, `write_set_complete: true`
- **Committed in:** part of the final metadata commit (docs)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Tooling-process fix only; no functional code affected. No scope creep.

## Issues Encountered
None beyond the deviation above.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- `generate_correlated_datasets()` is ready to be wired into `generator/generate_csv.py`'s CLI (`--correlated` flag, blocked by D-22) in Plan 07-02
- The live Oracle JOIN now genuinely returns rows for freshly-generated correlated data — Plan 07-02/07-03 (PK/index/trigger DDL, staging/rename, report DAG, benchmark re-verification) can build on top of this proven slice
- No blockers identified

---
*Phase: 07-correlated-customer-order-business-report*
*Completed: 2026-08-30*

## Self-Check: PASSED

All claimed files exist on disk and both task commit hashes (`07d0bce`, `e239b5b`) are present in git history.
