---
phase: 03-csv-processing-engine
plan: 01
subsystem: database
tags: [oracle, ddl, migration, config-contract]

# Dependency graph
requires:
  - phase: 01-environment-oracle-foundation
    provides: "CUSTOMERS_INVALID/ORDERS_INVALID base DDL (02_customers.sql/03_orders.sql) and verify_environment.py's verify_tables()/verify_columns() convention"
  - phase: 02-config-contract-csv-generator
    provides: "configs/datasets/customers.json and orders.json's file_pattern field (Pydantic-validated, plain str, no format validator)"
provides:
  - "docker/oracle/init/04_widen_invalid_columns.sql — CUSTOMERS_INVALID/ORDERS_INVALID data columns widened to nullable VARCHAR2 at original size, plus RAW_LINE column"
  - "verify_widened_invalid_columns(cursor, table, data_columns) in scripts/verify_environment.py — reusable nullable-VARCHAR2 assertion via ALL_TAB_COLUMNS"
  - "customers.json/orders.json file_pattern widened to match plain, .gz, and .zip filename variants"
affects: ["phase-4-oracle-bulk-load", "phase-5-dag-wiring"]

# Actuals (#2632)
actuals:
  tokens: 1750
  tasks: 2
  commits: 2

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Oracle ALTER TABLE ... MODIFY omits an explicit NULL clause for columns already nullable (avoids ORA-01451 'column to be modified to NULL cannot be modified to NULL'); only columns transitioning from NOT NULL to nullable carry the explicit NULL keyword"
    - "verify_widened_invalid_columns(cursor, table, data_columns) mirrors verify_columns()'s assert-and-name-the-culprit style, extended to check both data_type and nullable via ALL_TAB_COLUMNS"

key-files:
  created:
    - docker/oracle/init/04_widen_invalid_columns.sql
  modified:
    - scripts/verify_environment.py
    - configs/datasets/customers.json
    - configs/datasets/orders.json

key-decisions:
  - "Omitted explicit NULL keyword in MODIFY for columns already nullable (birth_date, order_date, amount) after the first direct-exec attempt hit ORA-01451 twice; only columns changing from NOT NULL (customer_id, name, country, event_ts, order_id) carry the explicit NULL clause"
  - "raw_line ADD statements were applied once during the first (partially-failing) direct-exec attempt and left in place; the second direct-exec pass applied only the corrected MODIFY statements, since ADD is not idempotent and the columns already existed"

patterns-established:
  - "verify_widened_invalid_columns(cursor, table, data_columns) — nullable-VARCHAR2 assertion via ALL_TAB_COLUMNS, reusable by Phase 4's Oracle integration tests exactly like verify_columns() already is"

requirements-completed: [ENGINE-06]

coverage:
  - id: D1
    description: "CUSTOMERS_INVALID and ORDERS_INVALID have every data column widened to nullable VARCHAR2 at its current declared size, plus a new RAW_LINE column, confirmed via ALL_TAB_COLUMNS against the live container"
    requirement: ENGINE-06
    verification:
      - kind: integration
        ref: "uv run python scripts/verify_environment.py (verify_widened_invalid_columns for CUSTOMERS_INVALID and ORDERS_INVALID)"
        status: pass
      - kind: integration
        ref: "Direct ALL_TAB_COLUMNS query via docker compose exec sqlplus confirming VARCHAR2/Y (nullable) for CUSTOMER_ID, BIRTH_DATE, EVENT_TS, RAW_LINE on CUSTOMERS_INVALID"
        status: pass
    human_judgment: false
  - id: D2
    description: "customers.json and orders.json's file_pattern each match both the plain CSV and the .gz/.zip compressed variant of that dataset's filename"
    requirement: ENGINE-06
    verification:
      - kind: unit
        ref: "fnmatch.fnmatch inline check: customers_20260829.csv / .csv.gz / .csv.zip all match 'customers_*.csv*'"
        status: pass
      - kind: unit
        ref: "tests/unit/test_config_loader.py, tests/unit/test_config_models.py -x -q (52 passed, unaffected by real configs/ file changes)"
        status: pass
    human_judgment: false

# Metrics
duration: 10min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 1: DDL Migration & file_pattern Widening Summary

**Widened CUSTOMERS_INVALID/ORDERS_INVALID data columns to nullable VARCHAR2 with a new RAW_LINE column, and widened both real dataset configs' file_pattern globs to match compressed CSV variants.**

## Performance

- **Duration:** ~10 min
- **Completed:** 2026-08-29
- **Tasks:** 2/2 completed
- **Files modified:** 4 (1 created, 3 modified)

## Accomplishments

- Created `docker/oracle/init/04_widen_invalid_columns.sql`, widening `customers_invalid`'s
  `customer_id`/`name`/`country`/`birth_date`/`event_ts`/`signup_country` and `orders_invalid`'s
  `order_id`/`customer_id`/`order_date`/`amount` to nullable VARCHAR2 at their current declared
  sizes (D-01, D-04), plus a new `raw_line VARCHAR2(4000)` column on both tables (D-06)
- Applied the equivalent `ALTER TABLE` statements directly against the already-running Oracle
  container (D-03 dual delivery, Phase 1's own precedent) so the existing database didn't need a
  volume wipe
- Extended `scripts/verify_environment.py` with `verify_widened_invalid_columns(cursor, table,
  data_columns)`, querying `ALL_TAB_COLUMNS` to assert every data column is `VARCHAR2` and
  `nullable == 'Y'`; wired into `main()` for both `CUSTOMERS_INVALID` and `ORDERS_INVALID`, plus a
  `verify_columns()` call confirming `RAW_LINE` presence on both tables
- Widened `configs/datasets/customers.json`'s and `orders.json`'s `file_pattern` from
  `"{dataset}_*.csv"` to `"{dataset}_*.csv*"` (D-31), so Phase 5's future file-sensor glob already
  matches `.gz`/`.zip` compressed variants without narrowing what it already matched

## Task Commits

Each task was committed atomically:

1. **Task 1: Widen CUSTOMERS_INVALID/ORDERS_INVALID DDL, apply against the live container, verify via Oracle metadata** - `a32440e` (feat)
2. **Task 2: Widen file_pattern to match compressed variants (D-31)** - `34a267f` (feat)

## Files Created/Modified

- `docker/oracle/init/04_widen_invalid_columns.sql` - new migration widening both `_INVALID` tables' data columns to nullable VARCHAR2 plus `raw_line`
- `scripts/verify_environment.py` - added `verify_widened_invalid_columns()`, wired into `main()` alongside two `verify_columns()` calls for `RAW_LINE`
- `configs/datasets/customers.json` - `file_pattern` widened to `"customers_*.csv*"`
- `configs/datasets/orders.json` - `file_pattern` widened to `"orders_*.csv*"`

## Decisions Made

- Oracle's `ALTER TABLE ... MODIFY` raises `ORA-01451` when a column already permits `NULL` and the
  statement explicitly re-specifies `NULL` — discovered live against the running container on the
  first attempt (`birth_date`, then `order_date`, both already-nullable `DATE` columns in the
  original DDL). Fixed by omitting the `NULL` keyword for columns that were already nullable
  (`birth_date`, `order_date`, `amount`), keeping it only for columns transitioning from `NOT NULL`
  (`customer_id`, `name`, `country`, `event_ts`, `order_id`). `signup_country` needed no change at
  all (already nullable VARCHAR2(64)) and was omitted from the `MODIFY` clause entirely.
- The `ADD (raw_line VARCHAR2(4000))` statements succeeded on the first (partially-failing) exec
  attempt, since `ADD` and `MODIFY` are independent statements in the same script. The corrected
  second exec pass applied only the fixed `MODIFY` statements — re-running `ADD` on an already-added
  column would itself fail. The final `docker/oracle/init/04_widen_invalid_columns.sql` file
  reflects the corrected, dependency-safe statement set, matching what was proven live.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `ALTER TABLE ... MODIFY` explicit `NULL` clause on already-nullable columns raised ORA-01451**
- **Found during:** Task 1, first direct-exec attempt against the live container
- **Issue:** `birth_date` (customers_invalid) and `order_date` (orders_invalid) were already nullable
  `DATE` columns in the original Phase 1 DDL (no `NOT NULL` constraint). Oracle rejects an explicit
  `MODIFY (col ... NULL)` on a column that already permits nulls with `ORA-01451`, aborting the
  entire `MODIFY` statement for that table.
- **Fix:** Rewrote both `MODIFY` statements to omit the `NULL` keyword for columns already nullable
  (`birth_date`, `order_date`, `amount`), keeping only the type change for those columns, while
  columns transitioning from `NOT NULL` (`customer_id`, `name`, `country`, `event_ts`, `order_id`)
  kept the explicit `NULL` clause. Confirmed the corrected statements applied cleanly, then verified
  via `ALL_TAB_COLUMNS` that every target column is `VARCHAR2`/`Y` (nullable).
- **Files modified:** `docker/oracle/init/04_widen_invalid_columns.sql`
- **Commit:** `a32440e`

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Necessary correction discovered only by actually running the DDL against the
live container — no scope change, migration still delivers exactly the schema shape the plan
specified.

## Issues Encountered

None beyond the auto-fixed ORA-01451 issue documented above.

## User Setup Required

None - no external service configuration required beyond the already-running docker-compose stack.

## Known Stubs

None.

## Next Phase Readiness

Both `_INVALID` tables' data columns are now nullable VARCHAR2 at their original declared sizes,
with a `RAW_LINE` column present on each, proven via `ALL_TAB_COLUMNS` against the live container —
unblocking Phase 4's future Oracle inserts of invalid rows carrying raw string values. Both real
dataset configs' `file_pattern` values now match plain, `.gz`, and `.zip` filename variants,
unblocking Phase 5's future file-sensor glob matching per D-29's compressed-input requirement. This
plan ran fully independent of the `csv_processor` engine work in Plan 03-02 (no shared files, no
blocking dependency). No blockers for Plan 03-02 or subsequent phases.

---
*Phase: 03-csv-processing-engine*
*Completed: 2026-08-29*

## Self-Check: PASSED
