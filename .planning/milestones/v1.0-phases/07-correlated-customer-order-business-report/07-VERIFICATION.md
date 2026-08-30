---
phase: 07-correlated-customer-order-business-report
verified: 2026-08-30T11:15:00Z
status: passed
score: 20/20 must-haves verified
behavior_unverified: 0
overrides_applied: 0
---

# Phase 07: Correlated Customer-Order Business Report Verification Report

**Phase Goal:** The customers⋈orders business report (D-10, `scripts/verify_evidence.sql`) actually
returns real joined rows on generated fixture data, proving the pipeline delivers a working
correlated business dataset, not just isolated valid tables.
**Verified:** 2026-08-30T11:15:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

#### Roadmap Success Criteria

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| SC1 | `orders.customer_id` values are drawn from an actual pool of `customers.customer_id` values generated in the same run | ✓ VERIFIED | `generate_correlated_datasets()` (`generator/generate_csv.py:294-348`) builds `valid_customer_pool` from `customers_generated` rows, then calls `generate_rows(orders_config, ..., customer_id_pool=valid_customer_pool)`; `zipf_weighted_sample()` samples from that pool. Unit tests `test_correlated_orders_customer_id_is_subset_of_valid_customer_pool` and `test_correlated_orders_customer_id_sampling_is_zipf_weighted` pass (`uv run pytest tests/unit/test_generate_csv.py -k correlated` → 5 passed). |
| SC2 | `make verify-evidence` against freshly-ingested customers+orders returns ≥1 real row with correct aggregates, verified live | ✓ VERIFIED | Ran `make verify-evidence` live against the running stack: `customers JOIN orders business report (D-10)` returned **20 rows** with real `REGION`/`ORDER_MONTH`/`ORDER_COUNT`/`TOTAL_AMOUNT`/`AVG_AMOUNT` values (e.g. `Serbia 01-AUG-13 1 ...`). Also confirmed via `tests/e2e/test_correlated_report_e2e.py::test_correlated_customers_orders_join_returns_at_least_one_row`, run live: **passed**. |
| SC3 | An automated regression test fails if ID-correlation is ever silently broken again | ✓ VERIFIED | `tests/unit/test_generate_csv.py` (pool-subset, Zipf-weighting, determinism, empty-pool, structured-ID-format tests) + `tests/e2e/test_correlated_report_e2e.py` (live JOIN ≥1 row) + `tests/integration/test_correlation_constraints.py` (DB-level trigger rejects unknown `customer_id`). All ran live and passed. |
| SC4 | README's Executive Summary business-report table reflects genuine non-empty results; `docs/oracle.md`/`docs/csv-engine.md` corrected if stale | ✓ VERIFIED | `README.md` "Customers x Orders business report (top 10)" table contains real rows (`Belarus 2004-02 1 5432347131.23 ...`), last regenerated `2026-08-30T10:49:16Z`. `docs/csv-engine.md` has zero `customer_id`/Faker mentions; `docs/oracle.md`'s `customer_id` mentions are schema-shape/JOIN-column references only, none describe stale independently-random generation. |

#### Plan-Level Must-Haves (by plan)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Same `--seed` produces byte-identical `CorrelatedDatasets` across runs | ✓ VERIFIED | `test_generate_correlated_datasets_is_deterministic_for_same_seed` passes. |
| 2 | `customer_id`/`order_id` are seed-derived structured IDs (`CUST-`/`ORD-`), never random Faker words | ✓ VERIFIED | `structured_id()`/`seed_component()` (`generator/generate_csv.py:72-83`) used for both IDs in `generate_rows()`; `test_correlated_ids_match_structured_id_format` passes. |
| 3 | Generating orders against an empty valid-customer pool raises immediately | ✓ VERIFIED | `generate_correlated_datasets()` raises `ValueError` before calling `generate_rows()` for orders (line 334-336); `generate_rows()` has a defense-in-depth mirror (line 233-235); `test_generate_correlated_datasets_raises_on_empty_valid_customer_pool` passes. |
| 4 | `make generate` produces both correlated CSVs via one combined invocation | ✓ VERIFIED | `Makefile:23-24` — `generate:` target is a single `generator/generate_csv.py --correlated` invocation. |
| 5 | A generated CSV is staged then atomically renamed into its watched directory, never written directly | ✓ VERIFIED | `write_staged()` (`generator/generate_csv.py:412-445`) writes to `staging_path()` then `Path.rename()`s into `output_path()`. Used by both `--correlated` and single-dataset CLI paths, and by `scripts/regenerate_readme_summary.py` (`grep -n write_staged` → only call site, no `write_csv` remains in that script). |
| 6 | `--dataset orders` alone (no `--correlated`) is rejected | ✓ VERIFIED | `main()` (`generator/generate_csv.py:484-488`) calls `parser.error(...)` for `args.dataset == "orders"`; `test_bare_dataset_orders_cli_is_rejected` passes. |
| 7 | `report_ready` DAG senses both `customers_valid`/`orders_valid` ingestion for today's real wall-clock partition, deferrable, never occupies a worker slot | ✓ VERIFIED | `OraclePartitionReadyTrigger`/`ReportReadySensor` (`airflow/dags/_common/oracle_partition_trigger.py`), `report_ready.py` wires `sensor >> build_report_task()`. Live e2e `tests/e2e/test_report_ready_dag.py::test_report_ready_dag_defers_then_fires_once_both_datasets_present` run live: **passed**. DAG-processor logs confirm `report_ready.py` parses with 0 import errors continuously. |
| 8 | Sensor determines "today" via real wall-clock `TRUNC(SYSDATE)`, never `logical_date`/`data_interval` | ✓ VERIFIED | `_POLL_QUERY` (`oracle_partition_trigger.py:83-87`) uses `TRUNC(SYSDATE)`, no Airflow context/logical_date reference in the module. `test_poll_query_uses_real_wall_clock_date_never_logical_date_or_data_interval` passes. |
| 9 | `report_ready` DAG runs alongside `scripts/regenerate_readme_summary.py`, not replacing it | ✓ VERIFIED | `regenerate_readme_summary.py` is unmodified in scope/purpose (still generates fixtures + drives README); `report_ready.py` is a wholly separate, additively-wired DAG. Both present and independently invokable. |
| 10 | `customers_valid`/`orders_valid` carry a `PRIMARY KEY` on their own id columns | ✓ VERIFIED | Live query against `ALL_CONSTRAINTS`: `PK_CUSTOMERS_VALID` / `PK_ORDERS_VALID`, both `CONSTRAINT_TYPE='P'`. |
| 11 | `orders_valid` has a plain index on `customer_id` supporting the JOIN workload | ✓ VERIFIED | Live query against `ALL_INDEXES`: `IX_ORDERS_VALID_CUSTOMER_ID` on `ORDERS_VALID`. |
| 12 | An `orders_valid` INSERT with unknown `customer_id` is rejected, whole batch fails | ✓ VERIFIED | `trg_orders_valid_customer_exists` BEFORE INSERT trigger (`docker/oracle/init/05_correlation_constraints.sql:51-62`), `ENABLED` per live `ALL_TRIGGERS` query. `tests/integration/test_correlation_constraints.py::test_orders_valid_insert_with_unknown_customer_id_is_rejected` run live: **passed**. |
| 13 | `customers_invalid`/`orders_invalid` remain completely unconstrained | ✓ VERIFIED | Live queries against `ALL_INDEXES`/`ALL_TRIGGERS` for both `_invalid` tables: 0 rows (no indexes, no triggers). Existing `ALL_CONSTRAINTS` rows are pre-existing `NOT NULL` check constraints (type `C`), no `P`/`U` constraint added. |
| 14 | `scripts/regenerate_readme_summary.py` generates both CSVs via the one shared `generate_correlated_datasets()`, never independently | ✓ VERIFIED | `main()` calls `generate_csv.generate_correlated_datasets()` exactly once (line 360); `_run_ingestion()` (line 158-188) consumes the pre-generated `GeneratedCsv` and writes via `write_staged()`. |
| 15 | Both correlated CSVs staged + atomically renamed before the live `csv_ingest` DAG picks them up | ✓ VERIFIED | `tests/e2e/test_correlated_report_e2e.py::test_correlated_ingestion_via_live_dag_trigger_reports_across_backdated_partitions` waits for `wait_for_file` to reach `deferred` before calling `write_staged()`, run live: **passed**. |
| 16 | Live e2e proof backdates orders rows across multiple partition days, report aggregates correctly across the boundary | ✓ VERIFIED (behavioral) | Same test asserts `len(distinct_months) > 1` over `_BUSINESS_REPORT_SQL` results after backdating; run live: **passed**. |
| 17 | `docs/benchmark.md`'s speedup figure reflects a real re-measurement against the post-DDL schema | ✓ VERIFIED | `docs/benchmark.md` documents a fresh run (`2026-08-30`) explicitly against the schema carrying `customers_valid`'s new PRIMARY KEY, with naive throughput dropped from Phase 6's 4,268.08 to 2,732.81 rows/sec, consistent with new index-maintenance overhead — not a stale copy-paste. |
| 18 | `make verify-phase7` exists, mirrors `verify-phase6`'s combined-gate shape | ✓ VERIFIED | `Makefile:98-103` — `verify-phase7` runs unit → e2e → integration → lint → verify-evidence, extending `verify-phase6`'s shape with the new integration suite. |
| 19 | `customer_id`/`order_id` structured IDs never a random Faker word | ✓ VERIFIED (duplicate of #2, roadmap-adjacent) | See #2. |
| 20 | Correlation logic path: generate customers → extract valid-ID pool → Zipf-weighted sample into orders (GEN-02) | ✓ VERIFIED | Confirmed directly in `generate_correlated_datasets()` source (see SC1 evidence). |

**Score:** 20/20 truths verified (0 present-but-behavior-unverified)

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `generator/generate_csv.py` | `zipf_weighted_sample()`, `seed_component()`, `structured_id()`, `generate_correlated_datasets()`, `CorrelatedDatasets`, extended `generate_rows()`, `staging_path()`, `write_staged()`, `--correlated` CLI | ✓ VERIFIED | All present, substantive (not stubs), wired into `main()`. |
| `tests/e2e/test_correlated_report_e2e.py` | Live Oracle JOIN e2e proof + staging/rename + backdated-partition proof | ✓ VERIFIED | 298 lines, 2 tests, both run live and passed. |
| `tests/unit/test_generate_csv.py` | Correlation property unit coverage | ✓ VERIFIED | 488 lines, includes pool-subset/Zipf/determinism/empty-pool/structured-ID tests, all pass. |
| `Makefile` | `generate` target as single `--correlated` invocation; `verify-phase7` target | ✓ VERIFIED | Both present and correctly shaped. |
| `airflow/dags/_common/oracle_partition_trigger.py` | `OraclePartitionReadyTrigger` + `ReportReadySensor` | ✓ VERIFIED | Present, substantive, unit-tested (4 tests pass), airflow-optional import fallback for local/CI. |
| `airflow/dags/report_ready.py` | `report_ready` DAG (sensor → report task) | ✓ VERIFIED | Present, parses cleanly in live dag-processor (0 import errors, confirmed via container logs), sensor→task wired. |
| `docker/oracle/init/05_correlation_constraints.sql` | PK/index/trigger DDL on `_valid` tables only | ✓ VERIFIED | Present, applied to live Oracle (confirmed via `ALL_CONSTRAINTS`/`ALL_INDEXES`/`ALL_TRIGGERS`), `_invalid` tables untouched. |
| `tests/integration/test_correlation_constraints.py` | Live DB constraint proof | ✓ VERIFIED | 223 lines, 4 tests, all run live and pass. |
| `scripts/regenerate_readme_summary.py` | Adopts `generate_correlated_datasets()` + `write_staged()` | ✓ VERIFIED | Single call site for `generate_correlated_datasets()`, only `write_staged()` used for writes. |
| `docs/benchmark.md` | Re-measured against post-DDL schema | ✓ VERIFIED | Contains fresh Run Metadata/Comparison Table dated 2026-08-30, explicitly notes post-DDL schema. |
| `README.md` | Live-regenerated non-empty business-report table | ✓ VERIFIED | Confirmed non-empty, timestamped 2026-08-30T10:49:16Z, matches live `make verify-evidence` output. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| `generate_correlated_datasets()` | `customers_valid`/`orders_valid` (Oracle) | `csv_processor.engine.process()` in e2e test | ✓ WIRED | `tests/e2e/test_correlated_report_e2e.py` calls `process()` directly against real Oracle; live run passed. |
| `generate_correlated_datasets()` | `zipf_weighted_sample()` | orders row generation | ✓ WIRED | `generate_rows()` calls `zipf_weighted_sample(rng, customer_id_pool, rows)` when a pool is supplied. |
| `Makefile generate` | `generator/generate_csv.py --correlated` | single subprocess invocation | ✓ WIRED | Confirmed via Makefile inspection. |
| `main() --correlated mode` | `generate_correlated_datasets()` | direct function call | ✓ WIRED | `main()` line 461. |
| `report_ready.py` | `OraclePartitionReadyTrigger` | `ReportReadySensor.execute()`'s `self.defer(trigger=...)` | ✓ WIRED | `oracle_partition_trigger.py:132-136`. |
| `OraclePartitionReadyTrigger.run()` | `ingestion_metadata` (Oracle) | `oracledb.connect_async()` polling | ✓ WIRED | Confirmed present; live e2e test observed the deferred-then-fired behavior. |
| `orders_valid` INSERT | `trg_orders_valid_customer_exists` trigger | `BEFORE INSERT FOR EACH ROW` | ✓ WIRED | Live-confirmed via integration test + `ALL_TRIGGERS` (status `ENABLED`). |
| `scripts/regenerate_readme_summary.py main()` | `generate_correlated_datasets()` | one call before per-dataset trigger/wait loop | ✓ WIRED | Confirmed at line 360. |
| new e2e test | `csv_ingest` DAG (live, HTTP-triggered) | `dag_polling.trigger_dag()` → `wait_for_file` deferred → staged rename → `wait_for_dag_run_result` | ✓ WIRED | Test run live, passed. |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Correlated ID sampling / Zipf weighting / determinism / empty-pool guard | `uv run pytest tests/unit/test_generate_csv.py -k "correlated or structured_id or zipf" -q` | 5 passed | ✓ PASS |
| Full unit suite | `uv run pytest tests/unit/ -q` | 224 passed | ✓ PASS |
| Oracle-polling trigger unit behavior (defer/fire/wall-clock-date) | `uv run pytest tests/unit/test_oracle_partition_trigger.py -q` | 4 passed | ✓ PASS |
| DB-level PK/index/trigger enforcement | `uv run pytest tests/integration/test_correlation_constraints.py -q` | 4 passed | ✓ PASS |
| Live customers⋈orders JOIN + backdated-partition report | `uv run pytest tests/e2e/test_correlated_report_e2e.py -q` (live stack) | 2 passed (23.76s) | ✓ PASS |
| `report_ready` DAG defers then fires live | `uv run pytest tests/e2e/test_report_ready_dag.py -q` (live stack) | 1 passed (34.66s) | ✓ PASS |
| `make verify-evidence` reproducible live evidence | `make verify-evidence` | 20 real joined report rows returned | ✓ PASS |
| DB metadata: PK/index/trigger exist, `_invalid` tables untouched | Direct `sqlplus` queries against `ALL_CONSTRAINTS`/`ALL_INDEXES`/`ALL_TRIGGERS` | PK/index/trigger present on `_valid`; 0 rows for `_invalid` indexes/triggers | ✓ PASS |
| `report_ready.py` DAG parses cleanly | `docker compose logs airflow-dag-processor \| grep report_ready` | `report_ready.py` parsed repeatedly, 0 import errors | ✓ PASS |

### Probe Execution

Not applicable — this phase has no `scripts/*/tests/probe-*.sh` convention; verification instead used the project's own `make verify-evidence`/`make verify-phase7` gates and direct pytest runs against the live stack (documented above).

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| DATA-01 | 07-01 | Correlated Zipf-weighted, with-replacement `customer_id` sampling | ✓ SATISFIED | `zipf_weighted_sample()`, unit + e2e tests pass. |
| DATA-02 | 07-01 | Seed-derived structured IDs (`CUST-`/`ORD-`) | ✓ SATISFIED | `structured_id()`/`seed_component()`, unit test passes. |
| GEN-02 | 07-02 | Correlation pipeline (generate customers → extract pool → Zipf-sample into orders), CLI enforcement | ✓ SATISFIED | `generate_correlated_datasets()` + CLI `--correlated`/`--dataset orders` rejection, tests pass. |
| DB-01 | 07-04 | PK/index on `_valid` tables | ✓ SATISFIED | Live `ALL_CONSTRAINTS`/`ALL_INDEXES` confirmation. |
| DB-02 | 07-04 | BEFORE INSERT trigger validates `customer_id` existence | ✓ SATISFIED | Live trigger confirmed `ENABLED`; integration test passes. |
| TEST-05 | 07-01 | Fast unit suite proves correlation properties | ✓ SATISFIED | `tests/unit/test_generate_csv.py` correlation tests pass. |
| TEST-06 | 07-05 | Live e2e test (staging/rename + backdated-partition) | ✓ SATISFIED | `tests/e2e/test_correlated_report_e2e.py` second test passes live. |
| INFRA-04 | 07-02, 07-05 | Staging + atomic-rename write path used everywhere | ✓ SATISFIED | `write_staged()` used by CLI and `regenerate_readme_summary.py`; live-proven. |
| DAG-06 | 07-03 | `report_ready` DAG with custom deferrable Oracle-polling trigger | ✓ SATISFIED | `OraclePartitionReadyTrigger`/`ReportReadySensor`/`report_ready.py`, unit + live e2e tests pass. |
| BENCH-01 | 07-06 | Benchmark re-measured against post-DDL schema | ✓ SATISFIED | `docs/benchmark.md` re-measurement dated 2026-08-30. |
| DOC-02 | 07-06 | README reflects genuine non-empty business-report results | ✓ SATISFIED | Confirmed live and in README. |

No orphaned requirements — all 11 IDs REQUIREMENTS.md maps to Phase 7 (`DATA-01/02, GEN-02, DB-01/02, TEST-05/06, INFRA-04, DAG-06, BENCH-01, DOC-02`) are claimed by a plan and satisfied.

### Anti-Patterns Found

No debt markers (`TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER`) found in any phase-modified file.

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `airflow/dags/_common/oracle_partition_trigger.py` | 111-125 | `OraclePartitionReadyTrigger.run()` has no exception handling around its Oracle polling calls (07-REVIEW.md CR-01) | ⚠️ Warning (does not block phase goal) | A transient Oracle error (container restart blip, momentary connection-limit condition) crashes the deferred trigger and permanently fails the sensor task with no retry/backoff. This is a genuine production-shaped reliability gap in the report-sensing mechanism, but it does not violate any declared must-have truth for this phase — the sensor's happy-path behavior (defer, poll, fire once both datasets are ready) is proven live and passing. Flagged here for visibility; not a blocker to phase completion since none of the phase's truths assert failure-mode resilience. |
| `airflow/dags/report_ready.py` / `scripts/regenerate_readme_summary.py` / `scripts/verify_evidence.sql` / `tests/e2e/test_correlated_report_e2e.py` | multiple | `_BUSINESS_REPORT_SQL` hand-duplicated across four locations with no automated consistency check (07-REVIEW.md WR-04) | ⚠️ Warning | Maintainability risk, not a goal-achievement blocker — all four copies were confirmed consistent at verification time (live evidence matches README matches `verify_evidence.sql` output). |
| `docs/benchmark.md` | 78 | "6,741% improvement" arithmetic is off by ~100 points vs. the correct ~6,641% (07-REVIEW.md WR-06) | ℹ️ Info | Cosmetic documentation error in prose; the underlying ×67.41 figure and its live re-measurement (BENCH-01's actual requirement) are correct. |

Five further review warnings (WR-01 no sensor timeout, WR-02 unchecked trigger event payload, WR-03 duplicated magic number, WR-05 missing `--rows` validation, IN-01 PYTHONPATH scope) are robustness/quality items, none of which affect the phase's declared truths — all are consistent with the full 07-REVIEW.md report and none introduce a debt marker or block the observable phase goal.

### Human Verification Required

None. All must-have truths were verified via live codebase evidence (direct DB metadata queries, live pytest runs against the running stack, live `make verify-evidence`/README output), not code-reading alone.

### Gaps Summary

No gaps. All 4 ROADMAP success criteria and all 20 plan-level must-have truths across the phase's 6 plans are verified against the live, running stack — not just SUMMARY.md claims. `make verify-evidence` independently re-run during this verification confirms the customers⋈orders business report returns 20 real joined rows on freshly-ingested correlated fixture data, and all relevant unit/integration/e2e test suites (224 unit tests, 4 integration tests, 3 e2e tests spanning this phase) pass live. Code review's one critical finding (CR-01, missing error handling in the deferred trigger's poll loop) is a real, documented robustness gap worth a follow-up fix, but does not block this phase's goal — the correlated business report demonstrably works end-to-end today.

---

_Verified: 2026-08-30T11:15:00Z_
_Verifier: Claude (gsd-verifier)_
