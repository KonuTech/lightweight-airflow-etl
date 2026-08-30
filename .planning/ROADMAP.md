# Roadmap: Lightweight Airflow CSV→Oracle ETL Platform

## Overview

The build follows a dependency-driven, horizontal-layer order: stand up the environment and
Oracle schema first (Phase 1), lock the config contract and a matching CSV fixture generator
(Phase 2), build and unit-test the Airflow-agnostic CSV processing engine in isolation
(Phase 3), wire it to real Oracle bulk-loading, idempotency, and its public entrypoint
(Phase 4), wrap it in a thin, HTTP-triggerable Airflow DAG with a deferrable file-wait
(Phase 5), and finally prove the whole path end-to-end, benchmark it at scale, and ship CI +
docs (Phase 6). Every phase produces something independently verifiable before the next phase
depends on it — the engine is fully proven before any DAG code exists to wrap it, and the DAG
is fully proven before the end-to-end/benchmark/CI/docs completion gate closes the milestone.

## Phases

**Phase Numbering:**

- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Environment & Oracle Foundation** - docker-compose stands up Airflow (LocalExecutor) + Airflow metadata DB + a pinned Oracle Database Free, with target schema ready, resource footprint documented, and a single dev credential pair used consistently everywhere (completed 2026-08-28)
- [x] **Phase 2: Config Contract & CSV Generator** - Pydantic v2 `config.json` contract per dataset, validated once per run, plus a deterministic CSV fixture generator covering valid and invalid rows (completed 2026-08-29)
- [x] **Phase 3: CSV Processing Engine** - Airflow-agnostic detect/parse/validate/normalize engine that splits valid (type-converted) rows from invalid (error-tagged) rows in bounded-memory chunks (completed 2026-08-29)
- [x] **Phase 4: Oracle Bulk Load, Idempotency & Engine Entrypoint** - `executemany()` bulk loading into `_VALID`/`_INVALID` tables, checksum-based idempotency via an ingestion metadata table, and the `process()` entrypoint with full status semantics (completed 2026-08-29)
- [x] **Phase 5: Airflow DAG Wiring & Deferrable File-Wait** - Thin, HTTP-triggerable TaskFlow DAG (`load_config` → `wait_for_file` → `process_csv` → `load_results` → `report_result`) with a non-blocking deferrable file-wait, identical for both datasets (completed 2026-08-29)
- [x] **Phase 6: End-to-End Verification, Benchmark, CI & Docs** - HTTP-to-Oracle end-to-end proof, a ~100K-row chunked-vs-row-by-row benchmark, minimal CI, and clone-to-ingest documentation (completed 2026-08-30)

## Phase Details

### Phase 1: Environment & Oracle Foundation

**Goal**: A developer can stand up the entire local stack from a fresh `git clone` — Airflow, its metadata DB, and a schema-ready Oracle Database Free instance — with documented resource requirements.
**Depends on**: Nothing (first phase)
**Requirements**: INFRA-01, INFRA-02, INFRA-03
**Success Criteria** (what must be TRUE):

  1. Running `docker-compose up` from a fresh clone brings up Airflow (LocalExecutor), Airflow's metadata DB, and a pinned (non-`latest`) Oracle Database Free image, all healthy and reachable from the host.
  2. Oracle's `<DATASET>_VALID`, `<DATASET>_INVALID`, and `ingestion_metadata` tables exist for both `customers` and `orders` immediately after the stack starts — confirmed by actually querying Oracle's own metadata/dictionary views (e.g. `USER_TABLES`, `ALL_TAB_COLUMNS`), not just by DDL exiting without error.
  3. The repo documents the CPU/RAM/disk allocation the stack actually needs under WSL2/Docker Desktop, matching what running it in practice requires.
  4. A single documented `admin`/`admin` credential pair, sourced from `.env`/docker-compose environment variables (no Vault, no per-service hardcoding), authenticates against both Oracle and the Airflow webserver — the same credential works everywhere it's needed.

**Plans**: 5/5 plans executed (Plan 5 is a UAT gap-closure plan for G-01-1)

Plans:
**Wave 1**

- [x] 01-01-PLAN.md — Tracer: docker-compose stack boots end-to-end (Oracle + Airflow + Postgres), one Oracle table verified via metadata views, admin/admin auth confirmed against both Oracle and Airflow's REST API, package-legitimacy checkpoint, host-side verify_environment.py scaffold

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 01-02-PLAN.md — Full Oracle schema: CUSTOMERS_VALID/INVALID + ORDERS_VALID/INVALID with daily INTERVAL partitioning (D-01/D-02/D-03), verify_environment.py extended to all 5 tables + columns

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 01-03-PLAN.md — Custom Airflow Dockerfile (D-12), docker-compose swapped to build from it, Oracle Connection registered for UI visibility (D-11), csv-processor/dags empty scaffolds (D-16)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 01-04-PLAN.md — Makefile (D-14/D-15), docs/environment.md with real observed resource numbers (INFRA-02), README entry point, full fresh-clone phase-gate verification

**Wave 5** *(gap closure — UAT G-01-1, blocked on Wave 4 completion)*

- [x] 01-05-PLAN.md — Gap closure: real healthchecks on airflow-apiserver/scheduler/dag-processor/triggerer in docker-compose.yml (false-Healthy cold-start race), broadened retry-with-backoff exception handling in verify_environment.py's verify_airflow_auth() (ConnectionResetError/OSError from the response-read phase)

### Phase 2: Config Contract & CSV Generator

**Goal**: A developer can fully describe a dataset's ingestion contract in `config.json` and generate a deterministic CSV fixture that matches it, with malformed configs rejected before any processing starts.
**Depends on**: Nothing new (parallel-safe with Phase 1; blocks Phase 3)
**Requirements**: CONFIG-01, CONFIG-02, GEN-01
**Success Criteria** (what must be TRUE):

  1. A dataset's file pattern, CSV dialect, per-column schema (types/nullability/date format), and Oracle target/invalid table names are all defined in one `config.json`, with working configs provided for both `customers` and `orders`.
  2. Loading a malformed `config.json` (bad type, missing required field, etc.) fails before any CSV processing begins and reports the complete list of validation errors in one pass, not just the first.
  3. Running the generator for a dataset produces a CSV file matching that dataset's config, containing a configurable mix of valid rows (covering every schema type) and invalid rows (wrong type, invalid date, missing required field).

**Plans**: 5/5 plans executed

Plans:
**Wave 1**

- [x] 02-01-PLAN.md — Tracer: config-contract package (models/loader) + business-row generator CLI wired end-to-end for `customers`, plus the project's first pytest infrastructure

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 02-02-PLAN.md — `orders` dataset config + comprehensive Pydantic validation-rule test suite (D-17)
- [x] 02-03-PLAN.md — Fixture-corpus subsystem core (manifest/generators/digests/CLI) + dialect/encoding fixture category (8 fixtures)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 02-04-PLAN.md — Structural, type/nullability, and byte-level-hard fixture categories (19 fixtures)

**Wave 4** *(blocked on Wave 3 completion)*

- [x] 02-05-PLAN.md — Large/compressed fixture category (3 fixtures) + RLIMIT_AS bounded-memory test + `make verify-phase2` phase gate

### Phase 3: CSV Processing Engine

**Goal**: Given a raw CSV file and a dataset config, the engine correctly separates valid, type-converted rows from invalid, error-tagged rows, processing in bounded-memory chunks, with zero Airflow dependency.
**Depends on**: Phase 2 (needs the config contract shape and generated fixtures to validate against)
**Requirements**: ENGINE-01, ENGINE-02, ENGINE-03, ENGINE-04, ENGINE-05, ENGINE-06, ENGINE-07, ENGINE-09, TEST-01
**Success Criteria** (what must be TRUE):

  1. A CSV with structural problems (wrong column count, missing or unexpected columns) is flagged as such before any type or nullability check runs against it.
  2. Processing a mixed valid/invalid fixture yields correctly type-converted valid rows and invalid rows carrying `error_code`/`error_message`/`source_file`/`row_number` alongside their original values — one bad row never halts processing of the rest of the file, and both counts are accurate.
  3. Processing a large CSV file runs in configurable chunks; memory use stays bounded rather than growing with file size, and detection (dialect/encoding/header) runs once per file, not once per chunk.
  4. The `csv_processor` package can be imported and its full test suite run in an environment with no Airflow installed.
  5. The unit test suite covering config parsing, CSV parsing, type conversion, date validation, valid/invalid row handling, and chunked processing passes.

**Plans**: 10/10 plans executed (Plan 6 is a verification gap-closure plan for
CR-01/CR-02; Plan 7 is a code-review gap-closure plan for the CR-02 fix's own sample-boundary
data-loss regression; Plan 8 is a code-review gap-closure plan for a genuinely malformed row silently
dropped at the same sample-tail boundary CR-03/Plan 7 fixed for well-formed rows; Plan 9 is a
code-review gap-closure plan for Plan 8's own residual — a contiguous run of 2+ candidate rows at the
sample boundary, plus a `sample_was_truncated` off-by-one on a file whose exact size equals the sample;
Plan 10 is a verification gap-closure plan (FTR-01) for an unconditional, always-on footer-detection
heuristic that silently dropped a genuinely malformed row at ANY file's true end, unrelated to sample
truncation — closed via a new per-dataset `has_footer` opt-in, default off)

Plans:
**Wave 1**

- [x] 03-01-PLAN.md — Oracle `_INVALID` DDL migration (widen columns, add `raw_line`) + `file_pattern` widening for compressed variants
- [x] 03-02-PLAN.md — Dependencies (clevercsv/charset-normalizer/chardet) + local exception hierarchy + Tier-A detect module vendoring (dialect/encoding/header/filename/schema), proven against corpus fixtures 1-8

**Wave 2** *(blocked on 03-02 completion)*

- [x] 03-03-PLAN.md — Tracer: source.py/validate.py/normalize.py/engine.py wired end-to-end, full type/nullability coverage (fixtures 17-22) and full structural coverage (fixtures 9-16)

**Wave 3** *(blocked on 03-03 completion)*

- [x] 03-04-PLAN.md — Compressed CSV input (magic-byte detection, streaming gzip/zip) wired into source.py + generate_csv.py --compress flag
- [x] 03-05-PLAN.md — Chunk-boundary/row_number/bounded-memory proof, byte_level_hard fixture coverage (23-27), ENGINE-09 no-Airflow-import enforcement, `make verify-phase3`

**Wave 4** *(gap closure — verification CR-01/CR-02, blocked on Wave 3 completion)*

- [x] 03-06-PLAN.md — Gap closure: `ColumnSpec.required` now filters `source.py`'s MISSING_REQUIRED_COLUMN check (CR-01), `detect_header()`'s preamble/footer/repeated-header row indices now consumed by PASS 2's real read (CR-02), plus a small `detect/filename.py` `dataplat`-import cleanup (WR-01)

**Wave 5** *(gap closure — code review G-03-3, blocked on Wave 4 completion)*

- [x] 03-07-PLAN.md — Gap closure: `source.py`'s `_filtered_rows()` re-validates every sample-derived footer/repeated-header candidate exclusion against the real full-file row content before excluding it, closing a silent data-loss regression 03-06's own CR-02 fix introduced on files larger than the 64 KiB detection sample

**Wave 6** *(gap closure — code review CR-04, blocked on Wave 5 completion)*

- [x] 03-08-PLAN.md — Gap closure: `_filtered_rows()`'s footer/repeated-header exclusion is now gated by a new `sample_covered_row_count` (provable sample-byte coverage), checked before 03-07's own content re-validation — closes a genuinely malformed row being silently dropped whenever it coincides with the sample's own arbitrary tail-scan position, without reopening G-03-2 or CR-03

**Wave 7** *(gap closure — code review CR-01/WR-01, blocked on Wave 6 completion)*

- [x] 03-09-PLAN.md — Gap closure: extracts `_uncoverable_tail_indices()` generalizing 03-08's single-index coverage gate to the full contiguous run of sample-derived candidate indices touching the sample boundary (CR-01), plus a `sample_was_truncated` fix reading one byte past `SAMPLE_BYTES` to correctly distinguish true EOF from truncation (WR-01)

**Wave 8** *(gap closure — verification FTR-01, blocked on Wave 7 completion)*

- [x] 03-10-PLAN.md — Gap closure: adds `CsvDialectConfig.has_footer` (default `False`) per-dataset opt-in; `prepare_source()` now forces `footer_row_indices` to empty for a non-opted-in dataset before building `excluded_indices`, so footer-shape exclusion never runs at all for a dataset that never declared one — closes an unconditional (not sample-truncation-related) silent-drop of a genuinely malformed last row, independently reproduced against this project's own generator/customers.json (seed=11: 49/50 rows accounted for pre-fix, 50/50 post-fix)

### Phase 4: Oracle Bulk Load, Idempotency & Engine Entrypoint

**Goal**: Validated rows from the engine land in Oracle via true bulk operations, re-processing an already-recorded file is a safe no-op, and the engine's public entrypoint returns a complete, correctly-classified result.
**Depends on**: Phase 1 (Oracle schema/target tables), Phase 3 (validated, split rows to load)
**Requirements**: LOAD-01, LOAD-02, LOAD-03, LOAD-04, ENGINE-08, TEST-02
**Success Criteria** (what must be TRUE):

  1. Processing a fixture file bulk-inserts its valid rows into `<DATASET>_VALID` and its invalid rows (with error metadata) into `<DATASET>_INVALID` using `executemany()` array binding — verified against a real Oracle Database Free container, not mocks.
  2. Every processed file produces exactly one row in the ingestion metadata table recording file_name, checksum, dataset, timestamp, and total/valid/invalid counts.
  3. Re-processing the same file (same filename + checksum + dataset) a second time does not duplicate rows in either target table.
  4. Calling `csv_processor.process(file_path, config)` returns a `ProcessingResult` carrying the correct status (SUCCESS / SUCCESS_WITH_INVALID_ROWS / FILE_NOT_FOUND / INVALID_FILE / CONFIGURATION_ERROR / DATABASE_ERROR / PROCESSING_ERROR) for each corresponding scenario.

**Plans**: 3/3 plans executed (Plan 3 is a code-review gap-closure plan for CR-01/WR-01 —
`process()` crashed with an unhandled `AttributeError` instead of returning `DATABASE_ERROR` when
`load.get_connection()` itself failed)

Plans:
**Wave 1**

- [x] 04-01-PLAN.md — Tracer: Oracle bulk-load primitives (`csv_processor.load`) + result models (`csv_processor.models`), proven end-to-end against real Oracle; closes the SQL-injection-via-identifier gap in Phase 2's config models (LOAD-01/02/03/04, TEST-02)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 04-02-PLAN.md — Tracer: `csv_processor.process()` entrypoint assembling Plan 04-01's loader with Phase 3's `process_chunks()`, proving all 7 `ProcessingResult` status codes (ENGINE-08, TEST-02)

**Wave 3** *(gap closure — code review CR-01/WR-01, blocked on Wave 2 completion)*

- [x] 04-03-PLAN.md — Gap closure: guards both unguarded `connection.rollback()` call sites (`except oracledb.Error:` and `except StructuralValidationError:`) on `connection is not None`, closing a real `AttributeError` crash when `load.get_connection()` itself fails — `process()` will correctly return `DATABASE_ERROR` instead of crashing (ENGINE-08)

### Phase 5: Airflow DAG Wiring & Deferrable File-Wait

**Goal**: A single, config-driven Airflow DAG orchestrates ingestion for either dataset end-to-end, triggerable over HTTP, waiting for files without occupying a worker slot.
**Depends on**: Phase 4 (needs a complete, tested `process()` entrypoint to call)
**Requirements**: DAG-01, DAG-02, DAG-03, DAG-04, DAG-05
**Success Criteria** (what must be TRUE):

  1. Posting a single HTTP request to Airflow's REST API with a dataset name and config path as runtime conf starts a DAG run that executes `load_config` → `wait_for_file` → `process_csv` → `load_results` → `report_result` in order, calling into `csv_processor` rather than reimplementing its logic.
  2. While waiting for the expected CSV file, the task shows as deferred (triggerer-managed, not occupying a worker slot) in Airflow's UI/API.
  3. A completed run's logs/report show a concise, human-readable summary — dataset, file, row counts, duration, status.
  4. The identical DAG definition runs successfully for both `customers` and `orders` purely by passing different config, with no dataset-specific code branches in the DAG.

**Plans**: 2/2 plans executed

Plans:
**Wave 1**

- [x] 05-01-PLAN.md — Tracer: `csv_ingest` DAG wired end-to-end (`load_config_task` → `route_after_config` → `wait_for_file` → `process_csv_task` → `load_results_task` → `report_result_task`), `_common/paths.py`/`_common/reporting.py` helpers, `docker-compose.yml` infra fixes (`ORACLE_DSN`, `fs_default` connection, `configs/` mount), proven via one live triggered run for `customers`, plus unit tests for the pure-Python helpers

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 05-02-PLAN.md — `scripts/trigger_dag.sh` + `make verify-phase5`, live proof of DAG-03 (deferred file-wait state) and DAG-05 (identical DAG, `orders` dataset) against the running stack, `docs/airflow-dag.md`

### Phase 6: End-to-End Verification, Benchmark, CI & Docs

**Goal**: The complete system is proven correct end-to-end via HTTP trigger, proven measurably faster/leaner at realistic scale than a naive approach, continuously checked on every PR, and documented well enough for a new developer to reproduce unaided.
**Depends on**: Phase 5 (needs the full DAG + HTTP trigger wired)
**Requirements**: TEST-03, TEST-04, CI-01, DOC-01
**Success Criteria** (what must be TRUE):

  1. An automated end-to-end test triggers a DAG run over HTTP and asserts the expected rows land in Oracle's `VALID`/`INVALID` tables for a real fixture file.
  2. A benchmark run at ~100K rows records rows/sec, peak memory, and Oracle load time for both a row-by-row approach and the chunked/bulk approach, and the results demonstrate the chunked/bulk approach's advantage.
  3. Opening a pull request automatically runs lint, type check, and unit tests via GitHub Actions, with pass/fail visible on the PR.
  4. Following only the README and `docs/`, a new developer can go from `git clone` to a completed HTTP-triggered ingestion with no undocumented manual steps.

**Plans**: 5/5 plans executed

Plans:
**Wave 1**

- [x] 06-01-PLAN.md — Tracer: automated HTTP-trigger -> deferred-wake -> Oracle-rows e2e proof (TEST-03), reusable `scripts/dag_polling.py`
- [x] 06-02-PLAN.md — Naive-vs-bulk Oracle write benchmark at ~100K rows (TEST-04), `docs/benchmark.md`
- [x] 06-03-PLAN.md — GitHub Actions CI (`lint-type-unit` + `oracle-e2e`, both PR-required) + whole-repo ruff/mypy (CI-01)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 06-04-PLAN.md — Oracle evidence script + business report + live-regenerated README Executive Summary + CI auto-commit job (TEST-03/DOC-01)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 06-05-PLAN.md — Topic docs (architecture/configuration/csv-engine/oracle/development), README summary+links rewrite, final Makefile targets (DOC-01)

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5 → 6

| Phase | Plans Complete | Status | Completed |
|-------|-----------------|--------|-----------|
| 1. Environment & Oracle Foundation | 5/5 | Complete    | 2026-08-28 |
| 2. Config Contract & CSV Generator | 5/5 | Complete    | 2026-08-29 |
| 3. CSV Processing Engine | 10/10 | Complete    | 2026-08-29 |
| 4. Oracle Bulk Load, Idempotency & Engine Entrypoint | 3/3 | Complete    | 2026-08-29 |
| 5. Airflow DAG Wiring & Deferrable File-Wait | 2/2 | Complete    | 2026-08-29 |
| 6. End-to-End Verification, Benchmark, CI & Docs | 5/5 | Complete    | 2026-08-30 |

### Phase 7: Correlated Customer-Order Business Report

**Goal:** The customers⋈orders business report (D-10, `scripts/verify_evidence.sql`) actually
returns real joined rows on generated fixture data, proving the pipeline delivers a working
business report — not just two independently-loaded tables that happen to share a column name.
**Root cause (found via live evidence-checking after Phase 6, not assumed):** both
`configs/datasets/customers.json` and `orders.json` declare `customer_id` as a generic
`"type": "string"`, and `generator/generate_csv.py` fills string fields independently per dataset
via Faker word-generation — so `customers.customer_id` and `orders.customer_id` are two disjoint
pools of random words with zero overlap. `scripts/verify_evidence.sql`'s JOIN has returned "no rows
selected" every time it has ever been run in this project's history; this was previously logged in
Phase 6's VERIFICATION.md as an accepted, "gracefully handled" limitation, but a JOIN that can
structurally never match isn't a working business report — it just fails silently instead of
loudly.
**Requirements**: DATA-01, DATA-02, GEN-02, DB-01, DB-02, TEST-05, TEST-06, INFRA-04, DAG-06,
BENCH-01, DOC-02 (new REQ IDs added to REQUIREMENTS.md during planning per D-30)
**Depends on:** Phase 6
**Plans:** 1/6 plans executed

Success Criteria (what must be TRUE):

  1. `orders.customer_id` values are drawn from an actual pool of `customers.customer_id` values
     generated for the same fixture run (not independently random) — e.g. generate `customers`
     first, feed its real IDs into the `orders` generator as a sampling pool.

  2. Running `scripts/verify_evidence.sql` (or `make verify-evidence`) against a freshly-ingested
     customers + orders pair returns **at least one real row** in the customers⋈orders report,
     with correct aggregate counts/metrics — verified live, not asserted from code reading alone.

  3. The change is covered by an automated regression test (unit or integration) that fails if the
     ID-correlation is ever silently broken again — never relies on manual re-verification alone.

  4. README's Executive Summary business-report table (D-11) reflects genuine non-empty results
     after this fix, and `docs/oracle.md`/`docs/csv-engine.md` are corrected if they describe
     `customer_id` generation in a way that no longer matches reality.

Plans:
**Wave 1**

- [x] 07-01-PLAN.md — Tracer: correlated ID generation (Zipf-weighted pool sampling, structured
  seed-derived IDs) wired end-to-end to a live customers⋈orders Oracle JOIN returning real rows,
  plus unit coverage of the correlation properties

**Wave 2** *(blocked on 07-01 completion)*

- [ ] 07-02-PLAN.md — `--correlated` CLI mode + staging/atomic-rename write helper; `make generate`
  becomes one combined invocation; CLI-level orders-standalone test remediation

- [ ] 07-03-PLAN.md — New `report_ready` DAG: custom deferrable `OraclePartitionReadyTrigger`
  (Oracle provider ships no sensor of its own), proven live to defer until both datasets have
  ingested today's partition

**Wave 3** *(gap-free DDL wave — blocked on 07-02 and 07-03 completion, to avoid a `make reset`
race against either plan's own live-stack verification)*

- [ ] 07-04-PLAN.md — PK/index/trigger DDL on `customers_valid`/`orders_valid` only (gated by a
  `checkpoint:decision` — one-way, requires `make reset`), live-Oracle proof of enforcement

**Wave 4** *(blocked on 07-02 and 07-04 completion)*

- [ ] 07-05-PLAN.md — `scripts/regenerate_readme_summary.py` adopts the shared correlated
  generator + staging/rename; live proof against the real Airflow stack with multi-day backdated
  partitions

**Wave 5** *(blocked on 07-04 and 07-05 completion — this phase's own completion gate)*

- [ ] 07-06-PLAN.md — Benchmark re-verification against the post-DDL schema, live README Executive
  Summary regeneration proof, docs correction, `make verify-phase7`

---
*Roadmap created: 2026-08-28*
*Granularity: standard (6 phases) — consolidated from research's 8-phase dependency order by folding the single-requirement "Engine Entrypoint" phase into Phase 4, and the "HTTP Trigger/E2E/Benchmark" phase into a combined completion-gate phase with CI/Docs, per granularity calibration guidance against thin standalone phases.*
