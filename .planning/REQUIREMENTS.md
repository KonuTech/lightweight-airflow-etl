# Requirements: Lightweight Airflow CSV→Oracle ETL Platform

**Defined:** 2026-08-28
**Core Value:** A single HTTP request can trigger an Airflow DAG that reads a generated CSV,
validates and bulk-loads its valid rows into Oracle, routes invalid rows (with error metadata)
into a separate table, and reports back a clear processing summary — end to end, reproducibly,
from a fresh `git clone`.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Configuration

- [x] **CONFIG-01**: Developer can define a dataset's ingestion contract in `config.json` (file
      pattern, CSV dialect, schema with types/nullability/date format, Oracle target/invalid table
      names)

- [x] **CONFIG-02**: System validates `config.json` once per run via Pydantic v2 before any CSV
      processing begins, failing fast with a complete list of errors on a malformed config

### Generator

- [x] **GEN-01**: Developer can generate a deterministic CSV file matching a dataset's config,
      containing a configurable mix of valid rows (all schema types) and invalid rows (wrong type,
      invalid date, missing required field)

### CSV Processing Engine

- [x] **ENGINE-01**: `csv_processor.process(file_path, config)` validates CSV structure (column
      count / missing / unexpected columns) before validating anything else

- [x] **ENGINE-02**: Engine validates each column's type (integer/decimal/date) per the config
      schema

- [x] **ENGINE-03**: Engine validates required (non-nullable) fields are non-empty
- [x] **ENGINE-04**: Engine explicitly converts each valid CSV string field to its configured
      Python/Oracle type (no implicit Oracle casting)

- [x] **ENGINE-05**: An invalid row does not stop processing of the rest of the file; valid and
      invalid rows are split and both counted

- [x] **ENGINE-06**: Each invalid row records `error_code`, `error_message`, `source_file`, and
      `row_number` alongside its original field values

- [x] **ENGINE-07**: CSV reading and validation processes rows in configurable chunks (not one row
      at a time, not the whole file loaded into memory)

- [x] **ENGINE-08**: `process()` returns a structured `ProcessingResult` (total/valid/invalid rows,
      duration, status) with distinct status codes (SUCCESS / SUCCESS_WITH_INVALID_ROWS /
      FILE_NOT_FOUND / INVALID_FILE / CONFIGURATION_ERROR / DATABASE_ERROR / PROCESSING_ERROR)

- [x] **ENGINE-09**: `csv_processor` has no Airflow import/dependency and can be unit-tested
      standalone

### Oracle Loading

- [x] **LOAD-01**: Valid rows are bulk-inserted into the dataset's `<DATASET>_VALID` Oracle table
      using `executemany()` array binding (no per-row INSERT)

- [x] **LOAD-02**: Invalid rows are bulk-inserted into the dataset's `<DATASET>_INVALID` Oracle
      table with their error metadata, using the same bulk mechanism

- [x] **LOAD-03**: Each processed file is recorded in a minimal ingestion metadata table
      (file_name, checksum, dataset, timestamp, total/valid/invalid counts, status)

- [x] **LOAD-04**: Re-processing a file already recorded (same filename + checksum + dataset) does
      not duplicate data — retrying an Airflow task is safe

### Airflow DAG

- [x] **DAG-01**: An Airflow TaskFlow DAG (`load_config` → `wait_for_file` → `process_csv` →
      `load_results` → `report_result`) orchestrates ingestion for a dataset, calling
      `csv_processor` rather than implementing CSV logic itself

- [x] **DAG-02**: The DAG can be triggered via a single HTTP request (Airflow's own REST API)
      passing dataset name and config path as runtime conf

- [x] **DAG-03**: The DAG waits for the expected CSV file using a deferrable operator/trigger
      (non-blocking, releases the Airflow worker slot while waiting)

- [x] **DAG-04**: After processing, the DAG reports a concise human-readable summary (dataset,
      file, row counts, duration, status)

- [x] **DAG-05**: The same DAG definition works for both the `customers` and `orders` datasets
      purely by config, with no dataset-specific code branches

### Infrastructure

- [x] **INFRA-01**: `docker-compose` stands up Airflow (LocalExecutor), Airflow's metadata DB, and
      a pinned Oracle Database Free image tag, runnable from WSL against Docker Desktop

- [x] **INFRA-02**: CPU/RAM/disk resource allocation for the environment is documented
- [x] **INFRA-03**: Oracle and Airflow credentials for local dev are managed consistently through
      one documented credential pair (`admin`/`admin`) via `.env`/docker-compose environment
      variables — not scattered inline or hardcoded differently across configs, connection
      strings, and DAG code

### Testing

- [x] **TEST-01**: Unit tests cover config parsing, CSV parsing, type conversion, date validation,
      valid/invalid row handling, and chunked processing

- [x] **TEST-02**: Integration tests exercise a real Oracle Database Free container (not mocked)
      and verify actual resulting rows

- [x] **TEST-03**: An end-to-end test exercises the full path: HTTP request → DAG run → config →
      file detection → `csv_processor` → Oracle VALID/INVALID tables

- [x] **TEST-04**: A performance benchmark at ~100K rows compares row-by-row vs. chunked/bulk
      processing and records rows/sec, peak memory, and Oracle load time

### CI & Docs

- [x] **CI-01**: A minimal GitHub Actions pipeline runs lint, type check, and unit tests on every PR
- [x] **DOC-01**: README and `docs/` let a new developer go from `git clone` to a completed
      HTTP-triggered ingestion with no undocumented manual steps

### Correlated Business Report (Phase 7)

Added during Phase 7 planning per CONTEXT.md D-30 — the phase grew well beyond the original
"fix a data bug" scope (ID correlation, PK/index/trigger DDL, staging/rename, a new report-sensing
DAG, benchmark re-verification), so it is scoped against its own REQ IDs rather than force-fit
against the already-`Complete` DOC-01/TEST-03 from Phase 6.

- [ ] **DATA-01**: `orders.customer_id` is sampled with replacement, Zipf-weighted (weight ∝
      1/rank), from the pool of `customer_id` values that will land in `customers_valid` for the
      same generation run — never independently random — and the assignment is fully
      deterministic/byte-identical given the same `--seed`; generating orders against an empty
      valid-customer pool raises immediately rather than silently falling back to uncorrelated IDs

- [ ] **DATA-02**: `customer_id`/`order_id` move from a random Faker word to a seed-derived
      structured ID (`CUST-{seed_hash}-{sequence}` / `ORD-{seed_hash}-{sequence}`) so numbering
      never collides across accumulating Oracle runs

- [ ] **GEN-02**: Correlation logic (generate customers -> extract valid-ID pool -> Zipf-weighted
      sample for orders) lives in exactly one shared function used by every call site
      (`make generate`, `scripts/regenerate_readme_summary.py`, the live e2e test's fixture setup)
      — `make generate` becomes a single combined invocation, not two independent per-dataset calls

- [ ] **DB-01**: `customers_valid`/`orders_valid` (never the `_invalid` tables) gain a `PRIMARY KEY`
      on their own id column plus a plain index on `customer_id` for the JOIN workload

- [ ] **DB-02**: An `orders_valid` `BEFORE INSERT` trigger validates the inserted `customer_id`
      exists in `customers_valid` as a DB-level safety net on top of the Python-side correlation —
      a violation fails the whole insert batch, matching Oracle's default `executemany()` behavior

- [ ] **TEST-05**: A fast unit test suite proves the correlation function's properties directly:
      `orders.customer_id` is a subset of the valid-customer pool, the Zipf-weighting is observable,
      and the same seed produces identical output across runs

- [ ] **TEST-06**: A live end-to-end test (its own file, wired into the required, blocking
      `oracle-e2e` CI check) ingests both datasets for real and asserts the customers-JOIN-orders
      report returns at least one row, including rows backdated across multiple partition days

- [ ] **INFRA-04**: A generated CSV is written to a staging path and atomically renamed into its
      watched directory, proven against the real, live Airflow stack (never mocked)

- [ ] **DAG-06**: A new Airflow DAG senses (via a custom Oracle-polling deferrable trigger, since
      the Oracle provider ships no sensor) when both `customers_valid` and `orders_valid` have data
      for the current wall-clock-date partition, then builds/logs the business report — running
      alongside, not replacing, `scripts/regenerate_readme_summary.py`

- [ ] **BENCH-01**: `docs/benchmark.md`'s chunked/bulk-vs-naive speedup figure is re-measured
      against the schema carrying the new trigger/constraint overhead

- [ ] **DOC-02**: README's Executive Summary business-report table reflects genuine non-empty
      results after the correlation fix, and `docs/oracle.md`/`docs/csv-engine.md` are corrected if
      they describe `customer_id` generation in a way that no longer matches reality

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Reliability

- **REL-01**: `batcherrors=True` / `getbatcherrors()` defensive layer on Oracle `executemany()`
  calls — add once the primary validate-then-split path is proven and a real DB-side rejection is
  observed in practice

### Configuration

- **CONFIG-03**: Regex-based file pattern matching (in addition to glob) — only if a real dataset's
  filenames can't be expressed as a glob

- **CONFIG-04**: Simple configurable business-rule checks (min/max, allowed-value sets) — only if a
  concrete dataset needs one

### CI

- **CI-02**: Oracle integration tests running inside CI (not just locally) — defer until local CI
  reliability with a containerized Oracle is proven

### Packaging

- **PKG-01**: Containerized `csv_processor` with its own Dockerfile — only relevant if the engine
  needs to run outside the Airflow worker process

- **PKG-02**: Resource-config JSON formalizing CPU/RAM documentation as data — only if README prose
  proves insufficient in practice

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| CDC (Change Data Capture) | Requires a persistent-cursor/log-following model — a different ingestion model than "detect a dropped file"; pure scope multiplication for a snapshot-based project |
| SCD (Slowly Changing Dimensions) / historization | Needs versioning/effective-dating/reconciliation on top of load — reference repo's superset feature, explicitly excluded |
| Referential integrity, uniqueness, volume-anomaly, completeness, circuit-breaker validation | Explicitly excluded by spec §28 even where a real relationship exists (`orders.customer_id → customers.customer_id`) — document the FK as unenforced, not a gap to quietly close |
| Full data lineage / complex schema registry | Operationally heavier than a 2-dataset fixed-schema pipeline justifies; the minimal ingestion metadata table is enough lineage here |
| Full data-quality framework (e.g. Great Expectations) | Disproportionate tooling weight for pure structural/type/nullability checks on two fixed schemas |
| Kubernetes / kind / KubernetesExecutor / KubernetesPodOperator | No orchestration need at this scale; Airflow LocalExecutor is sufficient |
| MinIO / S3-style object storage | Local filesystem CSV drop is the entire input surface; nothing to abstract over |
| Vault (secrets management) | Local dev credentials don't need dynamic secret leasing |
| Celery/Redis-backed executor | Two datasets on one machine never hits LocalExecutor's ceiling |
| Custom FastAPI wrapper around Airflow's trigger API | Airflow's own REST API already does `POST /dags/{dag_id}/dagRuns` with a conf payload — no capability gain from a wrapper |
| Production-grade observability stack (metrics/tracing platform) | No operational stakeholder depends on this locally; Airflow's own logging + structured Python logging suffices |
| Multi-database warehouse architecture | Single Oracle Database Free instance is the only target |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| CONFIG-01 | Phase 2 | Complete |
| CONFIG-02 | Phase 2 | Complete |
| GEN-01 | Phase 2 | Complete |
| ENGINE-01 | Phase 3 | Complete |
| ENGINE-02 | Phase 3 | Complete |
| ENGINE-03 | Phase 3 | Complete |
| ENGINE-04 | Phase 3 | Complete |
| ENGINE-05 | Phase 3 | Complete |
| ENGINE-06 | Phase 3 | Complete |
| ENGINE-07 | Phase 3 | Complete |
| ENGINE-08 | Phase 4 | Complete |
| ENGINE-09 | Phase 3 | Complete |
| LOAD-01 | Phase 4 | Complete |
| LOAD-02 | Phase 4 | Complete |
| LOAD-03 | Phase 4 | Complete |
| LOAD-04 | Phase 4 | Complete |
| DAG-01 | Phase 5 | Complete |
| DAG-02 | Phase 5 | Complete |
| DAG-03 | Phase 5 | Complete |
| DAG-04 | Phase 5 | Complete |
| DAG-05 | Phase 5 | Complete |
| INFRA-01 | Phase 1 | Complete |
| INFRA-02 | Phase 1 | Complete |
| INFRA-03 | Phase 1 | Complete |
| TEST-01 | Phase 3 | Complete |
| TEST-02 | Phase 4 | Complete |
| TEST-03 | Phase 6 | Complete |
| TEST-04 | Phase 6 | Complete |
| CI-01 | Phase 6 | Complete |
| DOC-01 | Phase 6 | Complete |
| DATA-01 | Phase 7 | Planned |
| DATA-02 | Phase 7 | Planned |
| GEN-02 | Phase 7 | Planned |
| DB-01 | Phase 7 | Planned |
| DB-02 | Phase 7 | Planned |
| TEST-05 | Phase 7 | Planned |
| TEST-06 | Phase 7 | Planned |
| INFRA-04 | Phase 7 | Planned |
| DAG-06 | Phase 7 | Planned |
| BENCH-01 | Phase 7 | Planned |
| DOC-02 | Phase 7 | Planned |

**Coverage:**

- v1 requirements: 41 total
- Mapped to phases: 41 (100%)
- Unmapped: 0

---
*Requirements defined: 2026-08-28*
*Last updated: 2026-08-30 after Phase 7 planning (added DATA-01/02, GEN-02, DB-01/02, TEST-05/06,
INFRA-04, DAG-06, BENCH-01, DOC-02 per CONTEXT.md D-30; 100% v1 requirement coverage across 7
phases)*
