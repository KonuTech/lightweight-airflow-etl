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

- [ ] **ENGINE-01**: `csv_processor.process(file_path, config)` validates CSV structure (column
      count / missing / unexpected columns) before validating anything else

- [ ] **ENGINE-02**: Engine validates each column's type (integer/decimal/date) per the config
      schema

- [ ] **ENGINE-03**: Engine validates required (non-nullable) fields are non-empty
- [ ] **ENGINE-04**: Engine explicitly converts each valid CSV string field to its configured
      Python/Oracle type (no implicit Oracle casting)

- [ ] **ENGINE-05**: An invalid row does not stop processing of the rest of the file; valid and
      invalid rows are split and both counted

- [ ] **ENGINE-06**: Each invalid row records `error_code`, `error_message`, `source_file`, and
      `row_number` alongside its original field values

- [ ] **ENGINE-07**: CSV reading and validation processes rows in configurable chunks (not one row
      at a time, not the whole file loaded into memory)

- [ ] **ENGINE-08**: `process()` returns a structured `ProcessingResult` (total/valid/invalid rows,
      duration, status) with distinct status codes (SUCCESS / SUCCESS_WITH_INVALID_ROWS /
      FILE_NOT_FOUND / INVALID_FILE / CONFIGURATION_ERROR / DATABASE_ERROR / PROCESSING_ERROR)

- [ ] **ENGINE-09**: `csv_processor` has no Airflow import/dependency and can be unit-tested
      standalone

### Oracle Loading

- [ ] **LOAD-01**: Valid rows are bulk-inserted into the dataset's `<DATASET>_VALID` Oracle table
      using `executemany()` array binding (no per-row INSERT)

- [ ] **LOAD-02**: Invalid rows are bulk-inserted into the dataset's `<DATASET>_INVALID` Oracle
      table with their error metadata, using the same bulk mechanism

- [ ] **LOAD-03**: Each processed file is recorded in a minimal ingestion metadata table
      (file_name, checksum, dataset, timestamp, total/valid/invalid counts, status)

- [ ] **LOAD-04**: Re-processing a file already recorded (same filename + checksum + dataset) does
      not duplicate data — retrying an Airflow task is safe

### Airflow DAG

- [ ] **DAG-01**: An Airflow TaskFlow DAG (`load_config` → `wait_for_file` → `process_csv` →
      `load_results` → `report_result`) orchestrates ingestion for a dataset, calling
      `csv_processor` rather than implementing CSV logic itself

- [ ] **DAG-02**: The DAG can be triggered via a single HTTP request (Airflow's own REST API)
      passing dataset name and config path as runtime conf

- [ ] **DAG-03**: The DAG waits for the expected CSV file using a deferrable operator/trigger
      (non-blocking, releases the Airflow worker slot while waiting)

- [ ] **DAG-04**: After processing, the DAG reports a concise human-readable summary (dataset,
      file, row counts, duration, status)

- [ ] **DAG-05**: The same DAG definition works for both the `customers` and `orders` datasets
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

- [ ] **TEST-01**: Unit tests cover config parsing, CSV parsing, type conversion, date validation,
      valid/invalid row handling, and chunked processing

- [ ] **TEST-02**: Integration tests exercise a real Oracle Database Free container (not mocked)
      and verify actual resulting rows

- [ ] **TEST-03**: An end-to-end test exercises the full path: HTTP request → DAG run → config →
      file detection → `csv_processor` → Oracle VALID/INVALID tables

- [ ] **TEST-04**: A performance benchmark at ~100K rows compares row-by-row vs. chunked/bulk
      processing and records rows/sec, peak memory, and Oracle load time

### CI & Docs

- [ ] **CI-01**: A minimal GitHub Actions pipeline runs lint, type check, and unit tests on every PR
- [ ] **DOC-01**: README and `docs/` let a new developer go from `git clone` to a completed
      HTTP-triggered ingestion with no undocumented manual steps

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
| ENGINE-01 | Phase 3 | Pending |
| ENGINE-02 | Phase 3 | Pending |
| ENGINE-03 | Phase 3 | Pending |
| ENGINE-04 | Phase 3 | Pending |
| ENGINE-05 | Phase 3 | Pending |
| ENGINE-06 | Phase 3 | Pending |
| ENGINE-07 | Phase 3 | Pending |
| ENGINE-08 | Phase 4 | Pending |
| ENGINE-09 | Phase 3 | Pending |
| LOAD-01 | Phase 4 | Pending |
| LOAD-02 | Phase 4 | Pending |
| LOAD-03 | Phase 4 | Pending |
| LOAD-04 | Phase 4 | Pending |
| DAG-01 | Phase 5 | Pending |
| DAG-02 | Phase 5 | Pending |
| DAG-03 | Phase 5 | Pending |
| DAG-04 | Phase 5 | Pending |
| DAG-05 | Phase 5 | Pending |
| INFRA-01 | Phase 1 | Complete |
| INFRA-02 | Phase 1 | Complete |
| INFRA-03 | Phase 1 | Complete |
| TEST-01 | Phase 3 | Pending |
| TEST-02 | Phase 4 | Pending |
| TEST-03 | Phase 6 | Pending |
| TEST-04 | Phase 6 | Pending |
| CI-01 | Phase 6 | Pending |
| DOC-01 | Phase 6 | Pending |

**Coverage:**

- v1 requirements: 30 total
- Mapped to phases: 30 (100%)
- Unmapped: 0

---
*Requirements defined: 2026-08-28*
*Last updated: 2026-08-28 after roadmap adjustment (added INFRA-03 credentials requirement; 100% v1 requirement coverage across 6 phases)*
