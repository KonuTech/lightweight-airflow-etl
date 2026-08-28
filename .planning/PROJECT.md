# Lightweight Airflow CSV→Oracle ETL Platform

## What This Is

A small, local Airflow environment that solves one concrete problem: detect, parse, validate,
and bulk-load generated CSV files into Oracle Database Free. A thin Airflow TaskFlow DAG
orchestrates two datasets (`customers` and `orders`) by delegating all parsing/validation/loading
logic to a reusable Python CSV processing engine. It's the deliberately-small sibling of an
existing 95-point production-shaped Airflow platform, built to answer one focused engineering
question: how do you build a clean, efficient, reusable CSV processing engine and use Airflow to
orchestrate it against Oracle — without Kubernetes, MinIO, Vault, CDC, SCD, or a full data-lake
architecture.

## Core Value

A single HTTP request can trigger an Airflow DAG that reads a generated CSV, validates and
bulk-loads its valid rows into Oracle, routes invalid rows (with error metadata) into a separate
table, and reports back a clear processing summary — end to end, reproducibly, from a fresh
`git clone`.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Airflow TaskFlow DAG (config → file-wait → process → load-results → report) triggerable via
      Airflow's own REST API, passing dataset + config path as runtime conf
- [ ] File-availability wait implemented as a Deferrable Operator/Trigger (async, non-blocking)
- [ ] Config-driven CSV processing: `config.json` per dataset defines file pattern, CSV dialect,
      schema (types/nullability/date-format), and Oracle target/invalid tables
- [ ] Reusable `csv_processor` Python package: read → parse → validate structure → validate types
      → normalize → split valid/invalid, exposed as `processor.process(file_path, config)`
- [ ] Chunked/bulk processing throughout (no per-row DB round-trips); configurable chunk size
- [ ] Oracle bulk loading via `python-oracledb` `executemany()` with array binding
- [ ] Two Oracle target tables per dataset: `<DATASET>_VALID` and `<DATASET>_INVALID` (invalid
      rows carry original data + error_code/error_message/source_file/row_number)
- [ ] Structural/type/nullability validation only (no referential/uniqueness/volume-anomaly/
      completeness/circuit-breaker validation — explicitly out of scope)
- [ ] Two datasets end-to-end: `customers` and `orders`, mirroring the reference repo's real
      `csv_ingest_customers.py` / `csv_ingest_orders.py` DAGs and `configs/datasets/*.yaml` shapes
      (orders' `customer_id` FK to customers is NOT enforced here — see Out of Scope)
- [ ] Deterministic CSV generator producing both valid and invalid rows per dataset schema
      (strings, integers, decimals, dates/timestamps, nullable fields)
- [ ] Idempotency: filename + file checksum + dataset identifies a processed file; retrying an
      Airflow task or re-encountering the same file does not duplicate data
- [ ] Minimal ingestion metadata table in Oracle (file_name, checksum, dataset, timestamp,
      total/valid/invalid row counts, status)
- [ ] Structured `ProcessingResult` (total/valid/invalid rows, duration) returned to Airflow, with
      distinct status semantics (SUCCESS / SUCCESS_WITH_INVALID_ROWS / FILE_NOT_FOUND /
      INVALID_FILE / CONFIGURATION_ERROR / DATABASE_ERROR / PROCESSING_ERROR)
- [ ] `config.json` itself validated (Pydantic v2, once per run) before CSV processing begins
- [ ] docker-compose provisioning: Airflow (LocalExecutor) + Airflow metadata DB + Oracle Database
      Free (pinned tag), with documented CPU/RAM/disk allocation
- [ ] Everything run from WSL (Linux filesystem, not `/mnt/c/...`); Docker Desktop as the host
- [ ] Tests: unit (config/parsing/type-conversion/validation), Oracle integration (real container,
      not mocked), one end-to-end test (HTTP → DAG → CSV → Oracle → VALID/INVALID tables)
- [ ] Performance/benchmark test at ~100K rows comparing row-by-row vs chunked/bulk approach,
      recording rows/sec, peak memory, Oracle load time
- [ ] Minimal GitHub Actions CI: lint, type check, unit tests, build/check
- [ ] Minimal docs: README (clone-to-first-ingest walkthrough) + architecture/configuration/
      csv-engine/oracle/development notes

### Out of Scope

- Kubernetes, kind, MinIO, Vault, CDC, SCD, complex data-lake/lineage architecture, distributed
  processing, complex schema registry, multi-database warehouse architecture — this project is
  deliberately smaller than the reference platform; see spec §3
- Referential integrity, uniqueness, volume-anomaly, completeness, and circuit-breaker validators
  — explicitly excluded per spec §28, even though `orders.customer_id → customers.customer_id` is
  a real relationship in the reference repo's config. Not enforced here.
- CDC/SCD-style historization — both datasets load as plain valid/invalid snapshots, no history
- Production-grade observability stack — logging only, no metrics/tracing platform
- Custom FastAPI wrapper around Airflow's trigger API — deferred; Airflow's own REST API is
  sufficient for HTTP-triggering with runtime conf
- A second full production deployment target — this stays a local WSL/Docker Desktop environment

## Context

**Reference repo (read, do not depend on):** `/home/user/projects/airflow-platform` (also
reachable at `/mnt/c/Users/borow/VSC/projects/airflow-platform` — same content; prefer the
`/home/user/projects/...` path from WSL). It's a prior, production-shaped sibling project solving
a superset of this problem (full CDC/SCD, Kubernetes, MinIO, Vault, Postgres). Its `dataplat`
package must never be imported, added as a dependency, or made a uv workspace member/git
submodule — it pulls in Vault, S3/boto3, and Postgres-`COPY`-specific loading with no place here.

**Two-tier reuse decision (recorded, drives phase implementation):**

- **Tier A — vendor the file, fix 1-2 lines of coupling:**
  `packages/csv-processor/src/csv_processor/detect/{dialect,encoding,header,filename,schema}.py`
  — each imports `from dataplat.errors import <SomeError>`; replace with a local exception class
  of the same name. The detection logic itself (clevercsv dialect sniffing, chardet/
  charset-normalizer encoding detection, header-scoring heuristic) is pure, zero Postgres/S3/
  Vault/K8s coupling — copy the file, fix the import, done.
  `packages/csv-processor/src/csv_processor/compression.py` — same treatment, plus swap its
  S3-backed `dataplat.storage.objectstore.open_text_stream` call for plain `open()`/
  `pathlib.Path.open()`.
- **Tier B — read the algorithm, do not extract the file:**
  `packages/csv-processor/src/csv_processor/source.py` — fully wired into dataplat's `Source`
  protocol/`SchemaRepository`/`RecordChunk` model; not portable as a file. Read it only for the
  *sequence* (detect compression → decode encoding → detect dialect → detect header → stream
  rows) and write a smaller orchestrator following that sequence.
  `packages/dataplat/src/dataplat/normalize/{dates,numeric,unicode,boolean_null}.py` and all of
  `packages/dataplat/src/dataplat/validate/*` — each is a "stage" plugged into dataplat's custom
  streaming pipeline engine (`StreamingStage`/`BarrierStage`, `RejectedRecord`/`StageResult`,
  baked-in observability calls). None of that scaffolding applies here. Read the algorithm
  *inside* each stage (e.g. strict-`strptime` date rejection in `normalize/dates.py`, regex checks
  in `validate/pattern.py`) and reimplement just that logic as a plain function against this
  project's own row model. Only structural/type/nullability validators are in scope.
  `airflow/dags/csv_ingest_customers.py` + `airflow/dags/csv_ingest_orders.py` +
  `airflow/dags/_common/` — the working reference for "thin TaskFlow DAG delegates to a
  processing engine." Skip the KubernetesPodOperator-specific parts (`_common/kpo.py`,
  `_common/tracing_kpo.py`) — this project has no Kubernetes; `process_csv` runs in-process under
  Airflow's LocalExecutor instead.
  `docker/csv-processor/Dockerfile` — optional reference only if this project containerizes its
  processor.
  `configs/datasets/customers.yaml` and `configs/datasets/orders.yaml` — real schema shapes to
  mirror for this project's own `config.json` contracts (adapted: no `quality:`/reconciliation/
  retention blocks — those map to out-of-scope validators here).

**Known technical decisions to resolve early (already pinned, not open questions):**

1. **Oracle driver**: `python-oracledb` (Oracle's actively-maintained driver, successor to
   `cx_Oracle`), thin mode (no separate Oracle Client install needed). Bulk insert via
   `cursor.executemany()` array binding — Oracle has no `COPY` equivalent. Pin an exact version.
2. **Config validation**: Pydantic v2 for `config.json`, validated once per run (not per row —
   construction cost, and it raises instead of collecting, which fights the
   collect-and-continue invalid-row model).
3. **Airflow executor**: LocalExecutor (no Kubernetes here; Celery/Redis unnecessary at this
   scale).
4. **Oracle Database Free image tag**: pin an explicit tag, never `latest`.

**Working preference:** when uncertain about Airflow (or any other library/framework) behavior —
API shape, version-specific semantics, whether a feature exists — verify against current docs via
MCP Context7 rather than answering from training data. Applies broadly: Airflow's REST API/
TaskFlow/Deferrable Triggers, `python-oracledb`, Pydantic v2.

## Constraints

- **Environment**: WSL-first development (Linux filesystem, e.g. `~/projects/lightweight-airflow-etl`,
  not `/mnt/c/...`); Docker Desktop hosts Airflow, Airflow's metadata DB, and Oracle Database Free
  — Why: avoids Windows filesystem I/O overhead and aligns with Linux-based containers (spec §49-50)
- **Dependency isolation**: never import from, depend on, or vendor-as-submodule the reference
  repo's `dataplat` package — Why: it pulls in Vault/S3/boto3/Postgres-`COPY` coupling with no
  place in this project; port logic in by reading and rewriting, never by importing
- **Tech stack**: `python-oracledb` (Oracle driver), Pydantic v2 (config validation), Airflow
  LocalExecutor, TaskFlow API — Why: pinned in CLAUDE.md as resolved technical decisions before
  planning begins
- **Validation scope**: structural + type + nullability only — Why: spec §28 explicitly excludes
  referential/uniqueness/volume-anomaly/completeness/circuit-breaker validation for this
  lightweight version, even where the reference repo has it (e.g. orders→customers FK)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| HTTP trigger via Airflow's own REST API, not a custom FastAPI wrapper | No extra service to build/maintain; Airflow's stable REST API already supports POST /dags/{dag_id}/dagRuns with a conf payload | — Pending |
| Benchmark target ~100K rows | Matches spec's own §32 example; big enough to show bulk-loading wins, small enough to run fast on Oracle Free locally | — Pending |
| Two datasets: customers + orders | Mirrors reference repo's real `csv_ingest_customers.py`/`csv_ingest_orders.py` and `configs/datasets/*.yaml` — proves config-drivenness generalizes without inventing a new domain | — Pending |
| docker-compose is a project deliverable, not pre-provisioned | Spec §22-23, 47-50 and DoD §59.1-4 expect the environment to be stood up from this repo | — Pending |
| Two-tier reuse of reference repo's csv-processor/dataplat (vendor pure detection files; reimplement pipeline-coupled normalize/validate logic) | Verified by reading actual imports — csv-processor's detect/* and compression.py have near-zero dataplat coupling (1-2 lines); dataplat's normalize/validate are fully wired into a custom streaming pipeline that has no place here | — Pending |
| orders.customer_id → customers.customer_id FK not enforced | Referential integrity validation is explicitly out of scope per spec §28, even though the reference repo enforces it | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-08-28 after initialization*
