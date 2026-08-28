# Project Research Summary

**Project:** Lightweight Airflow CSV→Oracle ETL Platform
**Domain:** Local, single-node Airflow-orchestrated CSV→database ETL (thin TaskFlow DAG + reusable Airflow-agnostic CSV processing engine, Oracle Database Free target)
**Researched:** 2026-08-28
**Confidence:** MEDIUM-HIGH

## Executive Summary

This is a small, purpose-built ETL platform, not a general data-integration product: one input surface (local-filesystem CSV drops), one target (Oracle Database Free), two datasets (customers, orders), one orchestrator (Airflow, LocalExecutor). Experts building this shape of system converge on the same architecture the research independently confirms: Airflow does orchestration only (HTTP-triggerable, thin TaskFlow DAG — validate config, wait for file, process, load results, report), while all CSV detection/parsing/validation/normalization logic lives in a separate, Airflow-agnostic Python package (`csv_processor`) that is unit-testable with zero Airflow runtime. The stack is largely pinned already (Airflow 3.3.1, `python-oracledb` 4.0.2 thin mode, Pydantic v2 for config-only validation, `clevercsv`/`charset-normalizer` for detection, `gvenzl/oracle-free:23.26.2-faststart`), with MEDIUM-HIGH confidence on core version numbers (verified via live PyPI/Docker Hub queries and official docs) and LOW confidence only on the Oracle image tag's exact behavior (needs a one-time manual boot check).

The recommended approach is config-driven and validation-first: Pydantic v2 validates `config.json` once per run (never per row — that would fight the "collect every invalid row and keep going" requirement); structural → type → nullability validation runs in that strict order on CSV rows; valid and invalid rows are split with rich error metadata and bulk-loaded into `<DATASET>_VALID`/`<DATASET>_INVALID` tables via `python-oracledb`'s `executemany()` (Oracle has no `COPY`, so array-bind bulk insert is the only real lever). A minimal ingestion-metadata table (file/checksum/dataset/status) is the load-bearing dependency for idempotency and doubles as "lineage" at the scale this project needs. The file-wait step should default to Airflow's built-in `FileSensor(deferrable=True)` rather than a hand-rolled Trigger, unless the required file-pattern matching genuinely can't be expressed as a glob.

The key risks cluster around three areas the research flags as critical: (1) Airflow's Triggerer runs all deferred tasks on one shared event loop — any blocking call inside a custom Trigger's `run()` stalls every other deferred task project-wide, not just one; (2) `python-oracledb`'s `executemany()` infers bind types from the first row unless `setinputsizes()` is used explicitly, so nullable columns with mixed None/typed values across chunks produce cryptic `DPY-3013`/`DPY-2010` errors that only appear at real data scale (~100K rows), not on small fixtures; (3) it's easy to build a "chunked" pipeline that still loads the whole file into memory first — the project's own benchmark test exists specifically to catch this, so chunking must be the only code path from day one, not a retrofit. Mitigations for all three are well-documented in official sources and should be baked into the relevant phases from first implementation, not fixed later.

## Key Findings

### Recommended Stack

Core: Apache Airflow 3.3.1 (LocalExecutor, Triggerer, REST API v2) + `apache-airflow-providers-standard` 1.18.0 (ships `FileSensor(deferrable=True)`), Python 3.12, `python-oracledb` 4.0.2 thin mode (no Oracle Client install needed), Pydantic 2.13.4 for one-time config validation, `gvenzl/oracle-free:23.26.2-faststart` pinned exactly. Supporting: `clevercsv` (dialect sniffing), `charset-normalizer` (encoding detection), `Faker` (deterministic test-data generation), `pytest` + `testcontainers`/docker-compose for real-Oracle integration tests. Dev tooling: `uv`, `ruff`, `mypy` — the now-standard fast/consolidated Python toolchain. Never: `cx_Oracle` (maintenance mode), per-row Pydantic validation, Celery/Kubernetes executors, `latest` image tags.

**Core technologies:**
- Apache Airflow 3.3.1 — orchestration (thin DAG, HTTP trigger, deferrable wait) — REST API v2 + Triggerer are load-bearing for the two hard requirements
- `python-oracledb` 4.0.2 (thin mode) — Oracle connectivity + bulk insert — official successor to `cx_Oracle`, no Oracle Client install needed, `executemany()` is Oracle's only bulk-insert primitive
- Pydantic v2 — validate `config.json` once per run — collects all field errors at once, matching "fail fast but tell me everything" for config, deliberately not used per-row
- `gvenzl/oracle-free:23.26.2-faststart` — Oracle Database Free container — community-standard, faststart cuts boot time from minutes to ~10-20s

### Expected Features

Feature research maps directly onto the project's own 60-point spec and PROJECT.md — there is no smaller "v0" than the full table-stakes list, since the spec's Definition of Done is already minimal and tightly scoped.

**Must have (table stakes):**
- HTTP-triggerable, thin TaskFlow DAG (config → wait → process → load-results → report)
- Config-driven processing via Pydantic v2 `config.json`, validated once per run
- Reusable, Airflow-agnostic `csv_processor.process()` engine (structural → type → nullability validation, in that order)
- Invalid-row quarantine with error metadata; one bad row never fails the whole file
- Chunked/bulk processing throughout; Oracle bulk load via `executemany()` array binding
- Two Oracle tables per dataset (`_VALID`/`_INVALID`) + minimal ingestion-metadata table
- Idempotency via filename + checksum + dataset
- Structured `ProcessingResult` with distinct status semantics (SUCCESS / SUCCESS_WITH_INVALID_ROWS / FILE_NOT_FOUND / INVALID_FILE / CONFIGURATION_ERROR / DATABASE_ERROR / PROCESSING_ERROR)
- Deterministic CSV generator; two full datasets (customers, orders) proving config-drivenness generalizes
- docker-compose provisioning (Airflow LocalExecutor + pinned Oracle Free); unit + Oracle integration + e2e tests; ~100K-row benchmark (row-by-row vs. chunked/bulk); minimal CI + docs

**Should have (differentiators, add after core is proven):**
- `batcherrors=True`/`getbatcherrors()` as a defensive layer on Oracle inserts (not a substitute for pre-insert validation)
- Regex file-pattern matching beyond glob
- Simple configurable business-rule checks (min/max, allowed values) — only if a concrete dataset needs one

**Defer / explicitly excluded (not v2, permanently out of scope):**
- CDC, SCD/historization, referential-integrity/uniqueness/volume-anomaly/completeness validation, full data lineage/schema registry, Great Expectations-class frameworks, Kubernetes/KPO, MinIO, Vault, Celery/Redis, custom FastAPI trigger wrapper, production observability stack, multi-DB warehouse — these belong to the reference repo's superset architecture, not this project's roadmap at any priority.

### Architecture Approach

Airflow's thin TaskFlow DAG never contains parsing/validation/SQL — every task body shapes XCom-safe dicts in/out and delegates to `csv_processor`, a plain importable package with zero Airflow imports, fully unit-testable without a scheduler or metadata DB. Everything downstream of the DAG (detect → parse → validate → normalize → chunk → load) runs in-process inside the LocalExecutor worker as a single function call (`process(file_path, config) -> ProcessingResult`) — no pod launches, no XCom-sidecar conventions. Config crosses task boundaries as a validated-then-serialized dict (Pydantic model → `model_dump(mode="json")` → XCom → `model_validate()` rehydration on the other side), and only summary-shaped `ProcessingResult` objects cross the DAG↔engine boundary — never raw row data.

**Major components:**
1. `airflow/dags/` (+ thin `_common/` factory and sensor wrapper) — wires 5 tasks per dataset DAG, contains zero business logic
2. `src/csv_processor/` — config (Pydantic v2), detect (vendored dialect/encoding/header/compression sniffing), source/normalize/validate (rewritten smaller from the reference repo), load (Oracle `executemany()` bulk loader), engine (`process()` public entrypoint) — Airflow-agnostic throughout
3. Oracle Database Free — `<DATASET>_VALID`, `<DATASET>_INVALID`, `ingestion_metadata` tables, pinned image tag
4. `generator/` — deterministic CSV fixture producer, decoupled from `csv_processor`, needs only the schema shape

Suggested build order (dependency-driven): (1) Oracle schema + docker-compose, (2) config models, (3) detect/parse/normalize/validate engine core (unit-testable, no Airflow/Oracle), (4) CSV generator (parallel with 2-3), (5) Oracle bulk loader + idempotency, (6) `process()` public entrypoint, (7) DAG wiring, (8) HTTP trigger + e2e test, (9) benchmark + CI.

### Critical Pitfalls

1. **Blocking calls inside a Trigger's `run()` stall the entire triggerer process, not just one task** — write the file-wait Trigger async-only from the start (`asyncio.sleep`, never `time.sleep`); review the diff specifically for non-`await`ed I/O before merging.
2. **`executemany()` infers bind types from the first row; mixed None/typed values across chunks raise cryptic `DPY-3013`/`DPY-2010`** — always call `cursor.setinputsizes(...)` derived from `config.json`'s schema before `executemany()`, and normalize every row to a fixed tuple shape with consistent Python types per column.
3. **Full-file CSV load balloons memory 3-4x file size, silently defeating the chunked-processing requirement** — stream rows via `csv.reader`/generator from the start; never materialize the whole file into a list/DataFrame; the project's own 100K-row benchmark exists to catch exactly this.
4. **`batcherrors=True` does not auto-commit successful rows** — leaves an open transaction/locks if not paired with explicit `getbatcherrors()` + `commit()`/`rollback()`; recommend not relying on it as the primary error path at all, since invalid rows are already filtered before DB load.
5. **BOM and locale-driven encoding surprises produce silent mojibake, not errors** — check for BOM explicitly (`utf-8-sig`) before falling back to statistical detection; include dedicated UTF-8-BOM and non-ASCII fixtures in the detection phase's own test suite from the start.

## Implications for Roadmap

Based on combined research, suggested phase structure (dependency-driven, matching ARCHITECTURE.md's build order and folding in FEATURES.md priorities and PITFALLS.md prevention points):

### Phase 1: Environment & Oracle Foundation
**Rationale:** Every downstream phase eventually needs a real Oracle instance (spec requires integration tests against a real container, not mocks); resolving the pinned image tag, connection, and DDL removes the biggest infrastructure unknown first, and unblocks parallel work on config/engine code.
**Delivers:** docker-compose (Airflow LocalExecutor + Airflow metadata DB + pinned `gvenzl/oracle-free:23.26.2-faststart`), Oracle DDL for `<DATASET>_VALID`/`<DATASET>_INVALID`/`ingestion_metadata`, `.wslconfig` documentation.
**Addresses:** docker-compose provisioning (table stakes)
**Avoids:** Oracle image tag drift (`latest`), WSL2/Docker Desktop memory & mirrored-networking gotchas

### Phase 2: Config Contract & CSV Generator
**Rationale:** Config schema is the contract everything else reads, and it's the only dependency the CSV generator needs — pulling both forward in parallel unblocks the engine, tests, and fixtures simultaneously with no dependency on Oracle or detection code.
**Delivers:** `csv_processor.config` (Pydantic v2 `DatasetConfig`/`ColumnSpec`/`OracleTargetSpec`, `load_config()`), `config.json` for customers + orders, deterministic CSV generator (valid + invalid rows, full type coverage).
**Addresses:** Config-driven processing, config validation (table stakes); deterministic CSV generator (table stakes)
**Avoids:** Per-row Pydantic validation anti-pattern

### Phase 3: CSV Processing Engine (Detect → Parse → Validate → Normalize)
**Rationale:** This is the project's core engineering deliverable and is entirely Airflow-agnostic — must be fully built and unit-tested standalone before any DAG wiring exists to call it.
**Delivers:** `csv_processor.detect` (vendored dialect/encoding/header/compression sniffing, run once per file), `csv_processor.source` (chunked-by-record-count streaming), `csv_processor.normalize`/`validate` (structural → type → nullability, in order, collect-and-continue), `RowError`/`Status` models.
**Addresses:** Structural/type/nullability validation, invalid-row quarantine and isolation, chunked processing (all table stakes)
**Avoids:** Full-file memory blowup, per-chunk re-detection, unbounded `csv.field_size_limit`, BOM/encoding misdetection

### Phase 4: Oracle Bulk Load & Idempotency
**Rationale:** First point where integration tests against a real Oracle container become meaningful; wires Phase 1's schema to Phase 3's validated rows.
**Delivers:** `csv_processor.load` (`executemany()` bulk loader with `setinputsizes()` derived from config schema), file checksum computation, `ingestion_metadata` idempotency check.
**Addresses:** Oracle bulk loading, ingestion metadata + idempotency (table stakes)
**Avoids:** `DPY-3013`/`DPY-2010` type-inference errors, `batcherrors` open-transaction misuse

### Phase 5: Engine Entrypoint & Status Semantics
**Rationale:** `process(file_path, config) -> ProcessingResult` assembles phases 2-4 into the exact function the DAG will call; must be complete and tested before any DAG code exists since the DAG is thin by design.
**Delivers:** `csv_processor.engine.process()`, full `ProcessingResult`/status-enum contract (SUCCESS / SUCCESS_WITH_INVALID_ROWS / FILE_NOT_FOUND / INVALID_FILE / CONFIGURATION_ERROR / DATABASE_ERROR / PROCESSING_ERROR).
**Uses:** All prior stack/architecture components combined into one public API
**Implements:** The `csv_processor.process()` component from ARCHITECTURE.md

### Phase 6: Airflow DAG Wiring & Deferrable File-Wait
**Rationale:** With a tested, complete engine to call, DAG plumbing has something real to wrap; the file-wait step is architecturally independent and could be built in parallel with phases 2-5 if desired.
**Delivers:** `airflow/dags/_common/` (dag factory, sensor wrapper, XCom helpers), `FileSensor(deferrable=True)` wait step (or custom Trigger only if glob can't express the required pattern), two thin per-dataset DAGs.
**Addresses:** Thin TaskFlow DAG, deferrable file-availability wait (table stakes)
**Avoids:** Blocking calls inside Trigger `run()`, business logic inside `@task` functions

### Phase 7: HTTP Trigger, End-to-End Test & Benchmark
**Rationale:** Only meaningful once the full path (trigger → DAG → engine → Oracle) is real and stable; benchmark specifically needs the chunked design to already be the only code path, not a retrofit.
**Delivers:** Airflow REST API trigger verification, end-to-end test (HTTP trigger → DAG → Oracle VALID/INVALID tables), ~100K-row benchmark (row-by-row vs. chunked/bulk).
**Addresses:** Unit + integration + e2e tests, performance benchmark (table stakes)
**Avoids:** Full-file memory blowup being discovered only at this stage instead of prevented in Phase 3

### Phase 8: CI & Documentation
**Rationale:** Completion-quality gate, sequenced last since it validates everything built in prior phases; Oracle-in-CI is explicitly optional per spec so it doesn't block this phase.
**Delivers:** Minimal CI (lint, type check, unit tests, build/check), README + architecture/config/csv-engine/oracle/development docs with copy-pasteable commands.
**Addresses:** Minimal CI + minimal docs (table stakes)
**Avoids:** README assuming prior Oracle/Airflow expertise; undocumented `.wslconfig`/IPv4 gotchas

### Phase Ordering Rationale

- Oracle schema/environment comes first because integration tests need a real container throughout, and it doesn't block the pure-Python engine work that follows.
- Config schema and the CSV generator are pulled forward together (Phase 2) because the generator only needs the schema shape, not the engine itself — this unblocks parallel work and produces fixtures the engine phase immediately needs.
- The engine (Phases 3-5) is built and unit-tested fully before any DAG code (Phase 6), matching both the architecture's explicit build-order guidance and the "thin DAG has nothing to wrap otherwise" rationale.
- Benchmark and e2e testing are sequenced after DAG wiring (Phase 7) because they need the full real path to exist, but the chunked design itself must be built correctly starting in Phase 3, not fixed here — the benchmark only measures/proves it.
- CI/docs come last as a completion-quality gate, consistent with FEATURES.md treating them as DoD items rather than sequencing-critical features.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 4 (Oracle Bulk Load & Idempotency):** `setinputsizes()` type-derivation from config schema and `batcherrors` semantics are subtle enough (per PITFALLS.md) to warrant a focused pass confirming exact Oracle type-mapping choices before implementation.
- **Phase 6 (DAG Wiring & Deferrable File-Wait):** whether the stock `FileSensor(deferrable=True)` glob support is sufficient or a custom `BaseTrigger` is required depends on the exact file-naming patterns chosen for the two datasets — worth a short verification pass once dataset configs are finalized.

Phases with standard patterns (skip research-phase):
- **Phase 1 (Environment & Oracle Foundation):** docker-compose + Oracle Free provisioning is well-documented; only needs the one-time manual pull-and-boot sanity check already flagged in STACK.md.
- **Phase 2 (Config Contract & Generator):** Pydantic v2 config modeling and deterministic fixture generation are standard, well-trodden patterns.
- **Phase 3 (CSV Processing Engine):** detection/validation logic has a working reference implementation to port from; anti-patterns are well-documented in PITFALLS.md.
- **Phase 5 (Engine Entrypoint):** straightforward assembly of already-built, already-tested components.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | MEDIUM-HIGH | Core version numbers verified live against PyPI/Docker Hub; Airflow/oracledb/Pydantic API shapes verified via Context7 official docs. Oracle image tag behavior is LOW confidence (web search only) — needs a one-time manual boot check. |
| Features | MEDIUM | Project's own spec/PROJECT.md pin most decisions at HIGH internal confidence; Airflow/python-oracledb specifics via Context7 are MEDIUM; broader market/competitor framing is LOW-confidence supporting color only. |
| Architecture | HIGH / MEDIUM | Component boundaries and build order derived from reading the actual working reference-repo code (HIGH); Airflow API specifics (deferral restricted to class-based operators, FileSensor deferrable support) verified via Context7 (MEDIUM) — cross-check against the installed provider version at implementation time. |
| Pitfalls | MEDIUM | Airflow trigger semantics and python-oracledb API behavior are HIGH (official docs via Context7); WSL2/Docker/CSV-sniffing community findings are MEDIUM (cross-checked across multiple independent sources, including primary-source GitHub issue reports). |

**Overall confidence:** MEDIUM-HIGH

### Gaps to Address

- **Oracle Free image tag/variant behavior (`23.26.2-faststart` vs. `full`/`slim`):** only web-search-corroborated — do a one-time manual `docker run` sanity check before locking it into docker-compose in Phase 1.
- **Airflow's exact constraints file for 3.3.1/Python 3.12:** verify the constraints URL exists before finalizing `pyproject.toml`, since Airflow's dependency graph (including its internal Pydantic pin) is easy to break without it.
- **Whether stock `FileSensor(deferrable=True)` glob matching is sufficient vs. needing a custom `BaseTrigger`:** depends on the final file-naming pattern per dataset; the spec's mention of "optionally regular expressions" suggests this should be revisited once dataset configs (Phase 2) are locked in, before committing to Phase 6's implementation approach.
- **`astral-sh/setup-uv` GitHub Action's exact latest immutable tag:** re-verify at implementation time (CI/docs phase) since action releases move faster than this research's cache TTL.

## Sources

### Primary (HIGH confidence)
- Context7 `/apache/airflow` — REST API v2 dagRuns/conf, Deferrable Operators & Triggers authoring guide, `FileSensor` deferrable mode
- Context7 `/oracle/python-oracledb` — `executemany()`/`batcherrors`/`setinputsizes()`/bind-variable semantics
- Context7 `/pydantic/pydantic` — `model_validate_json`, `ValidationError.errors()` collection behavior
- PyPI JSON API (live query, 2026-08-28) — exact version numbers and `requires_python` for all pinned packages
- Reference repo source (`/mnt/c/Users/borow/VSC/projects/airflow-platform` — `csv-processor/detect/*`, `dataplat/normalize/*`, `airflow/dags/csv_ingest_customers.py`, `_common/`) — read for architecture/build-order patterns, not imported
- Project's own spec and PROJECT.md — primary internal source for feature scope

### Secondary (MEDIUM confidence)
- Docker Hub API (live query, 2026-08-28) — `gvenzl/oracle-free` tag listing
- python-oracledb / apache-airflow GitHub issue trackers — `DPY-3013`, triggerer single-loop blocking discussion
- microsoft/WSL GitHub issues — vmmem memory growth, mirrored-networking IPv6 connection timeouts
- CleverCSV docs, pandas chunked-reading memory analysis (pythonspeed.com)

### Tertiary (LOW confidence)
- Web search — Oracle Free image ecosystem overview, 2026 GitHub Actions Python CI conventions, full-platform (Airbyte/Meltano) and data-quality-framework (Great Expectations) scope comparisons — directionally corroborative, not authoritative; re-verify at implementation time where flagged above

---
*Research completed: 2026-08-28*
*Ready for roadmap: yes*
