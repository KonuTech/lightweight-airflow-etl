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

- ✓ docker-compose provisioning: Airflow (LocalExecutor) + Airflow metadata DB + Oracle Database
      Free (pinned tag), with documented CPU/RAM/disk allocation — Phase 1
- ✓ Essential secrets maintenance: one documented `admin`/`admin` dev credential pair, sourced
      from `.env`/docker-compose environment variables, used consistently everywhere a credential
      is needed (Oracle, Airflow webserver) — Phase 1
- ✓ Reusable `csv_processor` Python package: read → parse → validate structure → validate types →
      normalize → split valid/invalid, exposed as `engine.process_chunks(file_path, config)` (the
      top-level `process()` wrapper returning a structured `ProcessingResult` is separate, still
      Active — see ENGINE-08 below) — Phase 3
- ✓ Structural/type/nullability validation only (no referential/uniqueness/volume-anomaly/
      completeness/circuit-breaker validation — explicitly out of scope) — Phase 3
- ✓ Config-driven CSV processing: `config.json` per dataset defines file pattern, CSV dialect,
      schema (types/nullability/date-format), and Oracle target/invalid tables — Phase 2
- ✓ Deterministic CSV generator producing both valid and invalid rows per dataset schema
      (strings, integers, decimals, dates/timestamps, nullable fields) — Phase 2
- ✓ `config.json` itself validated (Pydantic v2, once per run) before CSV processing begins —
      Phase 2
- ✓ Oracle bulk loading via `python-oracledb` `executemany()` with array binding — Phase 4
- ✓ Chunked/bulk processing throughout (no per-row DB round-trips); configurable chunk size —
      Phase 3 (CSV-side, `engine.process_chunks()`) + Phase 4 (Oracle-side, `executemany()`)
- ✓ Two Oracle target tables per dataset: `<DATASET>_VALID` and `<DATASET>_INVALID` (invalid
      rows carry original data + error_code/error_message/source_file/row_number) — Phase 1 (DDL)
      + Phase 3 (widened `_INVALID` columns)
- ✓ Idempotency: filename + file checksum + dataset identifies a processed file; retrying an
      Airflow task or re-encountering the same file does not duplicate data — Phase 4
- ✓ Minimal ingestion metadata table in Oracle (file_name, checksum, dataset, timestamp,
      total/valid/invalid row counts, status) — Phase 1 (DDL) + Phase 4 (usage)
- ✓ Structured `ProcessingResult` (total/valid/invalid rows, duration) returned to Airflow, with
      distinct status semantics (SUCCESS / SUCCESS_WITH_INVALID_ROWS / FILE_NOT_FOUND /
      INVALID_FILE / CONFIGURATION_ERROR / DATABASE_ERROR / PROCESSING_ERROR) — Phase 4
- ✓ Airflow TaskFlow DAG (config → file-wait → process → load-results → report) triggerable via
      Airflow's own REST API, passing dataset + config path as runtime conf — Phase 5. Built as a
      **single** config-driven `csv_ingest` DAG (not one DAG per dataset — see PROJECT.md's
      recorded technical-approach note below), live-verified via real HTTP-triggered runs, not
      just structurally.
- ✓ File-availability wait implemented as a Deferrable Operator/Trigger (async, non-blocking) —
      Phase 5. Stock `FileSensor(deferrable=True)` proved sufficient; no custom `BaseTrigger`
      needed. Live-observed `deferred` Airflow task state before the target file existed.
- ✓ Two datasets end-to-end: `customers` and `orders` — Phase 5. Proved via the identical,
      unmodified `csv_ingest` DAG completing successfully for both datasets against the real
      running stack (orders' `customer_id` FK to customers is NOT enforced here — see Out of
      Scope).
- ✓ Automated end-to-end test (HTTP → DAG → CSV → Oracle → VALID/INVALID tables), permanent
      regression coverage for TEST-03 — Phase 6. Live-verified on a genuinely fresh GitHub Actions
      runner (PR #1), not just locally — see the `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` Key
      Decision below for what a "warm dev stack only" verification habit had been masking.
- ✓ Performance/benchmark at ~100K rows, row-by-row vs. chunked/bulk, all three required metrics
      (rows/sec, peak memory, Oracle load time) — Phase 6. Chunked/bulk measured **182.85×** faster
      (780,429 rows/sec vs. 4,268 rows/sec), committed with a per-chunk timing breakdown to
      `docs/benchmark.md`.
- ✓ GitHub Actions CI: lint, type check, unit tests on every PR (`lint-type-unit`) — Phase 6.
      Scope deliberately widened beyond CI-01's literal text to a second required check,
      `oracle-e2e`, that stands up the real Oracle+Airflow stack and runs the e2e suite on every
      PR — see Key Decisions.
- ✓ Docs: README (Executive Summary + clone-to-first-ingest walkthrough) + `docs/architecture.md`,
      `configuration.md`, `csv-engine.md`, `oracle.md`, `development.md` — Phase 6, alongside
      Phase 5's `docs/airflow-dag.md`/`docs/environment.md`.
- ✓ `orders.customer_id` correlated to a real, Zipf-weighted, with-replacement sample of the
      `customers_valid` pool generated in the same run (never independently random); both IDs move
      to seed-derived structured IDs (`CUST-<hash>-NNNNNN`/`ORD-<hash>-NNNNNN`) — Phase 7.
- ✓ DB-level safety net on top of the Python-side correlation: `PRIMARY KEY` on
      `customers_valid.customer_id`/`orders_valid.order_id`, a supporting index on
      `orders_valid.customer_id`, and a `BEFORE INSERT` trigger rejecting any `orders_valid` row
      whose `customer_id` doesn't exist in `customers_valid` (whole batch fails) — Phase 7. This
      reverses "FK not enforced" from Phase 3 for this one relationship — see Out of Scope and Key
      Decisions below.
- ✓ New `report_ready` Airflow DAG: a custom, deferrable `OraclePartitionReadyTrigger` (the Oracle
      provider ships no sensor of its own) polls `ingestion_metadata` for both datasets' current-day
      partition and fires a thin report-logging task once both are present — Phase 7. Runs alongside
      `scripts/regenerate_readme_summary.py`, not replacing it.
- ✓ The `customers⋈orders` business report (D-10, `scripts/verify_evidence.sql`) genuinely returns
      non-empty joined rows on generated fixture data, including rows spanning multiple backdated
      daily partitions — Phase 7. This was this project's literal, previously-unmet ROADMAP goal;
      README's Executive Summary now reflects real, non-empty evidence, live-regenerated.
- ✓ Naive-vs-bulk benchmark re-measured against the schema carrying the new PK/index/trigger
      overhead: **67.41×** speedup (down from Phase 6's pre-DDL 182.85×, consistent with the new
      `customers_valid` PK/implicit-index cost) — Phase 7, `docs/benchmark.md`.

### Active

- [ ] Everything run from WSL (Linux filesystem, not `/mnt/c/...`); Docker Desktop as the host —
      ongoing environmental requirement, continuously true through Phase 7, not a one-time
      deliverable to check off
- [ ] `OraclePartitionReadyTrigger.run()` (Phase 7, `airflow/dags/_common/oracle_partition_trigger.py`)
      has no exception handling around its Oracle polling calls — a transient DB error currently
      crashes the deferred sensor permanently, with no retry/backoff. Flagged as a code-review
      Critical finding (07-REVIEW.md CR-01); did not block Phase 7 completion since the sensor's
      happy-path defer/poll/fire behavior is live-proven, but is a real production-shaped
      robustness gap worth a follow-up fix.

### Out of Scope

- Kubernetes, kind, MinIO, Vault, CDC, SCD, complex data-lake/lineage architecture, distributed
  processing, complex schema registry, multi-database warehouse architecture — this project is
  deliberately smaller than the reference platform; see spec §3
- Referential/uniqueness/volume-anomaly/completeness/circuit-breaker validators inside the Python
  `csv_processor` validation engine — still explicitly excluded per spec §28; the engine itself
  performs structural/type/nullability checks only, unchanged since Phase 3. **Narrowed by Phase
  7:** the `orders.customer_id → customers.customer_id` relationship specifically is no longer
  fully unenforced — Phase 7 added a DB-level `BEFORE INSERT` trigger on `orders_valid` (a
  different layer, not a `csv_processor` validator) as a safety net catching any future generator
  regression as a load failure. `customers_invalid`/`orders_invalid` remain fully unconstrained.
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

**Working preference:** don't trust DDL/setup exiting without error as proof a database object
exists correctly — confirm by actually querying Oracle's own metadata/dictionary views (e.g.
`USER_TABLES`, `ALL_TAB_COLUMNS`) after provisioning tables. Applies to Phase 1's environment
setup and any later schema change.

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
| HTTP trigger via Airflow's own REST API, not a custom FastAPI wrapper | No extra service to build/maintain; Airflow's stable REST API already supports POST /dags/{dag_id}/dagRuns with a conf payload | ✓ Applied — Phase 5 |
| Benchmark target ~100K rows | Matches spec's own §32 example; big enough to show bulk-loading wins, small enough to run fast on Oracle Free locally | ✓ Applied — Phase 6 |
| Two datasets: customers + orders | Mirrors reference repo's real `csv_ingest_customers.py`/`csv_ingest_orders.py` and `configs/datasets/*.yaml` — proves config-drivenness generalizes without inventing a new domain | ✓ Applied — Phase 5 |
| docker-compose is a project deliverable, not pre-provisioned | Spec §22-23, 47-50 and DoD §59.1-4 expect the environment to be stood up from this repo | ✓ Applied — Phase 1 |
| Two-tier reuse of reference repo's csv-processor/dataplat (vendor pure detection files; reimplement pipeline-coupled normalize/validate logic) | Verified by reading actual imports — csv-processor's detect/* and compression.py have near-zero dataplat coupling (1-2 lines); dataplat's normalize/validate are fully wired into a custom streaming pipeline that has no place here | ✓ Applied — Phase 3 |
| orders.customer_id → customers.customer_id FK not enforced | Referential integrity validation is explicitly out of scope per spec §28, even though the reference repo enforces it | ✓ Applied — Phase 3 |
| Single `admin`/`admin` dev credential everywhere, via env vars | Lightweight local project, Vault is explicitly out of scope; a single consistent credential keeps setup simple without scattering ad hoc secrets across configs | ✓ Applied — Phase 1 |
| Genuine naive-loop Oracle baseline (single `cursor.execute()` per row), never `executemany(chunk_size=1)`, isolated in a new `benchmark/` dir | A `chunk_size=1` run still goes through `executemany()` and wouldn't reproduce the actual per-round-trip cost the benchmark exists to demonstrate; both write strategies share the identical `csv_processor.engine.process_chunks()` parse pass so only the write strategy varies | ✓ Applied — Phase 6 |
| CI runs a real Oracle+Airflow `oracle-e2e` job as a second required check, beyond CI-01's literal "lint, type check, unit tests" | User-approved scope expansion (D-06) — GitHub Actions' native `services:` key can't express this project's `docker compose` dependency chain (no `depends_on` health-condition ordering, starts before checkout), so it reuses this project's own unmodified `docker-compose.yml` via `docker compose up -d --wait` as a CI step instead | ✓ Applied — Phase 6 |
| README Executive Summary is live/regenerated via a CI auto-commit job on every merge to `main`/`master`, not a static snapshot | User explicitly chose this over a simpler "snapshot with a last-verified date" option; the `readme-summary.yml` job uses the default `GITHUB_TOKEN` (never a PAT) so its own commit structurally cannot re-trigger further Actions runs, with `[skip ci]` as defense-in-depth | ✓ Applied — Phase 6 |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"` in `docker-compose.yml` | Discovered via PR #1's first genuinely-fresh CI run: Airflow's default (`true`) pauses `csv_ingest` the instant it's first parsed on a brand-new metadata DB, and the scheduler schedules zero task instances for a paused DAG's run — even a manually/API-triggered one. Every prior manual verification (Phase 5's own live evidence, and every earlier attempt this session) ran against an already-long-warm stack whose DAG had been unpaused once and stayed that way, masking a real gap that would have silently broken DOC-01's "fresh clone to a completed HTTP-triggered ingestion, no undocumented manual steps" promise for every actual new developer, not just CI. Confirmed live: unpausing mid-hang (no restart) let an already-stuck run complete within seconds. | ✓ Applied — Phase 6 |
| CI bootstrap must `chmod 666` the auth-manager passwords file and pre-create `data/customers`/`data/orders` before `docker compose up` | Both are Docker-bind-mount-ownership gotchas invisible on a long-warm local dev machine (files/dirs already had permissive ownership from earlier manual fixes) but deterministic on a genuinely fresh CI checkout: a plain `echo >` creates the passwords file at the container-unreadable default 644, and `./data` gets auto-created as `root` by Docker Engine on first bind-mount use, blocking the non-root CI runner from `mkdir`-ing a dataset subdirectory inside it | ✓ Applied — Phase 6 |
| Every docker-compose service must declare an explicit `healthcheck:` if anything depends on its readiness | UAT (Phase 1) found `docker compose up --wait` reports a service Healthy the instant its process starts when no `healthcheck:` exists — not when it's actually ready. Airflow's `apiserver` had none; caused a real, reproducible cold-start race (~12s false-positive window) against `/auth/token`. Fixed via gap-closure Plan 01-05. | ✓ Applied — Phase 1 |
| `apache-airflow-providers-oracle` added mid-Phase-1 (not in the original plan) | Discovered that `airflow connections test` needs a registered Hook class for `conn_type=oracle`, which only ships in this provider package — the raw `oracledb` driver alone isn't enough for Airflow's own connection-testing UI/CLI | ✓ Applied — Phase 1 |
| Footer-row detection requires an explicit per-dataset opt-in (`CsvDialectConfig.has_footer: bool = False`); never runs by heuristic alone | A 5-round gap-closure chain in Phase 3 (plans 03-06 through 03-10) found that heuristic footer/preamble detection running unconditionally on every dataset silently drops a genuinely malformed last row of ANY file — reproduced concretely via this project's own generator (`customers.json`, seed=11: 50 rows generated, only 49 accounted for). Root-caused to the heuristic having no way to distinguish "genuine footer" from "corrupted last row" without a config signal. | ✓ Applied — Phase 3 |
| One DAG (`csv_ingest`), not one DAG file per dataset | Corrects `research/ARCHITECTURE.md`'s original two-DAG-file sketch — ROADMAP.md's own success criteria are explicit: "the **identical DAG definition** runs successfully for both datasets **purely by passing different config**, with **no dataset-specific code branches**"; the reference repo's per-dataset DAG files were read only for the thin-DAG *pattern*, per PROJECT.md's own Tier-B guidance, not copied structurally | ✓ Applied — Phase 5 |
| `process()`'s domain-failure statuses (`INVALID_FILE`, `CONFIGURATION_ERROR`, etc.) never fail the Airflow task | `process_csv` never raises for any of the 7 closed `Status` values (verified in `engine.py`'s own docstring); every run — success or failure — must reach `report_result` so its "concise summary" success criterion holds even for a bad file/config. Only a genuinely unexpected exception fails the task. | ✓ Applied — Phase 5 |
| Stock `FileSensor(deferrable=True)`, no custom `BaseTrigger` | Confirmed sufficient: supports Jinja-templated `filepath` (verified against the pinned `apache-airflow-providers-standard==1.17.0` source) and this project's glob-style `file_pattern` values; resolves the research question flagged in STATE.md's Blockers | ✓ Applied — Phase 5 |
| `docker-compose.yml` needed 5 real fixes to actually run a DAG (not just structurally define one) | Discovered only by triggering a live DAG run for the first time in this project's history: missing `ORACLE_DSN`/credentials, missing `configs/` mount, unregistered `fs_default` connection, `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` defaulting to unreachable `localhost`, and each container minting its own random `AIRFLOW__API_AUTH__JWT_SECRET` (breaking scheduler↔apiserver task-token verification) | ✓ Applied — Phase 5 |
| `resolve_safe_config_path()` guards the HTTP-triggered `config_path` against path traversal / absolute-path escape | `dataset`/`config_path` arrive as untrusted runtime `conf`; a naive `Path.__truediv__`/`os.path.join` join silently discards the base directory when the joined operand is absolute, which would let an absolute `config_path` bypass the `configs/datasets/` allowlist entirely | ✓ Applied — Phase 5 |
| RNG-continuation: one `random.Random(seed)`/`Faker` pair constructed once and passed by object identity into both the `customers` and `orders` generation calls | `generate_rows()` gained optional keyword-only `rng`/`fake`/`customer_id_pool` params so every pre-existing 4-positional-arg caller stays byte-identical; the correlated path shares live RNG state across the customers→orders boundary so Zipf-sampling/determinism (D-05) holds by the most literal reading | ✓ Applied — Phase 7 |
| Zipf weight ∝ 1/rank, with-replacement pool sampling for `orders.customer_id` | Matches real-world order-frequency skew (a few customers order disproportionately often) while guaranteeing every sampled ID is a real `customers_valid` row | ✓ Applied — Phase 7 |
| Staging-path + atomic same-filesystem rename (`write_staged()`) adopted as the one write path for every production CSV writer (CLI, `regenerate_readme_summary.py`) | A file must never be visible to the watched-directory `FileSensor` mid-write; every prior direct `write_csv()` call site was migrated to this helper rather than adding a second, parallel write discipline | ✓ Applied — Phase 7 |
| Custom `OraclePartitionReadyTrigger(BaseTrigger)` polling `ingestion_metadata` via `oracledb.connect_async()`, rather than any stock sensor | `apache-airflow-providers-oracle==4.6.2` ships no sensor and no deferrable operator at all (confirmed against its own docs) — this is the only path to a deferrable "both datasets ready" check; `connect_async()` (never blocking `connect()`) avoids stalling the triggerer's shared event loop for every other deferred task project-wide | ✓ Applied — Phase 7 |
| DB-level `PRIMARY KEY`s + one supporting index (`orders_valid.customer_id`) + `BEFORE INSERT` FK-existence trigger on `orders_valid`, applied only after an explicit human-confirmed `checkpoint:decision` | One-way change requiring a full `make reset` (Oracle volume wipe) to apply or undo — gated behind a `blocking-human` checkpoint, never auto-approved even in unattended/auto-advance mode, since it destroys all current dev data | ✓ Applied — Phase 7 |
| `orders.customer_id → customers.customer_id` FK now enforced at the DB level (supersedes the Phase 3 "FK not enforced" decision above for this one relationship) | Catches any future generator regression as a hard load failure instead of silent bad data; the Python-side `csv_processor` validation engine itself still excludes referential validators per spec §28 — this is a separate DDL safety net, not a new validator stage | ✓ Applied — Phase 7 |
| `orders.amount` precision narrowed (12→6 digits) and `order_date` confined to a narrow, recent window (supersedes nothing — first time these were tuned) so realistic row counts produce multiple orders per (region, month) business-report bucket | Every bucket held exactly one order at the old precision/date-range combination, so the report's Avg Amount always trivially equaled Total Amount — not a bug in the report SQL, a data-generation gap | ✓ Applied — post-Phase-7 |
| `readme-summary.yml` opens a PR (via a repo-scoped `README_BOT_PAT`) and auto-merges it once `lint-type-unit`/`oracle-e2e` genuinely pass, rather than committing straight to `master` with the default `GITHUB_TOKEN` (supersedes the Phase 6 "default GITHUB_TOKEN, never a PAT" decision above) | Confirmed live: a GITHUB_TOKEN-authored push can never satisfy `master`'s required status checks (they only ever run via `pull_request`, and GitHub Actions app bypass actors are org-only — unavailable on this personal-account repo); a GITHUB_TOKEN-opened PR also can't trigger those checks itself (anti-recursion) or dispatch them via API (needs `workflow` scope). `README_BOT_PAT` is scoped to only this repo with only Contents/PR write, used solely to open this one PR — never to push `master` directly. D-13's actual concern (an infinite regenerate-commit loop) is closed by `[skip ci]` on the final squash-merge commit instead, a token-independent mechanism — confirmed live: the merge commit does not re-trigger `readme-summary.yml` | ✓ Applied — post-Phase-7 |

## Current State

**Shipped: v1.0 MVP** (2026-08-30) — 7 phases, 36 plans, 41/41 requirements validated. Full detail
archived at `.planning/milestones/v1.0-ROADMAP.md`/`v1.0-REQUIREMENTS.md`; retrospective at
`.planning/RETROSPECTIVE.md`.

The platform does everything the Core Value promises today: an HTTP request triggers `csv_ingest`,
which validates and bulk-loads CSV rows into Oracle with checksum-keyed idempotency; a second DAG
(`report_ready`) and `scripts/regenerate_readme_summary.py` both prove a live, non-empty
`customers ⋈ orders` business report; CI (`ci.yml` + `readme-summary.yml`, PR-based with a scoped
PAT — see Key Decisions) enforces this on every change.

### Next Milestone Goals

No v2 scope has been chosen yet. Two seeds are on file for when a next milestone is planned:
- `SEED-001-python-to-plsql-migration` — moving more data-processing logic from Python to Oracle
  PL/SQL, dormant until a concrete performance/complexity trigger appears
- The Airflow UI/logging robustness gap flagged in Phase 7's code review (`OraclePartitionReadyTrigger`
  has no retry/backoff around its Oracle polling calls) remains open as a known gap, not yet scoped

Run `/gsd-new-milestone` to start requirements gathering for v1.1/v2.0.

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
*Last updated: 2026-08-30 after v1.0 milestone completion — all 7 phases shipped, archived, and tagged*
