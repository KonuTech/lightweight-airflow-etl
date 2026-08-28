# Feature Research

**Domain:** Lightweight local Airflow-orchestrated CSV→database ETL (single reusable CSV processing
engine, thin TaskFlow DAG, Oracle Database Free target)
**Researched:** 2026-08-28
**Confidence:** MEDIUM (project's own spec/PROJECT.md already pin most decisions at HIGH internal
confidence; external validation via web search is LOW-confidence supporting color, Airflow/
python-oracledb specifics via Context7 are MEDIUM-confidence official docs)

## Feature Landscape

### Table Stakes (Project Doesn't Meet Its Own Definition of Done Without These)

These map directly to PROJECT.md's Active requirements and the 60-point spec. Missing any of
these means the "reusable CSV engine + thin DAG" story is incomplete, not just less polished.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| HTTP-triggerable DAG with runtime conf (dataset + config path) | Core Value statement requires "a single HTTP request can trigger" ingestion end to end | LOW | Airflow's own REST API (`POST /dags/{dag_id}/dagRuns`) is sufficient — no custom FastAPI wrapper needed |
| Thin TaskFlow DAG (config → wait → process → load-results → report) | Explicit architectural goal: Airflow orchestrates, does not implement CSV logic | LOW | Keep task count small; don't split every internal step into its own task (spec §6) |
| File-availability wait as deferrable operator/trigger | Async, non-blocking wait is called out as "especially appropriate" in spec §11 and is cheap given Airflow ships it | LOW | Airflow's built-in `FileSensor` (`airflow.providers.standard.sensors.filesystem.FileSensor`) already supports `deferrable=True` and releases the worker to the Triggerer — use this rather than hand-rolling a custom `Trigger` unless glob/pattern semantics require it (verified via Context7, MEDIUM confidence) |
| Config-driven processing (`config.json`: file pattern, dialect, schema, target/invalid tables) | Spec §7-8: config is "the contract between the generated CSV and the processing engine" | MEDIUM | Pydantic v2 model, validated once per run before any row processing (already pinned decision) |
| Reusable `csv_processor.process(file_path, config)` engine | This is the stated primary deliverable, not the DAG | MEDIUM | Engine must be Airflow-agnostic — importable and testable with no Airflow runtime present |
| Structural validation (column count/missing/unexpected columns) | Spec §28 category 1; a CSV missing a column shouldn't silently misalign into wrong fields | LOW-MEDIUM | Must run before type validation — bad structure makes type validation meaningless |
| Type validation (integer/decimal/date parsing) | Spec §28 category 2; Oracle has no forgiving implicit CSV-string coercion worth relying on | MEDIUM | Explicit CSV string → Python type → Oracle type conversion, no accidental implicit Oracle casts (spec §30) |
| Nullability validation (required field empty) | Spec §28 category 3 | LOW | Depends on schema config already parsed |
| Invalid-row quarantine with error metadata (error_code, error_message, source_file, row_number + original data) | Spec §26-27: "why was this row rejected?" must be answerable later, not just "row N failed" | MEDIUM | Confirmed as standard practice in lightweight CSV ETL more broadly (quarantine-not-crash pattern) — LOW-confidence web corroboration, but matches spec directly |
| Invalid row isolation (one bad row doesn't fail the whole file) | Spec §32; 100K rows with 150 bad ones should yield 99,850 valid + 150 invalid, not a hard failure | MEDIUM | Configurable behavior per spec, but default should be collect-and-continue |
| Chunked/bulk processing throughout (no per-row DB round-trips) | Spec §18-20; stated efficiency goal of the whole project | MEDIUM | Configurable chunk size; read via generator/iterator to avoid loading full file into memory (spec §19), balanced against not over-engineering for style |
| Oracle bulk loading via `python-oracledb` `executemany()` array binding | Oracle has no `COPY`; `executemany()` vs. row-by-row `execute()` is measurably and dramatically faster (confirmed via Oracle's own benchmark notebook, MEDIUM confidence) | MEDIUM | `cursor.setinputsizes()` avoids per-batch memory reallocation; consider `batcherrors=True` + `getbatcherrors()` as a defensive layer, not the primary invalid-row mechanism (validation already happens pre-insert) |
| Two Oracle tables per dataset (`<DATASET>_VALID`, `<DATASET>_INVALID`) | Spec §24-26; already decided | LOW | Straightforward DDL, one join key: none needed since it's a straight split, not a normalized model |
| Structured `ProcessingResult` + distinct status semantics (SUCCESS / SUCCESS_WITH_INVALID_ROWS / FILE_NOT_FOUND / INVALID_FILE / CONFIGURATION_ERROR / DATABASE_ERROR / PROCESSING_ERROR) | Spec §33-35; Airflow task state should reflect severity — data-quality issues aren't the same as technical failures | MEDIUM | Depends on invalid-row isolation existing first; without it there's nothing to distinguish "success with errors" from plain success |
| Idempotency via filename + checksum + dataset | Spec §36, §38; retrying an Airflow task must not duplicate data | MEDIUM | Requires the ingestion metadata table to exist as the source of truth for "have I seen this file+checksum before" |
| Minimal ingestion metadata table (file_name, checksum, dataset, timestamp, row counts, status) | Spec §37; supports idempotency and gives a query surface for "what ran, when, with what result" | LOW-MEDIUM | This is the dependency root for idempotency — build it before or alongside the idempotency check, not after |
| Config validation before CSV processing begins (Pydantic v2, once per run) | Spec §39; a bad config should fail fast, not mid-file | LOW | Already a pinned decision (CLAUDE.md); the "once per run, not per row" framing is deliberate — Pydantic's raise-on-first-error model fights the collect-and-continue row model, so it must stay scoped to config only |
| Deterministic CSV generator (valid + invalid rows, all schema types) | Spec §13-15; validator needs a repeatable adversarial fixture, not hand-written test files | LOW-MEDIUM | Feeds directly into unit tests, integration tests, and the benchmark — build early since almost everything downstream consumes its output |
| Two full datasets end-to-end (customers, orders) | PROJECT.md: proves config-drivenness generalizes beyond one hard-coded schema | MEDIUM | Second dataset should require zero engine code changes — if it does, the config contract is under-specified |
| docker-compose provisioning (Airflow LocalExecutor + metadata DB + pinned Oracle Free tag) | Spec §22-23, 47-50; DoD requires the whole environment stood up from this repo | MEDIUM | Pin exact Oracle Free image tag (not `latest`) — reproducibility requirement, not a nice-to-have |
| Unit + Oracle integration + one end-to-end test | Spec §51-54; explicit DoD item, and "do not mock Oracle for all tests" is explicit | MEDIUM-HIGH | End-to-end test is the "primary demonstration of the platform" per spec §54 — treat as a first-class deliverable, not an afterthought |
| Performance/benchmark test (~100K rows, row-by-row vs. chunked/bulk) | Spec §55-56; the project's stated purpose is understanding efficient CSV processing | MEDIUM | Needs the CSV generator and both a naive and bulk code path to compare against — comparison is the point, not just measuring the final approach alone |
| Minimal CI (lint, type check, unit tests, build/check) | Spec §57 | LOW | Oracle integration tests in CI are optional per spec — don't block on making that reliable |
| Minimal docs (README + architecture/config/csv-engine/oracle/development) | Spec §58; clone-to-first-ingest walkthrough with no undocumented manual steps | LOW-MEDIUM | This is a completion-quality gate, not a feature, but it's explicitly part of DoD |

### Differentiators (Nice-to-Have, Likely v2 — Not Required to Meet Project's Own Goals)

Nothing here is needed to satisfy the spec's Definition of Done. These are directions the project
could grow in later without contradicting its "lightweight, not another production platform" thesis
— include only if there's slack after table stakes are solid.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| `batcherrors=True` / `getbatcherrors()` on the Oracle `executemany()` call | Extra defense-in-depth against a bad row slipping past CSV-level validation and hitting a DB constraint (e.g. Oracle-side length/precision truncation) at insert time | LOW | Confirmed available in `python-oracledb` (Context7, MEDIUM confidence); purely additive — doesn't change the validation architecture, just adds a safety net at the DB boundary |
| Regex-based file pattern matching (in addition to glob) | Spec §9 explicitly says "optionally regular expressions" | LOW-MEDIUM | Glob alone (`customers_*.csv`) covers the stated examples; add regex only if a real dataset needs pattern matching glob can't express |
| Basic configurable business-rule checks beyond structural/type/nullability | Spec §28 mentions "basic business rules where configured" as an aside | MEDIUM | Genuinely optional per the spec's own wording ("where configured") — don't build a rules engine; if added, one or two simple per-column predicate checks (e.g. min/max range) is the ceiling |
| Resource-config JSON (documented CPU/RAM as data, not an enforcement mechanism) | Spec §48 frames this as documentation/dev-env config, not automation | LOW | Explicitly not meant to dynamically control Docker Desktop — treat as a docs artifact, and only formalize as JSON if the README prose isn't enough |
| Second-tier CI stage running Oracle integration tests in GitHub Actions | Would catch Oracle-specific regressions pre-merge | MEDIUM-HIGH | Spec explicitly makes this optional ("if the container can be reliably started within the workflow") — CI-hosted Oracle containers are the risky, high-maintenance part; defer unless local CI reliability is proven first |
| Containerized `csv_processor` (its own Dockerfile) | Reference repo has one; could make the engine deployable independent of the Airflow worker image | MEDIUM | Only relevant if this project later needs to run the engine outside Airflow's LocalExecutor process — not needed while everything runs in-process |

### Anti-Features (Explicitly Excluded — Do Not Build)

These are the reference repo's actual capabilities. Each looks like "the more complete/correct way
to do ETL," but PROJECT.md and the spec explicitly scope them out, and the market research below
confirms *why* that scoping is sound rather than merely cheap.

| Feature | Why It Looks Appealing | Why Problematic Here | Alternative |
|---------|------------------------|------------------------|-------------|
| CDC (Change Data Capture) | "Real" data platforms track incremental source changes, not full-file drops | Requires a persistent-cursor/log-following mechanism, a materially different ingestion model than "detect a dropped file" — pure scope multiplication for a project ingesting whole CSV snapshots | Each CSV drop is a complete valid/invalid snapshot; no incremental-change tracking |
| SCD (Slowly Changing Dimensions) / historization | Feels more "production-grade" than plain overwrite/append | Needs versioning, effective-dating, and reconciliation logic layered on top of load — the reference repo's superset feature this project explicitly excludes | Plain valid/invalid snapshot tables per load; no historical dimension modeling |
| Referential integrity / uniqueness / volume-anomaly / completeness / circuit-breaker validation | The reference repo enforces `orders.customer_id → customers.customer_id`, so it's tempting to "just add the FK check since it's a real relationship" | Explicitly excluded by spec §28 even where the relationship is real — expanding validation scope here reopens the door to reproducing the 95-point platform's full validation framework, which is the opposite of the stated goal | Structural + type + nullability only; document the FK as unenforced, not as a gap to quietly close later |
| Full data lineage / complex schema registry | Good practice at platform scale (Airbyte/Meltano/dlt-class tools bundle this) | Web research confirms these are meaningfully heavier operationally (deployment, monitoring, upgrades, connector maintenance) than a 2-dataset fixed-schema pipeline justifies (LOW-confidence web corroboration, but directionally consistent with spec's own framing) | Minimal ingestion metadata table (file/checksum/dataset/timestamp/counts/status) is enough lineage for "what ran, when, with what result" |
| Adopting a full data-quality framework (e.g. Great Expectations) for row validation | Mature, well-documented, "the standard tool" for data validation | Built for large-scale/distributed pipelines; for pure structural/type/nullability checks on two fixed schemas it's disproportionate tooling weight and an extra dependency surface (LOW-confidence web corroboration) | Custom Pydantic v2-config-driven validator functions, scoped exactly to spec §28's three categories |
| Kubernetes / kind / KubernetesExecutor / KubernetesPodOperator | Reference repo's real deployment target; "more correct" for production | No orchestration need at this scale; adds a cluster, RBAC, image-build/push loop with zero payoff for a 2-dataset local project | Airflow LocalExecutor; `process_csv` runs in-process |
| MinIO / S3-style object storage | Reference repo's storage layer | Local filesystem CSV drop is the entire input surface here; object storage adds a service and an abstraction layer with nothing to abstract over | Plain `pathlib`/`open()` against a local/WSL-native directory |
| Vault (secrets management) | "Correct" secrets handling at production scale | Local dev credentials (Oracle Free, Airflow) don't need dynamic secret leasing; adds a service dependency for a threat model this project doesn't have | `.env`/docker-compose environment variables or Airflow Connections, documented plainly |
| Celery/Redis-backed executor | Needed once LocalExecutor's single-machine parallelism isn't enough | Two datasets processed occasionally on one machine never hits LocalExecutor's ceiling | LocalExecutor (already pinned) |
| Custom FastAPI wrapper around Airflow's trigger API | Feels like a "proper" API surface for triggering ingestion | Airflow's own REST API already does `POST /dags/{dag_id}/dagRuns` with a `conf` payload — a wrapper adds a service with no capability gain | Trigger the DAG directly via Airflow's REST API |
| Production-grade observability stack (metrics/tracing platform) | Standard expectation for "real" data platforms | No operational stakeholder depends on this locally; adds Prometheus/Grafana/OTel-class infrastructure for a project run by one developer | Airflow's own logging + task logs; structured Python logging in the engine |
| Multi-database warehouse architecture | The reference repo targets a fuller warehouse shape | Single Oracle Database Free instance is the only target; multi-DB routing/federation has no use case here | One Oracle Database Free container, two datasets, two table-pairs each |

## Feature Dependencies

```
CSV Generator (deterministic, valid+invalid)
    └──feeds──> Unit tests (parser/type-conversion/validator)
    └──feeds──> Oracle integration tests
    └──feeds──> End-to-end test
    └──feeds──> Performance/benchmark test (row-by-row vs. chunked/bulk)

Config schema (Pydantic v2 model)
    └──requires──> Config validation (fail-fast before CSV processing)
                       └──gates──> csv_processor.process() entrypoint

Structural validation
    └──requires──> Config schema (column names/order known)
    └──precedes──> Type validation (garbage columns make type-checking meaningless)
                       └──precedes──> Nullability validation
                                          └──feeds──> Normalize/convert (CSV string → Python type)
                                                          └──feeds──> Split valid/invalid

Split valid/invalid
    └──requires──> Invalid-row error metadata model (error_code/message/source_file/row_number)
    └──feeds──> Chunked bulk load (VALID rows → executemany())
    └──feeds──> Chunked bulk load (INVALID rows → executemany())
                       └──enables──> ProcessingResult with SUCCESS_WITH_INVALID_ROWS semantics

Ingestion metadata table (file/checksum/dataset/status)
    └──requires──> File checksum computation
    └──enables──> Idempotency check (has this file+checksum+dataset been processed?)
                       └──gates──> process_csv() task (skip or short-circuit on duplicate)

File-availability wait (deferrable FileSensor or custom Trigger)
    └──precedes──> process_csv() task (nothing to process until file exists)

ProcessingResult + status semantics
    └──requires──> Split valid/invalid AND Invalid-row isolation (config: don't fail whole file on one bad row)
    └──feeds──> Airflow report_result() task (human-readable summary)

Two-dataset proof (customers + orders)
    └──requires──> Config schema being genuinely generic (no dataset-specific code branches in csv_processor)

batcherrors=True (differentiator)
    └──enhances──> Chunked bulk load (defense-in-depth, does not replace pre-insert validation)

Referential integrity / CDC / SCD / lineage / schema registry (anti-features)
    └──conflicts with──> "reusable, small, understandable" project thesis — explicitly excluded, not deferred
```

### Dependency Notes

- **Structural → Type → Nullability validation must run in that order**: validating types on a
  row with the wrong column count produces meaningless errors (misaligned fields look like wrong
  types). This ordering is implicit in spec §28's own listing and should be enforced in the
  processing pipeline, not left to convention.
- **Ingestion metadata table is a hard prerequisite for idempotency**, not a parallel feature —
  there is nowhere else to record "this checksum was already loaded." Build the metadata table
  and the checksum computation before wiring up the idempotency short-circuit.
- **Config validation (Pydantic v2) gates everything downstream**: a project decision already
  pinned in CLAUDE.md is that this validation happens once per run, not per row — this is a
  deliberate boundary, since per-row Pydantic validation would fight the collect-and-continue
  invalid-row model that structural/type/nullability validation depends on.
- **File-availability wait and the processing engine are independent** — the deferrable
  sensor/trigger only decides *when* `process_csv()` runs, not *how*. They can be built and tested
  in parallel once the config contract exists.
- **`batcherrors=True` enhances but does not replace** validation-then-split — it's a second,
  optional line of defense at the Oracle boundary, not a substitute for CSV-level type/nullability
  checks. Do not use it as an excuse to skip pre-insert validation.
- **Anti-features don't "unlock" anything else in this project's scope** — none of table
  stakes or differentiators require CDC/SCD/lineage/schema-registry/referential-integrity as a
  building block. They're excluded outright, not just sequenced later.

## MVP Definition

### Launch With (v1)

Everything in Table Stakes above is v1 — this project's Definition of Done (spec §59) is itself
already a minimal, tightly-scoped list; there isn't a smaller "v0" that still demonstrates the
stated goal ("build a clean, efficient, reusable CSV engine and orchestrate it against Oracle").
Highlighting the ones most load-bearing to sequence first:

- [ ] CSV generator (valid + invalid rows, full type coverage) — everything else needs fixtures
- [ ] Config schema + Pydantic v2 validation — the contract everything else reads
- [ ] `csv_processor.process()`: structural → type → nullability validation, split valid/invalid
- [ ] Ingestion metadata table + checksum-based idempotency
- [ ] Chunked Oracle bulk load via `executemany()` for both VALID and INVALID tables
- [ ] Thin TaskFlow DAG (config → wait → process → load-results → report), HTTP-triggerable
- [ ] Deferrable file-availability wait (built-in `FileSensor(deferrable=True)` unless pattern
      needs exceed it)
- [ ] `ProcessingResult` + status semantics surfaced to Airflow
- [ ] Second dataset (orders) proving config-drivenness — without it, "reusable engine" is
      unverified, just asserted
- [ ] Unit + Oracle integration + end-to-end tests
- [ ] Performance benchmark (row-by-row vs. chunked/bulk) at ~100K rows
- [ ] docker-compose (Airflow LocalExecutor + metadata DB + pinned Oracle Free tag)
- [ ] Minimal CI + minimal docs

### Add After Validation (v1.x)

- [ ] `batcherrors=True` defensive layer on Oracle inserts — add once the primary
      validate-then-split path is proven correct and a real DB-side rejection (constraint
      violation) is observed in practice
- [ ] Regex file-pattern support — add only if a real dataset's filenames can't be expressed as a
      glob
- [ ] Simple configurable business-rule checks (min/max, allowed-value sets) — add only if a
      concrete dataset needs one; don't speculatively build a rules engine

### Future Consideration (v2+) — Only If Project Scope Deliberately Expands

- [ ] Oracle-integration-test stage in CI — defer until local CI reliability with a containerized
      Oracle is proven; spec explicitly makes this optional
- [ ] Containerized `csv_processor` with its own Dockerfile — only relevant if the engine needs to
      run outside the Airflow worker process
- [ ] Resource-config JSON (formalizing CPU/RAM documentation as data) — only if README prose
      proves insufficient in practice

Everything else in the Anti-Features table (CDC, SCD, referential/uniqueness/volume/completeness/
circuit-breaker validation, lineage, schema registry, Kubernetes, MinIO, Vault, Celery/Redis,
custom FastAPI trigger wrapper, observability stack, multi-DB warehouse) is **not a future
version of this project** — it belongs to the reference repo's superset architecture and should be
treated as permanently out of scope for this project's own roadmap, not merely deferred.

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| CSV generator (valid+invalid, full types) | HIGH | LOW-MEDIUM | P1 |
| Config schema + Pydantic v2 validation | HIGH | MEDIUM | P1 |
| Structural/type/nullability validation | HIGH | MEDIUM | P1 |
| Invalid-row quarantine with error metadata | HIGH | MEDIUM | P1 |
| Chunked/bulk `executemany()` Oracle load | HIGH | MEDIUM | P1 |
| Ingestion metadata + checksum idempotency | HIGH | MEDIUM | P1 |
| Thin TaskFlow DAG + HTTP trigger | HIGH | LOW | P1 |
| Deferrable file-availability wait | MEDIUM-HIGH | LOW | P1 |
| `ProcessingResult` + status semantics | MEDIUM-HIGH | MEDIUM | P1 |
| Second dataset (orders) | HIGH | MEDIUM | P1 |
| Unit + integration + e2e tests | HIGH | MEDIUM-HIGH | P1 |
| Performance benchmark (100K rows) | MEDIUM | MEDIUM | P1 |
| docker-compose provisioning | HIGH | MEDIUM | P1 |
| Minimal CI + docs | MEDIUM | LOW-MEDIUM | P1 |
| `batcherrors=True` defensive layer | LOW-MEDIUM | LOW | P2 |
| Regex file-pattern matching | LOW | LOW-MEDIUM | P3 |
| Configurable basic business rules | LOW | MEDIUM | P3 |
| Oracle-integration CI stage | LOW-MEDIUM | MEDIUM-HIGH | P3 |
| Containerized `csv_processor` | LOW | MEDIUM | P3 |
| CDC / SCD / lineage / schema registry / K8s / MinIO / Vault | N/A (excluded) | N/A (excluded) | Excluded |

**Priority key:**
- P1: Must have — part of this project's own Definition of Done
- P2: Should have, add when core is proven and a concrete trigger appears
- P3: Nice to have, only if scope deliberately grows beyond the current spec
- Excluded: Not on this project's roadmap under any priority — reference repo's territory only

## Competitor / Comparable-Tool Feature Analysis

Framed against "comparable tools," not literal competitors — this project isn't shipping a
product, but it's useful to know where it sits relative to the two obvious alternatives someone
might reach for instead of a custom engine.

| Feature | Full platforms (Airbyte/Meltano/Singer/dlt) | Data-quality frameworks (Great Expectations) | This project's approach |
|---------|------------------------------------------|-----------------------------------------------|--------------------------|
| Connector breadth | Large connector catalogs, variable quality (esp. Singer) | N/A (validation-only tool) | One connector: local-filesystem CSV, by design |
| CDC | Batch CDC common; real-time CDC only in some managed offerings | N/A | Not supported — explicitly out of scope, full-snapshot model only |
| Validation depth | Varies by connector/tool | Deep: profiling, expectations-as-config, distributed-scale support | Narrow and explicit: structural + type + nullability only |
| Operational overhead | Real: deployment/monitoring/upgrades/connector maintenance even when self-hosted | Moderate: an extra framework/dependency and its own DSL | Minimal: one Python package, no extra service |
| Lineage/schema registry | Yes, to varying degrees | No | No — ingestion metadata table substitutes as minimal "what ran, when" record |
| Fit for a 2-dataset, fixed-schema, local project | Overkill — engineering overhead exceeds payoff at this scale | Overkill for validation scope this narrow | Right-sized — this is exactly the gap these heavier tools overshoot |

## Sources

- Project's own spec and requirements (PRIMARY, HIGH confidence, internal):
  `/home/user/projects/lightweight-airflow-etl/.planning/research/lightweight-spec.md`,
  `/home/user/projects/lightweight-airflow-etl/.planning/PROJECT.md`
- Apache Airflow official docs via Context7 (`/apache/airflow`, MEDIUM confidence): FileSensor
  deferrable mode, custom Trigger implementation requirements (`__init__`/`serialize`/async `run`),
  `end_from_trigger` — https://github.com/apache/airflow/blob/main/providers/standard/docs/sensors/file.rst,
  https://github.com/apache/airflow/blob/main/airflow-core/docs/authoring-and-scheduling/deferring.rst
- `python-oracledb` official docs/samples via Context7 (`/oracle/python-oracledb`, MEDIUM
  confidence): `executemany()` array binding, `setinputsizes()`, `batcherrors=True` +
  `getbatcherrors()`, `arraydmlrowcounts` —
  https://github.com/oracle/python-oracledb/blob/main/samples/notebooks/3-DML.ipynb,
  https://github.com/oracle/python-oracledb/blob/main/doc/src/user_guide/batch_statement.md
- Web search, general lightweight CSV/ETL quarantine and chunked-loading patterns (LOW confidence,
  supporting color only) — BulkFlow (PyPI), Integrate.io CSV ETL guidance
- Web search, full data-integration platform scope comparison (LOW confidence) — Airbyte vs.
  Meltano comparisons, ETL tool landscape roundups (Estuary, Weld, Domo, dataexpert.io)
- Web search, data-quality framework scope comparison (LOW confidence) — Great Expectations vs.
  lightweight validators (Medium/DEV Community articles on Great Expectations and Cerberus)

---
*Feature research for: Lightweight local Airflow CSV→Oracle ETL platform*
*Researched: 2026-08-28*
