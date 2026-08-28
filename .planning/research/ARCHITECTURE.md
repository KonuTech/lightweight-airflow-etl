# Architecture Research

**Domain:** Local Airflow-orchestrated CSV→Oracle ETL (single-node, two datasets, no Kubernetes)
**Researched:** 2026-08-28
**Confidence:** HIGH (component boundaries, build order — derived from reading the actual reference
repo's working code plus current Airflow docs) / MEDIUM (Airflow API specifics fetched via
Context7, cross-check against installed provider version during implementation)

## Standard Architecture

### System Overview

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         Airflow (LocalExecutor)                            │
│  ┌───────────────────────────────────────────────────────────────────┐    │
│  │  DAG per dataset (customers_ingest, orders_ingest) — thin TaskFlow │    │
│  │                                                                     │    │
│  │  validate_config → wait_for_file → process_csv → load_results →   │    │
│  │  report                                                            │    │
│  │       │               │ (deferred,        │                        │    │
│  │       │               │  class-based      │                        │    │
│  │       │               │  FileSensor,      │                        │    │
│  │       │               │  NOT @task)       │                        │    │
│  └───────┼───────────────┼───────────────────┼────────────────────────┘    │
│          │               │                   │ in-process function call    │
├──────────┼───────────────┼───────────────────┼──────────────────────────────┤
│          ▼               ▼                   ▼                              │
│   csv_processor.config   (filesystem      csv_processor.process(path, cfg) │
│   (Pydantic v2)           glob/mtime         │                              │
│                           check only —       ▼                              │
│                           no CSV parsing) detect → parse → validate →       │
│                                           normalize → chunk → load          │
│                                              │                              │
├──────────────────────────────────────────────┼──────────────────────────────┤
│                                                ▼                             │
│                                    Oracle Database Free (pinned tag)        │
│                              ┌─────────────┐ ┌──────────────┐ ┌──────────┐  │
│                              │ <DS>_VALID  │ │ <DS>_INVALID │ │ ingestion│  │
│                              │             │ │              │ │_metadata │  │
│                              └─────────────┘ └──────────────┘ └──────────┘  │
└───────────────────────────────────────────────────────────────────────────┘
```

Two things distinguish this from the reference platform's shape (K8s pods, S3, Vault, a custom
streaming pipeline engine): (1) everything downstream of the DAG runs **in-process**, in the same
Python interpreter the LocalExecutor worker uses — `process_csv` is a plain function call, not a
pod launch, so there is no XCom-sidecar-file convention, no pod resource sizing, no image-signing
retry logic; (2) `csv_processor` here is a plain importable package/module tree, not a separately
versioned, `pip`-installable sibling package — this project has exactly one consumer (its own DAGs)
and one runtime (the Airflow worker container), so there is no packaging boundary to maintain.

### Component Responsibilities

| Component | Responsibility | Typical Implementation |
|-----------|----------------|-------------------------|
| DAG module (`dags/customers_ingest.py`, `dags/orders_ingest.py`) | Wires 5 tasks in order, passes runtime `conf` (dataset/config path) through XCom as plain dicts. Contains **zero** parsing/validation/SQL. | `@dag`/`@task` TaskFlow, one module per dataset, both built from a shared factory to avoid duplicating the 5-step graph |
| `dags/_common/` (this project's shrunk equivalent) | Anything genuinely shared across the two dataset DAGs: the file-wait sensor/trigger, a `build_dataset_dag()` factory, XCom (de)serialization helpers | Plain Python module, imported by both DAG files — mirrors the reference repo's `_common/` role minus every K8s-pod-specific file (`kpo.py`, `tracing_kpo.py` are explicitly out of scope) |
| `csv_processor.config` | Load + validate `config.json` **once per run** via Pydantic v2; the single source of truth for file pattern, dialect, schema, target tables | `DatasetConfig(BaseModel)` + `load_config(path) -> DatasetConfig`, raises `ConfigurationError` (a plain project-local exception, not imported from anywhere) on invalid config |
| `csv_processor.detect` | Sniff compression / encoding / dialect / header **once**, from a small bounded sample, before any row is streamed | Tier-A vendored files (`dialect.py`, `encoding.py`, `header.py`, `filename.py`, `schema.py`, `compression.py`) with the 1-2 line `dataplat.errors` import swapped for a local exception module |
| `csv_processor.source` / `csv_processor.io` | Open the file, apply the detected profile, stream rows in bounded **record-count** chunks (never byte/line offsets) | Small orchestrator function following the reference `CsvSource`'s sequence (Tier B: read the algorithm, rewrite smaller) — no `Source`/`RecordStream` protocol, no schema-repository lookup, no multipart handling (all out of scope here) |
| `csv_processor.normalize` / `csv_processor.validate` | Per-chunk: convert CSV strings to typed Python values; check structural/type/nullability; split rows into valid/invalid with error metadata | Plain functions operating on `list[dict]` or `list[tuple]` per chunk — reimplemented per-stage logic from the reference's `normalize/*`/`validate/*` (Tier B), with none of the `StreamingStage`/`BarrierStage`/observability scaffolding |
| `csv_processor.process()` | The one public entrypoint: `process(file_path, config) -> ProcessingResult`. Owns the whole detect→parse→validate→normalize→chunk→load sequence and all status/exception translation | Orchestrator function; internally re-validates its `config` argument (cheap, once per file) so it is safely callable outside the DAG (tests, a future CLI) without relying on the DAG's `validate_config` task having already run |
| `csv_processor.load` (Oracle) | Bulk-insert each chunk's valid/invalid rows via `python-oracledb` `executemany()`; write the `ingestion_metadata` row | One connection per `process()` call (opened/closed inside the function, not held across DAG tasks) |
| Oracle Database Free | Durable store: `<DATASET>_VALID`, `<DATASET>_INVALID`, `ingestion_metadata` | DDL owned by this project (migration scripts or a bootstrap SQL file run at compose-up), pinned image tag |
| CSV generator | Deterministic fixture producer for both datasets (valid + invalid rows, all supported types) | Separate small script/package; depends only on the dataset schema shape, not on `csv_processor` |

## Recommended Project Structure

```
lightweight-airflow-etl/
├── docker-compose.yml                # Airflow (LocalExecutor) + Airflow metadata DB + Oracle Free
├── docker/
│   └── oracle/init/                  # DDL run at container init: VALID/INVALID/ingestion_metadata tables
├── configs/
│   └── datasets/
│       ├── customers.json            # config.json per dataset (this project's own contract shape)
│       └── orders.json
├── src/
│   └── csv_processor/                # the reusable engine — NO Airflow import anywhere in this tree
│       ├── config/
│       │   ├── models.py             # Pydantic v2: DatasetConfig, ColumnSpec, OracleTargetSpec
│       │   └── loader.py             # load_config(path) -> DatasetConfig
│       ├── detect/                   # Tier A: vendored from reference repo, import fixed
│       │   ├── dialect.py
│       │   ├── encoding.py
│       │   ├── header.py
│       │   ├── filename.py
│       │   └── schema.py
│       ├── compression.py            # Tier A: vendored, S3 stream swapped for plain open()
│       ├── source.py                 # Tier B: rewritten smaller — inspect() + chunked_records()
│       ├── normalize.py              # Tier B: reimplemented per-type conversion functions
│       ├── validate.py               # Tier B: reimplemented structural/type/nullability checks
│       ├── load.py                   # python-oracledb executemany() bulk loader
│       ├── models.py                 # ProcessingResult, RowError, Status enum
│       ├── errors.py                 # local exception hierarchy (replaces dataplat.errors)
│       └── engine.py                 # process(file_path, config) -> ProcessingResult — the public API
├── generator/
│   └── generate_csv.py               # deterministic valid+invalid row generator, both datasets
├── airflow/
│   └── dags/
│       ├── _common/
│       │   ├── dag_factory.py        # build_dataset_dag(dataset_name, config_path) shared by both DAGs
│       │   ├── sensors.py            # FileSensor(deferrable=True) wrapper / custom trigger if needed
│       │   └── xcom.py               # DatasetConfig <-> dict round-trip helpers
│       ├── customers_ingest.py
│       └── orders_ingest.py
└── tests/
    ├── unit/                         # config, detect, normalize, validate — no Airflow, no Oracle
    ├── integration/                  # csv_processor.load against a real Oracle container
    └── e2e/                          # HTTP trigger -> DAG -> Oracle VALID/INVALID tables
```

### Structure Rationale

- **`src/csv_processor/` has zero Airflow imports.** This is the single most important boundary in
  the whole system: every unit test for parsing/validation/normalization runs as plain Python,
  no Airflow scheduler, no DAG parsing, no metadata DB. The reference repo enforces the same
  direction with an import-linter contract (`csv_processor` may depend on `dataplat`, never the
  reverse) — this project doesn't need import-linter for two small packages, but the discipline
  (engine knows nothing about its orchestrator) is worth keeping.
- **`airflow/dags/_common/` is deliberately thin** compared to the reference repo's version. Only
  the file-wait mechanism and a DAG-factory function are genuinely shared; everything KPO-shaped
  (`kpo.py`, `tracing_kpo.py`, resource-sizing helpers, image-signing retry tuning) has no
  equivalent here because there is no Kubernetes.
- **`configs/datasets/*.json`, not YAML.** The spec calls the format `config.json`; the reference
  repo's `.yaml` shape (columns, types, nullability, business keys) is the right shape to mirror
  minus the `quality:`/`scd:`/`retention:` blocks, which map to explicitly out-of-scope validators.
- **`generator/` is independent of `csv_processor`.** It only needs to know the target schema
  (column names/types/nullable + which rows should be deliberately invalid), not the detection or
  validation code — this is what makes it buildable early and testable standalone.

## Architectural Patterns

### Pattern 1: Config validated once, rehydrated per task via XCom dict

**What:** `validate_config` is the DAG's first task. It calls `csv_processor.config.load_config`,
which raises on the first Pydantic validation error and never runs per-row. Its return value is
`config.model_dump(mode="json")` — a plain dict — because Airflow's XCom backend serializes to
JSON by default; a Pydantic model instance is not natively XCom-safe. Every downstream task that
needs the config calls `DatasetConfig.model_validate(config_dict)` to rehydrate it.

**When to use:** Any config/contract object that must cross a task boundary. Never pass ORM/Pydantic
model instances through XCom directly — pass their serialized dict form.

**Trade-offs:** A small amount of repeated `model_validate()` (cheap, once per task, not per row) in
exchange for keeping every task's input/output plain-JSON-serializable, which is required for
Airflow's default XCom backend and avoids surprises if the deployment later switches to a custom
XCom backend.

**Example:**
```python
@task
def validate_config(config_path: str) -> dict:
    config = load_config(Path(config_path))  # raises ConfigurationError on invalid config.json
    return config.model_dump(mode="json")

@task
def process_csv(config_dict: dict, file_path: str) -> dict:
    config = DatasetConfig.model_validate(config_dict)
    result = process(file_path, config)       # csv_processor's one public entrypoint
    return result.model_dump(mode="json")      # ProcessingResult, not raw row data, crosses XCom
```

### Pattern 2: File-wait is a class-based deferrable Sensor, not a `@task`

**What:** Airflow's deferral mechanism ("release the worker slot, resume later via a Trigger") is
**only available to traditional class-based operators/sensors — it cannot be used inside a
`@task`-decorated Python function** (confirmed in Airflow's own deferring docs). Airflow's
`airflow.providers.standard.sensors.filesystem.FileSensor` already supports `deferrable=True`
for the local filesystem and understands glob-style `filepath` patterns, so — unlike the reference
repo, which built this against S3 via `S3KeySensor` — this project does not need a hand-written
`BaseTrigger` subclass at all for the MVP:

```python
from airflow.providers.standard.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_default",
    filepath="/opt/airflow/data/customers/*.csv",
    deferrable=True,
    poke_interval=10,
)
```

**When to use:** Default to `FileSensor(deferrable=True)`. Only reach for a hand-rolled
`BaseTrigger` if a requirement emerges that `FileSensor` cannot express (e.g. "wait for a file AND
verify its checksum is new before waking the DAG" — idempotency is a project requirement, but it
can equally be checked inside `process_csv`, which already needs to read the file, rather than
inside the wait step). If a custom trigger does become necessary, organize it the way Airflow's own
provider packages do: a `triggers.py` module (the `BaseTrigger` subclass: `__init__`/`serialize`/
async `run`) is imported by a separate operator/sensor class, never inlined into the DAG file
itself — this keeps the DAG module itself thin and the trigger independently testable.

**Trade-offs:** `FileSensor`'s glob matching is coarse (existence only, no content inspection) —
acceptable here because idempotency/checksum logic already has to live in `process_csv` regardless
(a Sensor cannot easily also update Oracle's `ingestion_metadata` table before deferring).

### Pattern 3: `ProcessingResult`, not raw rows, is what crosses the DAG↔engine boundary

**What:** `process_csv`'s XCom payload is the aggregated `ProcessingResult` (total/valid/invalid
counts, duration, status) — never per-row invalid-row detail. The reference repo's `Receipt`
follows the identical discipline (`csv_processor/cli.py`'s `_write_xcom` always serializes a
`Receipt`-shaped or `{"status": ...}`-shaped payload, on every exit path, success or failure, never
a raw row dump) for exactly this reason: Airflow's metadata-DB-backed XCom is not sized for
row-level payloads, and a `report` task only ever needs the summary, not the data.

**When to use:** Always, for any pipeline whose "did it work" summary is orders of magnitude
smaller than the data it processed. Invalid-row detail belongs in Oracle's `<DATASET>_INVALID`
table (which already carries `error_code`/`error_message`/`source_file`/`row_number` per the
project's own requirements), not in an XCom payload.

**Trade-offs:** None significant here — this is a straightforward capacity/relevance match, not a
compromise.

## Data Flow

### Ingestion Flow (per DAG run)

```
HTTP POST /dags/{dag_id}/dagRuns  { conf: {dataset, config_path} }
    ↓
validate_config  →  DatasetConfig (validated once) → XCom (dict)
    ↓
wait_for_file  (deferred; FileSensor glob-matches configured pattern; resumes on match)
    ↓
process_csv  (in-process function call, LocalExecutor worker)
    │
    ├─ csv_processor.detect  (once: compression → encoding → dialect → header, bounded sample)
    ├─ csv_processor.source  (open file, apply detected profile)
    ├─ per chunk (bounded row count):
    │     parse → normalize (typed conversion) → validate (structural/type/nullable)
    │     → split into valid rows / invalid rows (+error metadata)
    │     → executemany() bulk insert into <DATASET>_VALID / <DATASET>_INVALID
    ├─ compute file checksum, upsert ingestion_metadata (idempotency key: filename+checksum+dataset)
    └─ return ProcessingResult(status, total, valid, invalid, duration)
    ↓
load_results  (XCom: ProcessingResult dict only)
    ↓
report  (log/summarize; no further DB writes)
```

### Key Data Flows

1. **Config flow:** `config.json` (disk) → `DatasetConfig` (validated once, in `validate_config`) →
   dict over XCom → rehydrated in every downstream task that needs it. Config never touches Oracle
   or the CSV file directly; it only parameterizes the engine call.
2. **Row flow:** CSV file (disk) → `csv_processor` in bounded chunks (never fully materialized in
   memory) → typed rows → Oracle, via `executemany()` array binding, chunk by chunk. Rows never
   cross an Airflow task boundary — only the file *path* does (into `process_csv`) and only the
   *summary* does (out of `process_csv`).
3. **Idempotency flow:** filename + file checksum + dataset is computed once, inside `process_csv`
   (it already has the file open), checked/recorded against Oracle's `ingestion_metadata` table
   before any row is bulk-loaded — a retried task or a re-encountered file short-circuits here
   rather than re-inserting.

## Scaling Considerations

This project's explicit target is "a single generated CSV file, ~100K rows, one Oracle Free
instance, one Airflow worker" — the table below is deliberately narrow in range; the spec has no
ambition beyond this.

| Scale | Architecture Adjustments |
|-------|---------------------------|
| Small files (≤ a few MB, low thousands of rows) | Default chunk size (e.g. 1,000 rows) is already generous; no tuning needed |
| ~100K rows (the project's own benchmark target) | Chunk size and `executemany()` array size become the two levers that matter — this is exactly what the required benchmark (row-by-row vs chunked/bulk) is meant to demonstrate; peak memory should track `chunk_size × row width`, not file size |
| Beyond this project's scope (millions of rows, multiple concurrent files) | Would need streaming backpressure, parallel file processing (Dynamic Task Mapping, as the reference repo does with KPO), and a real object store — all explicitly out of scope here |

### Scaling Priorities

1. **First (and only) bottleneck this project should hit:** Oracle round-trip count. Chunked
   `executemany()` array binding is the whole mitigation — never a per-row `INSERT` in a loop. The
   required benchmark test exists specifically to make this measurable.
2. **Not a real concern at this scale:** Airflow scheduler/worker throughput, XCom backend size
   (payloads are summaries, not row data — see Pattern 3), concurrent DAG runs (LocalExecutor,
   two datasets, one file each, no fan-out).

## Anti-Patterns

### Anti-Pattern 1: Business logic inside the `@task` function

**What people do:** Inline CSV parsing, type conversion, or `INSERT` statements directly inside a
DAG's `@task`-decorated function "because it's quick."
**Why it's wrong:** Makes the logic untestable outside Airflow (no DAG parsing/scheduler needed for
a unit test), breaks the "thin TaskFlow DAG delegates to a reusable engine" architecture the spec
requires, and silently duplicates logic once a second dataset DAG is added.
**Do this instead:** Every DAG task body is a thin wrapper calling into `csv_processor`. The task's
only jobs are: shape inputs from XCom, call the engine function, shape the engine's return value
back into XCom.

### Anti-Pattern 2: Detecting encoding/dialect/header per chunk

**What people do:** Re-run dialect/encoding/header sniffing on every chunk "to be safe."
**Why it's wrong:** Wasteful (a chunk is a slice of one already-opened, single-dialect file) and can
produce **inconsistent** results chunk-to-chunk if a heuristic sniffer's confidence varies with
sample content — the header row exists exactly once, at the top of the file, not once per chunk.
**Do this instead:** Run detection exactly once, from one small bounded sample (the reference repo
uses a fixed 64 KiB sample), before any `RecordStream`/chunk iterator is constructed. Every chunk
then reuses the same resolved dialect/encoding/header profile.

### Anti-Pattern 3: Chunking by byte offset or line count instead of record count

**What people do:** Split a file into chunks by seeking to byte offsets, or by counting `\n`
characters, to "resume" or parallelize reading.
**Why it's wrong:** A CSV field can legally contain an embedded newline inside a quoted value —
splitting by byte/line position can land mid-record, silently corrupting or truncating a row. This
is a documented, named pitfall in the reference repo (its own "PITFALLS.md E1").
**Do this instead:** Chunk by **record ordinal**, using the stdlib `csv.reader` as the sole
row-boundary authority (e.g. `itertools.batched(reader, chunk_size)`), and never claim resumability
from an arbitrary byte/line offset — resuming means re-streaming from the start and discarding
already-processed whole records, not seeking.

### Anti-Pattern 4: Unbounded `csv.field_size_limit`

**What people do:** Leave Python's default CSV field-size limit alone, or set it very high "to
avoid errors on wide fields."
**Why it's wrong:** A single malformed/unterminated quote can make `csv.reader` treat the rest of
the file as one giant field, growing without bound until the process is OOM-killed.
**Do this instead:** Set an explicit, documented `csv.field_size_limit` (e.g. 1 MiB) before
constructing the reader, sourced from the dataset's own config rather than a magic constant buried
in code.

### Anti-Pattern 5: Validating `config.json` with Pydantic on every row

**What people do:** Wrap each parsed CSV row in the same Pydantic model used for the dataset
contract, relying on its `ValidationError` to drive row-level rejection.
**Why it's wrong:** Pydantic model construction has real per-instance cost at 100K-row scale, and
critically, Pydantic **raises** on the first invalid field rather than collecting every violation —
this fights the "collect every invalid row with its error metadata, keep going" model the spec
requires for the `<DATASET>_INVALID` table.
**Do this instead:** Pydantic v2 validates `config.json` itself, exactly once per run (Pattern 1).
Per-row structural/type/nullability checks are plain, fast functions that **collect** violations
into a row-level error record rather than raising.

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|----------------------|-------|
| Airflow REST API | External HTTP trigger, `POST /dags/{dag_id}/dagRuns` with a JSON `conf` (dataset name + config path) | No custom FastAPI wrapper — explicitly out of scope; Airflow's own stable REST API is the trigger surface |
| Oracle Database Free | `python-oracledb`, thin mode (no Oracle Client install), one connection opened/closed per `process()` call | Bulk insert exclusively via `cursor.executemany()` array binding — Oracle has no `COPY` equivalent; connection lifecycle is scoped to the engine call, not held across DAG tasks |
| Local filesystem | `FileSensor(deferrable=True)` glob-matches the dataset's configured file pattern under a mounted volume | No object store (MinIO explicitly out of scope) — files land on a Docker-mounted local path readable by both the generator and the Airflow worker |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|----------------|-------|
| DAG ↔ `csv_processor` | Direct in-process Python function call (`process(file_path, config)`), input/output shaped as plain dicts at the task boundary for XCom | No network hop, no serialization protocol beyond XCom's own JSON — this is the biggest structural simplification versus the reference repo's KubernetesPodOperator pattern |
| `csv_processor.engine` ↔ `csv_processor.load` (Oracle) | Direct function call, chunk-by-chunk, inside one `process()` invocation | `load.py` has no knowledge of Airflow, config-loading, or CSV parsing — it only accepts already-validated/normalized rows plus target table names |
| `csv_processor` ↔ generator | None — deliberately zero coupling; the generator only needs the dataset schema shape (columns/types/nullability), which both it and `csv_processor.config` derive independently from the same `config.json` | Keeps the generator buildable and testable before the processing engine exists |

## Suggested Build Order

Ordered by genuine dependency, not by feature checklist order in the spec:

1. **Oracle schema + docker-compose environment.** Everything downstream needs a real Oracle
   instance to test against sooner or later (the project's own DoD requires integration tests
   against a real container, not mocks); getting the pinned image tag, connection, and
   `<DATASET>_VALID`/`<DATASET>_INVALID`/`ingestion_metadata` DDL working first removes the
   biggest infrastructure unknown early.
2. **`csv_processor.config`: Pydantic v2 models + `config.json` shape for both datasets.** This is
   also what the CSV generator needs (schema shape), so it unblocks two branches of work at once
   and has no dependency on Oracle or on the detection/parsing code.
3. **`csv_processor.detect` (Tier A vendoring) + `csv_processor.source`/parse/normalize/validate
   (Tier B rewrite), fully unit-testable with local fixture files, no Airflow, no Oracle.** This is
   the core engineering the project exists to prove out — build and test it standalone before
   wiring anything else to it.
4. **CSV generator**, built in parallel with or right after step 2 (only needs the schema shape) —
   needed before any real end-to-end run exists to test against, and useful for step 3's own test
   fixtures too.
5. **`csv_processor.load` (Oracle bulk loader via `executemany()`) + `ingestion_metadata`
   idempotency logic**, wiring steps 1 and 3 together. First point where integration tests against
   a real Oracle container become meaningful.
6. **`csv_processor.engine.process()`**: the public entrypoint assembling steps 2-5 into one
   function with the full `ProcessingResult`/status-enum contract. This is the exact function the
   DAG will call — it should be complete and tested *before* any DAG code exists, since the DAG is
   "thin" by design and has nothing to wrap otherwise.
7. **Airflow DAG wiring**: the shared `_common` factory, the `FileSensor(deferrable=True)` wait
   step, and the two thin per-dataset DAGs calling `process()`.
8. **HTTP trigger wiring + end-to-end test** (Airflow REST API → DAG → Oracle tables).
9. **Benchmark (row-by-row vs chunked/bulk) + CI**, once the whole path is real and stable enough
   to measure meaningfully.

**Why this order, explicitly:**
- The csv engine (steps 2-6) is entirely Airflow-agnostic and should be fully built and unit-tested
  *before* any DAG wiring — building DAG plumbing first would have nothing real to call.
- The generator only depends on the schema/config shape (step 2), not on the processing engine
  itself, so it can and should be pulled forward rather than sequenced after the engine.
- Oracle schema (step 1) has to exist before the bulk-load code (step 5) can be written against it,
  but does not block the pure-Python detect/parse/normalize/validate work (step 3), which needs no
  database at all — these two branches can proceed in parallel once config models exist.

## Sources

- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/source.py` — read
  for the detect→open→chunk sequence (Tier B, per this project's PROJECT.md reuse decision); not
  vendored as a file (fully coupled to `dataplat`'s `Source`/`SchemaRepository`/`RecordChunk`
  protocol, which has no equivalent here).
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/cli.py` — read for
  the "structured result written on every exit path, success or failure" pattern this project's own
  `ProcessingResult`/status-enum requirement mirrors.
- `/home/user/projects/airflow-platform/airflow/dags/csv_ingest_customers.py` and
  `/home/user/projects/airflow-platform/airflow/dags/_common/` — read for the thin-DAG-delegates-
  to-an-engine shape; Kubernetes-pod-specific parts (`kpo.py`, `tracing_kpo.py`) excluded per
  PROJECT.md's own scope decision.
- `/home/user/projects/airflow-platform/configs/datasets/customers.yaml` — read for the real
  per-column config shape (types/nullable/required/format) this project's smaller `config.json`
  should mirror, minus `quality:`/`scd:`/`retention:` blocks.
- Apache Airflow docs (Context7, `/apache/airflow`, MEDIUM confidence — verify exact provider
  version/behavior against the pinned Airflow release at implementation time):
  - `airflow-core/docs/authoring-and-scheduling/deferring.rst` — deferral is exclusive to
    class-based operators/sensors, unavailable inside `@task` TaskFlow functions; custom triggers
    belong in a `triggers.py` module (`BaseTrigger` subclass: `__init__`/`serialize`/async `run`),
    separate from the operator/sensor class that uses them.
  - `providers/standard/docs/sensors/file.rst` — `airflow.providers.standard.sensors.filesystem.
    FileSensor` supports `deferrable=True` for the local filesystem out of the box; no custom
    trigger is needed for the basic file-wait step this project requires.

---
*Architecture research for: Lightweight Airflow CSV→Oracle ETL Platform*
*Researched: 2026-08-28*
