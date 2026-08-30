# Phase 5: Airflow DAG Wiring & Deferrable File-Wait - Context

**Gathered:** 2026-08-29
**Status:** Ready for planning

<domain>
## Phase Boundary

A single, config-driven Airflow TaskFlow DAG (`load_config` → `wait_for_file` → `process_csv` →
`load_results` → `report_result`) orchestrates ingestion for either dataset end-to-end, triggerable
over a single HTTP request to Airflow's own REST API (dataset name + config path passed as runtime
`conf`, never hard-coded into the DAG), with the file-wait step implemented as a non-blocking
deferrable operator that releases the worker slot while waiting. This phase does NOT reimplement
any CSV/Oracle logic — it calls Phase 4's already-built, already-tested `csv_processor.engine.
process(file_path, config) -> ProcessingResult` entrypoint, which internally owns the whole
detect→parse→validate→normalize→chunk→load(Oracle) sequence atomically and never raises (always
returns one of 7 closed `Status` values). Phase 6 (E2E test, benchmark, CI, docs) is out of scope
here.

</domain>

<decisions>
## Implementation Decisions

**Note:** this phase ran in `--auto` mode (single-pass, no interactive discussion). Each decision
below was auto-selected as the recommended option, resolved primarily from the literal text of
ROADMAP.md's Phase 5 success criteria, REQUIREMENTS.md's DAG-01..05, PROJECT.md's Active-scope
bullet, and the original spec (`lightweight-spec.md` §5/§6/§11/§12) — logged here for the
researcher/planner to act on without re-asking.

### DAG topology: ONE dag_id, not one-per-dataset

- **D-01 (resolves a research-doc inconsistency):** This phase builds **exactly one DAG
  definition/dag_id** (e.g. `csv_ingest`), fully parameterized by runtime `conf` (`dataset` name +
  `config` path) — **not** two separate DAG files/dag_ids (`customers_ingest.py`/`orders_ingest.py`)
  as `ARCHITECTURE.md`'s "Recommended Project Structure" sketch and its Component Responsibilities
  table suggest. — **Reversibility:** costly — splitting into two DAGs later means re-registering
  new dag_ids (breaking any existing trigger/monitoring integration) and duplicating the 5-task
  graph.
  **Rationale (literal-text resolution, not a judgment call):**
  - ROADMAP.md Phase 5 success criterion 4: *"The **identical DAG definition** runs successfully
    for both `customers` and `orders` **purely by passing different config**, with **no
    dataset-specific code branches** in the DAG."* — singular DAG, config-driven, not two DAGs from
    a shared factory.
  - REQUIREMENTS.md DAG-02: *"triggered via **a single** HTTP request... passing **dataset name**
    and config path as runtime conf"* — passing a dataset name via conf is only meaningful if the
    target dag_id doesn't already encode which dataset it is.
  - `lightweight-spec.md` §5: *"Do not hard-code the dataset directly into the DAG."*
  - `ARCHITECTURE.md`'s two-DAG-file structure is a Tier-B **misreading of the reference repo's own
    per-dataset DAG file layout** (which predates this project's explicit "identical DAG definition"
    requirement) — PROJECT.md's own D-16/Tier-B guidance says read the reference DAGs "only for the
    *sequence*... write a smaller orchestrator following that sequence," not copy their per-dataset
    file split. Treat `ARCHITECTURE.md`'s file-tree sketch as **superseded** by ROADMAP.md's literal
    success criteria on this one point; everything else in that sketch (thin DAG, `_common/` helper
    module, no Airflow imports in `csv_processor`) still stands.
  - The `airflow/dags/_common/` helper module (config XCom helpers, the file-wait sensor wrapper)
    remains useful even with one DAG — it's just no longer a "shared-by-two-DAG-factory" module,
    it's the one DAG's own supporting code.

### Task boundary: `process_csv` vs `load_results` given Phase 4's atomic `process()`

- **D-02:** `process_csv` calls `csv_processor.engine.process(file_path, config)` directly — this
  single call **already performs the entire Oracle load** (valid rows, invalid rows, and the
  `ingestion_metadata` upsert, one transaction, per Phase 4 D-02) and returns a `ProcessingResult`
  (`model_dump(mode="json")` for XCom, per `ARCHITECTURE.md` Pattern 3). **No task in this DAG may
  open a second Oracle connection or re-attempt any insert** — `load_results` must not duplicate
  `process()`'s already-committed work.
  **Recommended shape (Claude's discretion within this constraint — see below):** keep `load_results`
  as its own thin TaskFlow task (matching DAG-01's literal 5-task list, which existed before Phase
  4 locked `process()`'s atomicity) whose job is to receive the `ProcessingResult` XCom and prepare/
  pass through whatever `report_result` needs — not to touch Oracle again. If the planner finds
  this task is genuinely a no-op pass-through once implemented, that's expected and fine; the
  5-name pipeline in DAG-01/ROADMAP.md is an acceptance-criterion checklist of visible task names,
  not a mandate that every named step does non-trivial independent work.

### `process()`'s closed Status enum never raises — the DAG must not treat domain failures as task failures

- **D-03 (resolves Phase 3 D-23's explicitly-deferred question):** `process()` **never raises** for
  any of its 7 closed `Status` outcomes (`SUCCESS`, `SUCCESS_WITH_INVALID_ROWS`, `FILE_NOT_FOUND`,
  `INVALID_FILE`, `CONFIGURATION_ERROR`, `DATABASE_ERROR`, `PROCESSING_ERROR` — verified directly
  in `packages/csv-processor/src/csv_processor/engine.py`'s `process()` docstring: *"never raises;
  every exception this function's own sequence can produce is caught and translated into a status
  instead"*). Therefore **`process_csv` must not raise `AirflowFailException` (or let any exception
  propagate) for a known-Status outcome** — every closed-enum result, success or failure, must flow
  through `load_results` → `report_result` so DAG-04's *"a completed run's logs/report show...
  status"* success criterion holds even for `INVALID_FILE`/`CONFIGURATION_ERROR`/`DATABASE_ERROR`
  runs. The Airflow task itself should only genuinely FAIL (retry/alert) on a truly unexpected,
  un-caught exception — a real bug, not a modeled domain outcome. — **Reversibility:** costly —
  flipping this later (making domain failures raise `AirflowFailException`) changes what "a
  completed run" means for every existing report/monitoring consumer.

### File-wait: stock `FileSensor(deferrable=True)`, no custom `BaseTrigger`

- **D-04 (resolves STATE.md's flagged Phase 5 research question):** Use Airflow's own
  `airflow.providers.standard.sensors.filesystem.FileSensor(deferrable=True)` for `wait_for_file` —
  **not** a hand-rolled `BaseTrigger`. Confirmed sufficient: `FileSensor` supports glob-style
  `filepath` matching (per `ARCHITECTURE.md` Pattern 2, Context7-verified against Airflow docs), and
  this project's actual `file_pattern` values (`customers_*.csv*`, `orders_*.csv*` — verified in
  `configs/datasets/{customers,orders}.json`, already widened for compressed variants per Phase 3
  D-31) are plain shell-style globs, not the "optionally regular expressions" case the original
  spec flagged as a maybe. `lightweight-spec.md` §12 itself says "do not overengineer file waiting."
  A custom `BaseTrigger` is only warranted if a concrete requirement emerges that `FileSensor`
  can't express — none does here. **Note for the phase-researcher:** still confirm at
  implementation time that `FileSensor`'s `filepath` argument accepts Jinja templating so the
  dataset-derived path (`/opt/airflow/data/{{ dag_run.conf['dataset'] }}/...`) can be resolved from
  runtime `conf` — `dag_run.conf` isn't available until DAG-run time, and `FileSensor` is a
  class-based operator (not a `@task`), so this cannot rely on Python f-string interpolation at
  DAG-parse time the way a `@task`-decorated function's body could.
- **D-05:** File input path is **not** a `config.json` field — it's the already-locked convention
  from Phase 2 D-06: `/opt/airflow/data/<dataset>/` inside the container (`./data/<dataset>/` on the
  host, already mounted in `docker-compose.yml` specifically for this phase: *"D-06: generate_csv.py
  writes to ./data/<dataset>/ on the host; mounted here so Phase 5's DAG can read the same files
  from inside the container."*). The DAG derives this path from the `dataset` runtime-conf value,
  joined with the dataset's `config.json`'s `file_pattern` for the glob.
- **D-06:** `poke_interval`/`timeout` for the deferred sensor are Claude's discretion — no
  requirement pins exact values; a short `poke_interval` (e.g. 10s, per `ARCHITECTURE.md`'s own
  example) with no hard timeout (or a generous one) is reasonable since deferral already avoids
  worker-slot cost, so there's little pressure to fail fast on a genuinely late file.

### `report_result`: logs only, no external notification

- **D-07:** `report_result`'s "concise, human-readable summary" (DAG-04: dataset, file, row counts,
  duration, status) is satisfied via **Airflow task logging** (visible in Airflow's UI/API logs) —
  no Slack/email/external notification integration. Nothing in PROJECT.md's scope or the spec calls
  for external delivery, and PROJECT.md's Out of Scope explicitly limits observability to "logging
  only, no metrics/tracing platform."

### `load_config`: validate runtime conf, not just the config file

- **D-08:** `load_config` is responsible for validating **both** halves of the runtime `conf`
  payload (`dataset` name, `config` path) and loading/validating the referenced `config.json` via
  the existing Phase 2 `csv_processor.config.load_config(path) -> DatasetConfig` (raises
  `ConfigurationError` on invalid config, per `ARCHITECTURE.md` Pattern 1 — caught here and surfaced
  as a `CONFIGURATION_ERROR`-shaped early exit, consistent with D-03's "domain failures don't fail
  the task" rule). Return value crossing XCom to later tasks is the serialized dict
  (`config.model_dump(mode="json")`), never a live Pydantic instance — matches Phase 2/4's existing
  XCom convention.

### Carried forward from Phase 3 (`03-CONTEXT.md`)

- **D-09:** (= Phase 3 D-31) `file_pattern` in both dataset configs already matches compressed
  variants (`customers_*.csv*`, `orders_*.csv*`) — the file-wait glob needs no special-casing for
  `.gz`/`.zip`.
- **D-10:** (= Phase 3 D-08) `source_file`/`file_name` on `ProcessingResult` is the **basename**
  only — matches what `wait_for_file`'s glob resolves to.

### Carried forward from Phase 4 (`04-CONTEXT.md`)

- **D-11:** (= Phase 4 D-01) Re-processing an already-recorded file returns the **original recorded
  outcome** via `process()` itself (idempotency is handled entirely inside `process()`, not by any
  DAG-level check) — `process_csv` doesn't need its own idempotency logic.
- **D-12:** (= Phase 4 D-02) One Oracle connection per `process()` call, opened/closed inside the
  function — never held across DAG tasks. Reinforces D-02 above: no other task should open its own
  Oracle connection.

### Claude's Discretion

- Exact `dag_id` string, task function names/file layout inside `airflow/dags/` and
  `airflow/dags/_common/` (a `dag_factory.py`-style single-DAG builder is fine, an inline `@dag`
  function is fine — this project has no other DAG demanding a shared factory now that D-01 settles
  on one DAG).
- Whether `load_results` ends up as a genuinely separate function/task or is folded tightly into
  `process_csv`'s immediate next step in code — constrained only by D-02 (no second Oracle write)
  and DAG-01's literal task-name list (the Airflow UI/API should still show a `load_results` step).
- Exact Jinja-templated `filepath` expression for `FileSensor` and how `dag_run.conf` values reach
  it — implementation detail once D-04's Jinja-templating question is confirmed by the researcher.
- Whether `report_result` formats its summary as a plain log line, a structured JSON log, or both —
  no requirement pins the exact format beyond "concise, human-readable."

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (this phase's exact scope)
- `.planning/REQUIREMENTS.md` — DAG-01, DAG-02, DAG-03, DAG-04, DAG-05 full text (lines 70-86).
- `.planning/ROADMAP.md` §Phase 5 — goal, depends-on, and the 4 literal success criteria this
  phase's D-01/D-02/D-03 resolve against.
- `.planning/research/lightweight-spec.md` §5 "HTTP DAG Trigger", §6 "TaskFlow API", §9 "File
  Pattern Matching", §10 "File Availability", §11 "Deferrable Operator/Trigger", §12 "Do Not
  Overengineer File Waiting" — original spec language behind D-01/D-04/D-07.

### Prior-phase decisions this phase builds on
- `.planning/phases/02-config-contract-csv-generator/02-CONTEXT.md` — D-06 (`./data/<dataset>/` →
  `/opt/airflow/data/<dataset>/` mount, explicitly anticipating this phase's FileSensor).
- `.planning/phases/03-csv-processing-engine/03-CONTEXT.md` — D-08 (source_file = basename), D-23
  (flagged `AirflowFailException` as "Phase 5's own decision" — resolved here as D-03), D-31
  (`file_pattern` widened for compressed variants).
- `.planning/phases/04-oracle-bulk-load-idempotency-engine-entrypoint/04-CONTEXT.md` — D-01
  (idempotency handled inside `process()`), D-02 (one Oracle connection per `process()` call, whole
  call is one transaction).

### Architecture (read with D-01's correction applied)
- `.planning/research/ARCHITECTURE.md` — Pattern 1 (config validated once, XCom dict rehydration),
  Pattern 2 (`FileSensor(deferrable=True)`, class-based, deferral unavailable to `@task`), Pattern 3
  (`ProcessingResult`, not raw rows, crosses the DAG↔engine boundary). **Its "Recommended Project
  Structure" section's two-DAG-file (`customers_ingest.py`/`orders_ingest.py`) sketch is superseded
  by this phase's D-01 — build one DAG, not two.**
- `.planning/research/SUMMARY.md` §"Research Flags" — originally flagged the
  `FileSensor(deferrable=True)` glob-sufficiency question this phase's D-04 resolves.

### Actual code/config this phase integrates with (more authoritative than research sketches)
- `packages/csv-processor/src/csv_processor/engine.py`'s `process(file_path: Path, config:
  DatasetConfig) -> ProcessingResult` — the exact, already-implemented entrypoint signature and
  behavior (never raises; see docstring) this phase's `process_csv` task calls.
- `packages/csv-processor/src/csv_processor/models.py` — `Status` enum (7 closed members) and
  `ProcessingResult`'s exact fields (`status`, `dataset`, `file_name`, `total_rows`, `valid_rows`,
  `invalid_rows`, `duration_seconds`, `checksum`) — the exact shape `report_result` formats.
- `configs/datasets/customers.json`, `configs/datasets/orders.json` — real `file_pattern` values
  the file-wait sensor must glob against.
- `docker-compose.yml` — the `./airflow/dags:/opt/airflow/dags` and `./data:/opt/airflow/data`
  mounts this phase's DAG file and file-wait path depend on.
- `airflow/dags/.gitkeep` — currently the only file in this phase's target directory; no prior DAG
  code exists yet.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `csv_processor.engine.process()` — the complete, tested (Phase 4) entrypoint; `process_csv` is a
  thin wrapper around this one call, nothing more.
- `csv_processor.config.load_config(path) -> DatasetConfig` (Phase 2) — reused directly by
  `load_config`, already raises `ConfigurationError` on bad config.
- Phase 2's XCom-dict convention (`config.model_dump(mode="json")` / `DatasetConfig.model_validate`)
  and Phase 4's `ProcessingResult.model_dump(mode="json")` — both already Pydantic `frozen`/
  `extra="forbid"` models designed to round-trip through XCom safely (per `models.py`'s own
  docstring, explicitly written with ARCHITECTURE.md Pattern 3 in mind).

### Established Patterns
- Every existing Pydantic model in this codebase (`config/models.py`, `models.py`) is
  `frozen=True`/`extra="forbid"` and fail-loud — this phase's DAG-level code should keep the same
  discipline where it touches config/result objects, not introduce silent defaults.
- `csv_processor` has zero Airflow imports anywhere (verified pattern from Phases 2-4) — this
  phase's `airflow/dags/` code is the first and only place Airflow imports are allowed to appear.

### Integration Points
- `wait_for_file`'s glob path is built from `dag_run.conf['dataset']` joined with
  `/opt/airflow/data/<dataset>/` (D-05) and the dataset's own `file_pattern` (read from the loaded
  config, D-08) — this is the one place DAG code needs to know the file-location convention.
- `process_csv` → `csv_processor.engine.process()` is the single integration point into the entire
  Phase 3/4 engine; no other module in `csv_processor` should be imported directly by DAG code.

</code_context>

<specifics>
## Specific Ideas

No user-specific vision beyond what's already locked in ROADMAP.md/REQUIREMENTS.md/PROJECT.md — this
phase's shape was substantially pre-settled before Phase 1 began, this discussion's job was
resolving one real inconsistency (D-01: one DAG vs. two) between the pre-settled research sketch and
the pre-settled literal requirements text, plus locking a few implementation-boundary questions the
requirements left open (D-02/D-03/D-06/D-07) or explicitly deferred (D-04, flagged in STATE.md;
D-23 from Phase 3, resolved as D-03 here).

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope (single `--auto` pass, no new scope surfaced).

### Reviewed Todos (not folded)
None — `todo.match-phase` returned zero matches for Phase 5.

</deferred>

---

*Phase: 5-Airflow DAG Wiring & Deferrable File-Wait*
*Context gathered: 2026-08-29*
