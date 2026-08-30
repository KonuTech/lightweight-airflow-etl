# Phase 4: Oracle Bulk Load, Idempotency & Engine Entrypoint - Context

**Gathered:** 2026-08-29
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 3's validated/split rows (the `process_chunks()` generator's per-chunk `(valid_rows, invalid_rows)`
output) get bulk-loaded into Oracle via `python-oracledb` `executemany()`, an idempotency guard
(filename + checksum + dataset) prevents duplicate loads on retry/re-encounter, and the whole
detect→parse→validate→normalize→chunk→load sequence is assembled behind one public entrypoint,
`csv_processor.process(file_path, config) -> ProcessingResult`, which owns all status/exception
translation. This phase does NOT build the Airflow DAG that calls `process()` (Phase 5) or the
HTTP-trigger/benchmark/CI work (Phase 6).

</domain>

<decisions>
## Implementation Decisions

**Note:** this phase ran in `--auto` mode (single-pass, no interactive discussion). Each decision
below was auto-selected as the recommended option per `.planning/research/ARCHITECTURE.md`'s own
already-settled design, logged here for the researcher/planner to act on without re-asking.

### Re-run / idempotency behavior

- **D-01:** [auto] Re-processing an already-recorded file (same filename + checksum + dataset)
  returns `ProcessingResult` with the **original recorded outcome** (status, total/valid/invalid
  counts read back from `ingestion_metadata`), not a distinct new status — the project's status
  enum is closed (`SUCCESS` / `SUCCESS_WITH_INVALID_ROWS` / `FILE_NOT_FOUND` / `INVALID_FILE` /
  `CONFIGURATION_ERROR` / `DATABASE_ERROR` / `PROCESSING_ERROR`, per ENGINE-08/REQUIREMENTS.md) —
  adding an 8th "already processed" member is out of scope for this phase. — **Reversibility:**
  costly — adding a new status later means updating every consumer of the enum (Phase 5's DAG,
  any future CLI) to handle it.

### Per-file load atomicity

- **D-02:** [auto] A file's load is **all-or-nothing**: valid-row inserts, invalid-row inserts, and
  the `ingestion_metadata` upsert commit together in one Oracle transaction per `process()` call.
  If the load fails partway (e.g. connection loss mid-`executemany()`), nothing commits and the
  file is NOT marked processed — a retry re-attempts the full load rather than resuming from a
  partial/chunk-level checkpoint. — **Reversibility:** costly — a later move to
  partial-commit-with-resume needs a different metadata schema (per-chunk progress tracking), not
  just a code change. — **rationale:** matches ARCHITECTURE.md's "one connection per `process()`
  call" design; partial-commit-with-resume adds real complexity not currently requested or in
  scope.

### Oracle bulk-insert batch size

- **D-03:** [auto] `executemany()`'s array size reuses each dataset's existing `chunk_size` config
  value (currently 5000 for both `customers` and `orders`) rather than introducing a second,
  Oracle-specific batch-size config key. If Oracle's own `executemany()` sweet spot later proves to
  differ from the CSV-side chunk size, a separate key can be added without breaking this decision.

### Carried forward from Phase 1 (`01-CONTEXT.md`)

- **D-04 [informational]:** (= Phase 1 D-01) `<DATASET>_VALID`/`<DATASET>_INVALID` table shapes are
  already DDL'd (Phase 1's work, not a decision this phase makes); `_INVALID` carries original
  columns plus `ERROR_CODE`/`ERROR_MESSAGE`/`SOURCE_FILE`/`ROW_NUMBER`.
- **D-05:** (= Phase 1 D-04) `INGESTION_METADATA` already has a `UNIQUE(dataset, checksum)`
  DB-level constraint — the idempotency guard this phase's application-level check (D-02) sits
  **in addition to**, not instead of.
- **D-06:** (= Phase 1 D-05) `scripts/verify_environment.py`'s `oracledb`-based verification
  pattern is the precedent for this phase's own Oracle integration tests (TEST-02).

### Carried forward from Phase 3 (`03-CONTEXT.md`)

- **D-07:** (= Phase 3 D-11) `process_chunks(file_path, config) -> Iterator[tuple[list[dict], list[dict]]]`
  is the exact generator this phase's loader consumes chunk-by-chunk — valid rows as
  `{col_name: typed_value, ...}`, invalid rows as `{col_name: original_string, ..., error_code,
  error_message, source_file, row_number}`.
- **D-08:** (= Phase 3 D-23) A whole-file structural reject surfaces as a plain
  `StructuralValidationError` (or subclass) from the engine — `process()` is the function that
  catches it and translates it into `ProcessingResult(status=INVALID_FILE)`. `csv_processor` stays
  Airflow-agnostic; no exception type in this package may import Airflow.

### Claude's Discretion

- Exact SQL/parameter-binding order for `executemany()` calls against `<DATASET>_VALID`/
  `<DATASET>_INVALID` — implementation detail once the table DDL (already fixed) is read.
- Exact sequencing of "compute checksum → check `ingestion_metadata` → short-circuit or load →
  upsert metadata" within one transaction — implementation detail, constrained by D-02's
  atomicity requirement.
- Which Oracle exception types map to `DATABASE_ERROR` vs `PROCESSING_ERROR` — flagged in
  `STATE.md`'s Blockers/Concerns as needing a research pass (`setinputsizes()` type-derivation,
  `batcherrors` semantics); the researcher should resolve this before planning locks it down.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture (pre-settled design for this exact phase)
- `.planning/research/ARCHITECTURE.md` — `csv_processor.process()`'s full signature and
  responsibility ("owns the whole detect→parse→validate→normalize→chunk→load sequence and all
  status/exception translation"), `csv_processor.load`'s Oracle bulk-insert design
  (`executemany()`, one connection per `process()` call), the idempotency flow ("compute checksum,
  upsert ingestion_metadata... before any row is bulk-loaded"), and the proposed module layout
  (`load.py`, `models.py` with `ProcessingResult`/`RowError`/`Status` enum, `engine.py`'s
  `process()`).

### Prior-phase decisions this phase builds on
- `.planning/phases/01-environment-oracle-foundation/01-CONTEXT.md` — D-01 (table shapes), D-03
  (INTERVAL partitioning), D-04 (`UNIQUE(dataset, checksum)` constraint), D-05 (verification
  script pattern), D-07 (Oracle schema/user setup).
- `.planning/phases/03-csv-processing-engine/03-CONTEXT.md` — D-07 (row_number rule), D-09
  (invalid-row dict shape), D-11 (`process_chunks()` generator contract), D-12 (`error_code`
  taxonomy), D-23 (`StructuralValidationError` → `process()` translates to `INVALID_FILE`).

### Requirements
- `.planning/REQUIREMENTS.md` — LOAD-01, LOAD-02, LOAD-03, LOAD-04, ENGINE-08, TEST-02 (this
  phase's exact requirement text).
- `.planning/STATE.md` §Blockers/Concerns — flagged Phase 4 research items (`setinputsizes()`
  type-derivation, `batcherrors` semantics).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/verify_environment.py`'s `oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD,
  dsn=ORACLE_DSN)` pattern — the precedent for this phase's own Oracle connection setup.
- `packages/csv-processor/src/csv_processor/engine.py`'s `process_chunks()` — already yields
  exactly the `(valid_rows, invalid_rows)` per-chunk shape this phase's loader consumes directly,
  no adapter needed.
- `configs/datasets/customers.json`/`orders.json`'s existing `chunk_size: 5000` field — reused as
  the `executemany()` array size per D-03, no new config key needed.

### Established Patterns
- Phase 3's invalid-row dict shape (`error_code`/`error_message`/`source_file`/`row_number`) maps
  directly onto `<DATASET>_INVALID`'s extra columns from Phase 1's DDL — no field renaming needed
  at the load boundary.

### Integration Points
- New `process()` entrypoint (per ARCHITECTURE.md's proposed `engine.py`) calls
  `csv_processor.engine.process_chunks()` (existing, Phase 3) chunk-by-chunk, feeding each
  chunk's `valid_rows`/`invalid_rows` into a new Oracle loader module (`load.py` per
  ARCHITECTURE.md), and catches `csv_processor.errors`'s exception hierarchy (existing, Phase 3)
  to translate into `ProcessingResult` status codes.

</code_context>

<specifics>
## Specific Ideas

No specific requirements beyond ARCHITECTURE.md's already-detailed design — this phase's shape was
locked during the project's original research pass, before Phase 1 began.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope (single `--auto` pass, no new scope surfaced).

</deferred>

---

*Phase: 4-Oracle Bulk Load, Idempotency & Engine Entrypoint*
*Context gathered: 2026-08-29*
