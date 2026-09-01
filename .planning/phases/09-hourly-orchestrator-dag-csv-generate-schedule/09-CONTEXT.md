# Phase 9: Hourly Orchestrator DAG (`csv_generate_schedule`) - Context

**Gathered:** 2026-09-01
**Status:** Ready for planning

<domain>
## Phase Boundary

A new `csv_generate_schedule` DAG runs `@hourly` (`catchup=False`, `max_active_runs=1`) and, fully
unattended, generates a fresh correlated customers+orders CSV pair, then sequentially chain-triggers
`csv_ingest` (customers → orders) and `report_ready` via `TriggerDagRunOperator`, then cleans up
CSVs older than the retention window. `csv_ingest.py` and `report_ready.py` stay byte-for-byte
unmodified and independently triggerable (SCHED-06, locked) — this phase only adds a new DAG file
that orchestrates them from the outside. Does NOT include the `OraclePartitionReadyTrigger`
exception-handling fix (Phase 10) or renaming any existing DAG.

</domain>

<decisions>
## Implementation Decisions

### Generation Params (SCHED-08)

- **D-01:** One shared `rows` Param drives both datasets' row counts (mirrors
  `generate_correlated_datasets(customers_rows=args.rows, orders_rows=args.rows)` — the CLI has no
  asymmetric-row-count path today, and introducing one would mean either modifying
  `generate_csv.py`'s stable `--correlated` contract or bypassing the subprocess-invocation pattern
  Pitfall 6 recommends). Default `rows=100` (matches `generate_csv.py`'s own `--rows` default).
- **D-02:** `invalid_ratio` is also exposed as a DAG Param (default `0.1`, matching the CLI
  default) — satisfies SCHED-08's literal "row counts AND invalid-ratio" wording.
- **D-03:** The generate task invokes `generator/generate_csv.py` via `subprocess.run([sys.executable,
  "/opt/airflow/generator/generate_csv.py", "--correlated", "--rows", ..., "--invalid-ratio", ...,
  "--seed", ..., "--compress"], check=True, capture_output=True, text=True)` — per PITFALLS.md
  Pitfall 6, never as a Python import (no `__init__.py`, not on `PYTHONPATH`, and importing would
  add pointless surface area for a script that's deliberately a standalone CLI).

### Per-run uniqueness / seed (SCHED-02, Pitfall 1)

- **D-04:** Seed is derived from the DAG run's own `logical_date`, not wall-clock time at execution:
  `int(logical_date.strftime("%Y%m%d%H"))`, obtained via `get_current_context()["dag_run"].logical_date`
  — the exact same `get_current_context()` access pattern `csv_ingest.py`'s `load_config_task`
  already uses (no new API surface). Confirmed against Airflow 3.3.1's own TaskFlow docs (WebSearch,
  2026-09-01): `get_current_context()["dag_run"].logical_date` is the documented way to reach it.
  **Consequence:** different real hours always get different seeds (satisfies SCHED-02); a manual
  retry/re-trigger of the SAME scheduled hour reproduces identical content — consistent with this
  project's existing `--seed` philosophy (determinism for a given identity) and keeps a specific
  hour's failure reproducible for debugging.

### Compression

- **D-05:** The generate task's subprocess call includes `--compress` (gzip). Verified safe with
  zero config changes: `configs/datasets/{customers,orders}.json`'s `file_pattern` is already
  `"customers_*.csv*"` / `"orders_*.csv*"` (the trailing `*` after `.csv` already matches `.gz`),
  and `packages/csv-processor/src/csv_processor/compression.py` already handles decompression —
  this path has just never been exercised via the scheduled route before.

### Chain-trigger retry/collision policy (Pitfall 7)

- **D-06:** Each of the three `TriggerDagRunOperator` tasks (customers, orders, `report_ready`)
  sets an explicit, deterministic `trigger_run_id` derived from the parent run's own identity (e.g.
  `"{{ dag_run.run_id }}__customers"`, Jinja-templated) — fixes Pitfall 7's finding that the
  auto-generated `run_id` is wall-clock-derived at *task-attempt* time, not retry-idempotent.
- **D-07:** `skip_when_already_exists=True` on all three trigger tasks — a retry that targets an
  already-existing child `run_id` quietly treats it as the intended target rather than raising
  `DagRunAlreadyExists`. Consistent with this project's checksum-keyed-idempotency philosophy (a
  retry is a safe no-op, not a visible failure). `reset_dag_run` stays unset/`False` per the
  Out-of-Scope table (documented upstream Airflow bug when combined with `deferrable=True` +
  explicit `trigger_run_id`).
- **D-08:** All three trigger tasks use `deferrable=True` (already locked at ROADMAP level,
  consistent with `FileSensor(deferrable=True)` in `csv_ingest` and `ReportReadySensor` in
  `report_ready`) alongside `wait_for_completion=True`.

### Task retry policy (new DAG's own tasks)

- **D-09:** `csv_generate_schedule`'s tasks use `retries=0` (Airflow default, explicit) — no
  automatic retries. Matches this project's established "fail loudly and immediately" instinct
  (SCHED-05's `fail_when_dag_is_paused=True`, the general no-silent-masking discipline) — any
  failure surfaces as a visible red DagRun right away; the next hourly schedule tries fresh an hour
  later regardless. (D-07's `skip_when_already_exists=True` still governs what happens on a *manual*
  clear/retry of a trigger task specifically.)

### Stuck-cycle timeout (Pitfall 8)

- **D-10:** `csv_generate_schedule` sets `dagrun_timeout=timedelta(minutes=45)` — leaves comfortable
  margin before the next hourly schedule fires while catching a genuinely hung chain before it eats
  into the next cycle's queue slot. Combines with the already-locked `max_active_runs=1` (SCHED-04),
  which queues rather than races a slow hour's overlap with the next.

### Auto-unpause on creation

- **D-11:** `csv_generate_schedule` follows this project's existing convention (no
  `is_paused_upon_creation` override) — `docker-compose.yml`'s
  `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false` (a Phase 6 fix) already makes every DAG
  auto-unpause on first parse, and this milestone's own goal is eliminating manual steps, not adding
  one (a deliberate opt-in-to-unpause step would contradict that).

### Cascade summary line (SCHED-07)

- **D-12:** A dedicated summary task **re-queries Oracle directly** (reads `ingestion_metadata`) for
  each dataset's row counts, rather than pulling XCom from the triggered `csv_ingest`/`report_ready`
  DagRuns — looser coupling to those DAGs' internal task/XCom shape, mirrors how `report_ready`
  itself already works (poll Oracle state, don't reach into another DAG's internals).
- **D-13:** The summary task selects **the latest `processed_at` row per dataset** from
  `ingestion_metadata` (no checksum-matching/XCom-threading needed) — safe because `max_active_runs=1`
  (SCHED-04) already guarantees no other hourly cycle can be concurrently writing
  `ingestion_metadata`, so "latest row" unambiguously means "this run's row." User explicitly
  confirmed: not solving late-arriving/out-of-order-event correctness in this milestone (may revisit
  with dbt later) — the correlated generator already guarantees customers+orders come from the same
  run (Phase 7's shared-RNG decision), so there's no cross-dataset ordering ambiguity to hedge
  against right now.
- **D-14:** The summary line's `report_ready` component is a **simple heartbeat** (`report_ready=OK`),
  not a re-run of the `customers⋈orders` business-report SQL. The summary task runs with
  `trigger_rule="none_failed_min_one_success"` (same convention as `csv_ingest.py`'s own
  `report_result_task`) downstream of the `report_ready` trigger step — reaching it at all means that
  step succeeded. **User explicitly rejected re-running the report SQL here**: it already exists in
  3 places (`report_ready.py`, `regenerate_readme_summary.py`, `verify_evidence.sql`); a 4th copy in
  this summary task would compound an existing duplication problem rather than solve anything —
  logic/scripts for the report should live in one place. See Deferred Ideas for the centralization
  follow-up.

### Retention (new requirement — bundled scope addition, Phase 8 D-04 precedent)

- **D-15:** **Scope addition, user-approved** — Phase 9 also adds a retention/cleanup task for
  `data/customers/` and `data/orders/`, following the same "bundle a related fix into the phase
  already touching this area" pattern as Phase 8's D-04 (which added ENV-03). This needs a new
  requirement ID during planning (e.g. `SCHED-09` or `RETAIN-01`) — exact ID is the planner's call,
  same as Phase 8 D-04's precedent.
- **D-16:** Retention window is **30 days** — deletes any dated CSV (`.csv` or `.csv.gz`) older than
  30 days from `data/customers/` and `data/orders/`.
- **D-17:** Cleanup runs as a **new task inside `csv_generate_schedule` itself**, at the end of each
  hourly cascade (after the report-ready summary) — one DAG, one place to look; no new DAG or
  external cron/Makefile mechanism.
- **D-18:** Cleanup is **best-effort — never fails the overall DagRun**. Generation/ingestion/report
  for the hour has already succeeded by the time cleanup runs; a housekeeping failure (e.g. a
  permission error deleting one old file) must not block the next hourly cycle via
  `max_active_runs=1`'s queuing. Log the error and move on rather than raising.

### DAG naming — explicitly NOT changed this phase

- **D-19:** `csv_ingest` and `report_ready` keep their current `dag_id`s. User raised renaming them
  to something more descriptive, but SCHED-06 (locked in REQUIREMENTS.md) requires both to stay
  **unmodified** and **independently triggerable** — renaming a `dag_id` is a modification. Per the
  user's own conditional framing (rename only if a single "master" DAG replaces standalone use;
  otherwise keep names) and because both DAGs remain independently used *and* orchestrated, current
  names stay. See Deferred Ideas — renaming would need a future phase/milestone with a
  REQUIREMENTS.md change explicitly authorizing touching those files.

### Claude's Discretion

- Exact REQ-ID for D-15's retention requirement (`SCHED-09` vs. a new `RETAIN-01` prefix) —
  planner's call, matching REQUIREMENTS.md's existing ID convention (same as Phase 8 D-04's
  precedent for `ENV-03`).
- Exact `poke_interval` for the three deferrable `TriggerDagRunOperator` tasks — no user-facing
  behavioral difference at this project's scale; pick a value consistent with `FileSensor`'s existing
  `poke_interval=10`.
- Exact retention-task implementation shape (glob + `Path.unlink()` loop vs. something more
  structured) — implementation detail once D-15/D-16/D-17/D-18 are locked.
- Exact wording/placement of `docs/airflow-dag.md` updates documenting the new DAG.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (this phase's exact scope)
- `.planning/ROADMAP.md` §Phase 9 — goal, depends-on (Phase 8, complete), the 8 literal success
  criteria (SCHED-01..08), and the "Implementation note" already locking `deferrable=True` for the
  three `TriggerDagRunOperator` tasks.
- `.planning/REQUIREMENTS.md` — SCHED-01 through SCHED-08 full text; SCHED-09 (FileSensor
  timeout-vs-hourly-period tightness) is a *different*, already-deferred item under "Future
  Requirements" — do not confuse with D-15's new retention requirement, which needs its own fresh ID.
  Out of Scope table: `reset_dag_run=True`, a derived `trigger_run_id` "for idempotency" (already
  solved one layer down by checksum-keyed idempotency), fan-out/parallel triggering, and modifying
  `csv_ingest.py`/`report_ready.py` are all explicitly excluded.

### Research (already resolved the deep technical questions — not open)
- `.planning/research/PITFALLS.md` — Pitfall 1 (seed must vary per run, drives D-04), Pitfall 6
  (subprocess invocation, not import, drives D-03), Pitfall 7 (retry-orphan `run_id` issue, drives
  D-06/D-07), Pitfall 8 (`max_active_runs`/`dagrun_timeout`, drives D-10).
- `.planning/research/SUMMARY.md` — "Open Decision — Not Resolved by Research" section on
  `TriggerDagRunOperator(deferrable=...)`, resolved in favor of Position B (`deferrable=True`) at
  ROADMAP level — already locked, not re-litigated here.
- `.planning/research/STACK.md` / `ARCHITECTURE.md` — `TriggerDagRunOperator` param surface
  (`trigger_run_id`, `wait_for_completion`, `poke_interval`, `allowed_states`/`failed_states`,
  `deferrable`), worker-slot semantics.

### Existing code this phase integrates with (more authoritative than research sketches)
- `airflow/dags/csv_ingest.py` — the DAG being chain-triggered; its `params` contract
  (`dataset: Param(..., enum=["customers","orders"])`, `config_path`) is what the new trigger tasks'
  `conf=` payload must match. Its `get_current_context()` import pattern (`from airflow.sdk import
  Param, dag, get_current_context, task`) is the exact pattern D-04's seed derivation reuses.
- `airflow/dags/report_ready.py` — the DAG being chain-triggered last; its `_BUSINESS_REPORT_SQL`
  is the query D-14 explicitly avoids re-running a 4th time.
- `airflow/dags/_common/paths.py` — `DATA_ROOT = Path("/opt/airflow/data")` convention; D-17's
  retention task operates under this same root (`DATA_ROOT / "customers"`, `DATA_ROOT / "orders"`).
- `airflow/dags/_common/reporting.py` — `format_summary_log()`'s existing one-line-summary
  convention; D-12/D-13/D-14's new summary task should follow the same shape/spirit (plain,
  unit-testable formatter + a thin task that calls it and logs).
- `generator/generate_csv.py` — `build_parser()` (the `--correlated`/`--rows`/`--invalid-ratio`/
  `--seed`/`--compress` CLI surface D-01-D-05 forward into), `output_path()` (confirms filenames are
  per-calendar-day, not per-hour — multiple hourly runs within one day overwrite the same dated
  path via `write_staged()`'s atomic rename; only the Oracle-side `ingestion_metadata` checksum
  changes per run, not the CSV filename itself).
- `configs/datasets/customers.json` / `orders.json` — `file_pattern: "customers_*.csv*"` /
  `"orders_*.csv*"` — confirmed (D-05) to already match a `.gz` suffix, no change needed for
  compression.
- `packages/csv-processor/src/csv_processor/compression.py` — existing decompression handling that
  makes D-05's `--compress` choice safe with zero other code changes.
- `docker-compose.yml` — `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"` (D-11's basis).

### Docs this phase should update
- `docs/airflow-dag.md` — needs a new section documenting `csv_generate_schedule` alongside the
  existing `csv_ingest` documentation.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `get_current_context()` (from `airflow.sdk`) — already the established way this codebase reaches
  runtime context (`csv_ingest.py`'s `load_config_task`/`route_after_config`) — D-04's seed
  derivation and the new summary task both reuse this, no new API surface.
- `_common/reporting.py`'s `format_summary_log()` pattern (plain function, zero Airflow import,
  called by a thin task) — direct template for D-12/D-13/D-14's new summary formatter.
- `_common/paths.py`'s `DATA_ROOT` constant and explicit-`Path`-constant style — template for any
  new path constants the retention task needs.

### Established Patterns
- "Fail loudly, don't silently mask" — SCHED-05's `fail_when_dag_is_paused=True`, D-09's
  `retries=0`, and D-19's refusal to silently rename locked DAG ids all echo this same instinct.
- "Never re-author the same SQL twice" (`report_ready.py`'s own docstring, re: `_BUSINESS_REPORT_SQL`
  already mirrored from `regenerate_readme_summary.py`/`verify_evidence.sql`) — directly drove D-14's
  rejection of a 4th copy.
- Checksum-keyed idempotency as the project's general safety net — informs D-07's
  `skip_when_already_exists=True` (a retry is expected to be a safe no-op, not an alarm).

### Integration Points
- The three `TriggerDagRunOperator` tasks are the sole integration point between the new DAG and the
  existing `csv_ingest`/`report_ready` DAGs — no other coupling.
- The new summary task (D-12/D-13) is a second, independent Oracle read path alongside
  `csv_ingest`/`report_ready`'s own — uses `csv_processor.load.get_connection()` the same way
  `report_ready.py`'s `build_report_task` already does.

</code_context>

<specifics>
## Specific Ideas

- User wants generated CSVs picked up as compressed (`.gz`) files by the ingestion pipeline going
  forward (D-05) — verified this requires zero config/engine changes, just adding `--compress` to
  the generate subprocess call.
- User explicitly does not want to solve late-arriving/out-of-order event correctness in this
  milestone ("we don't want to solve an issue of so called late events... might add dbt for that
  later") — drove D-13's simpler "latest row" query choice over checksum-matching.
- User strongly pushed back on duplicating the `customers⋈orders` report SQL a 4th time ("A
  script/logics for the report should be only in one place, not many") — drove D-14 and the SQL
  centralization deferred idea below.
- User wants a retention mechanism now, not deferred — explicit, deliberate scope addition (D-15),
  following the Phase 8 D-04 bundled-scope precedent the user was shown and accepted.

</specifics>

<deferred>
## Deferred Ideas

- **Centralize `_BUSINESS_REPORT_SQL` into one shared module.** Currently duplicated across
  `report_ready.py`, `regenerate_readme_summary.py`, and `verify_evidence.sql`. User flagged this as
  a real problem during Phase 9 discussion (D-14) but it's a refactor of *existing* code, not part
  of Phase 9's new-orchestrator-DAG scope — belongs in its own future phase.
- **Rename `csv_ingest`/`report_ready` to more descriptive DAG ids.** User raised this (D-19) but
  SCHED-06 locks both files as unmodified this milestone. Would need a future phase/milestone with
  an explicit REQUIREMENTS.md change authorizing modification of those files.

### Reviewed Todos (not folded)
None — `todo.match-phase 9` returned zero matches (no `.planning/todos/pending/` entries).

</deferred>

---

*Phase: 9-Hourly Orchestrator DAG (`csv_generate_schedule`)*
*Context gathered: 2026-09-01*
