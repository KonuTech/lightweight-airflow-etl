# Feature Research

**Domain:** Scheduled Airflow "orchestrator" DAG that regenerates fixture data and cascades into
existing config-driven ingestion DAGs (TriggerDagRunOperator chain-triggering)
**Researched:** 2026-09-01
**Confidence:** HIGH for Airflow operator mechanics (verified against `apache-airflow-providers-
standard` source at the exact pinned tag, `1.17.0`, and this project's own pinned `apache/
airflow:3.3.1-python3.12`); MEDIUM for open-bug applicability (some GitHub issues were filed
against older provider versions and fix-landing version isn't fully confirmed).

## Context recap (from files read)

- `csv_ingest` (`airflow/dags/csv_ingest.py`): `schedule=None`, `catchup=False`, two runtime
  `Param`s (`dataset` enum `["customers","orders"]`, `config_path` string). Internally: config
  load → branch → deferrable `FileSensor(deferrable=True, poke_interval=10, timeout=3600)` →
  `process_csv_task` → `load_results_task` → `report_result_task`. No `max_active_runs` set on the
  DAG (Airflow default applies). Domain failures (bad config, bad file) never raise — the task
  graph always reaches `report_result_task` and the DAG run still ends `success`.
- `report_ready` (`airflow/dags/report_ready.py`): `schedule=None`, `catchup=False`, no runtime
  params at all. Deferrable `ReportReadySensor` (custom `OraclePartitionReadyTrigger`, polls Oracle
  `ingestion_metadata` every 30s) → `build_report_task`. No `max_active_runs` set.
- `generator/generate_csv.py`: `output_path()` produces exactly one file per calendar day —
  `data/<dataset>/<dataset>_<YYYYMMDD>.csv` — **overwritten** on every call that day via
  `write_staged()`'s atomic staged-rename. Running the generator a second time in the same hour (or
  same day) replaces the prior file at the same path; it does not create a second, distinguishable
  file.
- Pinned versions: `apache/airflow:3.3.1-python3.12`, `apache-airflow-providers-standard==1.17.0`,
  `apache-airflow-providers-oracle==4.6.2`.

## Feature Landscape

### Table Stakes (users/operators expect these)

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| `@hourly`-scheduled parent DAG (`csv_generate_schedule`) with `catchup=False` | This is the literal ask — "no manual `make generate` step" | LOW | Standard `@dag(schedule="@hourly", catchup=False)`; `catchup=False` is non-negotiable here — with `catchup=True` a first deploy would immediately backfill every missed hourly interval since `start_date`, each one regenerating (overwriting) the *same* day's file and re-triggering downstream, which is pure wasted work for fixture data that has no real historical meaning. |
| `max_active_runs=1` on the **new parent DAG only** | Prevents two full generate→ingest→ingest→report cascades running concurrently if one cycle overruns into the next hour | LOW | This is a parameter on the *new* DAG, not a change to `csv_ingest`/`report_ready`. Directly motivated by a real collision risk found in this research: `csv_ingest`'s existing `wait_for_file` has `timeout=3600` — exactly one hour. If generation ever fails to produce a file, that FileSensor can occupy nearly the full hour before timing out, right up against the next scheduled cycle. Without `max_active_runs=1`, the next parent run could start generating (overwriting) the same day's CSV file out from under a `csv_ingest` run still in its `wait_for_file`/`process_csv_task` window from the previous cycle — a real race on the *same* filename. `max_active_runs=1` serializes cycles at the parent level and removes this risk entirely, at the cost of a run occasionally queuing rather than firing exactly on the hour. |
| A `generate_csv_task` (`@task` calling `generator.generate_csv`'s public functions directly, or `PythonOperator`/`BashOperator` wrapping the CLI) that runs `--correlated` generation | Produces the fresh customers+orders pair the rest of the chain depends on | LOW | Prefer calling `generate_correlated_datasets()` + `write_staged()` in-process (TaskFlow `@task`, mirrors `csv_ingest.py`'s own "thin DAG, no subprocess" style) over shelling out to the CLI via `BashOperator` — avoids a second process-invocation contract to maintain and keeps error handling in Python exceptions Airflow already understands. |
| Sequential `TriggerDagRunOperator` chain: generate → trigger `csv_ingest` (customers) → trigger `csv_ingest` (orders) → trigger `report_ready` | Matches the milestone's literal ordering requirement and the real data dependency (orders' `customer_id` FK-enforcing DB trigger needs `customers_valid` populated; `report_ready` needs both datasets' `ingestion_metadata` rows) | LOW–MEDIUM | Three separate `TriggerDagRunOperator` tasks (not a loop over a list) — keeps `conf={"dataset": ..., "config_path": ...}` per-call explicit and matches `csv_ingest`'s existing `Param` contract exactly. Chained with `>>`, each `wait_for_completion=True` (see below) so the DAG-level `>>` ordering is redundant-but-explicit documentation of the real dependency, not the actual blocking mechanism. |
| `wait_for_completion=True` on every `TriggerDagRunOperator` in the chain | Required for correct sequencing — orders must not start until customers' `csv_ingest` run has actually finished (not just been queued), and `report_ready` must not start until both `csv_ingest` runs have finished | LOW | Without this, `TriggerDagRunOperator` returns as soon as the triggered run is *created*, not when it *finishes* — the "cascade" would fire all three downstream runs almost simultaneously with no real ordering guarantee, defeating the point of chaining. |
| `deferrable=True` on every `TriggerDagRunOperator` in the chain | Matches this project's own established convention (`FileSensor(deferrable=True)`, custom `OraclePartitionReadyTrigger`) of never occupying a worker slot for a wait that can be minutes long | LOW | With `wait_for_completion=True` + `deferrable=True`, the operator defers to the stock `DagStateTrigger` (ships in `apache-airflow-providers-standard`) instead of blocking a LocalExecutor worker slot for the (potentially near-3600s, given `csv_ingest`'s FileSensor timeout) duration of the downstream run. |
| Leave `trigger_run_id` unset (`None`) on every call | Auto-generated run IDs are unique per invocation (timestamp-derived), so a fresh hourly cycle never collides with a prior cycle's run ID for the same target DAG | LOW | If left unset, there is no `DagRunAlreadyExists` scenario to handle in the normal/happy path — this sidesteps the entire `reset_dag_run` question. Do **not** synthesize a deterministic `trigger_run_id` (e.g. from the logical/schedule timestamp) unless there's a real reason to want idempotent re-triggering — see Anti-Features below. |
| Explicit `conf` payload matching `csv_ingest`'s existing `Param` contract | `csv_ingest` requires `dataset` + `config_path` as runtime conf; nothing about it changes for programmatic triggering vs. the existing HTTP-trigger path | LOW | `conf={"dataset": "customers", "config_path": "configs/datasets/customers.json"}` and the `orders` equivalent — identical shape to what a human/HTTP caller already passes. `report_ready` takes no conf at all (`schedule=None`, no `params={}` declared) — its `TriggerDagRunOperator` call needs no `conf` argument. |
| `fail_when_dag_is_paused=True` on every `TriggerDagRunOperator` in the chain | If `csv_ingest` or `report_ready` is ever manually paused by an operator, the parent should fail loudly and immediately rather than hang for up to `poke_interval`×many polls (or the task's `execution_timeout`, if any) waiting on a run that will never execute | LOW | Requires Airflow 3.2.0+ (this project pins 3.3.1, so it's available) — on Airflow 3.0/3.1 this parameter raises `NotImplementedError` if set, so this is version-sensitive; verify against the actually-pinned image tag before relying on it. Default is `False`, so it must be set explicitly. |
| Failure propagation: a failed `csv_ingest`/`report_ready` run fails the corresponding `TriggerDagRunOperator` task, which fails the parent DAG run | Standard, expected Airflow semantics — a cascade shouldn't silently report "success" if a downstream stage actually failed | LOW | Confirmed from the operator's own source: when `wait_for_completion=True`, after the triggered run finishes, the operator checks the final state against `allowed_states` (default `[SUCCESS]`) / `failed_states` (default `[FAILED]`); a `failed_states` match raises `AirflowException` in the parent task, which — with no non-default `trigger_rule` — fails the parent DAG run. No extra code needed to get this; it is the operator's built-in behavior, not something to opt into. |

### Differentiators (nice, not required for the milestone)

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Slack/email/logged summary of the full cascade at the parent level (e.g. "cycle N: customers X rows, orders Y rows, report ready") | Single glance at cascade health without opening three separate DAGs' logs | LOW | Mirrors the existing `report_result_task`/`build_report_task` "log only, no Slack/email" convention (D-07/D-27) — if added, should stay log-only for consistency, not introduce a new notification channel this project has explicitly avoided elsewhere. |
| Passing the parent's own trigger timestamp into the generator (e.g. as a seed component or log field) for cross-run traceability | Makes it possible to tie a specific hourly cascade back to the exact CSV it generated, useful for debugging a bad cycle | LOW | `generate_csv.py`'s existing `--seed` defaults to a fixed constant (`20260101`) for determinism; don't change generator determinism semantics to satisfy this — log the schedule timestamp alongside the (still-deterministic) generated file instead. |
| Configurable row counts / invalid-ratio per scheduled cycle (e.g. via `Param`s on the new parent DAG) | Lets an operator dial up/down synthetic load without editing code | LOW–MEDIUM | Optional convenience; `generate_correlated_datasets()` already accepts `customers_rows`/`orders_rows`/`invalid_ratio`/`seed` as parameters, so this is just exposing them as parent-DAG `Param`s, not new generator logic. |

### Anti-Features (seem good, cause real problems here)

| Feature | Why Requested | Why Problematic | Alternative |
|---------|---------------|------------------|-------------|
| `reset_dag_run=True` on any `TriggerDagRunOperator` in the chain | Looks like the "safe" way to handle "what if a run for this ID already exists" | Combined with `deferrable=True` + an explicit `trigger_run_id`, there is a documented, filed Airflow bug (apache/airflow#57756: "Deferrable mode of TriggerDagRunOperator stays stuck if used with `reset_dag_run`") where the triggering task remains permanently deferred even after the downstream run completes. Filed against `apache-airflow-providers-standard==1.9.0`; this project pins `1.17.0` and whether the fix (#57968) landed by then isn't confirmed from research — treat as still-risky. It is also simply unnecessary here: leaving `trigger_run_id` unset means no `DagRunAlreadyExists` case ever arises in normal operation, so there's nothing to "reset." | Leave `trigger_run_id=None` (table stakes above); never combine `reset_dag_run=True` with `deferrable=True`. |
| A deterministic/derived `trigger_run_id` (e.g. from the parent's logical date) "for idempotency" | Feels like it prevents duplicate downstream runs on retry | Retrying the parent's `generate_and_trigger` task would then call `TriggerDagRunOperator` again with the *same* `trigger_run_id` for a run that may already be in progress or finished, hitting `DagRunAlreadyExists` and forcing a `reset_dag_run=True`/`skip_when_already_exists=True` decision that this project doesn't need to make at all in the default (unset ID) path. `csv_ingest`'s own idempotency already comes from filename+checksum dedup at the `ingestion_metadata`/Oracle layer (v1.0, Phase 4) — a second trigger of the same day's file is already a safe no-op at that layer, so run-ID-level idempotency solves a problem that's already solved one layer down. | Leave `trigger_run_id` unset; rely on the existing checksum-based idempotency in `csv_processor`/`ingestion_metadata` if a retry re-triggers ingestion of the same file. |
| Fan-out / parallel triggering of `csv_ingest` for customers and orders simultaneously (e.g. two `TriggerDagRunOperator` tasks with no ordering edge between them) | Looks faster — "why wait for customers before starting orders?" | Phase 7's DB-level `BEFORE INSERT` trigger on `orders_valid` rejects any row whose `customer_id` doesn't yet exist in `customers_valid` — if orders' `csv_ingest` run starts loading before customers' run has committed, the whole orders batch fails at the DB layer on a data dependency that has nothing to do with Airflow scheduling. | Keep the customers → orders ordering strictly sequential via `wait_for_completion=True`, exactly as the milestone already specifies. |
| Modifying `csv_ingest.py`/`report_ready.py` to add a `dataset`-scoped `max_active_runs` guard, a "skip if already running" branch, or new params, in order to make chain-triggering "safer" | Feels like defense-in-depth against overlapping runs | The milestone explicitly requires both existing DAGs to remain unmodified and independently triggerable — any change here also risks breaking the still-live manual/HTTP-trigger path (`POST /dags/{dag_id}/dagRuns`) that v1.0 proved end-to-end. All the sequencing/overlap-prevention needed for this milestone is achievable entirely from the *new* parent DAG's own parameters (`max_active_runs=1`, `wait_for_completion=True`, `deferrable=True`) — see Table Stakes and Dependencies below. | Solve overlap/ordering entirely in the new `csv_generate_schedule` DAG; treat `csv_ingest.py`/`report_ready.py` as closed for this milestone. |
| `schedule="@hourly"` with `FileSensor`'s existing `timeout=3600` left unexamined, assuming "an hour is plenty of time" | The numbers *look* like they trivially fit (schedule period == sensor timeout) | They're the same number, not a comfortable margin — a slow generation step, a delayed cascade start (queued behind `max_active_runs=1`), or a genuinely late file write leaves zero slack before `wait_for_file` would time out right at the boundary of the next scheduled cycle. This is a real, structural tightness worth being aware of even though fixing `csv_ingest.py`'s sensor timeout is out of scope for this milestone (see Anti-Feature above). | Don't treat "schedule period == sensor timeout" as safe by inspection; rely on `max_active_runs=1` (Table Stakes) to contain the blast radius if a cycle does run long, and flag the 3600s/hourly coincidence for awareness rather than silently assuming it's fine. |

## Feature Dependencies

```
[generate_csv_task]
    └──produces file for──> [trigger csv_ingest(customers)]
                                  └──must commit customers_valid before──> [trigger csv_ingest(orders)]
                                                                                 └──required for──> [trigger report_ready]

[max_active_runs=1 on csv_generate_schedule]
    └──prevents──> [overlapping generate_csv_task writes to the same-day filename]

[wait_for_completion=True + deferrable=True]
    └──enables──> [correct sequential ordering without blocking a worker slot]

[trigger_run_id left unset]
    └──avoids needing──> [reset_dag_run] (conflicts with deferrable=True, see Anti-Features)

[fail_when_dag_is_paused=True]
    └──requires──> Airflow 3.2.0+ (available: pinned 3.3.1)
```

### Dependency Notes

- **`trigger csv_ingest(orders)` requires `trigger csv_ingest(customers)` to have finished (not just
  started):** Phase 7's `BEFORE INSERT` FK-existence trigger on `orders_valid` makes this a hard
  data dependency, not just a scheduling nicety — `wait_for_completion=True` is what turns "trigger
  is queued" into "trigger has actually finished" before the next step runs.
- **`max_active_runs=1` on the new parent DAG prevents `generate_csv_task` overlap:** because
  `generate_csv.py`'s `output_path()` always writes the same filename for "today," a second
  concurrent parent run's generation step would race the first cycle's still-in-flight
  `csv_ingest`/`report_ready` chain over that same file. This is the one new operational risk this
  research surfaced that isn't already handled by existing idempotency guarantees.
- **`reset_dag_run` conflicts with `deferrable=True`:** not a hard technical incompatibility in all
  cases, but a documented bug when combined with an explicit `trigger_run_id` and
  `wait_for_completion=True` — since this milestone has no actual need for `reset_dag_run` (see
  Anti-Features), simply never use it here rather than relying on an unconfirmed-fixed-by-1.17.0
  edge case.

## MVP Definition

### Launch With (v1.1, this milestone)

- [ ] `csv_generate_schedule` DAG, `schedule="@hourly"`, `catchup=False`, `max_active_runs=1` —
      required for the "no manual `make generate` step" goal and to close the same-filename race
      risk identified above
- [ ] `generate_csv_task` calling `generate_correlated_datasets()` + `write_staged()` in-process —
      required to actually produce fresh data each cycle
- [ ] Three sequential `TriggerDagRunOperator` tasks (customers `csv_ingest` → orders `csv_ingest`
      → `report_ready`), each `wait_for_completion=True`, `deferrable=True`,
      `fail_when_dag_is_paused=True`, `trigger_run_id` unset — required for correct ordering,
      worker-slot efficiency (matches project convention), and clean failure propagation
- [ ] No changes to `csv_ingest.py`/`report_ready.py` — required by the milestone's own stated
      scope; verified in this research that nothing about chain-triggering *needs* a change to
      either file

### Add After Validation (v1.x)

- [ ] Cascade-level summary logging at the parent DAG (mirrors existing `report_result_task`/
      `build_report_task` log-only convention) — add once the hourly cascade has run unattended
      long enough to know what's actually worth summarizing
- [ ] Configurable row counts/invalid-ratio via parent-DAG `Param`s — add if/when someone actually
      needs to vary synthetic load without a code change

### Future Consideration (v2+)

- [ ] Any fix to `csv_ingest.py`'s `wait_for_file` timeout being numerically equal to the new
      hourly schedule period — flagged here for awareness only; changing it is out of this
      milestone's scope (would touch a file this milestone must leave unmodified) and should be a
      deliberate, separately-scoped decision, not a side effect of adding the scheduler

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| `@hourly` parent DAG, `catchup=False` | HIGH | LOW | P1 |
| `max_active_runs=1` on parent | HIGH | LOW | P1 |
| In-process `generate_csv_task` | HIGH | LOW | P1 |
| Sequential `TriggerDagRunOperator` chain, `wait_for_completion=True` | HIGH | LOW | P1 |
| `deferrable=True` on chain operators | MEDIUM | LOW | P1 |
| `fail_when_dag_is_paused=True` | MEDIUM | LOW | P1 |
| Leave `trigger_run_id` unset (no `reset_dag_run`) | HIGH (avoids a known bug class) | LOW | P1 |
| Cascade summary logging | LOW–MEDIUM | LOW | P2 |
| Configurable row counts via `Param`s | LOW | LOW–MEDIUM | P3 |

**Priority key:**
- P1: Must have for this milestone
- P2: Should have, add when possible
- P3: Nice to have, future consideration

## Sources

- `airflow.providers.standard.operators.trigger_dagrun` source at the
  `providers-standard/1.17.0` tag (this project's exact pinned version) — fetched via
  `raw.githubusercontent.com`, HIGH confidence: full constructor parameter list (`trigger_dag_id`,
  `trigger_run_id`, `conf`, `logical_date`, `run_after`, `reset_dag_run`, `wait_for_completion`,
  `poke_interval`, `allowed_states`, `failed_states`, `skip_when_already_exists`,
  `fail_when_dag_is_paused`, `deferrable`, `openlineage_inject_parent_info`) and execution logic
  (DagRunAlreadyExists handling, deferred-vs-blocking wait flow, allowed/failed state evaluation
  and exception propagation on failure).
- [apache/airflow#57756 — "Deferrable mode of TriggerDagRunOperator stays stuck if used with
  `reset_dag_run`"](https://github.com/apache/airflow/issues/57756) — MEDIUM confidence: confirms
  the bug and its trigger conditions; filed against `apache-airflow-providers-standard==1.9.0`
  (older than this project's pinned `1.17.0`), fix PR #57968 exists but landing version not
  independently confirmed in this research.
- WebSearch: `fail_when_dag_is_paused` default value and Airflow 3.2.0+ version gate — MEDIUM
  confidence (search-engine synthesis citing the operator's own docs and
  [PR adding the parameter](https://github.com/apache/airflow/commit/96c6daa97c94b20b14ec5fa7f39de26b3f2d2559));
  consistent with the source-code read above.
- This project's own `.planning/PROJECT.md`, `airflow/dags/csv_ingest.py`,
  `airflow/dags/report_ready.py`, `airflow/dags/_common/oracle_partition_trigger.py`,
  `generator/generate_csv.py`, `docker/airflow/Dockerfile` — read directly, HIGH confidence, source
  of every "existing DAG behavior" and "pinned version" claim above.

---
*Feature research for: hourly CSV-generation-and-ingestion orchestrator DAG*
*Researched: 2026-09-01*
