# Phase 5: Airflow DAG Wiring & Deferrable File-Wait - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-29
**Phase:** 5-Airflow DAG Wiring & Deferrable File-Wait
**Areas discussed:** DAG topology, process_csv/load_results task boundary, domain-failure vs
task-failure semantics, deferrable file-wait mechanism, report_result output surface, load_config
responsibility

**Mode:** `--auto` (single-pass, autonomous). No `AskUserQuestion` calls were made; each area below
was resolved by Claude from the literal text of ROADMAP.md, REQUIREMENTS.md, PROJECT.md, the
original spec, and the actual committed code (Phase 3/4), not by interactive user choice.

---

## DAG topology: one DAG vs. per-dataset DAGs

| Option | Description | Selected |
|--------|-------------|----------|
| Two DAG files (`customers_ingest.py`/`orders_ingest.py`) from a shared factory | Mirrors `ARCHITECTURE.md`'s "Recommended Project Structure" sketch and the reference repo's own per-dataset DAG files | |
| One DAG definition/dag_id, dataset selected via runtime `conf` | Matches ROADMAP.md success criterion 4 ("identical DAG definition... purely by passing different config"), REQUIREMENTS.md DAG-02 ("passing dataset name... as runtime conf"), and spec §5 ("Do not hard-code the dataset directly into the DAG") | ✓ |

**Claude's choice:** One DAG (D-01). This is a correction to `ARCHITECTURE.md`'s own file-tree
sketch, which conflicts with the literal, more-authoritative requirements/roadmap text.
**Notes:** Reversibility rated `costly` — flagged for the planner to lock early rather than
discover mid-implementation.

---

## `process_csv` / `load_results` task boundary

| Option | Description | Selected |
|--------|-------------|----------|
| `load_results` re-implements/re-runs an Oracle load step | Matches the DAG-01 task name literally as an independent unit of work | |
| `load_results` is a thin pass-through — `process()` already loaded Oracle atomically inside `process_csv` | Consistent with Phase 4 D-02 (one Oracle transaction per `process()` call, already committed code) | ✓ |

**Claude's choice:** Thin pass-through (D-02). Flagged explicit invariant: no task besides
`process_csv`'s call into `process()` may touch Oracle.
**Notes:** Acknowledged as "Claude's discretion within this constraint" for exact task-code shape —
not fully locked, since the roadmap's task-name list predates Phase 4's atomicity decision.

---

## Domain-failure vs. Airflow-task-failure semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Raise `AirflowFailException` for permanent failures (`INVALID_FILE`, `CONFIGURATION_ERROR`, etc.) | Marks the Airflow task FAILED for visibility/alerting | |
| Never raise for any of `process()`'s 7 closed `Status` values — always flow through to `report_result` | Matches DAG-04 ("a completed run's logs/report show... status") and `process()`'s own docstring ("never raises") | ✓ |

**Claude's choice:** Never raise for known Status outcomes (D-03). This resolves the question Phase
3's D-23 explicitly deferred to this phase.
**Notes:** Reversibility rated `costly` — changes what "a completed run" means for any future
monitoring consumer.

---

## Deferrable file-wait mechanism

| Option | Description | Selected |
|--------|-------------|----------|
| Custom `BaseTrigger` subclass | Full control, matches the reference repo's `S3KeySensor`-based approach | |
| Stock `FileSensor(deferrable=True)` | Already supports glob-style `filepath`; this project's `file_pattern` values are plain globs, not regex; spec explicitly says "do not overengineer file waiting" | ✓ |

**Claude's choice:** Stock `FileSensor(deferrable=True)` (D-04). Resolves the research question
flagged in STATE.md's Blockers/Concerns and `research/SUMMARY.md`'s Research Flags.
**Notes:** Left an open verification item for the phase-researcher — confirm `FileSensor.filepath`
accepts Jinja templating for the runtime-conf-derived dataset path.

---

## `report_result` output surface

| Option | Description | Selected |
|--------|-------------|----------|
| External notification (Slack/email) | More visible outside Airflow's own UI | |
| Airflow task logging only | Matches PROJECT.md's Out of Scope ("logging only, no metrics/tracing platform"); no requirement calls for external delivery | ✓ |

**Claude's choice:** Logging only (D-07).

---

## Claude's Discretion

- Exact `dag_id` string and file/module layout inside `airflow/dags/` and `airflow/dags/_common/`.
- Whether `load_results` becomes its own function/task or folds tightly into `process_csv`'s next
  step in code, constrained by D-02's no-second-Oracle-write rule.
- Exact Jinja-templated `filepath` expression for `FileSensor`.
- `poke_interval`/`timeout` values for the deferred sensor (D-06).
- Exact `report_result` log format (plain line vs. structured JSON).

## Deferred Ideas

None — discussion stayed within phase scope (single `--auto` pass, no new scope surfaced).
