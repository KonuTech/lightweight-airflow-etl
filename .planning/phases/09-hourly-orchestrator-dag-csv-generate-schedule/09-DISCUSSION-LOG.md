# Phase 9: Hourly Orchestrator DAG (`csv_generate_schedule`) - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-09-01
**Phase:** 9-Hourly Orchestrator DAG (`csv_generate_schedule`)
**Areas discussed:** Row-count/invalid-ratio Params, Cross-DAG summary reporting, Retry/collision
policy, Stuck-cycle timeout, Auto-unpause on creation, Seed derivation basis, Task retry policy,
DAG naming, Compression, Retention

---

## Row-count/invalid-ratio Params (SCHED-08)

| Option | Description | Selected |
|--------|-------------|----------|
| Single shared 'rows' Param | Matches generate_correlated_datasets' single --rows shape | ✓ |
| Separate customers_rows / orders_rows Params | More flexible, but no CLI support today | |

**User's choice:** Single shared 'rows' Param.

| Option | Description | Selected |
|--------|-------------|----------|
| rows=100, invalid_ratio exposed | Matches CLI defaults, both configurable | ✓ |
| Smaller default (rows=20), invalid_ratio exposed | Faster, lighter cycles | |
| rows=100, invalid_ratio fixed (not a Param) | Simpler Param surface | |

**User's choice:** rows=100, invalid_ratio exposed as a Param.

---

## Cross-DAG summary reporting (SCHED-07)

| Option | Description | Selected |
|--------|-------------|----------|
| Re-query Oracle directly | Looser coupling, mirrors report_ready's own approach | ✓ |
| Pull XCom from triggered DagRuns | Avoids extra Oracle round-trip, tighter coupling | |

**User's choice:** Re-query Oracle directly.

**Follow-up — metadata row selection:**

| Option | Description | Selected |
|--------|-------------|----------|
| Latest processed_at per dataset | Simple, safe given max_active_runs=1 | ✓ |
| Match by exact file checksum | More bulletproof, more plumbing | |

**User's choice:** Latest row per dataset. **Notes:** User clarified they're not solving
late-arriving/out-of-order event correctness this milestone ("might add dbt for that later") — the
correlated generator already guarantees customers+orders come from the same run, so there's no
ordering ambiguity to hedge against right now. (First phrasing of this question confused the user —
re-asked in plainer language with a concrete example before they answered.)

**Follow-up — report_ready status meaning:**

| Option | Description | Selected |
|--------|-------------|----------|
| Terminal state only ("report_ready=OK" heartbeat) | Cheap, no duplicate SQL | ✓ |
| Re-run the business-report query | Richer signal, but a 4th copy of the SQL | |

**User's choice:** Heartbeat only. **Notes:** User pushed back strongly on SQL duplication: "A
script/logics for the report should be only in one place, not many." The `customers⋈orders` SQL is
already duplicated 3× (report_ready.py, regenerate_readme_summary.py, verify_evidence.sql) —
centralizing it was captured as a deferred idea rather than actioned in Phase 9.

---

## Retry/collision policy for chain triggers

| Option | Description | Selected |
|--------|-------------|----------|
| skip_when_already_exists=True | Retry quietly no-ops against the deterministic trigger_run_id | ✓ |
| Leave default (raises DagRunAlreadyExists) | Retry surfaces loudly as a failure | |

**User's choice:** skip_when_already_exists=True.

---

## Stuck-cycle timeout

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, ~45 minutes | Margin before next hourly schedule, catches hung chains | ✓ |
| Yes, ~55 minutes | More slack, blocks queue closer to a full hour if stuck | |
| No explicit timeout | Rely solely on max_active_runs=1 queuing | |

**User's choice:** ~45 minutes.

---

## Auto-unpause on creation

| Option | Description | Selected |
|--------|-------------|----------|
| Follow convention: auto-unpause | Consistent with csv_ingest/report_ready, no new manual step | ✓ |
| Override: is_paused_upon_creation=True | Safer default, but adds a manual step this milestone removes | |

**User's choice:** Follow convention (auto-unpause).

---

## Seed derivation basis

| Option | Description | Selected |
|--------|-------------|----------|
| logical_date-derived | Deterministic per scheduled hour, reproducible on retry | ✓ |
| Wall-clock-at-execute-derived | Maximum uniqueness, not reproducible on retry | |

**User's choice:** "Probably Option 1. consult Airflow documentation with MCP Context7." **Notes:**
Context7 MCP was not available in this session; verified via WebSearch against Airflow 3.3.1's own
TaskFlow docs instead — confirmed `get_current_context()["dag_run"].logical_date` is the documented
access pattern, matching `csv_ingest.py`'s existing `get_current_context()` usage. Re-confirmed with
the user before locking in.

---

## Task retry policy (new DAG's own tasks)

| Option | Description | Selected |
|--------|-------------|----------|
| No retries (retries=0) | Matches "fail loudly" instinct; next hour retries fresh anyway | ✓ |
| Small number of retries (1-2, with backoff) | Self-heals transient blips, delays failure visibility | |

**User's choice:** No retries.

---

## DAG naming

**User raised:** current names `csv_ingest`/`report_ready` aren't descriptive; asked to rename them
if used, or keep names if a single "master" DAG design is used instead.

**Resolution:** SCHED-06 (locked in REQUIREMENTS.md) requires both files to stay unmodified and
independently triggerable — renaming a `dag_id` is a modification. Per the user's own conditional
(both DAGs remain independently used *and* orchestrated by the new DAG), current names stay this
phase. Confirmed explicitly with the user; renaming captured as a deferred idea for a future
phase/milestone that would formally amend REQUIREMENTS.md.

---

## Compression

| Option | Description | Selected |
|--------|-------------|----------|
| Plain .csv, no compression | Matches current make generate default | |
| Compress with --compress | Saves disk space, needs config/glob to already handle .gz | ✓ |

**User's choice:** Compress with `--compress`. **Notes:** Verified `file_pattern` in
`configs/datasets/{customers,orders}.json` (`"customers_*.csv*"` / `"orders_*.csv*"`) already
matches `.gz`, and `csv_processor/compression.py` already handles decompression — zero config
changes needed.

---

## Retention

**User raised:** wants a retention/cleanup mechanism now, not deferred, when asked whether
unbounded CSV accumulation was acceptable for this milestone.

**Scope resolution:**

| Option | Description | Selected |
|--------|-------------|----------|
| Bundle into Phase 9 (new requirement) | Follows Phase 8 D-04's bundled-scope precedent | ✓ |
| Defer to its own follow-up phase | Keeps Phase 9 scoped exactly to SCHED-01..08 | |

**User's choice:** Bundle into Phase 9 as a new requirement (exact REQ-ID left to planner, per
Phase 8 D-04's `ENV-03` precedent).

**Retention window:**

| Option | Description | Selected |
|--------|-------------|----------|
| Keep last 7 days | Enough for recent debugging, minimal growth | |
| Keep last 3 days | Tightest footprint | |
| Keep last 30 days | Full month of history | ✓ |

**User's choice:** 30 days.

**Cleanup location:**

| Option | Description | Selected |
|--------|-------------|----------|
| New task inside csv_generate_schedule | One DAG, one place to look | ✓ |
| Separate mechanism (own DAG / outside Airflow) | Decoupled cadence, more moving parts | |

**User's choice:** New task inside `csv_generate_schedule`.

**Cleanup failure handling:**

| Option | Description | Selected |
|--------|-------------|----------|
| Best-effort, never fails the run | Housekeeping shouldn't block the real pipeline | ✓ |
| Fail the whole run | Matches "fail loudly" instinct, but disproportionate blast radius | |

**User's choice:** Best-effort, never fails the run.

---

## Claude's Discretion

- Exact REQ-ID for the new retention requirement (`SCHED-09` vs. a new `RETAIN-01` prefix).
- Exact `poke_interval` for the three deferrable `TriggerDagRunOperator` tasks.
- Exact retention-task implementation shape (glob + `Path.unlink()` loop vs. something more
  structured).
- Exact wording/placement of `docs/airflow-dag.md` updates for the new DAG.

## Deferred Ideas

- Centralize `_BUSINESS_REPORT_SQL` (currently duplicated across `report_ready.py`,
  `regenerate_readme_summary.py`, `verify_evidence.sql`) into one shared module — future phase.
- Rename `csv_ingest`/`report_ready` to more descriptive DAG ids — future phase/milestone requiring
  a REQUIREMENTS.md change to authorize modifying those files (currently locked unmodified by
  SCHED-06).
