---
phase: 9
slug: hourly-orchestrator-dag-csv-generate-schedule
status: final
nyquist_compliant: true
wave_0_complete: true
created: 2026-09-01
---

# Phase 9 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (already configured, `[tool.pytest.ini_options]` in root `pyproject.toml`) |
| **Config file** | `pyproject.toml` (`testpaths = ["tests"]`, `pythonpath = ["."]`) |
| **Quick run command** | `uv run pytest tests/unit/dags/ -x` |
| **Full suite command** | `uv run pytest tests/unit/ tests/integration/ tests/e2e/ -x` (integration/e2e require `make up` first) |
| **Estimated runtime** | ~5 seconds (quick, unit-only) / several minutes (full, requires live stack) |

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/unit/dags/ -x`
- **After every plan wave:** Run `make verify-phase9` (unit suite + live DagBag structural check, requires `make up`)
- **Before `/gsd:verify-work`:** `make verify-phase9` green, plus one real live-triggered/manually-triggered end-to-end run watched through the Airflow UI (confirming `deferred` states appear correctly for all three trigger tasks and the chain completes)
- **Max feedback latency:** 5 seconds (unit tier)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 09-02 Task 1 | 09-02 | 2 | SCHED-01 | — | DAG parses, `schedule="@hourly"`, `catchup=False` | structural | `make verify-phase9` | ❌ pre-execution | ⬜ pending |
| 09-01 Task 1 | 09-01 | 1 | SCHED-02 | — | Seed varies per `logical_date`, distinct checksums | unit | `pytest tests/unit/dags/test_generate_schedule_helpers.py::test_seed_varies_by_hour -x` | ❌ pre-execution | ⬜ pending |
| 09-02 Task 1 / 09-04 Task 1 | 09-02 / 09-04 | 2 / 4 | SCHED-03 | — | Sequential chain-trigger order (customers → orders → report_ready) | structural + e2e | `make verify-phase9` (task_ids present) + 09-04 Task 1's live-triggered run | ❌ pre-execution | ⬜ pending |
| 09-02 Task 1 | 09-02 | 2 | SCHED-04 | — | `max_active_runs=1` | structural | `make verify-phase9`'s `dag.max_active_runs == 1` assertion | ❌ pre-execution | ⬜ pending |
| 09-02 Task 1 | 09-02 | 2 | SCHED-05 | — | `fail_when_dag_is_paused=True` on trigger tasks | structural | `make verify-phase9`'s `dag.get_task('trigger_customers').fail_when_dag_is_paused is True` | ❌ pre-execution | ⬜ pending |
| 09-02 Task 2 / 09-04 Tasks 1-2 | 09-02 / 09-04 | 2 / 4 | SCHED-06 | — | `csv_ingest.py`/`report_ready.py` unmodified | code review | `git diff --stat -- airflow/dags/csv_ingest.py airflow/dags/report_ready.py` prints nothing | ❌ pre-execution | ⬜ pending |
| 09-01 Task 1 | 09-01 | 1 | SCHED-07 | T-9-01 | One-line cascade summary log, no SQL injection | unit | `pytest tests/unit/dags/test_generate_schedule_helpers.py::test_summary_format -x` | ❌ pre-execution | ⬜ pending |
| 09-02 Task 1 / 09-03 Task 1 | 09-02 / 09-03 | 2 / 3 | SCHED-08 | — | `rows`/`invalid_ratio` DAG Params, JSON-Schema validated | structural | `make verify-phase9`'s `dag.params["rows"].value == 100` | ❌ pre-execution | ⬜ pending |
| 09-01 Task 2 | 09-01 | 1 | SCHED-10 (retention) | T-9-02 | Deletes CSVs older than 30 days, never raises, no path traversal | unit | `pytest tests/unit/dags/test_generate_schedule_helpers.py::test_retention_deletes_old_files -x` and `::test_retention_never_raises` | ❌ pre-execution | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

*Task IDs reference the final plan set (09-01 through 09-04-PLAN.md, verified by gsd-plan-checker on 2026-09-01 — VERIFICATION PASSED, every requirement traced to a concrete task with an automated command). "File Exists"/"Status" reflect pre-execution state — `/gsd:execute-phase 9` will create these files and run these commands; this table's row mapping is what execution and `/gsd:verify-work` check against.*

---

## Wave 0 Requirements

- [ ] `tests/unit/dags/test_generate_schedule_helpers.py` — covers SCHED-02 (seed derivation as a plain, testable function extracted from `generate_task`), SCHED-07 (summary-line formatter, mirror `_common/reporting.py::format_summary_log()`'s pattern — extract a pure `format_cascade_summary()` helper rather than inlining string-building in the task body), SCHED-10 (retention date-parsing + deletion logic as pure functions, independently testable without a live Airflow context or Oracle connection). **Created by:** 09-01 Task 1 & Task 2.
- [ ] `Makefile`'s `verify-phase9` target — new, mirrors `verify-phase5`'s/`verify-phase8`'s exact shape. **Created by:** 09-03 Task 1.
- [ ] `docs/airflow-dag.md` update documenting `csv_generate_schedule` — not a test file, but a doc-update obligation per CONTEXT.md's canonical references. **Created by:** 09-03 Task 2 (initial section), 09-04 Task 3 (live evidence appended).

*Checkboxes above track actual file existence on disk — still unchecked pre-execution. Plan-checker confirmed all three are correctly scoped as concrete tasks in the plan set (2026-09-01).*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Sequential `deferred` state visible in Airflow UI for all three chain-trigger tasks; chain completes end-to-end | SCHED-03 | Requires observing live Airflow task state transitions (deferred → running → success) against the real triggerer, not reproducible in a unit/structural test | Trigger `csv_generate_schedule` manually via UI/REST API; watch the Gantt/Graph view confirm `trigger_customers` → `trigger_orders` → `trigger_report_ready` fire strictly in order and each shows `deferred` before completing |
| Two overlapping hourly cycles: second visibly queues, doesn't race | SCHED-04 | Requires forcing a slow first run and a second scheduled/triggered run while the first is still active — timing-dependent, not a deterministic unit test | Manually trigger `csv_generate_schedule` twice in quick succession; confirm the second run shows `queued` state until the first reaches a terminal state |
| Paused `csv_ingest` causes immediate visible failure, not a hang | SCHED-05 | Requires pausing a live DAG and observing the parent run's failure mode in real time | Pause `csv_ingest` in the UI, trigger `csv_generate_schedule`, confirm `trigger_customers` fails immediately (not deferred/hanging) |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies — confirmed by gsd-plan-checker (2026-09-01)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify — confirmed by gsd-plan-checker (2026-09-01)
- [x] Wave 0 covers all MISSING references — 09-01/09-03 create all three Wave 0 artifacts
- [x] No watch-mode flags — all commands are one-shot (`pytest -x`, `make verify-phase9`)
- [x] Feedback latency < 5s (unit tier) — quick-run command is unit-only, ~5s estimated
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** plan-level sign-off complete (2026-09-01, this validation strategy is confirmed compliant and ready for execution). Execution-level sign-off (all commands actually green) happens at `/gsd:verify-work` after `/gsd:execute-phase 9`.
