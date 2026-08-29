---
phase: 5
slug: airflow-dag-wiring-deferrable-file-wait
# status lifecycle: draft (seeded by plan-phase) → validated (set by validate-phase §6)
# audit-milestone §5.5 distinguishes NOT-VALIDATED (draft) from PARTIAL (validated + nyquist_compliant: false) (#2117)
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-29
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 (`pyproject.toml` `[dependency-groups] dev = ["pytest==9.1.1"]`) |
| **Config file** | root `pyproject.toml` `[tool.pytest.ini_options]` — `testpaths = ["tests"]`, `pythonpath = ["."]` |
| **Quick run command** | `uv run pytest tests/unit/dags/ -x` (new subdirectory this phase adds) |
| **Full suite command** | `uv run pytest tests/unit/ -x` (existing project convention, per `Makefile`'s `verify-phaseN` targets) |
| **Estimated runtime** | ~1-10s (pure-Python helper tier, no Airflow/Docker needed) |

**Key constraint:** `apache-airflow` is not installed in the root `.venv` (only inside the Docker
image), splitting this phase's testable surface into two tiers:
1. **Pure-Python helpers with zero Airflow import** (glob-resolution, `dataset`→`config_path`
   derivation, path-traversal validation, log-formatting) — unit-testable locally, no new dev
   dependency.
2. **DAG-structure/import validity** (does `csv_ingest.py` parse, exactly one `dag_id`, the 5 named
   tasks exist, `wait_for_file.deferrable is True`, no dataset-specific branch) — requires a real
   `airflow` import, run via `docker compose exec airflow-scheduler python -c "..."` (consistent
   with the project's zero-Airflow-in-local-venv discipline; avoids a ~200MB local dev dependency
   that would drift against the Dockerfile's own pin).

Full end-to-end proof (HTTP trigger → real DAG run → Oracle rows) is explicitly Phase 6's job
(TEST-03) — this phase's own validation stays at the unit/dag-structure tier.

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/unit/dags/ -x` (fast, no Airflow/Docker
  needed for the pure-Python-helper tier)
- **After every plan wave:** Full unit suite (`uv run pytest tests/unit/ -x`) + the `DagBag`
  import/structure check via `docker compose exec` (requires `make up` first)
- **Before `/gsd-verify-work`:** Both tiers green, plus a manual smoke trigger (curl against the
  real running stack)
- **Max feedback latency:** ~10s for the fast tier; the `docker compose exec` structure check adds
  container round-trip latency (bounded mostly by whether the stack is already up)

---

## Per-Task Verification Map

Filled in by the planner once tasks are defined — this phase's exact task/wave breakdown is TBD
pending `/gsd-plan-phase 5`. Requirement IDs in scope: DAG-01, DAG-02, DAG-03, DAG-04, DAG-05.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| *(populated during planning)* | | | | | | | | | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/unit/dags/__init__.py` + `tests/unit/dags/conftest.py` — new test subdirectory, no
      Airflow import needed for the pure-Python-helper tier
- [ ] `tests/unit/dags/test_dag_helpers.py` — covers DAG-05 (both-dataset parametrization over
      `customers.json`/`orders.json`) and the file-path glob-resolution helper
- [ ] `tests/unit/dags/test_load_config_helpers.py` — covers DAG-02's runtime-`conf` validation
      logic, including the path-traversal check flagged in `05-RESEARCH.md`'s Security Domain,
      extracted as a plain function so it's testable without Airflow
- [ ] `tests/unit/dags/test_report_result_format.py` — covers DAG-04's summary-formatting logic
      (dataset/file/row counts/duration/status), extracted as a plain function
- [ ] A `Makefile` target (e.g. `verify-phase5`) that runs the local unit tier plus the
      `docker compose exec` `DagBag` structure check, following the established `verify-phaseN`
      convention
- [ ] Framework install: none — pytest is already present; no new dependency needed for the
      pure-Python-helper tier

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Task shows as deferred (triggerer-managed) in Airflow's UI/API while waiting | DAG-03 | Deferral state is an Airflow-runtime/UI observation, not mechanically assertable from a unit test without standing up a real triggerer | Trigger a DAG run against a not-yet-present file; confirm the `wait_for_file` task state shows `deferred` in the Airflow UI or `GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances` |
| Full end-to-end HTTP-trigger-to-Oracle-rows proof | DAG-01 (integration), DAG-02 | Explicitly deferred to Phase 6 (TEST-03) — out of scope for this phase's own validation | See Phase 6 |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (dags test subdirectory, path-traversal/log-format
      helper tests, Makefile verify-phase5 target)
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s (fast tier)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
