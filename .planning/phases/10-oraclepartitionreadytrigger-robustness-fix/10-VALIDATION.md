---
phase: 10
slug: oraclepartitionreadytrigger-robustness-fix
status: final
nyquist_compliant: true
wave_0_complete: true
created: 2026-09-02
---

# Phase 10 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `pytest==9.1.1` |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` (`testpaths = ["tests"]`, `pythonpath = ["."]`) |
| **Quick run command** | `uv run pytest tests/unit/test_oracle_partition_trigger.py -x` |
| **Full suite command** | `uv run pytest tests/unit/ -x` |
| **Estimated runtime** | ~1 second (quick) / ~4 seconds (full unit suite) |

---

## Sampling Rate

- **After every task commit:** `uv run pytest tests/unit/test_oracle_partition_trigger.py -x`
- **After every plan wave:** `uv run pytest tests/unit/ -x` (full unit suite)
- **Before `/gsd:verify-work`:** Full unit suite green; optionally one manual live check
  (`docker compose stop oracle` mid-poll, or a deliberately typo'd `_POLL_QUERY`) recorded in a
  `10-HUMAN-UAT.md` if the planner chooses to include it, mirroring Phase 9's precedent — no
  existing Makefile fault-injection tooling exists for this, so unit coverage is the primary gate.
- **Max feedback latency:** ~1 second (quick tier)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 10-01 Task 2 | 10-01 | 1 | ROBUST-01 (criterion 1) | T-10-01 | Transient `OperationalError` on first poll, success on retry — trigger eventually yields the ready event | unit | `uv run pytest tests/unit/test_oracle_partition_trigger.py::test_run_retries_transient_operational_error_then_succeeds -x` | ❌ pre-execution | ⬜ pending |
| 10-01 Task 2 | 10-01 | 1 | ROBUST-01 (criterion 2) | T-10-02 | `OperationalError` on every attempt through the cap — re-raises after exactly 10 consecutive failures | unit | `uv run pytest tests/unit/test_oracle_partition_trigger.py::test_run_reraises_after_exhausting_transient_retries -x` | ❌ pre-execution | ⬜ pending |
| 10-01 Task 2 | 10-01 | 1 | ROBUST-01 (criterion 3) | T-10-01 | Non-transient error (`oracledb.ProgrammingError`) on first attempt — propagates immediately, no retry | unit | `uv run pytest tests/unit/test_oracle_partition_trigger.py::test_run_propagates_non_transient_error_immediately -x` | ❌ pre-execution | ⬜ pending |
| 10-01 Task 2 | 10-01 | 1 | ROBUST-01 (criterion 4) | T-10-03 | `connection.close()` itself raising inside `finally` never masks the original exception | unit | `uv run pytest tests/unit/test_oracle_partition_trigger.py::test_run_close_failure_does_not_mask_original_exception -x` | ❌ pre-execution | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

*Task IDs reference the final plan (10-01-PLAN.md — Task 1 "Rewrite run() with bounded retry/backoff
(D-01 through D-07)", Task 2 "D-08 test coverage + verify-phase10 Makefile target" — verified by
gsd-plan-checker on 2026-09-02, VERIFICATION PASSED, all 4 test names confirmed present verbatim
in the plan). "File Exists"/"Status" reflect pre-execution state — `/gsd:execute-phase 10` will
create these and run these commands.*

---

## Wave 0 Requirements

None — `tests/unit/test_oracle_partition_trigger.py` already exists with the exact mocking
scaffolding (`AsyncMock`, `_mock_connection()`, `_collect_events()`) the 4 new D-08 tests need; no
new fixture file, conftest, or framework install required.

*Existing infrastructure covers all phase requirements.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| A real, extended Oracle outage (`docker compose stop oracle` mid-poll) triggers bounded retry/backoff then a visible failure, not silent `deferred` forever | ROBUST-01 (criteria 1-2) | Requires observing live trigger behavior against the real triggerer over several minutes (10 consecutive failures × backoff) — timing-dependent, not a deterministic unit test | Trigger `report_ready`, run `docker compose stop oracle` while `wait_for_both_datasets` is deferred, watch the Airflow UI/logs for retry warnings then eventual task failure once the cap is exceeded; `docker compose start oracle` to restore |
| A deliberately typo'd `_POLL_QUERY` column name surfaces as an immediate visible failure, not silent infinite retry | ROBUST-01 (criterion 3) | Requires a live code mutation + live trigger run to observe real Airflow UI failure surfacing — optional confidence check beyond the unit test's mocked `ProgrammingError` | Temporarily typo a column in `_POLL_QUERY`, trigger `report_ready`, confirm the deferred task fails immediately (not `deferred` indefinitely); revert the typo |

*RESOLVED (2026-09-02, planner + plan-checker): unit-test-only — the planner did not include a
`10-HUMAN-UAT.md` file. Both manual checks above remain optional, undertaken confidence checks, not
a phase-gate obligation — all 4 ROADMAP success criteria are fully covered by the automated unit
tests in the Per-Task Verification Map above.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies — confirmed by gsd-plan-checker (2026-09-02)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify — only 2 tasks total, both automated
- [x] Wave 0 covers all MISSING references — no Wave 0 needed, existing test scaffolding sufficient
- [x] No watch-mode flags — all commands are one-shot (`pytest -x`)
- [x] Feedback latency < 1s (unit tier) — quick-run command is single-file unit test, ~1s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** plan-level sign-off complete (2026-09-02, gsd-plan-checker VERIFICATION PASSED).
Execution-level sign-off (all commands actually green) happens at `/gsd:verify-work` after
`/gsd:execute-phase 10`.
