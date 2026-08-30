---
phase: 4
slug: oracle-bulk-load-idempotency-engine-entrypoint
# status lifecycle: draft (seeded by plan-phase) → validated (set by validate-phase §6)
# audit-milestone §5.5 distinguishes NOT-VALIDATED (draft) from PARTIAL (validated + nyquist_compliant: false) (#2117)
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-29
---

# Phase 4 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (uv-managed) |
| **Config file** | `pyproject.toml` (`[tool.pytest.ini_options]`, existing since Phase 1) |
| **Quick run command** | `uv run pytest tests/unit -q` |
| **Full suite command** | `uv run pytest tests/ -q` (includes real-Oracle integration tests, requires the docker-compose Oracle container up per TEST-02) |
| **Estimated runtime** | ~5-30s unit; Oracle integration tests add real DB round-trip latency (seconds per test, container must be running) |

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/unit -q`
- **After every plan wave:** Run `uv run pytest tests/ -q` (full suite including Oracle integration — bring the container up first if needed)
- **Before `/gsd-verify-work`:** Full suite must be green against a real, running Oracle Database Free container (TEST-02 explicitly forbids mocking Oracle)
- **Max feedback latency:** ~60 seconds (bounded mostly by Oracle container startup if not already running)

---

## Per-Task Verification Map

Filled in by the planner once tasks are defined — this phase's exact task/wave breakdown is TBD pending `/gsd-plan-phase 4`. Requirement IDs in scope: LOAD-01, LOAD-02, LOAD-03, LOAD-04, ENGINE-08, TEST-02.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| *(populated during planning)* | | | | | | | | | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/integration/test_oracle_load.py` (or equivalent) — real-Oracle bulk-load fixtures for LOAD-01/02/03/04, per TEST-02's explicit "not mocked" requirement
- [ ] Existing `docker-compose` Oracle service (Phase 1) is the shared fixture — no new container infra needed, just a running-container precondition on integration tests
- [ ] `packages/csv-processor/src/csv_processor/config/models.py`'s identifier-allowlist validation (flagged as a security finding in `04-RESEARCH.md` — `valid_table`/`invalid_table` config fields need format validation before being interpolated into dynamically-built SQL) needs a Wave 0 stub test proving the allowlist rejects a malformed identifier, since this is a structural gap in already-existing Phase 2 config code that this phase's loader will newly expose to injection risk

---

## Manual-Only Verifications

*None — all four Success Criteria (bulk insert via `executemany()`, one metadata row per file, no duplication on retry, correct `ProcessingResult` status per scenario) are mechanically verifiable against a real Oracle container.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (identifier-allowlist stub, Oracle integration fixture)
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
