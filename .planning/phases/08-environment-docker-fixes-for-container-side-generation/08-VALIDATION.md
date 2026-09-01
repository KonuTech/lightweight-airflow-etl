---
phase: 8
slug: environment-docker-fixes-for-container-side-generation
status: final
nyquist_compliant: true
wave_0_complete: true
created: 2026-09-01
---

# Phase 8 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `pytest` (project-wide) — but this phase's own gate is a live-stack `docker compose exec` check, not pytest-based (matches `verify-phase5`'s precedent of a live exec check alongside, not replacing, the pytest suite) |
| **Config file** | none new — this phase adds no new unit-testable pure-Python logic (Dockerfile/compose/docs edits + one new function pair in `scripts/verify_environment.py`) |
| **Quick run command** | `uv run python scripts/verify_environment.py` (requires `make up` first) |
| **Full suite command** | `uv run pytest tests/unit/ -x && uv run python scripts/verify_environment.py` |
| **Estimated runtime** | ~30-60 seconds (unit suite is fast; the live exec checks depend on the already-running stack, no new container starts) |

---

## Sampling Rate

- **After every task commit:** Run `uv run python scripts/verify_environment.py` (requires `make up`/`make rebuild` already run once per the task's own instructions)
- **After every plan wave:** Run `uv run pytest tests/unit/ -x && uv run python scripts/verify_environment.py`
- **Before `/gsd:verify-work`:** Full `make destroy && make up` (genuinely fresh state) followed by `scripts/verify_environment.py`, per Success Criteria 2 and 3's own literal wording
- **Max feedback latency:** ~60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 08-01-T1 | 01 | 1 | ENV-01, ENV-02, ENV-03 | T-08-01, T-08-02, T-08-03, T-08-04 | Scoped chown/rmdir paths; conditional password seed; chmod 664 not 666 | live check | `docker compose config > /dev/null && echo CONFIG_OK` | ✅ | ⬜ pending |
| 08-01-T2 | 01 | 1 | ENV-01 | T-08-SC | `faker` pinned exact version, vetted supply chain | live exec | `docker compose exec -T airflow-apiserver python -c "import faker; from generator.generate_csv import main; print('IMPORT_OK')"` | ✅ | ⬜ pending |
| 08-02-T1 | 02 | 2 | ENV-01, ENV-02 | T-08-05, T-08-06, T-08-07 | Probe file never matches FileSensor glob; try/finally cleanup; no secrets in exec'd code | live exec (committed, permanent) | `uv run python scripts/verify_environment.py` | ✅ | ⬜ pending |
| 08-02-T2 | 02 | 2 | ENV-02, ENV-03 | — | N/A | live exec + fresh-clone dry run (permanent Makefile target) | `make verify-phase8` (preceded by `rm -f docker/airflow/simple_auth_manager_passwords.json.generated && make destroy && make up` to genuinely reproduce and repair the original bug) | ✅ | ⬜ pending |
| 08-02-T3 | 02 | 2 | ENV-01 (docs) | — | N/A | live check | `grep -q "## Generator Container Mount" docs/environment.md && ! grep -q "mkdir -p docker/airflow" docs/environment.md && echo DOCS_OK` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Task IDs/threat refs reflect the final 08-01-PLAN.md/08-02-PLAN.md as written by the planner and confirmed by gsd-plan-checker (VERIFICATION PASSED) — no Wave 0 stubs were needed, every task ships a real `<automated>` command directly.*

---

## Wave 0 Requirements

*None — every task in both plans ships a real, directly-runnable `<automated>` verification command (confirmed by gsd-plan-checker's Nyquist Compliance check). No pytest-level gap either — this phase's verification surface is entirely live-stack-exec-based by design, matching the project's established split between `tests/unit/` (pure logic) and `scripts/verify_environment.py`/`Makefile` live checks (anything requiring a running Docker stack).*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| `docker compose up` never crashes when the passwords file is missing/wrong-typed (the original "auto-created as a directory" gotcha) | D-04 (bundled fix) | Genuinely reproducing "Docker auto-creates a missing bind-mount path as a directory" requires deleting the file and running a truly fresh `docker compose up` — this checkout's file already exists correctly, so it can't be a committed regression test without deliberately destroying local dev state on every run | `rm -f docker/airflow/simple_auth_manager_passwords.json.generated && docker compose down -v && docker compose up -d --wait`; confirm no `PermissionError`/`IsADirectoryError` in `docker compose logs` for any service |

*The idempotency half of this requirement (Success Criterion 3 — re-running `make up` against an already-initialized state doesn't fail) IS automatable: run `make up` twice in a row and confirm the second run exits 0.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (none needed)
- [x] No watch-mode flags
- [x] Feedback latency < 60s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-09-01 (gsd-plan-checker VERIFICATION PASSED)
