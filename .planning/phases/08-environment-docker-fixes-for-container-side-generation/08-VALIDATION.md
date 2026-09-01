---
phase: 8
slug: environment-docker-fixes-for-container-side-generation
status: draft
nyquist_compliant: false
wave_0_complete: false
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
| 08-01-01 | 01 | 0 | ENV-01 | — | N/A | unit (Wave 0 stub) | `grep -q "def verify_generator_importable" scripts/verify_environment.py` | ❌ W0 | ⬜ pending |
| 08-01-02 | 01 | 0 | ENV-02 | — | N/A | unit (Wave 0 stub) | `grep -q "def verify_data_write_access" scripts/verify_environment.py` | ❌ W0 | ⬜ pending |
| 08-0X-0X | TBD | 1+ | ENV-01 | — | N/A | live exec | `docker compose exec -T airflow-apiserver python -c "import faker; from generator.generate_csv import main"` | ❌ W0 | ⬜ pending |
| 08-0X-0X | TBD | 1+ | ENV-02 | — | N/A | live exec + fresh-clone dry run | `docker compose exec -T airflow-apiserver python -c "..."` (write-then-delete probe); fresh-clone proof via `make destroy && make up` then re-running the check | ❌ W0 | ⬜ pending |
| 08-0X-0X | TBD | 1+ | D-04 (bundled passwords-file fix) | — | N/A | manual (not automatable as a committed regression test) | `rm -f docker/airflow/simple_auth_manager_passwords.json.generated && docker compose down -v && docker compose up -d --wait`, confirm no `PermissionError`/`IsADirectoryError` in any service's logs | N/A | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Exact Plan/Task IDs filled in by the planner — this table's shape is fixed by 08-RESEARCH.md's Phase Requirements → Test Map; the planner assigns concrete plan/wave numbers.*

---

## Wave 0 Requirements

- [ ] `scripts/verify_environment.py` — add `verify_generator_importable()` and `verify_data_write_access()` (or equivalently named functions), wired into `main()`
- [ ] `Makefile` — add `verify-phase8` target

*No pytest-level Wave 0 gap — this phase's verification surface is entirely live-stack-exec-based by design, matching the project's established split between `tests/unit/` (pure logic) and `scripts/verify_environment.py`/`Makefile` live checks (anything requiring a running Docker stack).*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| `docker compose up` never crashes when the passwords file is missing/wrong-typed (the original "auto-created as a directory" gotcha) | D-04 (bundled fix) | Genuinely reproducing "Docker auto-creates a missing bind-mount path as a directory" requires deleting the file and running a truly fresh `docker compose up` — this checkout's file already exists correctly, so it can't be a committed regression test without deliberately destroying local dev state on every run | `rm -f docker/airflow/simple_auth_manager_passwords.json.generated && docker compose down -v && docker compose up -d --wait`; confirm no `PermissionError`/`IsADirectoryError` in `docker compose logs` for any service |

*The idempotency half of this requirement (Success Criterion 3 — re-running `make up` against an already-initialized state doesn't fail) IS automatable: run `make up` twice in a row and confirm the second run exits 0.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
