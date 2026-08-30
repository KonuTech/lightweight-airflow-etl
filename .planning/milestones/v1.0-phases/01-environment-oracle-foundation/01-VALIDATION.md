---
phase: 1
slug: environment-oracle-foundation
# status lifecycle: draft (seeded by plan-phase) → validated (set by validate-phase §6)
# audit-milestone §5.5 distinguishes NOT-VALIDATED (draft) from PARTIAL (validated + nyquist_compliant: false) (#2117)
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-28
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `python-oracledb` + plain assertions inside `scripts/verify_environment.py` (per CONTEXT.md D-05) — no pytest framework exists yet in this greenfield repo |
| **Config file** | none yet — this phase introduces the repo's first Python tooling |
| **Quick run command** | `python scripts/verify_environment.py` (once Oracle is up) |
| **Full suite command** | same — this phase has no larger test suite to run |
| **Estimated runtime** | ~20 seconds (Oracle Free `-faststart` boot measured at 16s this session) |

---

## Sampling Rate

- **After every task commit:** Run `docker compose up -d && python scripts/verify_environment.py`
- **After every plan wave:** Run `make reset && make up`, confirm every ROADMAP.md Phase 1 success criterion from a genuinely fresh state
- **Before `/gsd-verify-work`:** All 4 success criteria in ROADMAP.md's Phase 1 section must be independently demonstrated true
- **Max feedback latency:** ~20 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | TBD | 0 | INFRA-01 | — | `docker-compose up` brings all services to healthy | smoke | `docker compose up -d && docker compose ps --format json \| jq -e 'all(.Health == "healthy" or .State == "running")'` | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | 0 | INFRA-03 | — | `admin`/`admin` authenticates against both Oracle and Airflow | integration | `python scripts/verify_environment.py` (Oracle half) + `curl -X POST .../auth/token -d '{"username":"admin","password":"admin"}'` (Airflow half, expect 200 + JWT) | ❌ Wave 0 | ⬜ pending |
| TBD | TBD | 0 | (Success Criterion 2) | — | 5 tables + expected columns exist, verified via `USER_TABLES`/`ALL_TAB_COLUMNS`, in the correct `FREEPDB1`/`ADMIN` schema context (not `SYS`/`CDB$ROOT`) | integration | `python scripts/verify_environment.py` | ❌ Wave 0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `scripts/verify_environment.py` — covers CONTEXT.md D-05, INFRA-03 (Oracle half), Success Criterion 2
- [ ] Minimal Python project scaffolding (`pyproject.toml`, `uv`) — needed before `verify_environment.py` can import `oracledb`; this repo has none yet
- [ ] No pytest/test framework installed yet — acceptable for this phase (D-05 asks for a standalone script, not a pytest suite); Phase 3+ will need to introduce pytest properly

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Documented CPU/RAM/disk numbers match real usage | INFRA-02 | Requires human observation of `docker stats` on the actual dev machine over a real working session — not scriptable as pass/fail; RESEARCH.md's Open Question 1 flags the combined Airflow+Oracle peak as unmeasured beyond summed vendor minimums | Run `make up` (full stack), work through a normal session, observe `docker stats` for peak CPU/RAM, check `docker system df` for disk usage, compare against `docs/environment.md`'s documented floor (Airflow's own preflight: 4GB RAM / 2 CPU / 10GB disk, plus Oracle Free's separate footprint) and revise the doc if the observed peak exceeds it |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 20s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
