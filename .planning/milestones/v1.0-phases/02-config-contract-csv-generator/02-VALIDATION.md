---
phase: 2
slug: config-contract-csv-generator
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-28
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest — not yet installed anywhere in the project; Phase 1's one existing test (`tests/test_verify_environment.py`) deliberately uses only stdlib `unittest`, explicitly deferring the project's formal test suite to a later phase |
| **Config file** | none yet — this phase's Wave 0 gap (`[tool.pytest.ini_options]` or `pytest.ini`) |
| **Quick run command** | `uv run pytest tests/unit/ -x` |
| **Full suite command** | `uv run pytest tests/unit/ tests/fixtures -x && make fixtures-verify` |
| **Estimated runtime** | ~10-30 seconds (unit tests) + corpus regeneration/digest time (small at this phase's fixture count) |

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/unit/ -x`
- **After every plan wave:** Run `uv run pytest tests/unit/ -x && make fixtures-verify`
- **Before `/gsd-verify-work`:** `make verify-phase2` (D-16g) must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 0 | Wave 0 | — | N/A | infra | `uv run pytest --version` | ❌ W0 | ⬜ pending |
| 02-0x-0x | TBD | TBD | CONFIG-01 | — | `config.json` schema accepts a fully-specified valid dataset config | unit | `pytest tests/unit/test_config_models.py -x` | ❌ W0 | ⬜ pending |
| 02-0x-0x | TBD | TBD | CONFIG-02 | — | Malformed config fails validation with ALL errors, not just the first | unit | `pytest tests/unit/test_config_loader.py -x` | ❌ W0 | ⬜ pending |
| 02-0x-0x | TBD | TBD | GEN-01 | — | Generator produces deterministic valid+invalid rows for a given seed | unit | `pytest tests/unit/test_generate_csv.py -x` | ❌ W0 | ⬜ pending |
| 02-0x-0x | TBD | TBD | D-16 (corpus) | — | Corpus regenerates byte-identical to the committed digest oracle | filesystem | `make fixtures-verify` | ❌ W0 | ⬜ pending |
| 02-0x-0x | TBD | TBD | D-16b | — | Large fixture streams under a bounded `RLIMIT_AS` without exceeding it | unit (subprocess) | `pytest tests/unit/test_corpus_bounded_memory.py -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Exact Task IDs/Plan/Wave assignments filled in once gsd-planner produces PLAN.md.*

---

## Wave 0 Requirements

- [ ] `pytest` installed as a dev dependency (root `pyproject.toml`, uv-managed) — nothing in this
  project installs a real test runner yet
- [ ] `[tool.pytest.ini_options]` (or `pytest.ini`) — no pytest config exists yet
- [ ] `tests/unit/__init__.py` or equivalent test-discovery setup
- [ ] `tests/fixtures/` directory structure for the corpus (manifest + digest oracle + gitignored
  `csv/`)

---

## Manual-Only Verifications

*None — all Phase 2 behaviors (config validation, generator determinism, corpus byte-identity,
bounded-memory proof) have automated verification per the map above.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
