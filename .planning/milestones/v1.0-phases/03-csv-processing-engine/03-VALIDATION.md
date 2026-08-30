---
phase: 3
slug: csv-processing-engine
# status lifecycle: draft (seeded by plan-phase) → validated (set by validate-phase §6)
# audit-milestone §5.5 distinguishes NOT-VALIDATED (draft) from PARTIAL (validated + nyquist_compliant: false) (#2117)
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-29
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 (confirmed pinned in root `pyproject.toml`) |
| **Config file** | root `pyproject.toml` `[tool.pytest.ini_options]` — `testpaths = ["tests"]`, `pythonpath = ["."]` |
| **Quick run command** | `pytest tests/unit -x -q` |
| **Full suite command** | `pytest tests/unit -q` (no integration/e2e tests exist yet — those are Phase 4/6) |
| **Estimated runtime** | ~10 seconds (unit-only, no Oracle/Airflow) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/unit -x -q`
- **After every plan wave:** Run `pytest tests/unit -q`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

*Filled by the planner as tasks are created — this table starts empty at plan-phase seed time and
is populated by `gsd-planner` per task, following the Phase Requirements → Test Map below as the
source of truth for which command verifies which requirement.*

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | TBD | — | — | unit | TBD | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

### Phase Requirements → Test Map (from 03-RESEARCH.md's Validation Architecture)

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|--------------------|-------------|
| ENGINE-01 | Structural validation runs before type/nullability; header mismatches reject whole file | unit | `pytest tests/unit/test_structural_validation.py -x` | ❌ Wave 0 |
| ENGINE-02 | Type validation (int/decimal/date) per config schema | unit | `pytest tests/unit/test_type_validation.py -x` | ❌ Wave 0 |
| ENGINE-03 | Required-field nullability check | unit | `pytest tests/unit/test_type_validation.py::test_nullability -x` | ❌ Wave 0 |
| ENGINE-04 | Explicit type conversion (string → Python type) | unit | `pytest tests/unit/test_normalize.py -x` | ❌ Wave 0 |
| ENGINE-05 | Invalid row doesn't halt file; valid/invalid split + accurate counts | unit | `pytest tests/unit/test_engine_chunks.py -x` | ❌ Wave 0 |
| ENGINE-06 | Invalid row carries error_code/message/source_file/row_number + original values | unit | `pytest tests/unit/test_engine_chunks.py::test_invalid_row_shape -x` | ❌ Wave 0 |
| ENGINE-07 | Chunked, bounded-memory processing; detect-once-per-file | unit | `pytest tests/unit/test_engine_chunks.py::test_chunking -x` (peak-memory assertion mirrors `tests/unit/test_corpus_bounded_memory.py`'s existing RLIMIT_AS pattern) | ❌ Wave 0 |
| ENGINE-09 | No Airflow import anywhere in `csv_processor` | unit | `pytest tests/unit/test_no_airflow_import.py -x` (grep-based or `ast`-based import scan) | ❌ Wave 0 |
| TEST-01 | Full coverage: config/CSV/type/date/valid-invalid/chunked | unit | `pytest tests/unit -q` | ❌ Wave 0 (config parsing already covered by existing `test_config_loader.py`/`test_config_models.py`) |

---

## Wave 0 Requirements

- [ ] `tests/unit/test_detect_dialect.py`, `test_detect_encoding.py`, `test_detect_header.py` — one
      per vendored Tier-A module, using corpus fixtures 1-8 (`dialect_encoding` category)
- [ ] `tests/unit/test_compression.py` — magic-byte sniffing + streaming open, using corpus fixtures
      28-30 (`large_compressed` category) plus the new `--compress`-generated fixtures (D-32)
- [ ] `tests/unit/test_structural_validation.py` — corpus fixtures 9-16 (`structural` category)
- [ ] `tests/unit/test_type_validation.py` — corpus fixtures 17-22 (`type_nullability` category)
- [ ] `tests/unit/test_normalize.py` — type conversion, including the `Decimal.as_tuple()`
      precision/scale check and the strict-`strptime` date check
- [ ] `tests/unit/test_engine_chunks.py` — `process_chunks()` generator: chunk boundaries, bounded
      memory (reuse `test_corpus_bounded_memory.py`'s RLIMIT_AS pattern), row_number counting across
      chunks (D-07)
- [ ] `tests/unit/test_no_airflow_import.py` — ENGINE-09 enforcement
- [ ] `tests/conftest.py` — shared fixtures if not already sufficient from Phase 2
- [ ] Framework install: none needed — pytest already installed and configured

**Note on corpus fixtures 23/24/25/27 (`byte_level_hard`):** these fixtures' headers don't match
either `customers.json`'s or `orders.json`'s declared columns, so D-17/D-18 (header-level structural
reject) would hard-reject the whole file before the byte-level content they're designed to test is
ever reached. Test these against parsing primitives directly or a fixture-local ad hoc config, not
against the real dataset configs — surfaced by 03-RESEARCH.md so the Wave 0 test files above account
for this rather than discovering it mid-implementation.

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
