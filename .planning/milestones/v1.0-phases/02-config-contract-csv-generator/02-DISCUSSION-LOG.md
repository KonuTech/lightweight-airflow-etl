# Phase 2: Config Contract & CSV Generator - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-28
**Phase:** 2-Config Contract & CSV Generator
**Areas discussed:** CSV dialect, File & directory layout, Schema notation, Oracle target &
processing config, Config schema & testing, Generator behavior

---

## CSV Dialect

| Option | Description | Selected |
|--------|-------------|----------|
| Comma (,) | Standard CSV default | |
| Semicolon (;) | Matches spec's own worked example | |
| Configurable, default comma | Per-dataset override, default comma | |

**User's choice:** Free text — "Data config contract should allow for any delimiter, encoding,
quote style, etc. Check /home/user/projects/airflow-platform/packages for developed solutions."
**Notes:** Checked reference repo's `dataplat.config.model.CsvParsingConfig` and
`csv_processor/detect/dialect.py` before proceeding — confirmed arbitrary-value fields (no fixed
enum) is the proven precedent.

| Option | Description | Selected |
|--------|-------------|----------|
| Minimal quoting | Python csv.QUOTE_MINIMAL default | |
| Quote all fields | Every field wrapped regardless of content | |

**User's choice:** Free text — "Any scenario should be possible."

| Option | Description | Selected |
|--------|-------------|----------|
| Mirror reference (encoding/delimiter/quotechar/header only) | Matches proven reference scope | |
| Fully open — add escapechar/doublequote/lineterminator too | Exceeds reference's own scope | ✓ |

**User's choice:** Fully open.
**Notes:** User added: "Remember to do tests" — threaded into the test-layout decision (D-17).

---

## File & Directory Layout

| Option | Description | Selected |
|--------|-------------|----------|
| configs/datasets/customers.json + orders.json | Matches ARCHITECTURE.md's sketch | ✓ |
| Inside packages/csv-processor/ | Couples configs to the engine package | |

**User's choice:** configs/datasets/customers.json + orders.json.

| Option | Description | Selected |
|--------|-------------|----------|
| ./data/<dataset>/ on host → mount to /opt/airflow/data/ | Added now, in Phase 2 | ✓ |
| Defer the docker-compose mount to Phase 5 | Generator writes locally, wiring later | |

**User's choice:** Free text — "Decide for me best approach. From my perspective volume mounts
sounds attractive and useful." Claude decided: add the mount now.

| Option | Description | Selected |
|--------|-------------|----------|
| Dated: customers_YYYYMMDD.csv | Matches spec §9's example | ✓ |
| Fixed name: customers.csv | Simplest, no date logic | |
| Configurable pattern + explicit filename argument | Fully decoupled from config | |

**User's choice:** Dated filenames.

---

## Schema Notation

| Option | Description | Selected |
|--------|-------------|----------|
| Python strptime strings | Matches Phase 3's strict validator, reference repo's convention | ✓ |
| Friendly notation ("YYYY-MM-DD") | Matches spec's own illustrative example | |

**User's choice:** Python strptime strings.

| Option | Description | Selected |
|--------|-------------|----------|
| Single 'nullable' flag only | Sufficient for two fixed-schema datasets | |
| Both nullable + required, mirroring the reference repo | Future-proofing | ✓ |

**User's choice:** Both nullable + required.

| Option | Description | Selected |
|--------|-------------|----------|
| Explicit precision/scale | Matches Oracle NUMBER(12,2) exactly | ✓ |
| Just "decimal", no precision/scale | Simpler, relies on Oracle to round/truncate | |

**User's choice:** Explicit precision/scale.

---

## Oracle Target & Processing Config

| Option | Description | Selected |
|--------|-------------|----------|
| Table names only | Consistent with Phase 1's admin/admin credential decision | ✓ |
| Table names + connection details | Duplicates Phase 1's connection setup | |

**User's choice:** Table names only.

| Option | Description | Selected |
|--------|-------------|----------|
| Per-dataset field in config.json | Tunable per dataset | ✓ |
| Hardcoded/global default | Single constant for the whole engine | |

**User's choice:** Per-dataset config.json field.

---

## Config Schema & Testing

| Option | Description | Selected |
|--------|-------------|----------|
| Scope to the 5 types actually needed | Matches real Oracle DDL exactly | |
| Add boolean now too | Rounds out the type system ahead of need | ✓ |

**User's choice:** Add boolean now too.

| Option | Description | Selected |
|--------|-------------|----------|
| Fully self-contained per dataset | No merge-layer indirection | |
| configs/defaults.json + per-dataset override merge | Mirrors reference repo's defaults.yaml | ✓ |

**User's choice:** Shared defaults file with per-dataset overrides.

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — add a local verification gate | Mirrors Phase 1's make verify precedent | ✓ |
| No dedicated gate | Run pytest and make fixtures-verify separately | |

**User's choice:** Add a local combined verification gate.

| Option | Description | Selected |
|--------|-------------|----------|
| One test file per concern | test_config_models.py + test_config_loader.py | ✓ |
| Single test_config.py covering everything | One file for all config tests | |

**User's choice:** One test file per concern.

---

## Generator Behavior

| Option | Description | Selected |
|--------|-------------|----------|
| CLI flags: --rows N --invalid-ratio 0.X --seed N | Direct GEN-01 coverage | ✓ |
| Config-driven generation params in config.json | Couples generator tuning to the ingestion contract | |

**User's choice:** CLI flags.

**Invalid-row categories (multiSelect):** Wrong type ✓, Invalid date/timestamp ✓, Missing required
field ✓, Structural: wrong column count ✓ — all four selected. User also asked: "Research more
cases and add them. Let's be thorough. Wrong delimiter, encoder, serialization, etc." — this led
directly into the corpus/digest-oracle discussion below.

| Option | Description | Selected |
|--------|-------------|----------|
| Add a lightweight malformed-file mode | A handful of malformed files, no manifest machinery | |
| Adopt the full corpus/digest-oracle architecture | Ports reference repo's tools/corpus/ wholesale | ✓ |

**User's choice:** Adopt the full architecture.
**Notes:** Claude disclosed the tradeoff explicitly (2600-line reference implementation, no
secret-scanner/build-context problem to solve here) before the user chose the heavier option.
Followed the user's explicit call per session policy — this is a real implementation-approach
choice within Phase 2's domain, not scope creep.

| Option | Description | Selected |
|--------|-------------|----------|
| Start small, grow as needed | Author ~10-15 fixtures, grow incrementally | |
| Author a comprehensive set upfront | Mirrors reference repo's own Phase-1 precedent | ✓ |

**User's choice:** Comprehensive set upfront.

| Option | Description | Selected |
|--------|-------------|----------|
| Skip it — TEST-04 already covers this | Avoids duplicating the benchmark's memory claim | |
| Include a large fixture in the corpus too | RLIMIT_AS-based unit-level proof | ✓ |

**User's choice:** Include a large fixture.

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, keep expect: permissive | Phase 3 owns the real error_code vocabulary | ✓ |
| Define the error_code enum now in Phase 2's manifest | Locks in Phase 3's design early | |

**User's choice:** Keep expect: permissive.

| Option | Description | Selected |
|--------|-------------|----------|
| Include gzip + zip fixtures | Exercises Tier-A vendored compression.py | ✓ |
| Skip compression | Out of this project's real file surface | |

**User's choice:** Include gzip + zip fixtures.

| Option | Description | Selected |
|--------|-------------|----------|
| tools/corpus/ | Exact path match with the reference repo | ✓ |
| generator/corpus/ | Keeps everything under one generator/ directory | |

**User's choice:** tools/corpus/.

| Option | Description | Selected |
|--------|-------------|----------|
| make fixtures / make fixtures-verify / make generate | Mirrors reference repo's naming exactly | ✓ |
| Decide later during planning | Let the planner propose target names | |

**User's choice:** Adopt the reference repo's exact Makefile target names.

---

## Claude's Discretion

- Exact Pydantic v2 model class names/module layout within `packages/csv-processor/src/csv_processor/config/`.
- CONFIG-02's multi-error aggregation relies on Pydantic v2's native `ValidationError` behavior —
  no custom mechanism needed.
- Exact fixture count/categories within the "comprehensive upfront" corpus (D-16a), scoped down from
  the reference repo's 69 to what's relevant given this project's structural/type/nullability-only
  validation.
- gitleaks/secret-scanning noted as explicitly NOT a concern here (unlike the reference repo's
  ADR-0005 rationale) — Phase 6's CI-01 scope never included a secret scanner.

## Deferred Ideas

None — all discussion stayed within Phase 2's config/generator domain. CONFIG-03 and CONFIG-04
were already deferred to v2 in REQUIREMENTS.md before this discussion and were not reopened.
