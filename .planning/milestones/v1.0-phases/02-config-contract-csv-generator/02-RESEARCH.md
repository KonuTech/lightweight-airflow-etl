# Phase 2: Config Contract & CSV Generator - Research

**Researched:** 2026-08-28
**Domain:** Pydantic v2 config contracts; deterministic CSV/business-row generation; manifest +
digest-oracle byte-level fixture corpora (ported from a sibling reference repo)
**Confidence:** HIGH (config schema is verified against this repo's own committed Oracle DDL and
the reference repo's actual, read source files; corpus subsystem is verified against the reference
repo's actual `tools/corpus/` implementation and its ADR) / MEDIUM (exact fixture count/category
list for the scoped-down corpus is new authorship, not verified against any canonical source — see
Assumptions Log)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**CSV Dialect**
- **D-01:** `config.json`'s `csv` block accepts arbitrary values for every dialect field — no fixed
  enum for delimiter, encoding, or quoting style. Checked the reference repo's actual
  `dataplat.config.model.CsvParsingConfig` (encoding/delimiter/quotechar/header_row, all
  arbitrary/nullable) and `csv_processor/detect/dialect.py` (contract-declared delimiter always
  overrides detection) before deciding — this project's own generator controls both sides, so
  every dataset can declare its own real dialect rather than picking from a fixed set.
- **D-02:** Went beyond the reference repo's own scope: `csv` block also includes `escapechar`,
  `doublequote`, and `lineterminator` as explicit config fields (the reference repo fixes these by
  convention — `doublequote=True`, `escapechar=None`, `QUOTE_MINIMAL`, `\n` — and does not expose
  them). User's explicit call after being shown this tradeoff. — **Reversibility:** reversible —
  additive Pydantic fields with sane defaults; nothing downstream depends on their absence yet.
  **Note:** user asked to remember test coverage for these fields specifically (ties into TEST-01
  and the "one test file per concern" test-layout decision below).
- **D-03:** Quoting policy is config-driven per dataset, not a single hardcoded default across both
  datasets.

**File & Directory Layout**
- **D-04:** Per-dataset config files live at `configs/datasets/customers.json` and
  `configs/datasets/orders.json`, per ARCHITECTURE.md's own proposed structure and the reference
  repo's layout convention.
- **D-05:** `configs/defaults.json` holds shared settings, merged with per-dataset overrides
  (mirrors the reference repo's `configs/defaults.yaml` pattern) — chosen over fully self-contained
  per-dataset files.
- **D-06:** Generated CSVs land at `./data/<dataset>/` on the host, mounted into docker-compose as
  `/opt/airflow/data/<dataset>/` — added now in Phase 2, not deferred to Phase 5. Gitignored (content
  is generated, never committed). No such volume mount exists yet in `docker-compose.yml` — this
  phase adds it. — **Reversibility:** costly — **rationale:** Phase 5's `FileSensor(deferrable=True)`
  filepath and every later reference to the mount path assume this location; changing it after
  Phase 5 wires the DAG means touching docker-compose, the DAG's sensor config, and any committed
  path references.
- **D-07:** Generated filenames are dated: `customers_YYYYMMDD.csv` / `orders_YYYYMMDD.csv`,
  matching spec §9's own worked example. `config.json`'s `file_pattern` field stays the glob (e.g.
  `customers_*.csv`).

**Schema Notation**
- **D-08:** Date/timestamp formats in `config.json` are Python `strptime` strings (e.g.
  `"%Y-%m-%d"`, `"%Y-%m-%dT%H:%M:%S%z"`) — directly usable by Phase 3's strict-`strptime` validator
  (already pinned in STACK.md) with no translation layer, matching the reference repo's
  `columns.yaml` convention exactly.
- **D-09:** Both `nullable` (can the value be empty) and `required` (must the column be present in
  the file) are separate booleans per column, mirroring the reference repo's `columns.yaml`, even
  though neither of this project's two fixed-schema datasets currently needs the distinction —
  explicit future-proofing choice by the user.
- **D-10:** Decimal columns declare explicit `precision`/`scale` (e.g. `type: "decimal", precision:
  12, scale: 2` for `orders.amount`, matching Oracle's `NUMBER(12,2)` exactly) so ENGINE-02's type
  validator can reject a value with too many decimal places instead of relying on Oracle to silently
  round/truncate on insert.
- **D-11:** The column type enum includes `boolean` in addition to the five types the current Oracle
  DDL actually needs (string/integer/decimal/date/timestamp) — added for generality ahead of a
  concrete need, per explicit user choice.

**Oracle Target & Processing Config**
- **D-12:** `config.json` declares only Oracle target/invalid table names (`oracle.valid_table` /
  `oracle.invalid_table`, e.g. `"customers_valid"`/`"customers_invalid"`) — never connection details
  or credentials. Consistent with Phase 1's D-11 (single `admin`/`admin` credential via
  env vars/Airflow Connection); keeps `config.json` safe to log or version without ever containing a
  secret.
- **D-13:** Chunk size (ENGINE-07) is a per-dataset `config.json` field (e.g.
  `processing.chunk_size: 5000`), not a hardcoded engine-wide constant.

**Generator Behavior**
- **D-14:** The business-row generator (`generate_csv.py`) is controlled via CLI flags: `--dataset`,
  `--rows N`, `--invalid-ratio 0.X`, `--seed N` (seed defaults to a fixed value for reproducibility,
  overridable) — not via generation parameters embedded in `config.json` itself.
- **D-15:** The generator deliberately produces four invalid-row categories: wrong type, invalid
  date/timestamp, missing required field, and wrong column count (structural) — covering spec
  §14-15's canonical set plus the structural case from this phase's own success criteria.
- **D-16 (major scope expansion — user's explicit choice):** In addition to the business-row
  generator above, this phase adopts the reference repo's full manifest + digest-oracle byte-level
  fixture architecture from `tools/corpus/` (`generators.py`, `manifest.py`, `digests.py`,
  `__main__.py` — ~2600 lines in the reference, verified to have **zero `dataplat` coupling**, pure
  stdlib + PyYAML, so it's portable without violating the dependency-isolation constraint in
  PROJECT.md). This was offered as a real tradeoff against a lighter "a few malformed files, no
  manifest machinery" option and the user chose the full architecture explicitly, twice, after
  seeing the size/scope disclosed. — **Reversibility:** costly — **rationale:** once dozens of
  fixtures + a committed digest oracle exist and Phase 3's detection tests are written against them,
  descoping back to a lighter approach means discarding committed fixtures, the digest file, and any
  Phase 3 tests written against the manifest's `expect:` vocabulary.
  - **D-16a:** Fixture manifest scope is comprehensive-upfront (mirrors the reference repo's own
    precedent of authoring all 69 fixtures in its Phase 1, rather than a smaller subset grown
    incrementally).
  - **D-16b:** Includes a large/oversized fixture with an `RLIMIT_AS`-based unit test proving bounded
    memory independently of Phase 6's TEST-04 ~100K-row benchmark (which proves it empirically with
    timing/throughput numbers instead).
  - **D-16c:** Includes gzip + zip compressed fixture variants, specifically to exercise the Tier-A
    vendored `compression.py` module end-to-end — even though no requirement in this project calls
    for compressed CSVs in its actual pipeline.
  - **D-16d:** The manifest's `expect:` blocks stay **permissive/free-form** (prose describing
    intent), not a fixed `error_code` enum — mirrors the reference repo's own resolution. Phase 3
    (not Phase 2) owns the real `error_code` vocabulary (ENGINE-06); Phase 2 must not pre-lock it.
  - **D-16e:** Repo location: `tools/corpus/` (exact path match with the reference repo, easiest to
    cross-reference against the proven implementation). `tests/fixtures/corpus.yaml` (manifest) +
    `tests/fixtures/CORPUS.sha256` (digest oracle) are committed; `tests/fixtures/csv/**` (the
    generated fixture bytes) is gitignored.
  - **D-16f:** New Makefile targets: `make fixtures` (materializes the corpus, rewrites the digest
    file), `make fixtures-verify` (regenerates to a temp dir, diffs SHA-256 against the committed
    oracle), `make generate` (runs the business-row generator for customers/orders) — names matched
    exactly to the reference repo's own convention, extending Phase 1's Makefile-as-project-entrypoint
    precedent (D-14 in Phase 1's context).
  - **D-16g:** Phase 2 adds its own local combined verification gate (e.g. `make verify-phase2`, or
    an extension of Phase 1's `make verify`) running config/generator unit tests plus
    `make fixtures-verify` together — mirroring Phase 1's own `make verify` precedent. Wiring any of
    this into GitHub Actions CI is still explicitly Phase 6's job (CI-01); this gate is local-only.

**Testing**
- **D-17:** Config-model unit tests split one-file-per-concern:
  `tests/unit/test_config_models.py` (Pydantic validation rules: types, precision/scale,
  delimiter/decimal-separator collision, nullable/required) and `tests/unit/test_config_loader.py`
  (`load_config()` success/failure paths, multi-error aggregation) — mirrors the reference repo's
  own module-per-concern test split.

### Claude's Discretion
- Exact Pydantic v2 model class names/module layout within
  `packages/csv-processor/src/csv_processor/config/` (already-locked package path from Phase 1's
  D-16) — ARCHITECTURE.md's sketch (`models.py`, `loader.py`) is a reasonable starting point.
  ARCHITECTURE.md's own directory sketch used a slightly different top-level `src/csv_processor/`
  path; the actually-scaffolded, phase-1-locked path is `packages/csv-processor/src/csv_processor/`
  — use the latter.
- CONFIG-02's "complete list of errors in one pass" is Pydantic v2's native behavior
  (`ValidationError` aggregates all failures) — no custom error-collection mechanism needed.
- Exact `tools/corpus/manifest.py` fixture count and categories for the "comprehensive upfront" set
  (D-16a) — informed by the reference repo's own README §73-referenced category list, scoped down to
  what's relevant given this project's structural/type/nullability-only validation (no quarantine,
  SCD, locale-profile, or referential-integrity vocabulary to fixture against).
- gitleaks/secret-scanning is NOT a concern here (unlike the reference repo's ADR-0005 rationale) —
  this project's CI-01 scope (lint/typecheck/unit tests, Phase 6) never included a secret scanner,
  and generated fixture bytes are gitignored regardless.

### Deferred Ideas (OUT OF SCOPE)
None raised outside this phase's domain — all discussion stayed within Phase 2's config/generator
scope. CONFIG-03 (regex file patterns) and CONFIG-04 (business-rule min/max/allowed-values checks)
were already deferred to v2 in REQUIREMENTS.md before this discussion and were not reopened.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CONFIG-01 | Developer can define a dataset's ingestion contract in `config.json` (file pattern, CSV dialect, schema with types/nullability/date format, Oracle target/invalid table names) | "Config Model Shape" below gives the exact Pydantic v2 model, field-by-field, verified against the actual Oracle DDL (`docker/oracle/init/02_customers.sql`, `03_orders.sql`) |
| CONFIG-02 | System validates `config.json` once per run via Pydantic v2 before any CSV processing begins, failing fast with a complete list of errors on a malformed config | "Pattern 1: Pydantic v2 all-errors-at-once" + `load_config()` merge-then-validate pattern (ported from the reference repo's `dataplat.config.loader.load_config`) |
| GEN-01 | Developer can generate a deterministic CSV file matching a dataset's config, containing a configurable mix of valid rows (all schema types) and invalid rows (wrong type, invalid date, missing required field) | "Business-Row Generator" pattern (Faker + `random.Random(seed)`) + "Corpus/Digest-Oracle Fixture Subsystem" (the D-16 expansion) |
</phase_requirements>

## Summary

This phase has two genuinely distinct halves that must not be conflated. **Half one** (CONFIG-01/
CONFIG-02) is ordinary, well-precedented work: a frozen, `extra="forbid"` Pydantic v2 model tree
almost identical in shape to the reference repo's `dataplat.config.model.DatasetConfig`, minus every
field that belongs to the reference platform's out-of-scope SCD/quality/retention/freshness/
referential machinery. The exact column shape is now nailed down against this project's own,
already-created Oracle DDL (`docker/oracle/init/02_customers.sql`, `03_orders.sql`) rather than the
reference repo's illustrative YAML or the seed spec's example — this is the single most
load-bearing verified fact in this research, because a config schema that doesn't match the DDL's
real `NOT NULL` constraints will pass Phase 2's own validation cleanly and then fail at Oracle
`INSERT` time in Phase 4, surfacing as a confusing `DATABASE_ERROR` far from its real cause.

**Half two** (GEN-01, expanded by D-16) is the CSV generator, and it is really *two* generators with
different purposes, different determinism requirements, and different dependency footprints: a small
Faker-based **business-row generator** (`generate_csv.py`) that produces the two datasets' everyday
valid/invalid fixture files, and a much larger **manifest + digest-oracle byte-level corpus**
(`tools/corpus/`) ported from the reference repo's proven `tools/corpus/` package, whose entire
purpose is to pin byte-identical CSV fixtures — encodings, dialects, BOMs, malformed structure, one
oversized file, two compressed variants — that Phase 3's Tier-A vendored detection modules will be
tested against. The corpus subsystem is **not** business-data generation; it emits fixtures whose
bytes matter down to the byte, verified by a committed SHA-256 oracle rather than trusted on faith.

A finding not called out anywhere in CONTEXT.md but critical for planning correctly: **this project
has three separate places a Python dependency must be declared**, and Phase 2 is the first phase
that needs to add new ones. The root `pyproject.toml` (a `uv`-managed project, currently only
declaring `oracledb`) is what `make fixtures`/`make fixtures-verify`/`make generate`/local pytest
runs against — Faker, PyYAML, and Pydantic all need to land here. `packages/csv-processor`'s own
`pyproject.toml` (currently `dependencies = []`) is installed into the Airflow image with
`pip install --no-deps`, meaning its own declared dependencies are documentation only inside that
image — the actual install list lives a third place, hardcoded in `docker/airflow/Dockerfile`'s
`pip install` line (which already has `pydantic==2.13.4` from Phase 1). Getting this wrong (e.g.
adding Pydantic only to `csv-processor`'s `pyproject.toml` and expecting it to appear in the Airflow
container) is a real, easy-to-hit trap for this phase specifically, documented as a pitfall below.

**Primary recommendation:** Build `csv_processor/config/{models.py,loader.py}` as a frozen,
`extra="forbid"` Pydantic v2 tree validated once against the merged `defaults.json` + per-dataset
override (shallow, top-level-only merge — exactly the reference repo's `{**defaults, **dataset}`,
not a recursive deep-merge); build `generator/generate_csv.py` as a small Faker + `random.Random(seed)`
CLI tool with no dependency on `csv_processor` at all; and port `tools/corpus/` as a close structural
copy of the reference implementation (R1-R10 determinism rules verbatim), scoped down to ~25-30
fixtures covering dialect/encoding, structural, type/nullability, and byte-level-hard cases relevant
to this project's structural/type/nullability-only validation scope — dropping every reference-repo
fixture category tied to SCD, locale profiles, quarantine reasons, or referential integrity.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| `config.json` schema definition (Pydantic v2 models) | API / Backend (`csv_processor` package) | — | Config validation is engine-internal logic with zero Airflow/Oracle coupling — lives in the Airflow-agnostic `csv_processor` package per Phase 1's locked layout, not in DAG code |
| `config.json` load + merge (defaults + override) | API / Backend (`csv_processor.config.loader`) | — | Same package; the merge algorithm is pure Python, no I/O beyond reading two JSON files |
| Business-row CSV generation (`generate_csv.py`) | Database / Storage boundary (filesystem generator) | — | Writes files to `./data/<dataset>/`; deliberately zero coupling to `csv_processor` (ARCHITECTURE.md's own stated rationale) so it's buildable/testable before the engine exists |
| Fixture corpus generation (`tools/corpus/`) | Database / Storage boundary (dev/test tooling) | — | Not part of the runtime pipeline at all — a build-time tool invoked via `make fixtures`, output consumed only by Phase 3's test suite |
| Oracle DDL / schema shape (read-only input to this phase) | Database / Storage | — | Already created in Phase 1; this phase's config models must match it, never redefine it |

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `pydantic` | 2.13.4 (already pinned/approved in Phase 1; `2.13.5` is now latest on PyPI [VERIFIED: PyPI registry, `pip index versions pydantic`] but there is no reason to drift from the already-approved, already-deployed-in-`docker/airflow/Dockerfile` pin) | `config.json` schema validation, once per run | Already the project's locked choice (PROJECT.md); `model_validate()` raises one `ValidationError` aggregating every field error via `.errors()` — exactly CONFIG-02's "complete list, one pass" requirement, no custom collection code needed |
| `Faker` | 40.37.0 [VERIFIED: PyPI registry, `pip index versions Faker` — this is the current latest] | Realistic-looking valid-row field values (names, etc.) in the business-row generator | Already recommended in STACK.md; combine with `Faker.seed(n)` for determinism, and plain `random.Random(seed)` for numeric/date fields and invalid-row injection (Faker's own randomness is not what needs to be reproduced field-by-field — the *ratio and category* of invalid rows does) |
| `PyYAML` | 6.0.3 [VERIFIED: PyPI registry, `pip index versions PyYAML` — this is the current latest, and matches the reference repo's own pin exactly] | Parse `tests/fixtures/corpus.yaml` (the fixture manifest) | The reference repo's `tools/corpus/manifest.py` uses `yaml.safe_load` exclusively (never `yaml.load`) — this project's ported manifest loader must do the same; `config.json`/`configs/defaults.json` themselves stay plain JSON (stdlib `json`, no new dependency) per D-04/D-05's explicit `.json` naming |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| stdlib `csv` | n/a (stdlib) | Writing the business-row generator's CSV output | The generator needs an actual RFC-4180-aware writer (quoting, embedded commas) for realistic valid rows — hand-joining fields here (as the corpus generator does for byte-exactness) would be the wrong tool; reserve hand-joined lines for the corpus subsystem only, where exact bytes are the entire point |
| stdlib `hashlib` | n/a (stdlib) | SHA-256 digests for `CORPUS.sha256` | Matches the reference `digests.py`'s own approach — chunked `hashlib.sha256()` over the file, never loading a whole fixture (including the ~oversized one) into memory to hash it |
| stdlib `argparse` | n/a (stdlib) | CLI shape for both `generate_csv.py` and `python -m tools.corpus` | Reference repo's `tools/corpus/__main__.py` uses `argparse` with subparsers (`generate`/`verify`); mirror this shape for consistency and because it needs no new dependency |
| stdlib `resource` (POSIX only) | n/a (stdlib) | `RLIMIT_AS`-bounded subprocess for D-16b's memory-boundedness unit test | POSIX-only (`resource` module has no Windows equivalent) — acceptable here since the project runs under WSL2/Linux per its own stated environment; document this constraint in the test's own docstring |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Shallow `{**defaults, **dataset}` top-level merge for `config.json` | A recursive/deep-merge library (e.g. `deepmerge`) | The reference repo deliberately does NOT deep-merge (its own `configs/defaults.yaml` comment explicitly warns: overriding one sub-key of a nested block loses every sibling default under a naive deep-merge too, so the codebase commits to flat top-level keys instead). Adding deep-merge here would diverge from the ported precedent and hide a real footgun rather than making it visible; only reach for a merge library if a future phase needs genuinely nested per-dataset overrides of a shared nested block. |
| Hand-rolled `sha256sum`-format digest read/write | A checksum library | Not worth a dependency — the format is four lines of stdlib `hashlib` + string formatting (reference `digests.py`, ported near-verbatim); no reason to add a dependency for something this small and already proven |
| `Faker` for ALL generated values including invalid-row injection | Pure `random`/`string` module hand-rolling | Faker only for "looks realistic" fields (names, countries); use plain `random.Random(seed)` for anything whose determinism must be provable by inspection (invalid-row category selection, numeric/date ranges) — matches STACK.md's own already-stated split |

**Installation:**
```bash
# Root pyproject.toml (uv-managed) — local dev/test/generator tooling
uv add "pydantic==2.13.4" "Faker==40.37.0" "PyYAML==6.0.3"
uv add --dev "pytest==9.1.1"   # D-17's test files need a real test runner; nothing installs one yet

# packages/csv-processor/pyproject.toml — the engine's own declared dependency
# (documentation-only inside the Docker image, see "Don't Hand-Roll"/Pitfalls below,
# but still correct to declare for anyone installing csv-processor standalone,
# e.g. `pip install -e packages/csv-processor` for local unit tests)
# Add to packages/csv-processor/pyproject.toml's [project] dependencies:
#   "pydantic==2.13.4"
```

**Version verification:** Ran `pip index versions <package>` live against PyPI for `pydantic`,
`Faker`, and `PyYAML` (2026-08-28) — see the Core table above for exact results. `pydantic` has a
newer `2.13.5` release than the already-approved/deployed `2.13.4` pin; recommend staying on
`2.13.4` for consistency with the already-built `docker/airflow/Dockerfile` rather than bumping
mid-project without a concrete reason.

## Package Legitimacy Audit

Ran `gsd-tools query package-legitimacy check --ecosystem pypi` against every package this phase
newly introduces (`pydantic` was already vetted and approved in Phase 1 via its own checkpoint —
re-included here only for completeness, not re-litigated).

| Package | Registry | Age | Downloads | Source Repo | Verdict | Disposition |
|---------|----------|-----|-----------|-------------|---------|-------------|
| `Faker` | PyPI | First published 2012 (version `0.0.2` on PyPI [VERIFIED: PyPI JSON API, `pypi.org/pypi/Faker/json`]); latest `40.37.0` released 2026-08-21 | Unknown to the legitimacy tool (PyPI download-count API not plumbed in) | `github.com/joke2k/faker` | `SUS` (reasons: `too-new`, `unknown-downloads` — both artifacts of the tool checking the *latest release's* publish date, not the package's actual age, and having no PyPI download-stats source wired in) | **Overridden to Approved** — 14+ years of continuous releases, a real, long-standing GitHub repo, and this exact package/version was already independently recommended in `.planning/research/STACK.md` before this phase. Not a slopsquat risk. |
| `PyYAML` | PyPI | First published as far back as version `3.01` on PyPI (the JSON API's earliest indexed release) [VERIFIED: PyPI JSON API, `pypi.org/pypi/PyYAML/json`] — one of the oldest, most foundational packages in the Python ecosystem | Unknown to the legitimacy tool | `pyyaml.org` | `SUS` (reason: `unknown-downloads` only) | **Overridden to Approved** — this is the de facto standard YAML library for Python (used by `pip` itself internally); the reference repo's own `tools/corpus/manifest.py` depends on it identically. |
| `pydantic` | PyPI | Already approved in Phase 1's own package-legitimacy checkpoint at `2.13.4` | — | `github.com/pydantic/pydantic` | Re-checked here: `SUS` (`too-new` — because `2.13.5` released same-day as this research; `unknown-downloads`) for the *latest* version; the pinned `2.13.4` is one release older and already deployed | **Already Approved (Phase 1)** — no new action; continue pinning `2.13.4`, do not bump to `2.13.5` without a reason. |

**Packages removed due to `[SLOP]` verdict:** none.
**Packages flagged as suspicious `[SUS]`:** `Faker`, `PyYAML` — both overridden above with concrete
age/reputation evidence (first-PyPI-release lookups, real GitHub/foundational-project status), not
merely re-asserted from training-data confidence. The tool's `SUS` verdicts here are an artifact of
missing PyPI download-count data and a `too-new` heuristic that inspects the *latest* release date
rather than the package's first release — not a genuine legitimacy concern. No `checkpoint:human-verify`
task is required before installing either package, given the concrete evidence above, but the planner
may still add one if the project's risk tolerance prefers a human glance regardless.

*Note: `clevercsv` and `charset-normalizer` (STACK.md's Tier-A detection dependencies) are **not**
part of this phase's dependency set — they are consumed by Phase 3's vendored `detect/dialect.py`/
`detect/encoding.py` modules, which this phase does not create (see "What This Phase Does NOT Do"
below). Do not add them to any `pyproject.toml` in Phase 2.*

## Architecture Patterns

### System Architecture Diagram

```
                         ┌─────────────────────────────┐
                         │   configs/defaults.json     │
                         │   (shared csv/oracle/proc.   │
                         │    defaults, D-05)           │
                         └──────────────┬───────────────┘
                                        │ shallow merge, dataset keys win
                                        │ (top-level only — see Pitfalls)
              ┌─────────────────────────▼─────────────────────────┐
              │  configs/datasets/{customers,orders}.json (D-04)  │
              └─────────────────────────┬─────────────────────────┘
                                        │ merged dict
                                        ▼
              ┌───────────────────────────────────────────────────┐
              │  csv_processor.config.loader.load_config(path)    │
              │    -> DatasetConfig.model_validate(merged)         │
              │    -> raises ConfigurationError with ALL field    │
              │       errors on failure (CONFIG-02), never one    │
              │       at a time                                    │
              └───────────────────────────┬───────────────────────┘
                                        │ validated, frozen DatasetConfig
                     ┌──────────────────┼──────────────────────┐
                     ▼                                          ▼
     ┌───────────────────────────────┐          ┌──────────────────────────────────┐
     │ generator/generate_csv.py     │          │ (consumed later, Phase 3)        │
     │  --dataset --rows             │          │ csv_processor.detect/*, .validate│
     │  --invalid-ratio --seed       │          │ read the SAME config shape       │
     │  Faker + random.Random(seed)  │          │ (nothing new produced here)      │
     └───────────────┬────────────────┘          └───────────────────────────────────┘
                     │ writes
                     ▼
      ./data/<dataset>/<dataset>_YYYYMMDD.csv  (D-06/D-07, gitignored, mounted
                                                 into the Airflow container)


  ── separate, parallel subsystem — no data or config dependency on the above ──

  tests/fixtures/corpus.yaml (manifest, committed, D-16e)
              │  load_manifest() — yaml.safe_load + hand-validated model
              ▼
  tools/corpus/generators.py  (R1-R10 determinism rules)
              │  per-fixture RNG: Random(sha256(f"{seed}|{name}"))
              ▼
  tests/fixtures/csv/**  (gitignored — regenerated via `make fixtures`)
              │  sha256_file() per emitted path
              ▼
  tests/fixtures/CORPUS.sha256  (digest oracle, committed, D-16e)
              │
              ▼
  `make fixtures-verify`: regenerate to a temp dir, diff SHA-256 against the
  committed oracle — exit non-zero on ANY mismatch (consumed later by Phase 3's
  detection tests, which read tests/fixtures/csv/** directly)
```

### Recommended Project Structure

```
lightweight-airflow-etl/
├── configs/
│   ├── defaults.json                        # D-05: shared csv/oracle/processing defaults
│   └── datasets/
│       ├── customers.json                   # D-04
│       └── orders.json                      # D-04
├── packages/csv-processor/
│   ├── pyproject.toml                       # add pydantic==2.13.4 as a real dependency here
│   └── src/csv_processor/
│       ├── __init__.py                      # exists (Phase 1 scaffold)
│       └── config/
│           ├── __init__.py
│           ├── models.py                    # DatasetConfig, ColumnSpec, CsvDialectConfig,
│           │                                 # OracleTargetSpec, ProcessingConfig
│           └── loader.py                    # load_config(dataset_path, defaults_path) -> DatasetConfig
├── generator/
│   └── generate_csv.py                      # business-row generator (D-14/D-15), zero csv_processor coupling
├── tools/
│   └── corpus/                              # D-16e: exact path match with the reference repo
│       ├── __init__.py
│       ├── manifest.py                      # ported, scoped-down fixture/manifest model
│       ├── generators.py                    # ported near-verbatim (R1-R10)
│       ├── digests.py                       # ported near-verbatim
│       └── __main__.py                      # `python -m tools.corpus generate|verify`
├── tests/
│   ├── unit/
│   │   ├── test_config_models.py            # D-17
│   │   ├── test_config_loader.py            # D-17
│   │   └── test_generate_csv.py             # generator determinism/ratio tests
│   └── fixtures/
│       ├── corpus.yaml                      # D-16e: committed manifest
│       ├── CORPUS.sha256                    # D-16e: committed digest oracle
│       └── csv/                             # D-16e: gitignored, regenerated via `make fixtures`
├── data/                                    # D-06: gitignored, generator output, mounted into compose
│   ├── customers/
│   └── orders/
└── Makefile                                 # extended: fixtures, fixtures-verify, generate, verify-phase2
```

### What This Phase Does NOT Do

Explicitly out of scope for Phase 2, despite D-16's large corpus footprint — stated because
CONTEXT.md's phrasing ("exercises the Tier-A vendored `detect/*`/`compression.py` modules ahead of
Phase 3 actually using them") could be misread as "Phase 2 vendors those modules":

- **No vendoring of `detect/dialect.py`, `detect/encoding.py`, `detect/header.py`,
  `detect/filename.py`, `detect/schema.py`, or `compression.py` happens in this phase.** Those are
  Phase 3's two-tier-reuse work (PROJECT.md), consuming `clevercsv`/`charset-normalizer`. Phase 2
  only *produces the fixture bytes* those modules will later be tested against — the corpus is
  input to Phase 3's test suite, not a consumer of Phase 3's code.
- **No `csv_processor.detect`/`.validate`/`.normalize`/`.load` code is written here.** Those are
  ENGINE-01 through ENGINE-09 (Phase 3) and LOAD-01 through LOAD-04 (Phase 4).
- **No Airflow DAG code, no `docker-compose.yml` service changes beyond the one new volume mount
  (D-06).**

### Config Model Shape (verified against the actual Oracle DDL)

The column shape below is **not** taken from the reference repo's `customers.yaml`/`orders.yaml`
(those encode a different, superset schema with `scd_type`, `business_key`, etc. — explicitly out of
scope per CLAUDE.md) or from the seed spec's illustrative example. It is derived directly from this
project's own, already-created DDL, read this session:

**`docker/oracle/init/02_customers.sql:11-19`** [VERIFIED: docker/oracle/init/02_customers.sql:11-19]:
```sql
CREATE TABLE customers_valid (
  customer_id      VARCHAR2(64)  NOT NULL,
  name             VARCHAR2(255) NOT NULL,
  country          VARCHAR2(64)  NOT NULL,
  birth_date       DATE,
  event_ts         TIMESTAMP WITH TIME ZONE NOT NULL,
  signup_country   VARCHAR2(64),
  ingested_at      DATE          DEFAULT SYSDATE NOT NULL
)
```

**`docker/oracle/init/03_orders.sql:11-17`** [VERIFIED: docker/oracle/init/03_orders.sql:11-17]:
```sql
CREATE TABLE orders_valid (
  order_id       VARCHAR2(64) NOT NULL,
  customer_id    VARCHAR2(64) NOT NULL,
  order_date     DATE,
  amount         NUMBER(12,2),
  ingested_at    DATE         DEFAULT SYSDATE NOT NULL
)
```

`ingested_at` (both tables) and `error_code`/`error_message`/`source_file`/`row_number` (the
`_invalid` tables only, e.g. `customers_invalid` at `docker/oracle/init/02_customers.sql:24-36`
[VERIFIED: docker/oracle/init/02_customers.sql:32-35], quoted verbatim: `error_code VARCHAR2(64)
NOT NULL, error_message VARCHAR2(4000) NOT NULL, source_file VARCHAR2(255) NOT NULL, row_number
NUMBER NOT NULL`) are **engine-managed columns** — never part of `config.json`'s per-column schema
and never present in a generated CSV. They're written by Phase 3/4's engine/loader, not declared by
this phase's config contract.

**Derived column contract** (DDL nullability is the authority; `nullable`/`required` in
`config.json` must never be LESS restrictive than the DDL's actual `NOT NULL` — see Pitfall below):

| Dataset | Column | Type | DDL nullability | Recommended `nullable`/`required` | Notes |
|---------|--------|------|------------------|-------------------------------------|-------|
| customers | `customer_id` | string | `NOT NULL` | `false` / `true` | |
| customers | `name` | string | `NOT NULL` | `false` / `true` | |
| customers | `country` | string | `NOT NULL` | `false` / `true` | |
| customers | `birth_date` | date | nullable | `true` / `true` | format e.g. `"%Y-%m-%d"` (D-08) |
| customers | `event_ts` | timestamp | `NOT NULL` | `false` / `true` | format e.g. `"%Y-%m-%dT%H:%M:%S%z"` (D-08) |
| customers | `signup_country` | string | nullable | `true` / `false` (or `true` — Claude's discretion) | D-09's "future-proofing, neither dataset strictly needs the distinction" applies exactly here |
| orders | `order_id` | string | `NOT NULL` | `false` / `true` | |
| orders | `customer_id` | string | `NOT NULL` | `false` / `true` | FK to `customers.customer_id`, unenforced per spec §28 — do not add a referential check |
| orders | `order_date` | date | nullable | `true` / `true` | format e.g. `"%Y-%m-%d"` |
| orders | `amount` | decimal | nullable | `true` / `true` | `precision: 12, scale: 2` — matches `NUMBER(12,2)` exactly (D-10) |

### Pattern 1: Frozen, `extra="forbid"` Pydantic v2 model tree

**What:** Every model in `csv_processor/config/models.py` sets
`model_config = ConfigDict(extra="forbid", frozen=True)` — an unrecognized key in `config.json` is a
validation-time error (catches typos), and a validated `DatasetConfig` instance can never be mutated
after construction. This is the reference repo's own convention (`dataplat/config/model.py:89`,
`:127`, etc. — every single model class), read directly this session.

**When to use:** Every model in this phase's config tree, no exceptions.

**Example** (scoped-down skeleton, column types/nullability per the table above — the `escapechar`/
`doublequote`/`lineterminator` fields are D-02's explicit additions beyond the reference repo's own
`CsvParsingConfig` shape):
```python
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator

_COLUMN_TYPES = Literal["string", "integer", "decimal", "date", "timestamp", "boolean"]  # D-11

class ColumnSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str
    type: _COLUMN_TYPES
    nullable: bool
    required: bool
    format: str | None = None        # strptime string, D-08 — required for date/timestamp only
    precision: int | None = None     # D-10 — decimal only
    scale: int | None = None         # D-10 — decimal only

class CsvDialectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    delimiter: str = ","
    encoding: str = "utf-8"
    quotechar: str = '"'
    header: bool = True
    escapechar: str | None = None     # D-02: beyond the reference repo's own scope
    doublequote: bool = True          # D-02
    lineterminator: str = "\n"        # D-02

class OracleTargetSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    valid_table: str
    invalid_table: str

class ProcessingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    chunk_size: int = Field(gt=0)      # D-13

class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    dataset: str
    file_pattern: str                  # D-07: glob, e.g. "customers_*.csv"
    csv: CsvDialectConfig = Field(default_factory=CsvDialectConfig)
    columns: list[ColumnSpec]
    oracle: OracleTargetSpec
    processing: ProcessingConfig
```

### Pattern 2: `load_config()` merges then validates, never lets `ValidationError` escape

**What:** Ported from `dataplat/config/loader.py:39-72`, read directly this session — the load
function reads two JSON files (not YAML, per D-04/D-05's `.json` naming), does a **shallow,
top-level-only merge** (`{**defaults, **dataset}` — dataset keys win on collision), and re-raises any
`pydantic.ValidationError` as a project-local `ConfigurationError` carrying `exc.errors()` — the
structured list Pydantic already collected, satisfying CONFIG-02's "complete list of errors in one
pass" with zero custom collection code:

```python
# Source: ported pattern from dataplat/config/loader.py:39-72 (read this session)
import json
from pathlib import Path
from pydantic import ValidationError

class ConfigurationError(Exception):
    def __init__(self, message: str, *, context: dict) -> None:
        super().__init__(message)
        self.context = context

def load_config(path: Path, *, defaults_path: Path) -> DatasetConfig:
    defaults = json.loads(defaults_path.read_text(encoding="utf-8")) if defaults_path.exists() else {}
    dataset = json.loads(path.read_text(encoding="utf-8"))
    merged = {**defaults, **dataset}   # shallow, top-level only — see Pitfall below
    try:
        return DatasetConfig.model_validate(merged)
    except ValidationError as exc:
        raise ConfigurationError(
            f"invalid dataset config at {path}: {exc}",
            context={"path": str(path), "errors": exc.errors()},
        ) from exc
```

### Pattern 3: Business-row generator is Faker + `random.Random(seed)`, zero `csv_processor` coupling

**What:** `generator/generate_csv.py` takes `--dataset`, `--rows`, `--invalid-ratio`, `--seed`
(D-14) and writes rows via stdlib `csv.writer` (not hand-joined lines — that discipline is reserved
for the byte-exact corpus subsystem below). Determinism comes from `Faker.seed(seed)` for
realistic-looking fields and a separate `random.Random(seed)` instance for: (a) which rows are
invalid, (b) which of the four invalid categories (D-15: wrong type / invalid date / missing
required field / wrong column count) a given invalid row uses, and (c) numeric/date value ranges.
It reads a dataset's `config.json` (via `csv_processor.config.loader.load_config`) purely to learn
column names/types/nullability — never imports any detection/validation code.

**When to use:** This is the only generator pattern for GEN-01's core business-row requirement.

### Corpus/Digest-Oracle Fixture Subsystem (the D-16 expansion)

This is genuinely new authorship for this project (Tier B: read the algorithm, adapt scope — not a
file-for-file vendor), so the guidance below is prescriptive about *mechanism* (verified against the
reference repo's actual code) and explicitly flagged `[ASSUMED]`/recommendation-only about *exact
scope* (fixture count/categories), per CONTEXT.md's own "Claude's Discretion" note.

#### The ten determinism rules (verbatim from the reference repo's ADR, read this session)

These are non-negotiable mechanisms, reproduced here because CONTEXT.md's canonical_refs flags them
as "load-bearing for D-16's byte-identity guarantee and must be reproduced, not reinvented"
[VERIFIED: /home/user/projects/airflow-platform/docs/adr/0005-fixture-corpus-generated-from-a-seed.md:75-86]:

| # | Rule | Mechanism it defeats |
|---|---|---|
| R1 | Derive a **per-fixture** RNG: `Random(int.from_bytes(sha256(f"{MASTER_SEED}\|{name}").digest(), "big"))` | A single shared stream makes fixture *N*'s bytes depend on how many values fixtures 1..*N*-1 consumed |
| R2 | Consume randomness **only** through `.random()`/`.getrandbits()`. Derive integers/choices by arithmetic over `.random()` | `choice`/`shuffle`/`sample`/`randrange`/`randint` are not guaranteed stable across Python versions; only `.random()`'s sequence is |
| R3 | Build a `str`, encode to the declared encoding, write with `open(path, "wb")` — never text-mode `open` | Text-mode `open` consults `locale.getpreferredencoding(False)`, which differs machine-to-machine |
| R4 | The line terminator is an explicit manifest field, hand-joined — never `csv.writer`'s default or a translated `"\n"` | `csv.writer` defaults to `\r\n`; text-mode writes translate `\n` per platform |
| R5 | `gzip.GzipFile(..., mtime=0, filename="")`; `zipfile.ZipInfo(name, date_time=(1980,1,1,0,0,0))` | Both formats embed wall-clock timestamps (gzip also the source filename) in headers |
| R6 | No `datetime.now()`, `uuid4()`, `os.urandom()`, `time.time()`, `os.getpid()` anywhere in the corpus package | Easy to reintroduce via a "helpful" `generated_at` comment |
| R7 | Iterate the manifest in **declared order** — never a `set`, never `sorted()` a heterogeneous key | `str.__hash__` is `PYTHONHASHSEED`-salted |
| R8 | Never read `os.listdir()`/`glob` ordering as generation input | Filesystem ordering varies by filesystem type |
| R9 | Build NFC/NFD-style variants with explicit `unicodedata.normalize(...)` calls, never by pasting visually-identical strings into source | Editors/git filters silently renormalize source files |
| R10 | Format numbers with explicit format strings or `Decimal`; never `str(float)` | Float arithmetic precision is not a risk worth taking in a corpus about numeric fidelity |

**Scope note:** this project's validation is structural/type/nullability only (per CLAUDE.md), so
R9 (NFC/NFD) is the one rule with genuinely no applicable fixture in this project's scope — no
Unicode-normalization-sensitive validator exists here. Keep the *rule* documented (it constrains the
generator package regardless of which fixtures use it) but it is fine for zero fixtures to actually
exercise it, unlike R1-R8/R10 which every fixture exercises implicitly.

#### RNG derivation and gzip/zip determinism (verified code, read this session)

```python
# Source: tools/corpus/generators.py:127-139 (read this session) — R1
def stream_for(master_seed: str, name: str) -> random.Random:
    digest = hashlib.sha256(f"{master_seed}|{name}".encode()).digest()
    return random.Random(int.from_bytes(digest, "big"))
```

```python
# Source: tools/corpus/generators.py:406-435 (read this session) — R5
# gzip: mtime=0 and filename="" are non-negotiable — both are embedded in the
# gzip header and vary run-to-run otherwise.
with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=9, mtime=0, filename="") as compressed:
    compressed.write(source.read())

# zip: ZipInfo.date_time defaults to wall-clock time — pin it explicitly.
member = zipfile.ZipInfo(filename=wrapped_name, date_time=(1980, 1, 1, 0, 0, 0))
with zipfile.ZipFile(raw, mode="w") as archive:
    archive.writestr(member, target_bytes)
```

#### RLIMIT_AS bounded-memory technique (D-16b) — mechanism only, no reference implementation exists

The reference repo's ADR *describes* this technique (`resource.setrlimit(RLIMIT_AS, 128 MiB)` in a
subprocess makes a streaming reader pass and a buffering reader die with `MemoryError`) but a search
of the reference repo's actual `tests/` and `packages/` trees found **no committed implementation of
it** — only a manifest-level validation rule (`approx_bytes > 2 * rlimit_as_bytes`, enforced in
`tests/unit/test_corpus_manifest.py:146-173`, read this session) that checks the *declaration* is
internally consistent, not a live subprocess memory test. This means D-16b's actual test is new
authorship for this project, following the ADR's described mechanism:

```python
# New authorship — mechanism described in the reference repo's ADR-0005, no
# existing implementation to port. POSIX-only (the `resource` module has no
# Windows equivalent); acceptable given this project's WSL2/Linux target.
import resource
import subprocess
import sys

def _child_read_streaming(path: str, limit_bytes: int) -> None:
    resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
    with open(path, newline="") as handle:
        for _ in handle:  # streaming: one line materialized at a time
            pass

def test_streaming_read_stays_under_memory_limit(large_fixture_path):
    result = subprocess.run(
        [sys.executable, "-c", "<inline call to _child_read_streaming>", large_fixture_path, "134217728"],
        capture_output=True,
    )
    assert result.returncode == 0  # streaming reader survives a 128 MiB address-space cap
```

Recommend a large fixture sized so `approx_bytes > 2 * rlimit_as_bytes` (mirroring the reference's
own validated rule) — e.g. a ~50-100 MB fixture against a 16-32 MiB `RLIMIT_AS`, comfortably smaller
than the reference repo's own 241 MB/128 MiB pair since this project's benchmark target is ~100K
rows (Phase 6's TEST-04), not the reference platform's larger target.

#### Manifest model shape (ported, with D-16d's permissive `expect:` block)

The reference repo's `tools/corpus/manifest.py` model (read this session, `manifest.py:50-118`)
defines five `generator` kinds (`tabular`, `literal`, `literal_unicode`, `wrapper`, `multipart`) and
four `row_spec` column kinds (`zero_padded_int`, `pick`, `decimal`, `repeat`). Recommend porting all
five generator kinds and all four column kinds unchanged — they are generic byte-construction
primitives with no reference-platform-specific vocabulary baked in. **Drop:** the `multipart` kind
has no use case here (no multi-part file delivery in this project's scope, per PROJECT.md's own
out-of-scope list not mentioning it) — keep it in the model for parity/future use but do not author
any fixture using it. The `expect:` block stays a permissive `dict[str, Any]` — per D-16d, this
project's own `error_code` vocabulary (owned by Phase 3/ENGINE-06) must not be pre-locked into the
manifest schema.

#### Recommended scoped fixture category list

`[ASSUMED — new authorship, not verified against any canonical source; user/planner should confirm
before finalizing the manifest]`. Roughly 25-30 fixtures (vs. the reference repo's 69), because this
project drops every fixture category tied to SCD, locale/normalization profiles, quarantine reasons,
or referential integrity (none of which this project validates, per CLAUDE.md's explicit "only
structural/type/nullability validators are in scope"):

| Category | Approx. count | Examples | Reference repo's equivalent category (dropped/kept) |
|----------|---------------|----------|-------------------------------------------------------|
| Dialect/encoding | 6-8 | comma/semicolon/pipe/tab delimiters, quotechar variants, `escapechar`/`doublequote` on/off (D-02's own explicit test-coverage ask), CRLF/LF/CR line terminators, UTF-8 BOM, UTF-16, Windows-1252/non-ASCII | Kept — matches reference `01-09`, `26-27`, `30-31` |
| Structural | 8-10 | missing column, extra column, duplicate column, wrong column count, empty file, header-only, no header, ragged rows, blank lines interspersed, no trailing newline | Kept — matches reference `11-19`, `33`, `45-47` |
| Type/nullability | 5-6 | invalid integer, invalid decimal (too many decimal places — exercises D-10's precision/scale), invalid date/timestamp format, empty required field, empty nullable field (should pass) | New — this project's own vocabulary, no reference-repo file-for-file match, since the reference repo's type-damage fixtures (`50-60`) are entangled with its locale/normalization profile, which this project has no equivalent of |
| Byte-level-hard | 4-5 | embedded newline in quoted field, embedded delimiter in quoted field, doubled-quote escaping, NUL byte, field exceeding a configured size limit | Kept — matches reference `09-10`, `32`, `67` |
| Large/compressed | 3 | one `profile: large` oversized file (D-16b), one gzip-wrapped fixture, one zip-wrapped fixture (D-16c) | Kept — matches reference `29`, `61`, `71` |

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Multi-error config validation | A custom error-accumulator that walks the config dict field by field | Pydantic v2's native `ValidationError.errors()` | It already collects every field failure in one pass — CONFIG-02's exact requirement, verified via Context7-fetched docs in STACK.md and confirmed again here reading the reference repo's actual `loader.py` usage |
| SHA-256 digest file format | A custom checksum manifest format | The `sha256sum`-compatible format (`{digest}  {name}\n`) from `tools/corpus/digests.py` | `sha256sum -c` must work against it from the repo root — a private format loses that independent-verification property for no benefit |
| Deep-merge for `config.json` + `defaults.json` | A recursive dict-merge utility/library | Flat, shallow, top-level-only `{**defaults, **dataset}` | Matches the reference repo's own deliberate choice (its `defaults.yaml` comment explains why); a shallow merge is what CONFIG-01/CONFIG-02's flat schema actually needs, and a deep-merge here would hide the exact footgun the reference repo calls out (see Pitfalls) |
| CSV dialect sniffing in the generator | Auto-detecting a "sensible" dialect from data | Read the dialect straight from the dataset's own already-validated `config.json` | The generator and the (future) engine are meant to agree on dialect via the *same config*, not via independent guessing — this is the whole point of D-01 making the dialect config-driven |

**Key insight:** Every "don't hand-roll" item above already has a working, read implementation one
directory tree away (`/home/user/projects/airflow-platform`) — the discipline this phase needs is
reading and porting correctly, not inventing.

## Common Pitfalls

### Pitfall 1: Adding a new dependency to the wrong `pyproject.toml` — it silently never reaches the Airflow container

**What goes wrong:** `packages/csv-processor/pyproject.toml` currently declares
`dependencies = []` [VERIFIED: packages/csv-processor/pyproject.toml:10]. `docker/airflow/Dockerfile`
installs it with `pip install --no-deps packages/csv-processor/`
[VERIFIED: docker/airflow/Dockerfile — `RUN pip install --no-cache-dir --no-deps packages/csv-processor/`],
meaning **whatever `csv-processor`'s own `pyproject.toml` declares is never actually installed
inside the Airflow image** — the real install list is the separate, hardcoded `pip install` line
above it in the same Dockerfile, which already lists `pydantic==2.13.4` explicitly. A developer who
adds `pydantic` only to `packages/csv-processor/pyproject.toml` (the "obviously correct" place,
since that's where `config/models.py` lives) will find it works in local tests (if also added to the
root `pyproject.toml`) but has zero effect on the Airflow container's actual Python environment.

**Why it happens:** `--no-deps` is easy to miss when scanning the Dockerfile quickly, and declaring
a dependency in the package that imports it is the natural instinct.

**How to avoid:** Any new import inside `packages/csv-processor/src/csv_processor/` needs its
package added in **three** places if it must run inside the Airflow container: (1)
`packages/csv-processor/pyproject.toml` (for correctness/documentation and for anyone who
`pip install -e`s the package standalone), (2) `docker/airflow/Dockerfile`'s explicit `pip install`
line (the one that actually matters for the running container), and (3) the root `pyproject.toml`
(for local `pytest`/`make fixtures`/`make generate` runs, which never touch the Docker image at
all). Phase 2 only needs `pydantic` inside `csv_processor` — it's already in the Dockerfile from
Phase 1, so only (1) and (3) are new work this phase.

**Warning signs:** `ModuleNotFoundError` inside an Airflow task log for a package that "is definitely
installed" (checked locally, works in `pytest`) — this is the signature of the dependency having
been added to the wrong file.

**Phase to address:** This phase, since it's the first to add a real dependency to
`csv-processor`'s previously-empty `dependencies = []`. Document the three-place rule directly in
`packages/csv-processor/pyproject.toml` as a comment so Phase 3 (which will add `clevercsv`/
`charset-normalizer` the same way) doesn't rediscover this.

### Pitfall 2: `config.json`'s `nullable`/`required` looser than the Oracle DDL's actual `NOT NULL`

**What goes wrong:** If `config.json` declares `nullable: true` for a column the DDL marks
`NOT NULL` (e.g. accidentally marking `event_ts` nullable), a "valid" row with an empty `event_ts`
passes Phase 2/3's own validation cleanly, then fails at Oracle `INSERT` time in Phase 4 with a
constraint-violation error — which the engine's status-code model would likely surface as
`DATABASE_ERROR`, not `CONFIGURATION_ERROR`, even though the real root cause is a config/DDL
mismatch, not a database outage.

**Why it happens:** `config.json`'s `nullable`/`required` and the DDL's `NOT NULL` are two
independent declarations (there's no automated cross-check between them in this project's own
CONTEXT.md decisions) — nothing stops them from silently disagreeing.

**How to avoid:** Use the verified column table in "Config Model Shape" above as the source of
truth for every `nullable`/`required` value written into `configs/datasets/*.json`. Optionally, add
a unit test (fits naturally into D-17's `test_config_models.py`) that documents the DDL's `NOT NULL`
columns as a literal list and asserts the committed `customers.json`/`orders.json` never mark one of
them nullable — cheap insurance against future drift.

**Phase to address:** This phase, when authoring the two dataset config files.

### Pitfall 3: A shallow merge silently drops sibling defaults inside a nested block

**What goes wrong:** `{**defaults, **dataset}` only overrides *top-level* keys. If
`configs/defaults.json` declares a nested block (e.g. `"processing": {"chunk_size": 5000, "some_other_field": true}`)
and a dataset config overrides `"processing": {"chunk_size": 200}"`, the dataset's `processing` dict
**entirely replaces** the default's — `some_other_field` is silently gone, not merged in.

**Why it happens:** This is a real, documented footgun the reference repo's own
`configs/defaults.yaml` comment calls out explicitly (read this session) — it's a consequence of
choosing a flat, shallow merge (which this phase should still choose, per "Don't Hand-Roll" above)
rather than a hidden bug in the merge code itself.

**How to avoid:** Keep every nested config block's fields either fully defaulted (never partially
overridden per-dataset) or fully declared per-dataset (never inherited). For this phase's schema,
`processing.chunk_size` (D-13) is the only nested-ish field likely to appear in `defaults.json` —
recommend NOT putting `processing` in `defaults.json` at all, and requiring every dataset config to
declare its own complete `processing` block, sidestepping the footgun entirely rather than
documenting around it.

**Phase to address:** This phase — decide the merge boundary explicitly when writing
`configs/defaults.json`.

### Pitfall 4: Corpus fixture manifest changes silently re-baseline every downstream test

**What goes wrong:** A tweak to `tools/corpus/generators.py` (e.g. "fix" a rendering detail) changes
fixture bytes without anyone noticing, because nothing compares the new bytes against the old ones
automatically at commit time.

**Why it happens:** Without a committed digest oracle, a generator change and a data change look
identical from the outside — this is the entire failure mode ADR-0005 exists to prevent.

**How to avoid:** `make fixtures-verify` must run as part of this phase's own `make verify-phase2`
gate (D-16g) and — per the reference repo's own migration-trigger note — stay part of `make check`
until it measurably slows down (their stated trigger: `fixtures-verify` alone approaching ~30s).
Never silently regenerate `CORPUS.sha256` as a side effect of running tests; only `make fixtures`
(an explicit, reviewable action) may rewrite it.

**Phase to address:** This phase, when wiring the Makefile targets.

## Code Examples

### Loading and validating a dataset config, with all errors surfaced at once

```python
# Source: pattern verified against pydantic 2.13's documented ValidationError.errors()
# behavior (STACK.md, Context7 /pydantic/pydantic) and the reference repo's
# dataplat/config/loader.py:39-72 (read this session)
try:
    config = load_config(Path("configs/datasets/customers.json"),
                          defaults_path=Path("configs/defaults.json"))
except ConfigurationError as exc:
    for error in exc.context["errors"]:
        print(f"{'.'.join(str(p) for p in error['loc'])}: {error['msg']}")
    # prints EVERY invalid field, not just the first — CONFIG-02
```

### Corpus CLI shape (ported from `tools/corpus/__main__.py:34-72`, read this session)

```python
# python -m tools.corpus generate --manifest tests/fixtures/corpus.yaml \
#     --out tests/fixtures/csv --write-digests tests/fixtures/CORPUS.sha256
# python -m tools.corpus verify --manifest tests/fixtures/corpus.yaml \
#     --digests tests/fixtures/CORPUS.sha256
```

Names in the digest oracle are written **relative to the repository root** (e.g.
`tests/fixtures/csv/01_simple.csv`), specifically so `sha256sum -c tests/fixtures/CORPUS.sha256`
works as an independent check from the repo root — not a private format only this project's own
tooling can validate [VERIFIED: /home/user/projects/airflow-platform/tools/corpus/digests.py:1-15].

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| Committing hand-crafted malformed CSV fixtures directly to git | Generate deterministically from a committed manifest + digest oracle, gitignore the bytes | Reference repo's ADR-0005 (accepted 2026-08-11) | Byte-identical corpus without git bloat or secret-scanner false positives; a generator change becomes a reviewable diff instead of a silent re-baseline |
| `cx_Oracle` | `python-oracledb` | Already resolved in PROJECT.md/Phase 1 | Not this phase's concern directly, but the config's `oracle.valid_table`/`invalid_table` fields are what Phase 4's `oracledb` loader will consume |

**Deprecated/outdated:** None specific to this phase's own scope beyond what Phase 1's research
already flagged (`cx_Oracle`, Airflow's legacy REST API).

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Exact fixture category list and ~25-30 fixture count for the scoped-down corpus | "Recommended scoped fixture category list" | If the user/planner wants a different scope (more/fewer categories), the manifest authored in this phase's plan would need revision before Phase 3's detection tests are written against it — cheap to fix now, costly per D-16's own "Reversibility: costly" note once fixtures + digests are committed and Phase 3 tests reference them |
| A2 | `signup_country`'s recommended `required: false` (vs. `true`) | "Derived column contract" table | Low risk — D-09 explicitly says neither dataset needs the nullable/required distinction to matter functionally yet; either value validates cleanly against the DDL (column is nullable either way) |
| A3 | Recommended large-fixture size (~50-100 MB vs. an RLIMIT_AS of 16-32 MiB) for D-16b's memory test | "RLIMIT_AS bounded-memory technique" | If too large, `make fixtures`/`make fixtures-verify` runtime grows noticeably (the reference repo's own 241 MB fixture took ~3.6s to generate at their measured throughput); if too small relative to the RLIMIT_AS, the assertion becomes vacuous (mirrors the reference repo's own validated `approx_bytes > 2 * rlimit_as_bytes` rule, which this project should keep) |
| A4 | The RLIMIT_AS subprocess memory test itself is new authorship (no reference implementation exists to port, only the described technique) | "RLIMIT_AS bounded-memory technique" | If the described technique doesn't behave as expected on this project's actual WSL2/Linux environment (e.g. `RLIMIT_AS` interacting oddly with CPython's own memory allocator under WSL2), the test may need iteration during implementation — flagged now so the planner doesn't assume a copy-paste-ready implementation exists |

**If this table is empty:** N/A — see rows above.

## Open Questions

1. **(RESOLVED) Should `configs/defaults.json` exist at all for this project's two datasets, or
   would two fully self-contained dataset configs be simpler?**
   - What we know: D-05 already locked "yes, `defaults.json` exists, mirroring the reference repo's
     pattern" — this question is really about *what* goes in it.
   - What's unclear: With only two datasets and a fairly small config schema, the set of genuinely
     shared fields (candidates: `csv.delimiter`/`csv.encoding` if both datasets end up using plain
     comma/UTF-8; unlikely `processing.chunk_size`, per Pitfall 3 above) may be small enough that
     `defaults.json` ends up nearly empty.
   - Recommendation: Populate `defaults.json` only with fields both `customers.json` and
     `orders.json` genuinely share unchanged (verify by writing both dataset configs first, then
     factoring out the identical top-level keys) — don't force artificial sharing to justify the
     file's existence.
   - RESOLVED: Plan 02-01 implements the recommendation directly — `defaults.json` holds only the
     shared `csv` block, not `processing`.

2. **(RESOLVED) Exact wording/shape of `error_code` values the corpus's `expect:` blocks describe
   in prose.**
   - What we know: D-16d locks this as explicitly out of scope for Phase 2 — `expect:` stays
     permissive prose, Phase 3 owns the real vocabulary (ENGINE-06).
   - What's unclear: Nothing blocking for this phase; flagged only so the planner doesn't
     accidentally invent and lock in `error_code` strings while writing fixture `expect:` blocks.
   - Recommendation: Write `expect:` blocks as free-text descriptions of intent (e.g.
     `expect: {reason: "customer_id column is empty on a required field"}`), never as a
     structured/enum-shaped value.
   - RESOLVED: Enforced structurally across 02-03/04/05's fixture authoring instructions and the
     manifest schema — no `error_code` field is ever declared in `manifest.py`.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|--------------|-----------|---------|----------|
| Python | config models, generator, corpus tooling | ✓ | 3.12.3 | — |
| `uv` | dependency management, `make generate`/`make fixtures` invocation | ✓ | 0.12.3 | — |
| Docker / Oracle container | NOT required for this phase's own work | n/a | n/a | Phase 2 produces files on the host filesystem only; no live Oracle connection is needed to validate config or generate CSVs — Phase 4 is the first phase that actually connects |

**Missing dependencies with no fallback:** none — Phase 2 has no external service dependency.
**Missing dependencies with fallback:** none applicable.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | `pytest` — not yet installed anywhere in the project; Phase 1's one existing test
(`tests/test_verify_environment.py`) deliberately uses only stdlib `unittest`, explicitly deferring
"the project's formal test suite" to a later phase [VERIFIED: tests/test_verify_environment.py:14-17] |
| Config file | none yet — this phase's Wave 0 gap |
| Quick run command | `uv run pytest tests/unit/ -x` (once added) |
| Full suite command | `uv run pytest tests/unit/ tests/fixtures -x` plus `make fixtures-verify` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|---------------------|-------------|
| CONFIG-01 | `config.json` schema accepts a fully-specified valid dataset config | unit | `pytest tests/unit/test_config_models.py -x` | ❌ Wave 0 |
| CONFIG-02 | Malformed config fails validation with ALL errors, not just the first | unit | `pytest tests/unit/test_config_loader.py -x` | ❌ Wave 0 |
| GEN-01 | Generator produces deterministic valid+invalid rows for a given seed | unit | `pytest tests/unit/test_generate_csv.py -x` | ❌ Wave 0 |
| D-16 (corpus) | Corpus regenerates byte-identical to the committed oracle | integration-ish (filesystem) | `make fixtures-verify` | ❌ Wave 0 (Makefile target + `tools/corpus/` package) |
| D-16b | Large fixture streams under a bounded `RLIMIT_AS` | unit (subprocess) | `pytest tests/unit/test_corpus_bounded_memory.py -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `uv run pytest tests/unit/ -x`
- **Per wave merge:** `uv run pytest tests/unit/ -x && make fixtures-verify`
- **Phase gate:** `make verify-phase2` (D-16g) green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `pytest` installed as a dev dependency — nothing in this project installs a real test runner
  yet (Phase 1 explicitly deferred this)
- [ ] `pyproject.toml` `[tool.pytest.ini_options]` (or a `pytest.ini`) — no config exists
- [ ] `tests/unit/__init__.py` or equivalent test-discovery setup
- [ ] `tests/fixtures/` directory structure for the corpus (manifest + digest oracle + gitignored
  `csv/`)

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|----------------|---------|--------------------|
| V2 Authentication | no | Not applicable — no auth surface in config/generator code |
| V3 Session Management | no | Not applicable |
| V4 Access Control | no | Not applicable |
| V5 Input Validation | yes | Pydantic v2 (`extra="forbid"`, typed fields) is the standard control for `config.json`; `tools/corpus/manifest.py`'s hand-written validation (per its own module docstring, read this session) is the standard control for `tests/fixtures/corpus.yaml`, since both are untrusted-shaped input the safe YAML/JSON loaders alone don't fully validate |
| V6 Cryptography | yes (narrowly) | SHA-256 (`hashlib.sha256`) for the digest oracle — this is an **integrity check**, not a security/secrecy boundary; no key material, no need for a stronger primitive |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|------------------------|
| Arbitrary YAML deserialization from `corpus.yaml` | Tampering | `yaml.safe_load` only, never `yaml.load`/`yaml.unsafe_load` — the reference repo's `manifest.py` module docstring calls this out explicitly as defending against "straight deserialisation vulnerability (threat T-01-13, ASVS V5)" [VERIFIED: /home/user/projects/airflow-platform/tools/corpus/manifest.py:1-8]; this project's ported loader must do the same |
| `config.json` accepting an unrecognized/typo'd key silently | Tampering / Information Disclosure (a silently-ignored key can mask a misconfigured security-relevant setting) | `extra="forbid"` on every model (Pattern 1) |
| Credentials embedded in `config.json` | Information Disclosure | D-12 already locks this out structurally — `config.json` declares only table names, never connection strings/credentials; nothing in this phase's schema introduces a secret field |

## Sources

### Primary (HIGH confidence — direct file reads, this session)
- `docker/oracle/init/02_customers.sql`, `docker/oracle/init/03_orders.sql` — this project's own
  Oracle DDL, the source of truth for the config column shape
- `packages/csv-processor/pyproject.toml`, `docker/airflow/Dockerfile`, root `pyproject.toml`,
  `uv.lock` — this project's actual dependency-declaration surfaces (the three-place pitfall)
- `Makefile`, `docker-compose.yml` — this project's existing entrypoint/service conventions
- `/home/user/projects/airflow-platform/docs/adr/0005-fixture-corpus-generated-from-a-seed.md` —
  the corpus architecture's rationale and the ten determinism rules, verbatim
- `/home/user/projects/airflow-platform/tools/corpus/{manifest,generators,digests,__main__}.py` —
  the actual ported implementation
- `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/config/{model,loader}.py` —
  the config model/loader pattern being adapted (scoped down)
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/dialect.py`
  — read for D-01 precedent (contract-override-beats-detection), confirming this phase's config
  schema is what Phase 3's detection code will consume
- `/home/user/projects/airflow-platform/configs/{datasets/customers.yaml,datasets/orders.yaml,defaults.yaml}`
  — read for shape precedent and the shallow-merge footgun documented in `defaults.yaml`'s own comment
- `/home/user/projects/airflow-platform/tests/fixtures/corpus.yaml`,
  `/home/user/projects/airflow-platform/tests/fixtures/CORPUS.sha256`,
  `/home/user/projects/airflow-platform/tests/unit/test_corpus_manifest.py` — manifest shape example
  and the `approx_bytes > 2 * rlimit_as_bytes` validation rule
- PyPI JSON API (`pypi.org/pypi/<pkg>/json`) and `pip index versions <pkg>`, queried live
  2026-08-28, for `Faker`, `PyYAML`, `pydantic` current versions and first-release history
- `gsd-tools query package-legitimacy check --ecosystem pypi` — ran live against `Faker`, `PyYAML`,
  `pydantic`, `clevercsv`, `charset-normalizer`

### Secondary (MEDIUM confidence)
- `.planning/research/STACK.md`, `.planning/research/ARCHITECTURE.md`, `.planning/research/PITFALLS.md`
  — pre-existing project research (Context7-sourced for Pydantic/Airflow/oracledb API shapes),
  cross-referenced against this phase's own file reads rather than taken standalone

### Tertiary (LOW confidence)
- None used for a load-bearing claim in this document — every recommendation traces to either a
  direct file read this session or a locked CONTEXT.md decision.

## Metadata

**Confidence breakdown:**
- Config schema (CONFIG-01/02): HIGH — verified directly against the project's own committed DDL
- Business-row generator (GEN-01 core): HIGH — STACK.md's Faker/`random.Random(seed)` pattern,
  no new uncertainty introduced
- Corpus/digest-oracle subsystem mechanism (D-16 architecture): HIGH — verified against the
  reference repo's actual, working, ADR-documented implementation
- Corpus fixture count/category scoping (D-16a's "comprehensive-upfront" set): MEDIUM — new
  authorship, flagged in the Assumptions Log, needs planner/user sign-off on the exact list
- RLIMIT_AS bounded-memory test (D-16b): MEDIUM — the technique is documented in the reference
  repo's ADR but has no existing implementation to port; flagged as new authorship

**Research date:** 2026-08-28
**Valid until:** 30 days (stable domain — Pydantic v2/stdlib-based, no fast-moving external API
surface in this phase's own scope)

---
*Phase 2 research for: Lightweight Airflow CSV→Oracle ETL Platform*
*Researched: 2026-08-28*
