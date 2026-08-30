# Phase 2: Config Contract & CSV Generator - Context

**Gathered:** 2026-08-28
**Status:** Ready for planning

<domain>
## Phase Boundary

A developer can fully describe a dataset's ingestion contract in `config.json` (Pydantic v2,
validated once per run, failing fast with the complete list of errors) and generate a deterministic
CSV fixture matching that contract — for both `customers` and `orders`. This phase also takes on a
significantly expanded fixture/testing subsystem (see Generator Behavior below): a manifest +
digest-oracle byte-level corpus, ported from the reference repo's `tools/corpus/`, that exercises
the Tier-A vendored `detect/*`/`compression.py` modules ahead of Phase 3 actually using them.

Requirements: CONFIG-01, CONFIG-02, GEN-01. No dependency on Phase 1 beyond what's already
complete; blocks Phase 3 (engine needs the config shape and generated fixtures to validate against).

</domain>

<decisions>
## Implementation Decisions

### CSV Dialect

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

### File & Directory Layout

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

### Schema Notation

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

### Oracle Target & Processing Config

- **D-12:** `config.json` declares only Oracle target/invalid table names (`oracle.valid_table` /
  `oracle.invalid_table`, e.g. `"customers_valid"`/`"customers_invalid"`) — never connection details
  or credentials. Consistent with Phase 1's D-11 (single `admin`/`admin` credential via
  env vars/Airflow Connection); keeps `config.json` safe to log or version without ever containing a
  secret.
- **D-13:** Chunk size (ENGINE-07) is a per-dataset `config.json` field (e.g.
  `processing.chunk_size: 5000`), not a hardcoded engine-wide constant.

### Generator Behavior

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

### Testing

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

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project-level requirements & decisions
- `.planning/PROJECT.md` — two-tier reuse decision, pinned tech decisions, dependency-isolation
  constraint (never import `dataplat`)
- `.planning/REQUIREMENTS.md` — CONFIG-01, CONFIG-02, GEN-01 full text; v2-deferred CONFIG-03
  (regex file patterns) and CONFIG-04 (min/max/allowed-value business rules) — do not reopen these,
  they're already scoped out of v1
- `.planning/ROADMAP.md` §Phase 2 — goal and success criteria for this phase
- `.planning/phases/01-environment-oracle-foundation/01-CONTEXT.md` — D-16 (repo layout, package
  paths locked as costly-to-reverse), D-14/D-15 (Makefile conventions), D-11 (Oracle Connection +
  admin/admin credential)
- `docker/oracle/init/02_customers.sql`, `docker/oracle/init/03_orders.sql` — the ACTUAL, already-
  created Oracle DDL for `customers_valid`/`customers_invalid`/`orders_valid`/`orders_invalid`.
  `config.json`'s schema section must match these column names/types/nullability exactly (this is
  the real source of truth, more authoritative than the seed spec's illustrative example or the
  reference repo's `.yaml` files).

### Research (produced before this discussion)
- `.planning/research/ARCHITECTURE.md` — original (pre-expansion) proposed structure for
  `configs/datasets/*.json`, `generator/generate_csv.py`; Pattern 1 (config validated once,
  rehydrated per task via XCom dict); note its directory sketch's `src/csv_processor/` path is
  superseded by Phase 1's actually-locked `packages/csv-processor/src/csv_processor/`
- `.planning/research/STACK.md` — Faker (seeded) + `random.Random(seed)` for generator determinism;
  strict `datetime.strptime()` validation (no flexible date parsing)
- `.planning/research/PITFALLS.md` §"BOM and locale-driven encoding surprises" — generator's own
  output encoding should be pinned and treated as a defensive-code concern, informs D-16c's
  compressed/BOM fixture inclusion
- `.planning/research/FEATURES.md`, `.planning/research/SUMMARY.md` — phase-to-research mapping

### Reference repo (read-only — never imported, never a dependency)
- `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/config/model.py` —
  `CsvParsingConfig` field shape (encoding/delimiter/quotechar/header_row), read for precedent
  behind D-01/D-02
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/dialect.py`
  — dialect detection semantics (contract override vs. detection), read for precedent behind D-01
- `/home/user/projects/airflow-platform/configs/datasets/customers.yaml`,
  `/home/user/projects/airflow-platform/configs/datasets/orders.yaml` — real per-column shape
  (type/nullable/required/format), behind D-08/D-09/D-10; note their `quality:`/`scd:`/`retention:`/
  `freshness:`/`reconciliation:` blocks are explicitly out of scope here (spec §28)
- `/home/user/projects/airflow-platform/docs/adr/0005-fixture-corpus-generated-from-a-seed.md` —
  **MANDATORY reading for whoever plans/implements D-16.** Full rationale for the manifest +
  digest-oracle pattern, the ten determinism rules (R1-R10: per-fixture RNG derivation via
  `sha256(seed|name)`, `.random()`-only randomness, binary-mode writes with explicit encoding,
  hand-joined line terminators, zeroed compressed-archive timestamps, no wall-clock/UUID/PID calls
  anywhere in the generator, declared-order iteration, no filesystem-order dependence, explicit
  NFC/NFD construction, explicit `Decimal` formatting never `str(float)`) — these rules are load-
  bearing for D-16's byte-identity guarantee and must be reproduced, not reinvented.
- `/home/user/projects/airflow-platform/tools/corpus/generators.py`,
  `/home/user/projects/airflow-platform/tools/corpus/manifest.py`,
  `/home/user/projects/airflow-platform/tools/corpus/digests.py`,
  `/home/user/projects/airflow-platform/tools/corpus/__main__.py`,
  `/home/user/projects/airflow-platform/tools/corpus/dated_series.py` — the actual implementation
  behind D-16. Verified zero `dataplat` import (grepped directly). Read the algorithm and adapt it
  scoped to this project's two fixed-schema datasets (Tier B-style treatment: the underlying
  manifest/digest-oracle *pattern* is being adopted wholesale per D-16, but the *content* — 69
  fixtures encoding the reference platform's own SCD/quarantine/locale/referential vocabulary — must
  NOT be copied verbatim; author fixtures scoped to structural/type/nullability validation only).
- `/home/user/projects/airflow-platform/tests/fixtures/corpus.yaml`,
  `/home/user/projects/airflow-platform/tests/fixtures/CORPUS.sha256` — the reference repo's actual
  manifest + digest-oracle files, as a shape reference for this project's own (much smaller) versions.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `packages/csv-processor/src/csv_processor/` — currently an empty scaffold (Phase 1 D-16):
  `pyproject.toml` + `__init__.py` only. This phase adds `config/models.py` and `config/loader.py`
  inside it.
- `docker-compose.yml` — no data volume mount exists yet (only `./airflow/dags` is mounted). This
  phase must add the `./data` → `/opt/airflow/data` mount (D-06).
- Reference repo's `tools/corpus/` package (see canonical_refs) — zero `dataplat` coupling, directly
  portable pattern for D-16.

### Established Patterns
- Phase 1's D-16 repo layout (`packages/csv-processor/src/csv_processor/`, `docker/oracle/init/*.sql`,
  `Makefile` as project-wide command entrypoint) is locked and costly to reverse — this phase builds
  within it, not around it.
- Phase 1's Makefile convention: `make up`/`down`/`reset`/`logs`/`verify`/`smoke-test` already exist;
  this phase extends the same file with `fixtures`/`fixtures-verify`/`generate`/a local phase-2
  verification target, per D-16f/D-16g, rather than inventing a separate tool.

### Integration Points
- Phase 3 (CSV Processing Engine) reads `config.json`'s schema shape (D-08/D-09/D-10/D-11) and the
  Tier-A vendored `detect/*` modules that D-16's corpus exercises.
- Phase 3 also owns the real `error_code` enum that D-16d's permissive `expect:` blocks defer to.
- Phase 4 (Oracle Bulk Load) reads `oracle.valid_table`/`oracle.invalid_table` (D-12) and
  `processing.chunk_size` (D-13).
- Phase 5 (DAG Wiring) reads the `./data/<dataset>/` → `/opt/airflow/data/<dataset>/` mount (D-06)
  for its `FileSensor(deferrable=True)` filepath, and `file_pattern`/dated-filename convention (D-07).
- Phase 6 (CI) wires `make fixtures-verify` and the config/generator unit tests (D-17) into GitHub
  Actions (CI-01) — Phase 2 only builds the local gate (D-16g), not the CI wiring itself.

</code_context>

<specifics>
## Specific Ideas

- User wants config.json's CSV dialect support to be maximally general (any delimiter/encoding/
  quote style) rather than picking a fixed convention — this shaped D-01/D-02/D-03.
- User explicitly wants "thorough" invalid-CSV coverage, including byte/encoding-level cases (wrong
  delimiter, wrong encoding, malformed serialization) beyond the spec's three canonical business-row
  categories — this is the origin of D-16's full corpus/digest-oracle adoption. User confirmed this
  choice twice after being shown the size/scope tradeoff against a lighter alternative.
- User asked to "remember to check" the reference repo's already-developed CSV processing modules
  before deciding dialect/generator shape — done; findings are threaded through D-01/D-02 and D-16's
  canonical refs.
- User asked to "remember to do tests" for the extended dialect fields (D-02) — captured in D-17's
  test-file-organization decision.

</specifics>

<deferred>
## Deferred Ideas

None raised outside this phase's domain — all discussion stayed within Phase 2's config/generator
scope. CONFIG-03 (regex file patterns) and CONFIG-04 (business-rule min/max/allowed-values checks)
were already deferred to v2 in REQUIREMENTS.md before this discussion and were not reopened.

### Reviewed Todos (not folded)
None — `todo.match-phase` returned zero matches for Phase 2.

</deferred>

---

*Phase: 2-Config Contract & CSV Generator*
*Context gathered: 2026-08-28*
