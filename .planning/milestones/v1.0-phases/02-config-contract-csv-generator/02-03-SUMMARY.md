---
phase: 02-config-contract-csv-generator
plan: 03
subsystem: testing
tags: [pyyaml, fixture-corpus, digest-oracle, determinism]
requires:
  - phase: 02-config-contract-csv-generator
    provides: (independent subsystem — no dependency on Plans 01-02's config/generator code)
provides:
  - tools/corpus package (manifest.py, generators.py, digests.py, __main__.py) — deterministic
    byte-level fixture generation and SHA-256 digest-oracle verification
  - make fixtures / make fixtures-verify Makefile targets
  - tests/fixtures/corpus.yaml (8 dialect_encoding fixtures) + tests/fixtures/CORPUS.sha256
    (committed digest oracle), proving the manifest→generator→digest round-trip end-to-end
affects: [phase 3 detection/parsing tests (Tier-A vendored detect/*/compression.py fixtures)]
actuals:
  tokens: 15600
  tasks: 3
  commits: 3
tech-stack:
  added: [PyYAML==6.0.3]
  patterns:
    - "Per-fixture RNG derivation via sha256(master_seed|name) (R1) — never a shared random stream"
    - "Randomness consumed only through .random() index arithmetic, never rng.choice() (R2)"
    - "Binary-mode writes, explicit hand-joined line terminators, never csv.writer defaults (R3/R4)"
    - "sha256sum-compatible digest oracle format, independently checkable via `sha256sum -c`"
key-files:
  created:
    - tools/corpus/__init__.py
    - tools/corpus/manifest.py
    - tools/corpus/generators.py
    - tools/corpus/digests.py
    - tools/corpus/__main__.py
    - tests/unit/test_corpus_manifest.py
    - tests/fixtures/corpus.yaml
    - tests/fixtures/CORPUS.sha256
  modified:
    - Makefile
    - pyproject.toml
    - uv.lock
key-decisions:
  - "PyYAML==6.0.3 legitimacy checkpoint approved by user (Task 1) before uv add"
  - "Fixture model kept deliberately smaller than the reference repo's ~20-field flat dataclass: generator stays a single dict keyed by kind, since this project's scope (structural/type/nullability only) never needs wrapper/multipart/splice/BOM-mid-file machinery"
  - "load_manifest(path) -> list[Fixture] matches Task 2's exact tested contract; a second load_manifest_with_seed() exposes master_seed internally for the CLI's stream_for() calls without changing the tested public API"
  - "pyproject.toml gains [tool.pytest.ini_options] pythonpath = ['.'] so pytest can resolve the tools namespace package (never pip-installed, unlike csv_processor via the uv workspace)"
patterns-established:
  - "Dialect-aware field quoting (escapechar/doublequote-driven escaping vs. doubling) lives in generators.py's _quote_field(), reusable by any future tabular fixture needing embedded delimiters/quotes"
requirements-completed: [GEN-01]
coverage:
  - id: D1
    description: "tools/corpus manifest/generator/digest/CLI mechanism works end-to-end"
    verification:
      - kind: unit
        ref: "tests/unit/test_corpus_manifest.py"
        status: pass
      - kind: manual
        ref: "make fixtures && make fixtures-verify (run twice, byte-identical both times)"
        status: pass
    human_judgment: false
  - id: D2
    description: "8 dialect/encoding fixtures committed with a passing digest-oracle round-trip"
    verification:
      - kind: manual
        ref: "sha256sum -c tests/fixtures/CORPUS.sha256 (independent check from repo root)"
        status: pass
    human_judgment: false
duration: ~15min
completed: 2026-08-28
status: complete
---

# Phase 02 Plan 03: Fixture-Corpus Manifest + Digest-Oracle Subsystem (Dialect/Encoding Category) Summary

**Ported the reference repo's manifest + digest-oracle byte-level fixture architecture into a
scoped `tools/corpus` package (Tier B: read the algorithm, adapt the schema — never copy
verbatim), and proved it end-to-end with 8 committed dialect/encoding fixtures whose SHA-256
digests independently verify via `sha256sum -c`.**

## Performance
- **Duration:** ~15min (continuation session; resumed at Task 2 after Task 1's checkpoint was
  approved by the user in a prior session)
- **Completed:** 2026-08-28
- **Tasks:** 3 (1 checkpoint + 2 auto)
- **Files modified:** 11 (8 created, 3 modified)

## Accomplishments
- Task 1: PyYAML==6.0.3 package-legitimacy checkpoint approved by the user ("approved") — no
  code change, gate satisfied.
- Task 2: `tools/corpus` package core built RED→GREEN (TDD) — `manifest.py` (yaml.safe_load-only
  Fixture/Manifest model, permissive `expect:` block per D-16d, root/fixture-level unrecognized-key
  rejection), `generators.py` (`stream_for()` R1 per-fixture RNG, `tabular`/`literal`/
  `literal_unicode` generators with dialect-aware field quoting), `digests.py` (near-verbatim
  sha256sum-format oracle read/write/parse), `__main__.py` (`generate`/`verify` CLI subcommands),
  wired into `Makefile`'s new `fixtures`/`fixtures-verify` targets.
- Task 3: authored 8 `dialect_encoding` fixtures into `tests/fixtures/corpus.yaml` — semicolon/
  pipe/tab delimiters, a custom quotechar forced by an embedded comma, an escapechar+
  `doublequote: false` fixture with an embedded quote (D-02's explicit test-coverage ask), a
  hand-joined CRLF terminator (R4), a UTF-8-BOM-prefixed file, and a full UTF-16-encoded file.
  `make fixtures && make fixtures-verify` run twice in a row, byte-identical both times.

## Task Commits
1. **Task 1: PyYAML legitimacy checkpoint** — approved by user in prior session, no code commit.
2. **Task 2 RED: failing test** — `ca233d1` `test(02-03): add failing test for corpus
   manifest/generators/digests`
3. **Task 2 GREEN: tools/corpus core** — `4b84f5d` `feat(02-03): implement tools/corpus manifest,
   generators, digests, CLI`
4. **Task 3: 8 fixtures + digest oracle** — `ae5f965` `feat(02-03): author dialect/encoding
   fixture category (8 fixtures)`

## Files Created/Modified
- `tools/corpus/__init__.py` — re-exports `Fixture`/`Manifest`/`ManifestError`/`load_manifest`
- `tools/corpus/manifest.py` — yaml.safe_load-only manifest parser and frozen `Fixture`/`Manifest`
  dataclasses
- `tools/corpus/generators.py` — `stream_for()` (R1), `generate_fixture()` dispatcher for
  `tabular`/`literal`/`literal_unicode`, dialect-aware quoting
- `tools/corpus/digests.py` — `sha256_file()`, `format_digests()`, `parse_digests()`,
  `read_digests()`, `write_digests()`
- `tools/corpus/__main__.py` — `build_parser()`/`command_generate()`/`command_verify()`/`main()`
- `tests/unit/test_corpus_manifest.py` — 8 tests covering Task 2's `<behavior>` block
- `tests/fixtures/corpus.yaml` — 8 `dialect_encoding` fixture declarations (committed)
- `tests/fixtures/CORPUS.sha256` — committed digest oracle, 8 entries
- `Makefile` — adds `fixtures`/`fixtures-verify` targets to `.PHONY` and the file
- `pyproject.toml` — `uv add "PyYAML==6.0.3"`; adds `[tool.pytest.ini_options] pythonpath = ["."]`
- `uv.lock` — updated by `uv add`

## Decisions Made
- Kept `Fixture`'s shape exactly as Task 2 specified (`name`/`category`/`generator: dict`/
  `expect: dict`) rather than porting the reference repo's ~20-field flat dataclass — this
  project's scope (structural/type/nullability validation only) never needs `wrapper`'s
  gzip/zip fields, `multipart`'s part-count field, or splice/BOM-mid-file fields on every
  fixture regardless of kind. `wrapper`/`multipart` stay in `GeneratorKind` for parity (per
  02-RESEARCH.md) but raise `GeneratorError` if referenced — 02-05-PLAN.md implements `wrapper`.
- Added `manifest.load_manifest_with_seed()` alongside the plan-specified `load_manifest(path) ->
  list[Fixture]`: the CLI needs `master_seed` to derive each fixture's RNG, but Task 2's tested
  public contract is explicitly `list[Fixture]` with no seed. Keeping `load_manifest`'s return
  type exactly as specified and adding one CLI-only entry point avoids widening the tested
  contract.
- Dialect-aware field quoting (`_quote_field` in `generators.py`) is new authorship, not present
  in the reference repo's `generators.py` (whose tabular writer never quotes — its fixtures never
  need embedded delimiters). Required because this project's D-02 explicitly wants
  `escapechar`/`doublequote` fixture coverage (04/05), which the reference repo's own tabular
  generator has no mechanism for.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking issue] `tools.corpus` unimportable by pytest without an explicit
`pythonpath` entry**
- **Found during:** Task 2, first RED→GREEN verification run.
- **Issue:** `tools/` is a namespace package living at the repo root (no `__init__.py` at the
  `tools/` level, matching the reference repo's own convention) and is never `pip install`-ed
  — unlike `csv_processor`, which the uv workspace installs editable into the venv. `pytest`
  (unlike `python -m tools.corpus`, which always prepends cwd to `sys.path`) does not add the
  invocation directory to `sys.path` by default, so `import tools.corpus...` failed with
  `ModuleNotFoundError` even after all four modules existed.
- **Fix:** Added `[tool.pytest.ini_options] pythonpath = ["."]` to the root `pyproject.toml`
  (pytest 9.1.1's native option, no extra dependency).
- **Files modified:** `pyproject.toml`.
- **Verification:** `uv run pytest tests/unit/test_corpus_manifest.py -x` — 8/8 pass; full suite
  `uv run pytest -q` — 81/81 pass, no regressions.
- **Commit:** `4b84f5d` (folded into Task 2's GREEN commit — an enabling infra change alongside
  the modules it makes importable, not a separate fix commit).

**2. [Rule 1 - Bug] Own newly-authored policy test was over-broad, tripped by its own module's
docstring prose**
- **Found during:** Task 2, GREEN verification run.
- **Issue:** `test_load_manifest_never_uses_the_unsafe_yaml_loader` originally scanned the whole
  file's raw text for the substring `"yaml.unsafe_load"`, which also matched `manifest.py`'s own
  module docstring (which legitimately *names* `yaml.load`/`yaml.unsafe_load` in prose to explain
  why neither is used) — a false failure on correct code.
- **Fix:** Scoped the scan to non-docstring/non-comment code lines only (filtering lines starting
  with `"""`, `` ``` ``, or `#`, and lines containing the double-backtick markdown form
  `` ``yaml ``), so the check can only be satisfied by removing the actual `yaml.load(`/
  `yaml.unsafe_load(` call, not by deleting the rationale.
- **Files modified:** `tests/unit/test_corpus_manifest.py`.
- **Verification:** Re-ran `uv run pytest tests/unit/test_corpus_manifest.py -x` — all 8 pass.
- **Commit:** `4b84f5d` (test file was still uncommitted at this point in the RED→GREEN cycle;
  the fix landed in the same GREEN commit as the implementation it verifies).

## Issues Encountered
None beyond the two auto-fixed deviations above.

## User Setup Required
None — no external service configuration required. PyYAML is dev/test-tooling-only and does not
need to be added to `docker/airflow/Dockerfile` (it never runs inside the Airflow container).

## Next Phase Readiness
Ready for 02-04/02-05 (scale the fixture corpus across the remaining four planned categories —
structural; type/nullability; byte-level-hard; large/compressed). 02-05 also implements the
`wrapper` generator kind (gzip/zip, R5) that this plan intentionally left unimplemented.

---
*Phase: 02-config-contract-csv-generator*
*Completed: 2026-08-28*

## Self-Check: PASSED

All 8 created files (`tools/corpus/{__init__,manifest,generators,digests,__main__}.py`,
`tests/unit/test_corpus_manifest.py`, `tests/fixtures/{corpus.yaml,CORPUS.sha256}`) confirmed
present on disk. All 4 commits (`ca233d1`, `4b84f5d`, `ae5f965`, `c932084`) confirmed in `git log`.
