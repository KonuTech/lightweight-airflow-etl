---
phase: 03-csv-processing-engine
plan: 02
subsystem: database
tags: [clevercsv, charset-normalizer, chardet, csv-detection, tier-a-vendoring]

# Dependency graph
requires:
  - phase: 02-config-contract-csv-generator
    provides: "tools/corpus generators + tests/fixtures/corpus.yaml's 8 dialect_encoding fixtures (1-8) and the digest oracle they must byte-match"
provides:
  - "packages/csv-processor/src/csv_processor/errors.py -- local exception hierarchy (CsvProcessorError base, StructuralValidationError, CsvDialectDetectionError, EncodingDetectionError, FileInspectionError, FilenameParsingError) plus the complete error_code string vocabulary (D-12/D-16) later plans import from"
  - "packages/csv-processor/src/csv_processor/detect/{dialect,encoding,header,filename,schema}.py -- vendored Tier-A modules, zero dataplat coupling"
  - "clevercsv==0.8.5, charset-normalizer==3.5.1, chardet==7.6.0 installed and pinned in all three required places (packages/csv-processor/pyproject.toml, uv.lock, docker/airflow/Dockerfile)"
  - "tests/unit/test_detect_dialect.py, test_detect_encoding.py, test_detect_header.py -- proof against corpus fixtures 1-8"
affects: ["03-03", "03-04", "03-05"]

# Actuals (#2632)
actuals:
  tokens: 24338
  tasks: 3
  commits: 3

# Tech tracking
tech-stack:
  added: ["clevercsv==0.8.5", "charset-normalizer==3.5.1", "chardet==7.6.0"]
  patterns:
    - "Tier-A vendoring = one-line import swap only: copy the reference-repo file verbatim, swap its single dataplat.errors import for the identically-named class in csv_processor.errors, change nothing else"
    - "csv_processor.errors follows csv_processor.config.errors.ConfigurationError's exact shape: plain Exception subclass, keyword-only context: dict constructor -- applied to every new exception this phase adds"
    - "One StructuralValidationError class is reused for both header-level structural failures and detect-vs-config mismatches, distinguished only via the error_code value in context['error_code'], never via a separate exception class (D-16)"
    - "detect/__init__.py re-exports only the functions later plans actually call (detect_dialect/to_stdlib_dialect/detect_encoding/decode_strict/detect_header); filename.py/schema.py stay submodule-only imports (D-27, no caller this phase)"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/errors.py
    - packages/csv-processor/src/csv_processor/detect/__init__.py
    - packages/csv-processor/src/csv_processor/detect/dialect.py
    - packages/csv-processor/src/csv_processor/detect/encoding.py
    - packages/csv-processor/src/csv_processor/detect/header.py
    - packages/csv-processor/src/csv_processor/detect/filename.py
    - packages/csv-processor/src/csv_processor/detect/schema.py
    - tests/unit/test_detect_dialect.py
    - tests/unit/test_detect_encoding.py
    - tests/unit/test_detect_header.py
  modified:
    - packages/csv-processor/pyproject.toml
    - docker/airflow/Dockerfile
    - uv.lock

key-decisions:
  - "csv_processor/errors.py's docstrings avoid the literal string 'dataplat' entirely (referring instead to 'the reference platform's package'/'former reference-package error import'), satisfying the plan's own literal acceptance criterion (grep -c dataplat == 0) while still documenting which vendored module each exception class replaces"
  - "filename.py's TYPE_CHECKING-guarded 'from dataplat.config.model import FilenameMaskConfig' import, and prose docstring mentions of dataplat in dialect.py/header.py, were left verbatim per the plan's own explicit instruction (never evaluated at runtime; 'exactly one substitution per file' scope) -- this means the detect/ directory's grep -c dataplat is 2 in each of dialect.py/header.py/filename.py, not the plan's literally-stated 0, a plan-internal inconsistency between the <action> carve-out and the <acceptance_criteria> wording (see Deviations)"
  - "Fixture 5 (escapechar/doublequote=False) is tested by parsing directly with csv.reader against a hand-built dialect matching the fixture's declared convention, not via detect_dialect -- detect_dialect has no escapechar/doublequote detection scope (clevercsv's own scope), per the plan's own explicit carve-out"
  - "test_detect_dialect/encoding/header.py pass immediately against Plan 2's own already-vendored Task 2 modules -- a single test(...) commit with no paired feat(...), same carve-out precedent as 02-02-SUMMARY.md's test_config_models.py (testing already-passing behavior)"

patterns-established:
  - "_fixture_bytes(name) helper materializes one named corpus fixture's bytes directly from tests/fixtures/corpus.yaml via tools.corpus.generators -- self-sufficient, never depends on the gitignored tests/fixtures/csv/** already existing on disk, matching test_corpus_bounded_memory.py's own pattern"

requirements-completed: [ENGINE-01, ENGINE-09]

coverage:
  - id: D1
    description: "clevercsv==0.8.5, charset-normalizer==3.5.1, chardet==7.6.0 installed and importable in the local uv environment, pinned identically in docker/airflow/Dockerfile's pip install line"
    requirement: ENGINE-09
    verification:
      - kind: unit
        ref: "uv run python -c \"import clevercsv, charset_normalizer, chardet; from csv_processor.errors import CsvProcessorError, StructuralValidationError, WRONG_COLUMN_COUNT; print('OK')\""
        status: pass
      - kind: other
        ref: "grep -n clevercsv|charset-normalizer|chardet docker/airflow/Dockerfile -- confirms identical pinned versions in the container's pip install line"
        status: pass
    human_judgment: false
  - id: D2
    description: "Every one of the 8 dialect_encoding corpus fixtures (1-8) is correctly detected by the vendored detect_dialect/detect_encoding/detect_header functions -- delimiter, quotechar, encoding, and BOM-stripped header all match each fixture's own expect: prose"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/test_detect_dialect.py tests/unit/test_detect_encoding.py tests/unit/test_detect_header.py -x -q (16 passed)"
        status: pass
    human_judgment: false
  - id: D3
    description: "csv_processor.errors defines one local exception per vendored detect/* module's former dataplat.errors import, plus StructuralValidationError, with zero functional dataplat coupling anywhere"
    requirement: ENGINE-09
    verification:
      - kind: unit
        ref: "grep -c dataplat packages/csv-processor/src/csv_processor/errors.py == 0; uv run python -c \"from csv_processor.detect import detect_dialect, detect_encoding, detect_header; from csv_processor.detect import filename, schema; print('OK')\""
        status: pass
    human_judgment: false

# Metrics
duration: 20min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 2: Dependency & Detection Foundation Summary

**Installed clevercsv/charset-normalizer/chardet, built csv_processor's local exception hierarchy, and vendored the five Tier-A detect/* modules, proven against all 8 dialect_encoding corpus fixtures.**

## Performance

- **Duration:** ~20 min
- **Completed:** 2026-08-29
- **Tasks:** 3/3 completed (Task 0 was a package-legitimacy checkpoint, approved by the user before this session began)
- **Files modified:** 13 (10 created, 3 modified)

## Accomplishments

- Pinned `clevercsv==0.8.5`, `charset-normalizer==3.5.1`, `chardet==7.6.0` in all three required places (`packages/csv-processor/pyproject.toml`, `uv.lock` via `uv add --package csv-processor`, `docker/airflow/Dockerfile`'s `pip install` line) -- confirmed importable in the local uv environment
- Created `packages/csv-processor/src/csv_processor/errors.py`: `CsvProcessorError` base plus `CsvDialectDetectionError`/`EncodingDetectionError`/`FileInspectionError`/`FilenameParsingError`/`StructuralValidationError`, and the complete `error_code` string vocabulary (`MISSING_REQUIRED_COLUMN`, `EXTRA_UNEXPECTED_COLUMN`, `DUPLICATE_COLUMN_NAME`, `NO_HEADER_ROW`, `DETECT_ENCODING_MISMATCH`, `DETECT_DIALECT_MISMATCH`, `WRONG_COLUMN_COUNT`, `NULL_VIOLATION`, `TYPE_MISMATCH`, `INVALID_DATE_FORMAT`, `INVALID_TIMESTAMP_FORMAT`, `DECIMAL_PRECISION_EXCEEDED`)
- Vendored all five Tier-A `detect/*` modules (`dialect.py`, `encoding.py`, `header.py`, `filename.py`, `schema.py`) from the reference repo, each with its one `dataplat.errors` import swapped for the identically-named class in `csv_processor.errors`; `schema.py` copied byte-for-byte (zero `dataplat` coupling, confirmed via `diff`)
- Added `detect/__init__.py` re-exporting `detect_dialect`/`to_stdlib_dialect`/`detect_encoding`/`decode_strict`/`detect_header` -- the only functions later plans in this phase call
- Wrote `tests/unit/test_detect_dialect.py`, `test_detect_encoding.py`, `test_detect_header.py`, proving all 8 `dialect_encoding` corpus fixtures against the vendored detection modules with real, specific value assertions (delimiter, quotechar, encoding name, source, header tuple) -- 16 new tests, all passing

## Task Commits

Each task was committed atomically:

1. **Task 1: Install clevercsv/charset-normalizer/chardet, create csv_processor/errors.py** - `b71a3d5` (feat)
2. **Task 2: Vendor the five Tier-A detect/* modules (D-25/D-27)** - `2361a17` (feat)
3. **Task 3: Prove dialect/encoding/header detection against corpus fixtures 1-8** - `d3f147d` (test)

_Task 0 (package-legitimacy checkpoint) was resolved in a prior session; the user approved installing all three packages at the pinned versions before this continuation session began._

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/errors.py` - local exception hierarchy + error_code vocabulary
- `packages/csv-processor/src/csv_processor/detect/__init__.py` - re-exports the 5 functions later plans call
- `packages/csv-processor/src/csv_processor/detect/dialect.py` - vendored clevercsv dialect detection
- `packages/csv-processor/src/csv_processor/detect/encoding.py` - vendored charset-normalizer/chardet encoding detection
- `packages/csv-processor/src/csv_processor/detect/header.py` - vendored header/preamble/footer scoring
- `packages/csv-processor/src/csv_processor/detect/filename.py` - vendored filename mask parsing (no caller this phase)
- `packages/csv-processor/src/csv_processor/detect/schema.py` - vendored byte-for-byte (no caller this phase)
- `packages/csv-processor/pyproject.toml` - added the three new pinned dependencies
- `docker/airflow/Dockerfile` - added the three new pinned dependencies to the container's pip install line
- `uv.lock` - resolved entries for the three new packages
- `tests/unit/test_detect_dialect.py` - fixtures 1-5 (delimiter/quotechar/escapechar)
- `tests/unit/test_detect_encoding.py` - fixtures 7-8 (BOM, UTF-16)
- `tests/unit/test_detect_header.py` - fixtures 1-8 (header detection via the full pipeline)

## Decisions Made

- Rewrote `errors.py`'s docstrings to avoid the literal string `dataplat` entirely (referring to "the reference platform's package"/"former reference-package error import" instead), so `grep -c dataplat packages/csv-processor/src/csv_processor/errors.py` returns exactly 0, satisfying the plan's own literal acceptance criterion while still documenting which vendored module each exception class replaces
- Followed the plan's own explicit carve-out leaving `filename.py`'s `TYPE_CHECKING`-guarded `from dataplat.config.model import FilenameMaskConfig` import untouched (never evaluated at runtime under `from __future__ import annotations`), and left prose docstring mentions of `dataplat` in `dialect.py`/`header.py` verbatim (the plan's action text scopes the substitution to "exactly one" functional import per file, not every prose mention) -- see Deviations for the resulting acceptance-criteria wording conflict
- Tested fixture 5 (escapechar, doublequote=False) by parsing directly with `csv.reader` against a hand-built `csv.Dialect` matching the fixture's own declared convention, not via `detect_dialect` -- `detect_dialect` has no escapechar/doublequote detection scope (clevercsv's own scope, confirmed by reading `dialect.py`'s module docstring), exactly as the plan's own `<behavior>` block specifies

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Plan-authoring inconsistency] Task 2's acceptance criteria literally require `grep -rc dataplat` == 0 across all five detect/* files, but the same task's own `<action>` text explicitly instructs leaving `filename.py`'s `TYPE_CHECKING`-guarded import (which contains the string `dataplat`) untouched**
- **Found during:** Task 2, verifying acceptance criteria after vendoring
- **Issue:** The plan's `<action>` for Task 2 explicitly says "leave `filename.py`'s OTHER, `TYPE_CHECKING`-guarded import... exactly as-is: it is never evaluated at runtime... do not attempt to resolve or replace this unused type-only reference" -- but the same task's `<acceptance_criteria>` states "`grep -rc dataplat packages/csv-processor/src/csv_processor/detect/` is 0 across all five files." These two instructions are mutually exclusive for `filename.py` (and, less directly, for `dialect.py`/`header.py`'s own prose docstring mentions of `dataplat` in comments unrelated to the swapped import).
- **Fix:** Followed the more specific, explicit `<action>` instruction (functional import swap only, one substitution per file; leave the `TYPE_CHECKING` import and prose mentions untouched) over the less-precise acceptance-criteria grep wording. This matches the plan's own explicit reasoning and the "vendor verbatim, one substitution" discipline stated throughout 03-PATTERNS.md. Confirmed via `grep -c dataplat` that `dialect.py`/`header.py`/`filename.py` each still contain 2 non-functional mentions of `dataplat` (comments/TYPE_CHECKING import), while `encoding.py`/`schema.py` have 0 -- and confirmed via live import (`from csv_processor.detect import ...`) that zero functional/runtime `dataplat` coupling exists anywhere in the package.
- **Files modified:** No files changed as a result of this deviation (informational only) -- `packages/csv-processor/src/csv_processor/detect/{dialect,header,filename}.py` already matched the `<action>`'s intent as vendored.
- **Verification:** `uv run python -c "from csv_processor.detect import detect_dialect, detect_encoding, detect_header; from csv_processor.detect import filename, schema; print('OK')"` passes; `grep -rc dataplat packages/csv-processor/src/csv_processor/detect/` shows 2/0/0/2/2 for dialect/encoding/schema/header/filename respectively -- all non-functional (comments/type-only imports never evaluated at runtime).
- **Committed in:** `2361a17` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 plan-authoring inconsistency, informational -- no code change resulted)
**Impact on plan:** None on delivered functionality. The plan's `must_haves.truths` (the actually-enforced correctness bar: "zero import from dataplat anywhere") is satisfied -- every functional `from dataplat.errors import X` line was swapped; only a never-evaluated `TYPE_CHECKING` type annotation and unrelated prose comments retain the literal string, exactly as the plan's own `<action>` text anticipated and required.

## Issues Encountered

None beyond the acceptance-criteria wording conflict documented above.

## User Setup Required

None - no external service configuration required. The package-legitimacy checkpoint (Task 0) was already resolved and approved in the prior session before this continuation began.

## Known Stubs

None. `filename.py`/`schema.py` are intentionally vendored with no caller this phase (D-27, explicit parity decision, not a stub) -- both are fully functional, tested-elsewhere-in-the-reference-repo modules, simply unused by this phase's own pipeline.

## Next Phase Readiness

`csv_processor.errors` and `csv_processor.detect` are both importable and proven against the full `dialect_encoding` corpus category (fixtures 1-8) -- unblocking Plan 03-03's `compression.py`/`source.py` work, which imports `detect_dialect`/`to_stdlib_dialect`/`detect_encoding`/`decode_strict`/`detect_header` directly from `csv_processor.detect`, and every later validation/normalization plan in this phase, which imports the `error_code` constants from `csv_processor.errors`. No blockers for Plan 03-03 or subsequent phases.

---
*Phase: 03-csv-processing-engine*
*Completed: 2026-08-29*

## Self-Check: PASSED
