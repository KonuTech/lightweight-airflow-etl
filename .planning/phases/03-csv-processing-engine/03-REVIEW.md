---
phase: 03-csv-processing-engine
reviewed: 2026-08-29T00:00:00Z
depth: standard
files_reviewed: 30
files_reviewed_list:
  - Makefile
  - configs/datasets/customers.json
  - configs/datasets/orders.json
  - docker/airflow/Dockerfile
  - docker/oracle/init/04_widen_invalid_columns.sql
  - generator/generate_csv.py
  - packages/csv-processor/pyproject.toml
  - packages/csv-processor/src/csv_processor/compression.py
  - packages/csv-processor/src/csv_processor/detect/__init__.py
  - packages/csv-processor/src/csv_processor/detect/dialect.py
  - packages/csv-processor/src/csv_processor/detect/encoding.py
  - packages/csv-processor/src/csv_processor/detect/filename.py
  - packages/csv-processor/src/csv_processor/detect/header.py
  - packages/csv-processor/src/csv_processor/detect/schema.py
  - packages/csv-processor/src/csv_processor/engine.py
  - packages/csv-processor/src/csv_processor/errors.py
  - packages/csv-processor/src/csv_processor/normalize.py
  - packages/csv-processor/src/csv_processor/source.py
  - packages/csv-processor/src/csv_processor/validate.py
  - scripts/verify_environment.py
  - tests/unit/test_byte_level_hard.py
  - tests/unit/test_compression.py
  - tests/unit/test_detect_dialect.py
  - tests/unit/test_detect_encoding.py
  - tests/unit/test_detect_header.py
  - tests/unit/test_engine_chunks.py
  - tests/unit/test_generate_csv.py
  - tests/unit/test_no_airflow_import.py
  - tests/unit/test_normalize.py
  - tests/unit/test_structural_validation.py
  - tests/unit/test_type_validation.py
  - uv.lock
findings:
  critical: 2
  warning: 4
  info: 3
  total: 9
status: issues_found
---

# Phase 3: Code Review Report

**Reviewed:** 2026-08-29T00:00:00Z
**Depth:** standard
**Files Reviewed:** 30
**Status:** issues_found

## Summary

Phase 3's CSV processing engine (`compression.py`, `detect/*`, `source.py`, `validate.py`,
`normalize.py`, `engine.py`) is well-tested against the corpus for the paths its own test suite
exercises, and the decompression-bomb guard, dialect/encoding detection edge cases, and
chunk/row-number bookkeeping are all careful, deliberate work with good docstrings explaining
*why*.

However, tracing the actual data flow (not just the paths the shipped tests exercise) surfaced two
correctness bugs that will misbehave the moment they're triggered by real input rather than the
current fixture set, plus several smaller integrity/documentation issues:

1. `ColumnSpec.required` (the flag that is supposed to let a column be legitimately absent from a
   file, e.g. `customers.json`'s own `signup_country`) is read nowhere in the processing pipeline —
   `source.prepare_source()`'s missing-column check treats every declared column as required
   regardless of this flag, so a real "optional column absent" file the config's own author
   intended to support will incorrectly be whole-file-rejected.
2. `source.prepare_source()` only ever skips exactly one physical row before treating everything
   else as data (`next(reader)`), and never consults `header_detection.header_row_index` /
   `preamble_row_count` / `footer_row_indices` / `repeated_header_row_indices`. `detect_header()`'s
   own module docstring states explicitly that excluding preamble/footer/repeated-header rows "is a
   later wiring plan's job" — `source.py` is that wiring plan, and it doesn't do it. A file with a
   real metadata preamble, trailing footer, or a repeated header line mid-file will have those rows
   silently fed into row-level validation as if they were ordinary data.

Neither of these is caught by the current test suite because the corpus's `structural` category
(fixtures 09–16) never actually contains a preamble/footer/repeated-header case, and no test
constructs a customers CSV that omits an optional column — both are real gaps in the shipped
behavior, not merely untested code paths.

## Critical Issues

### CR-01: `ColumnSpec.required` is parsed but never enforced — a config-declared optional column can never actually be absent from a file

**File:** `packages/csv-processor/src/csv_processor/source.py:230-242`
**Issue:**
`prepare_source()`'s header-vs-config cross-check is:

```python
declared_names = {c.name for c in config.columns}
header_names = set(header_detection.raw_header)
missing = declared_names - header_names
if missing:
    raise StructuralValidationError(..., error_code=errors.MISSING_REQUIRED_COLUMN, ...)
```

`declared_names` is built from **every** column in `config.columns`, with no filter on
`column.required`. A repo-wide grep confirms `ColumnSpec.required` is never read anywhere in
`source.py`, `engine.py`, `validate.py`, or `normalize.py` — the only other hit is
`detect/schema.py`'s unrelated `suggest_column_contracts()` helper, which always hardcodes
`"required": True` in its own suggestions and is not part of the real pipeline.

`configs/datasets/customers.json:37-42` declares exactly this scenario in the project's own
shipped config:

```json
{
  "name": "signup_country",
  "type": "string",
  "nullable": true,
  "required": false
}
```

Per `ColumnSpec`'s own docstring ("`required` (must the column be present in the file at all)"),
a `customers_*.csv` file that genuinely omits the `signup_country` column should be structurally
valid. In the current implementation it is not: `prepare_source()` will raise
`StructuralValidationError(error_code=MISSING_REQUIRED_COLUMN)` and reject the entire file, because
`required` is never consulted. (`generator/generate_csv.py` compounds this — it also never omits an
optional column when writing a header/row, so nothing in this phase's own deliverables ever
exercises the "optional column literally absent" case end-to-end.)

**Fix:**
```python
# source.py
required_names = {c.name for c in config.columns if c.required}
missing = required_names - header_names
...
# and: an EXTRA column that IS one of the non-required declared columns simply
# absent vs. present needs to stop being conflated with "extra unexpected" —
# only a name that is in neither `required_names` nor `declared_names` at all
# is EXTRA_UNEXPECTED_COLUMN.
```
Also thread `column.required` through `validate.check_row`/`normalize_row` (or explicitly document
that "required" only ever means "must appear in the header", never "must appear on every row" —
whichever is intended) so the flag has *some* observable effect, and add a corpus/unit test that
writes a customers CSV missing `signup_country` and asserts it processes successfully.

---

### CR-02: Detected metadata preamble / footer / repeated-header rows are computed by `detect_header()` but never excluded by `source.prepare_source()`

**File:** `packages/csv-processor/src/csv_processor/source.py:253-267`
**Issue:**
`detect/header.py`'s module docstring is explicit about the division of responsibility:

> "Both are recorded on `HeaderDetection` rather than silently dropped from `rows` — excluding
> them from the loaded record set is a **later wiring plan's job** (the streaming reader named in
> this plan's own `<objective>`); this module only detects."

`source.prepare_source()` is that wiring plan's actual code, and it does not perform the exclusion.
The PASS-2 real-read section is:

```python
next(reader)  # skip the header row -- already validated above
return text_stream, _rows_with_raw_line(reader, wrapper), header_detection.raw_header
```

This unconditionally skips exactly **one** physical row, and nothing else in the function reads
`header_detection.header_row_index`, `header_detection.preamble_row_count`,
`header_detection.footer_row_indices`, or `header_detection.repeated_header_row_indices`. Concretely:

- If `detect_header()` ever finds the header at an index `> 0` (a genuine metadata preamble —
  exactly the case `detect_header`'s own scoring loop is built to handle), PASS 2 still only skips
  1 row. Every preamble row before the last one, and the real header row itself, get fed into
  `process_chunks()` as ordinary data rows.
- Rows `detect_header()` classified as footer (`footer_row_indices`) or as a repeated header line
  mid-file (`repeated_header_row_indices`) are never removed from the iterator `prepare_source()`
  returns. They flow straight into `validate.check_row()` like any other row. A repeated-header row
  whose values are all valid strings for string-typed columns (the common case — a literal column
  name is itself a valid string) will be silently accepted as a **valid** business row, polluting
  `*_valid` with a row that is just the header repeated.

This is not caught by `tests/unit/test_structural_validation.py` because the corpus's `structural`
category (fixtures 09–16) contains no preamble/footer/repeated-header fixture at all — confirmed via
`tests/fixtures/corpus.yaml` (only `missing_column`, `extra_unexpected_column`,
`duplicate_column_name`, `wrong_column_count_row`, `empty_file`, `header_only_no_rows`,
`no_header_row`, `ragged_rows_and_blank_lines`). The feature `detect_header()` implements is real
and shipped, but its output is discarded by its only caller.

**Fix:**
```python
header_row_index = header_detection.header_row_index  # already known before PASS 2
for _ in range(header_row_index + 1):
    next(reader)  # skip every preamble row AND the header row itself

excluded = set(header_detection.footer_row_indices) | set(header_detection.repeated_header_row_indices)
def _filtered(rows):
    for absolute_index, item in enumerate(rows, start=header_row_index + 1):
        if absolute_index not in excluded:
            yield item

return text_stream, _filtered(_rows_with_raw_line(reader, wrapper)), header_detection.raw_header
```
(Adjust indices/signature to taste — the point is that `header_row_index`/`footer_row_indices`/
`repeated_header_row_indices` must be consumed, not just computed.) Add a corpus fixture or ad hoc
test (mirroring `test_byte_level_hard.py`'s ad hoc-config pattern) that puts a real preamble line
and a real footer line around otherwise-valid data and asserts they never appear in either
`valid_rows` or `invalid_rows`.

## Warnings

### WR-01: `detect/filename.py` still imports the forbidden reference-repo package it was supposed to be vendored clean of

**File:** `packages/csv-processor/src/csv_processor/detect/filename.py:58,362`
**Issue:**
CLAUDE.md's two-tier reuse rule for `detect/filename.py` says: "each imports `from dataplat.errors
import <SomeError>`. Replace with a local exception class of the same name... Copy the file, fix the
one import, done." `errors.py`'s own docstring confirms the *exception* import was fixed
(`FilenameParsingError` "replaces `detect/filename.py`'s former reference-package error import").

But a second, un-fixed reference-repo import remains, gated only by `TYPE_CHECKING`:

```python
if TYPE_CHECKING:
    from dataplat.config.model import FilenameMaskConfig
...
def parse_filename(mask_config: FilenameMaskConfig, filename: str) -> dict[str, object]:
```

`dataplat` is not a dependency anywhere in this project (`pyproject.toml`, `uv.lock`, the Dockerfile
all confirm this), and `csv_processor.config.models` has no `FilenameMaskConfig` equivalent at all —
this project's actual `DatasetConfig.file_pattern` is a plain glob string, not a strptime-token mask
object. This means: (a) any type-checker (mypy/pyright) run against this package will fail to
resolve the import, and (b) the function's own signature documents a dependency on a config shape
that doesn't exist anywhere in this codebase. Low runtime risk today only because `parse_filename`
has no caller in this phase (per the module's own docstring), but it is a real, provable violation
of the project's stated "port logic in by rewriting, never by importing" rule, and it will surface
the moment CI adds a type-check step or a future plan wires `parse_filename` in.
**Fix:** Either define a small local `FilenameMaskConfig`-equivalent (even a `TypedDict`/dataclass
with just a `.mask: str` attribute) in this package and import that under `TYPE_CHECKING`, or drop
the type annotation to `object`/a documented `Any` placeholder until a real caller and config shape
exist.

### WR-02: `normalize.convert_value`'s integer/decimal parsing is more permissive than the module's own stated contract

**File:** `packages/csv-processor/src/csv_processor/normalize.py:61-82,136-140`
**Issue:** The module docstring states every conversion "is always rejected via an `error_code`
string, never silently truncated/rounded/coerced into a 'close enough' typed value." In practice:

- `int(raw)` (line 138) accepts leading/trailing whitespace, a leading `+`, and underscore digit
  separators — `int(" 123 ")`, `int("+123")`, and `int("1_000")` all succeed and silently return
  `123`/`123`/`1000` rather than being rejected as malformed input. Verified live.
- `Decimal(raw)` (line 62, inside `parse_decimal_strict`) likewise accepts leading/trailing
  whitespace and underscore separators (`Decimal(" 1.5 ")`, `Decimal("1_000.5")` both succeed), and
  accepts scientific notation (`Decimal("1E2")` succeeds and is scored at `scale=0`,
  `precision=len(digits)+exponent`) — none of which is rejected even though `detect/schema.py`'s
  own `infer_column_type` explicitly treats scientific notation as a hard "red flag" for a damaged
  numeric identifier. The two modules disagree on what counts as a legitimate decimal shape.

None of the two current dataset configs (`customers.json`, `orders.json`) declares an `integer`
column, so this is currently latent, but it will silently corrupt data the moment any future
dataset does (a CSV field `"1_000"` or `" 42 "` would be accepted as a clean integer instead of
being flagged `TYPE_MISMATCH`).
**Fix:** Reject values containing whitespace/underscores/leading `+` explicitly before calling
`int()`/`Decimal()`, e.g. a cheap `if raw != raw.strip() or "_" in raw: return None, errors.TYPE_MISMATCH`
guard, and reject scientific-notation-shaped strings for `decimal` the same way `detect/schema.py`
already does via its `_SCIENTIFIC_NOTATION_PATTERN`.

### WR-03: `detect/encoding.py`'s empirical-calibration docstring cites library versions that are not what's actually pinned/shipped

**File:** `packages/csv-processor/src/csv_processor/detect/encoding.py:1-13`
**Issue:** The module docstring states the near-tie epsilon, confidence ceiling, and
`DEFAULT_MIN_CONFIDENCE` constants were "verified live this phase against the pinned
`clevercsv==0.8.5`, `chardet==7.5.1`, `charset-normalizer==3.5.0`." The project's actual pinned
versions (`packages/csv-processor/pyproject.toml:19-24`, `docker/airflow/Dockerfile:8-9`, and
`uv.lock` — confirmed: `chardet` 7.6.0, `charset-normalizer` 3.5.1) are different patch/minor
versions for both `chardet` and `charset-normalizer`. `_best_corroborating_match()` also relies on
an internal implementation detail — that `CharsetMatches.__iter__` yields candidates in ascending-
chaos order — that isn't part of either library's documented public contract. Since the tests
currently pass against the *actually* pinned versions, this is not presently causing failures, but
the docstring's specific "verified live" provenance claim is simply false for what ships, which
undermines the documented rationale for the magic thresholds (`_NEAR_TIE_CHAOS_EPSILON = 0.01`,
`DEFAULT_MIN_CONFIDENCE = 0.5`, `_NO_BOM_CONFIDENCE_CEILING = 0.99`) the next time either dependency
is bumped.
**Fix:** Update the docstring to name the actually-pinned versions, and add a short comment at each
magic constant noting it must be re-verified against the corpus fixtures on any `chardet`/
`charset-normalizer` version bump (a `make fixtures-verify`-style gate would be even better).

### WR-04: `normalize.convert_value` uses bare `assert` for contract enforcement that config validation is expected to guarantee

**File:** `packages/csv-processor/src/csv_processor/normalize.py:143-153`
**Issue:**
```python
if column.type == "decimal":
    assert column.precision is not None
    assert column.scale is not None
    return parse_decimal_strict(raw, precision=column.precision, scale=column.scale)
if column.type == "date":
    assert column.format is not None
    ...
if column.type == "timestamp":
    assert column.format is not None
    ...
```
`assert` statements are stripped entirely when Python runs with `-O`/`PYTHONOPTIMIZE`. If that ever
happens (a container base image, a perf-tuned deployment flag, etc.), a `decimal` column missing
`precision`/`scale` would pass `None` straight into `parse_decimal_strict`, which then raises an
unhandled `TypeError` (`'>' not supported between instances of 'int' and 'NoneType'`) instead of a
clean, named failure — a much worse failure mode than the `ValueError` the equivalent guards in
`generator/generate_csv.py` raise for the identical scenario (see its `# pragma: no cover - guarded
by config validation` comments, which raise real exceptions rather than asserting).
**Fix:** Replace the `assert`s with explicit `if ... is None: raise ValueError(...)` (or trust
`ColumnSpec`'s own pydantic validator entirely and drop the redundant runtime check) so this
invariant holds regardless of interpreter optimization flags.

## Info

### IN-01: `detect/filename.py` and `detect/schema.py` are fully vendored, fully untested, and have zero callers anywhere in this phase

**File:** `packages/csv-processor/src/csv_processor/detect/filename.py`, `packages/csv-processor/src/csv_processor/detect/schema.py`
**Issue:** Both modules are acknowledged as vendored-for-parity dead code in their own docstrings
and in `detect/__init__.py`'s docstring ("no caller in this config-driven design"). No test file in
this phase exercises either module (`tests/unit/` has no `test_filename.py`/`test_schema.py`), and
neither is imported by `engine.py`/`source.py`/`validate.py`. This is a deliberate decision, not an
oversight, but it means ~760 combined lines of intricate regex/strptime/type-inference logic ship
with zero runtime coverage and zero consumers — a maintenance liability if either module bit-rots
silently until a later phase tries to wire it in.
**Fix:** No action required this phase; consider a tracking note (or a `pytest.mark.skip`-free smoke
test) so a future phase doesn't have to re-derive that these are safe-but-unused before relying on
them.

### IN-02: `docker/oracle/init/04_widen_invalid_columns.sql`'s stated goal ("preserving each column's current size exactly") doesn't apply to `birth_date`/`event_ts`

**File:** `docker/oracle/init/04_widen_invalid_columns.sql:1-20,32-33`
**Issue:** The file's header comment claims the widening "preserv[es] each column's current size
exactly," but `birth_date`/`event_ts` are being converted from `DATE`/`TIMESTAMP WITH TIME ZONE` —
types with no VARCHAR2 "size" to preserve — to a newly invented `VARCHAR2(64)`. This is a reasonable
choice (matches the format strings both dataset configs declare) but the top-of-file comment's
"preserving...exactly" framing is inaccurate for these two columns specifically, which could mislead
a future author widening a similar column into thinking 64 is a derived/verified size rather than
an arbitrary one.
**Fix:** Adjust the comment to call out `birth_date`/`event_ts` as a deliberately-chosen new size,
not a preserved one (the per-column comment at line 19 already does this correctly — just the
file-level summary at the top doesn't).

### IN-03: `engine.py`/`source.py` import `Iterator` from `typing` instead of `collections.abc`

**File:** `packages/csv-processor/src/csv_processor/engine.py:17`, `packages/csv-processor/src/csv_processor/source.py:27`
**Issue:** `typing.Iterator`/`typing.BinaryIO`/`typing.TextIO` generic aliases sourced from `typing`
(rather than `collections.abc.Iterator` for the first) are the deprecated-since-3.9 spelling; other
modules in the same package (`detect/header.py`, `detect/schema.py`) correctly use
`from collections.abc import Sequence` under `TYPE_CHECKING`. Purely stylistic/consistency, no
behavioral effect given `from __future__ import annotations` is present in both files.
**Fix:** `from collections.abc import Iterator` for consistency with the rest of the package.

---

_Reviewed: 2026-08-29T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
