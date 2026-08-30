---
phase: 02-config-contract-csv-generator
reviewed: 2026-08-29T06:08:05Z
depth: standard
files_reviewed: 27
files_reviewed_list:
  - .gitignore
  - Makefile
  - configs/datasets/customers.json
  - configs/datasets/orders.json
  - configs/defaults.json
  - docker-compose.yml
  - generator/generate_csv.py
  - packages/csv-processor/pyproject.toml
  - packages/csv-processor/src/csv_processor/config/__init__.py
  - packages/csv-processor/src/csv_processor/config/errors.py
  - packages/csv-processor/src/csv_processor/config/loader.py
  - packages/csv-processor/src/csv_processor/config/models.py
  - pyproject.toml
  - tests/fixtures/CORPUS.sha256
  - tests/fixtures/corpus.yaml
  - tests/unit/test_config_loader.py
  - tests/unit/test_config_models.py
  - tests/unit/test_corpus_bounded_memory.py
  - tests/unit/test_corpus_generators.py
  - tests/unit/test_corpus_manifest.py
  - tests/unit/test_generate_csv.py
  - tools/corpus/__init__.py
  - tools/corpus/__main__.py
  - tools/corpus/digests.py
  - tools/corpus/generators.py
  - tools/corpus/manifest.py
  - uv.lock
findings:
  critical: 1
  warning: 7
  info: 4
  total: 12
status: issues_found
---

# Phase 2: Code Review Report

**Reviewed:** 2026-08-29T06:08:05Z
**Depth:** standard
**Files Reviewed:** 27
**Status:** issues_found

## Summary

Full re-review of the complete Phase 2 scope (27 files, including the two files a later plan added:
`tests/unit/test_corpus_bounded_memory.py` and `tests/unit/test_corpus_generators.py`), covering the
Pydantic v2 config contract (`csv_processor.config`), the deterministic Faker-based business-row CSV
generator (`generator/generate_csv.py`), and the independent fixture-corpus subsystem
(`tools/corpus/`). `uv run pytest tests/unit/ -q` (89 tests) and
`uv run python -m tools.corpus verify --manifest tests/fixtures/corpus.yaml --digests
tests/fixtures/CORPUS.sha256` (30/30 fixtures) were both run live and pass. `yaml.safe_load` is used
exclusively for the untrusted manifest input (T-02-04); no hardcoded secrets, `eval`/`exec`, or bare
`except:` blocks were found; `uv.lock` versions match the pinned `pyproject.toml` declarations
exactly.

One BLOCKER stands: `CsvDialectConfig` accepts `doublequote: false` with no `escapechar`, a
combination that validates cleanly but crashes stdlib `csv.writer` the moment a field needs escaping
— reproduced live below. Several WARNING-level schema gaps were also reproduced live (missing
cross-field checks that the rest of the model tree's own precedent, e.g. the
delimiter/decimal-separator collision validator and `ProcessingConfig.chunk_size`'s `Field(gt=0)`,
would suggest should exist), plus a genuine data-correctness bug newly found in this pass:
`tools/corpus/generators.py`'s `_decimal_renderer` silently emits mathematically wrong digits for
negative decimal ranges (not currently reachable — no committed fixture declares a negative
`min`/`max` — but a real, verified latent defect in a module whose entire purpose is byte-exact
correctness). `generate_csv.py`'s D-15 "missing_required" invalid-row category was confirmed to test
only `column.nullable`, never `column.required` — `ColumnSpec.required` is dead code everywhere
outside its own model/test.

## Critical Issues

### CR-01: `CsvDialectConfig` permits `doublequote=false` without `escapechar`, crashing the CSV writer at runtime

**File:** `packages/csv-processor/src/csv_processor/config/models.py:64-79`

**Issue:** `CsvDialectConfig` has no cross-field validation ensuring `escapechar` is set whenever
`doublequote` is `false`. Reproduced live — this validates with zero errors:

```python
>>> from csv_processor.config import DatasetConfig
>>> DatasetConfig.model_validate({
...   "dataset": "x", "file_pattern": "x_*.csv",
...   "csv": {"doublequote": False},
...   "columns": [{"name": "a", "type": "string", "nullable": False, "required": True}],
...   "oracle": {"valid_table": "t", "invalid_table": "t2"},
...   "processing": {"chunk_size": 1},
... })
# validates OK, csv.doublequote=False, csv.escapechar=None
```

But feeding that dialect into stdlib `csv.writer` (exactly what `generator/generate_csv.py:write_csv`
does with `config.csv.escapechar`/`config.csv.doublequote`) crashes the moment any field needs
quoting — also reproduced live:

```python
>>> import csv, io
>>> w = csv.writer(io.StringIO(), delimiter=',', quotechar='"', escapechar=None, doublequote=False)
>>> w.writerow(['a,b', 'c"d'])
_csv.Error: need to escape, but no escapechar set
```

`tools/corpus/generators.py`'s own `_quote_field` (lines 442-450) explicitly guards this exact
combination and raises a clear `GeneratorError` — the production config model that flows into the
same stdlib `csv` module has no equivalent guard, so the failure this phase's own sibling module was
written to prevent is still reachable through `csv_processor.config`, undermining CONFIG-02's
"validated once … before any CSV processing begins" intent.

**Fix:** Add a `model_validator` to `CsvDialectConfig` mirroring `DatasetConfig`'s existing
delimiter/decimal-separator collision check:

```python
@model_validator(mode="after")
def _check_escapechar_present_when_doublequote_disabled(self) -> CsvDialectConfig:
    if not self.doublequote and not self.escapechar:
        msg = (
            "csv.doublequote is false but csv.escapechar is unset; a field "
            "requiring escaping would crash at write/parse time with no way to represent it"
        )
        raise ValueError(msg)
    return self
```

## Warnings

### WR-01: `OracleTargetSpec` does not reject `valid_table == invalid_table`

**File:** `packages/csv-processor/src/csv_processor/config/models.py:82-90`

**Issue:** Reproduced live: `OracleTargetSpec.model_validate({"valid_table": "t", "invalid_table":
"t"})` validates successfully. The entire point of this model is to keep valid and rejected rows in
separate tables — a typo'd config naming the same table twice would silently merge them with no
validation-time signal.

**Fix:**
```python
@model_validator(mode="after")
def _check_valid_and_invalid_tables_differ(self) -> OracleTargetSpec:
    if self.valid_table.lower() == self.invalid_table.lower():
        msg = f"oracle.valid_table and oracle.invalid_table must differ, both are {self.valid_table!r}"
        raise ValueError(msg)
    return self
```

### WR-02: `ColumnSpec` accepts `precision`/`scale` on non-decimal columns

**File:** `packages/csv-processor/src/csv_processor/config/models.py:46-61`

**Issue:** `_check_type_specific_fields` only validates that `precision`/`scale` are present *when*
`type == "decimal"`; it never rejects them when present on a different type. Reproduced live:
`ColumnSpec.model_validate({"name": "x", "type": "string", "nullable": False, "required": True,
"precision": 10, "scale": 2})` validates with zero errors — `extra="forbid"`'s copy-paste-typo
discipline doesn't cover this specific cross-field combination.

**Fix:** Extend the validator's `else` branch:
```python
elif self.precision is not None or self.scale is not None:
    msg = f"column {self.name!r}: 'precision'/'scale' are only valid for type 'decimal'"
    raise ValueError(msg)
```

### WR-03: `DatasetConfig.columns` allows duplicate column names

**File:** `packages/csv-processor/src/csv_processor/config/models.py:101-116`

**Issue:** The only structural constraint on `columns` is `Field(min_length=1)`; nothing rejects two
`ColumnSpec` entries sharing the same `name`. Reproduced live — a two-column list both named `"a"`
validates cleanly. The project cares enough about this exact failure mode to build a dedicated
detection-side fixture for it (`tests/fixtures/corpus.yaml`'s `11_duplicate_column_name`), but the
config model that declares the intended schema doesn't protect against declaring the duplicate in
the first place.

**Fix:**
```python
@model_validator(mode="after")
def _check_column_names_are_unique(self) -> DatasetConfig:
    names = [c.name for c in self.columns]
    if len(names) != len(set(names)):
        dupes = sorted({n for n in names if names.count(n) > 1})
        msg = f"duplicate column name(s) in 'columns': {dupes}"
        raise ValueError(msg)
    return self
```

### WR-04: `generate_csv.py`'s "missing_required" category never reads `ColumnSpec.required`

**File:** `generator/generate_csv.py:54-55`, `127-129`

**Issue:** `applicable_categories()` (lines 54-55) and `_generate_invalid_row()` (lines 127-129) both
compute "missing_required" eligibility/behavior from `not column.nullable`, never from
`column.required`. Confirmed by search: `column.required`/`.required` is read nowhere in production
code, anywhere in the repo, outside `ColumnSpec`'s own model and one test assertion
(`tests/unit/test_config_models.py:197`) — `required` is effectively dead schema state. Concretely,
`customers.json`'s `birth_date` column is `nullable: true, required: true`; because the generator
keys off `nullable`, `birth_date` can never be selected for "missing_required" even though it is the
one column in that dataset explicitly flagged `required: true`, while a hypothetical
`nullable: false, required: false` column would be wrongly treated as "missing_required". The
category actually implemented tests a nullability violation (empties a non-nullable value), never a
presence violation (dropping a `required` column from the row/header), which is what the category's
name and `models.py`'s own "deliberately two separate booleans" (D-09) docstring implies.

**Fix:** Either rename the category (e.g. `empty_required_value`) to match its actual behavior, or
implement a distinct category that drops a `required=True` column from the header/row entirely to
test column-presence rather than value-nullability.

### WR-05: `tools/corpus/generators.py` row-spec renderers raise raw `KeyError` instead of `GeneratorError` for malformed row_spec entries

**File:** `tools/corpus/generators.py:351-360` (`_zero_padded_renderer`), `363-376`
(`_pick_renderer`), `379-403` (`_decimal_renderer`), `406-414` (`_repeat_renderer`)

**Issue:** Every one of these renderer builders reads required keys via direct dict indexing
(`spec["width"]`, `spec["values"]`, `spec["scale"]`, `spec["char"]`, etc.) with no `try`/`except`.
Reproduced live: a manifest typo (`width` misspelled as `wdith`) surfaces as a bare `KeyError:
'width'` with no fixture name and no context, breaking the module's otherwise-consistent discipline
of naming the offending fixture in every other raised error (`_renderer_for`, `_generate_tabular`,
`_generate_wrapper`, `_encode`, `_bom_for` all name `fixture.name`/`fixture_name`).

**Fix:** Wrap the key lookups and re-raise as `GeneratorError` naming the fixture and column, e.g.:
```python
def _zero_padded_renderer(fixture_name: str, column: str, spec: dict[str, Any]) -> Renderer:
    try:
        width, start = spec["width"], spec["start"]
    except KeyError as exc:
        msg = f"{fixture_name}: column {column!r} row_spec missing {exc.args[0]!r}"
        raise GeneratorError(msg) from exc
    ...
```
(requires threading `fixture_name`/`column` into the renderer builders, which `_renderer_for`
already has in scope).

### WR-06: `_decimal_renderer` emits mathematically wrong digits for negative decimal ranges

**File:** `tools/corpus/generators.py:398-401`

**Issue:** New finding (not in the prior review pass). The scale>0 branch formats a negative decimal
via Python floor division/modulo:

```python
def _render(rng: random.Random, row_index: int) -> str:
    del row_index
    units = low + min(int(rng.random() * span), span - 1)
    return f"{units // power}{separator}{units % power:0{scale}d}"
```

For a negative `units`, Python's `//` floors toward negative infinity and `%` returns a
non-negative remainder — this does not decompose a negative decimal into "sign on the integer part,
magnitude in the fraction" the way the format string assumes. Reproduced live for `min: -10, max: -1,
scale: 2`: internal `units = -439` (the true value `-4.39`) renders as the string `"-5.61"` — a wrong
value, not merely a rounding artifact:

```python
>>> units, power, scale = -439, 100, 2
>>> f"{units // power}.{units % power:0{scale}d}"
'-5.61'   # should be -4.39
```

No fixture in the committed `tests/fixtures/corpus.yaml` currently declares a negative `min`/`max`,
so this is not live-shipped-wrong data today (`make fixtures-verify` passes), but the very next
manifest author who adds a "negative amount" fixture (a realistic future need — `orders.amount` could
plausibly need a negative-value edge case) will get a byte-exact digest oracle baked from silently
incorrect data, with no error raised anywhere in the pipeline to catch it.

**Fix:** Decompose sign and magnitude explicitly before formatting, e.g.:
```python
sign = "-" if units < 0 else ""
magnitude = abs(units)
return f"{sign}{magnitude // power}{separator}{magnitude % power:0{scale}d}"
```

### WR-07: `ColumnSpec`'s decimal validator doesn't reject non-positive `precision`/`scale`, causing a downstream `TypeError` instead of a clear config error

**File:** `packages/csv-processor/src/csv_processor/config/models.py:43-44`, `generator/generate_csv.py:60-68`

**Issue:** New finding. `precision`/`scale` are declared as plain `int | None = None` with no
`Field(gt=0)` constraint (unlike `ProcessingConfig.chunk_size: int = Field(gt=0)`, the model tree's
own precedent for exactly this kind of numeric guard). Reproduced live — this validates cleanly:

```python
>>> ColumnSpec.model_validate({"name": "x", "type": "decimal", "nullable": True,
...                             "required": True, "precision": -1, "scale": -2})
# validates OK: precision=-1, scale=-2
```

Feeding that column into `generate_csv.py`'s `format_decimal()` (which every valid-decimal-row
generation path calls) then crashes with an unrelated, confusing error instead of the clean
`ConfigurationError` CONFIG-02 promises:

```python
>>> from generate_csv import format_decimal
>>> import random
>>> format_decimal(random.Random(1), precision=-1, scale=-2)
TypeError: 'float' object cannot be interpreted as an integer
```

(`max_value = 10**precision - 1` evaluates to a `float` for negative `precision`, which
`random.Random.randint()` then rejects.)

**Fix:** Add `Field(gt=0)` to both `precision` and `scale` on `ColumnSpec`, consistent with
`ProcessingConfig.chunk_size`'s existing pattern.

## Info

### IN-01: `digests.parse_digests`'s `name.lstrip(" *")` can corrupt a legitimately-named fixture

**File:** `tools/corpus/digests.py:92`

**Issue:** `parsed[name.lstrip(" *")] = digest` strips *all* leading spaces and asterisks from the
parsed name, not just the single binary-mode prefix marker (`" *"`). Reproduced live: parsing
`"aaaa...  ***weird_name.csv"` yields the name `"weird_name.csv"`, silently eating two of the three
leading asterisks. Not currently reachable (all 30 committed fixture names start with a two-digit
numeric prefix), but worth a narrower fix.

**Fix:** Match GNU coreutils' actual rule more precisely — strip a single leading `*` only, and only
immediately after the mandatory separator, e.g. `name[1:] if name.startswith("*") else name`.

### IN-02: `_decimal_renderer` has no `min <= max` guard, silently returns an out-of-range value instead of erroring

**File:** `tools/corpus/generators.py:379-388`

**Issue:** If a manifest declares a `decimal` row_spec with `min > max`, `span = high - low + 1`
becomes `<= 0`; `min(int(rng.random() * span), span - 1)` then yields a negative offset, so
`_render`/`_render_integral` silently returns a value below the declared minimum rather than raising.
Related to, but distinct from, WR-06 above (WR-06 is about a legitimate non-empty span still
formatting the wrong digits for a negative value; this is about an inverted/empty span never being
rejected at all). Low severity — misfires only on a manifest-authoring mistake, doesn't crash — but
inconsistent with the module's otherwise fail-loud philosophy (`GeneratorError` everywhere else, e.g.
`_quote_field`, `_encode`, `_bom_for`).

**Fix:** Raise `GeneratorError` in `_decimal_renderer` when `span <= 0` (i.e. `minimum > maximum`).

### IN-03: `generate_csv.py --dataset` argument is interpolated directly into a filesystem path with no sanitization

**File:** `generator/generate_csv.py:231-234` (`output_path`), `239-244` (`main`)

**Issue:** `args.dataset` flows unsanitized into both the config path
(`_CONFIGS_DIR / "datasets" / f"{args.dataset}.json"`) and the output path
(`_DATA_DIR / dataset / ...`). A value like `--dataset ../../etc/passwd` would attempt to read/write
outside the intended directories. This is a locally-invoked dev CLI (via `make generate`) with no
untrusted/remote input path today, so risk is low in the current threat model, but worth a
`str.isidentifier()`-style guard if this script is ever wrapped by anything accepting external input
(e.g. a future Airflow DAG parameter).

**Fix:** Validate `args.dataset` against an allow-list pattern (e.g. `^[a-zA-Z0-9_-]+$`) before use.

### IN-04: `_ALL_CATEGORIES` module-level constant is dead code

**File:** `generator/generate_csv.py:41`

**Issue:** New finding. `_ALL_CATEGORIES = ("wrong_type", "invalid_date", "missing_required",
"wrong_column_count")` is defined but never referenced anywhere else in the file or the codebase
(`applicable_categories()` builds its own list independently, `_generate_invalid_row()`'s
`if`/`elif` chain hardcodes the same four strings again). Either wire this constant into both
functions to keep the category vocabulary single-sourced, or remove it.

**Fix:** Use `_ALL_CATEGORIES` as the source of truth in `_generate_invalid_row`'s dispatch (e.g. a
dict keyed by category name) instead of maintaining the same four strings in three places, or delete
the unused constant.

---

_Reviewed: 2026-08-29T06:08:05Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
