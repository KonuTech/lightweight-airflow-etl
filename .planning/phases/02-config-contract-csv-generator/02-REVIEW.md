---
phase: 02-config-contract-csv-generator
reviewed: 2026-08-28T22:13:23Z
depth: standard
files_reviewed: 26
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
findings:
  critical: 1
  warning: 5
  info: 3
  total: 9
status: issues_found
---

# Phase 2: Code Review Report

**Reviewed:** 2026-08-28T22:13:23Z
**Depth:** standard
**Files Reviewed:** 26
**Status:** issues_found

## Summary

Reviewed the Pydantic v2 config contract (`csv_processor.config`), the deterministic Faker-based
business-row CSV generator (`generator/generate_csv.py`), and the independent fixture-corpus
subsystem (`tools/corpus/`). The YAML-safety requirement is met cleanly: `tools/corpus/manifest.py`
calls `yaml.safe_load` exclusively, and a dedicated test (`test_load_manifest_never_uses_the_unsafe_yaml_loader`)
polices that at the source-line level. No hardcoded secrets, `eval`/`exec`, or bare `except:` blocks
were found anywhere in the reviewed files. `uv.lock`/`pyproject.toml` dependency pins are consistent
across the three declared locations (root, `packages/csv-processor`, and the documented Dockerfile
constraint). The corpus determinism rules (R1–R10) are implemented and cross-checked correctly —
verified `stream_for`'s per-fixture SHA-256-derived seeding, the gzip `mtime=0`/`filename=""` and
zip `date_time=(1980,1,1,...)` pinning, and the batched/plain tabular byte-equivalence claim by
tracing both code paths by hand.

The main defect found is a genuine BLOCKER: the `CsvDialectConfig` Pydantic model accepts
`doublequote: false` with no `escapechar`, a combination that is provably valid per the schema but
crashes `csv.writer` at runtime (reproduced live) the moment a written field needs escaping — this
directly undermines CONFIG-02's "validated once … before any CSV processing begins" design intent
that the rest of the model tree (e.g. the delimiter/decimal-separator collision validator) otherwise
honors. Several further WARNING-level schema gaps were found by testing the model directly (accepting
`precision`/`scale` on non-decimal columns, `valid_table == invalid_table`, duplicate column names),
plus a semantic/naming mismatch in `generate_csv.py`'s D-15 "missing_required" invalid-row category,
which never actually reads `ColumnSpec.required` and instead only tests nullability.

## Critical Issues

### CR-01: `CsvDialectConfig` permits `doublequote=false` without `escapechar`, causing a runtime crash in the CSV writer

**File:** `packages/csv-processor/src/csv_processor/config/models.py:64-79`

**Issue:** `CsvDialectConfig` has no cross-field validation ensuring `escapechar` is set whenever
`doublequote` is `false`. Such a config passes `DatasetConfig.model_validate()` cleanly (verified
directly):

```python
>>> DatasetConfig.model_validate({
...   "dataset": "x", "file_pattern": "x_*.csv",
...   "csv": {"doublequote": False},
...   "columns": [{"name": "a", "type": "string", "nullable": False, "required": True}],
...   "oracle": {"valid_table": "t", "invalid_table": "t"},
...   "processing": {"chunk_size": 1},
... })
# validates with zero errors
```

But feeding that dialect into stdlib `csv.writer` (exactly what `generator/generate_csv.py:write_csv`
does with `config.csv.escapechar`/`config.csv.doublequote`) crashes the moment any field needs
quoting:

```python
>>> import csv, io
>>> w = csv.writer(io.StringIO(), delimiter=',', quotechar='"', escapechar=None, doublequote=False)
>>> w.writerow(['a,b', 'c"d'])
_csv.Error: need to escape, but no escapechar set
```

Notably, `tools/corpus/generators.py`'s own `_quote_field` (lines 442-450) explicitly guards this
exact combination and raises a clear `GeneratorError` — the production config model that flows into
the same stdlib `csv` module has no equivalent guard, so the failure mode this phase's own sibling
module was written to prevent is still reachable through `csv_processor.config`.

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

**Issue:** Confirmed directly that `OracleTargetSpec.model_validate({"valid_table": "t", "invalid_table": "t"})`
validates successfully. The entire point of this model is to keep valid and invalid rows in separate
tables (D-12's docstring: "Keeps config.json safe to log or version" plus the project's whole
valid/invalid split design) — a typo'd config that names the same table twice would silently merge
valid and rejected rows with no validation-time signal.

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

**Issue:** `_check_type_specific_fields` only validates that `precision`/`scale` are present
*when* `type == "decimal"`; it never rejects them when present on a different type. Confirmed
directly:

```python
>>> ColumnSpec.model_validate({"name": "x", "type": "string", "nullable": False,
...                             "required": True, "precision": 10, "scale": 2})
# validates with zero errors — precision/scale silently accepted on a string column
```

This is exactly the kind of typo'd/copy-pasted config the model's `extra="forbid"` discipline was
built to reject elsewhere, but the type-conditional check leaves this specific combination open.

**Fix:** Extend the validator's `else` branch:
```python
elif self.precision is not None or self.scale is not None:
    msg = f"column {self.name!r}: 'precision'/'scale' are only valid for type 'decimal'"
    raise ValueError(msg)
```

### WR-03: `DatasetConfig.columns` allows duplicate column names

**File:** `packages/csv-processor/src/csv_processor/config/models.py:101-116`

**Issue:** The only structural constraint on `columns` is `Field(min_length=1)`; nothing rejects
two `ColumnSpec` entries sharing the same `name`. The project explicitly cares about this exact
failure mode enough to build a dedicated fixture for it on the *detection* side
(`tests/fixtures/corpus.yaml`'s `11_duplicate_column_name`), but the config model that declares the
intended schema in the first place does not protect against declaring a duplicate itself.

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

**File:** `generator/generate_csv.py:44-57`, `114-135`

**Issue:** `applicable_categories()` (line 54) and `_generate_invalid_row()` (line 128) both compute
their "missing_required" eligibility/behavior from `not column.nullable`, never from
`column.required`. Confirmed by search: `column.required` is never read anywhere in this module (or
anywhere in the codebase outside its own Pydantic model and unit test) —
`generator/generate_csv.py` only ever reads `.nullable`. The category actually implemented sets a
non-nullable column's *value* to `""` (a nullability violation), it never removes a required column
from the row/header (a presence violation), which is what the category's name and D-15's own
description ("missing_required") implies. `models.py`'s own docstring explicitly frames `nullable`
and `required` as "deliberately two separate booleans" for exactly this kind of distinction, so this
generator silently collapses that distinction back into one, mislabeling the resulting fixture data.

**Fix:** Either rename the category (e.g. `empty_required_value`) to match its actual behavior, or
implement a distinct category that drops a `required=True` column from the header/row entirely to
test column-presence rather than value-nullability.

### WR-05: `tools/corpus/generators.py` row-spec renderers raise raw `KeyError` instead of `GeneratorError` for malformed row_spec entries

**File:** `tools/corpus/generators.py:351-360` (`_zero_padded_renderer`), `363-376`
(`_pick_renderer`), `379-403` (`_decimal_renderer`), `406-414` (`_repeat_renderer`)

**Issue:** Every one of these renderer builders reads required keys via direct dict indexing
(`spec["width"]`, `spec["values"]`, `spec["scale"]`, `spec["char"]`, etc.) with no
`try`/`except`. A manifest author's typo (e.g. `width` misspelled as `wdith`) surfaces as a bare
`KeyError: 'width'` with no fixture name and no context, breaking the module's otherwise-consistent
discipline of naming the offending fixture in every other raised `GeneratorError` (see
`_renderer_for`, `_generate_tabular`, `_generate_wrapper`, `_encode`, `_bom_for`, all of which name
`fixture.name`/`fixture_name` in their error messages).

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

## Info

### IN-01: `digests.parse_digests`'s `name.lstrip(" *")` can corrupt a legitimately-named fixture

**File:** `tools/corpus/digests.py:92`

**Issue:** `parsed[name.lstrip(" *")] = digest` strips *all* leading spaces and asterisks from the
parsed name, not just the single binary-mode prefix marker (`" *"`). A future fixture name starting
with a literal `*` or space would have that character silently eaten. Not currently reachable (all
30 committed fixture names start with a two-digit numeric prefix), but worth a narrower fix
(`name[1:] if name.startswith("*") else name`, applied only once).

**Fix:** Match GNU coreutils' actual rule more precisely — strip a single leading `*` only, and only
after the mandatory two-space (or one-space) separator has already been consumed by `partition`.

### IN-02: `_decimal_renderer` has no `min <= max` guard, silently produces a wrong value instead of erroring

**File:** `tools/corpus/generators.py:379-403`

**Issue:** If a manifest declares a `decimal` row_spec with `min > max`, `span = high - low + 1`
becomes `<= 0`. `min(int(rng.random() * span), span - 1)` then yields a negative offset (e.g. for
`span == 0`, offset is `-1`), so `_render`/`_render_integral` silently returns `low - 1` — one unit
below the declared minimum — rather than raising. Since this only misfires on a manifest-authoring
mistake and doesn't crash, it's low severity, but it's inconsistent with the module's otherwise
fail-loud philosophy (`GeneratorError` everywhere else).

**Fix:** Raise `GeneratorError` in `_decimal_renderer` when `span <= 0` (i.e. `minimum > maximum`).

### IN-03: `generate_csv.py --dataset` argument is interpolated directly into a filesystem path with no sanitization

**File:** `generator/generate_csv.py:239-244` (`main`), `231-234` (`output_path`)

**Issue:** `args.dataset` flows unsanitized into both the config path
(`_CONFIGS_DIR / "datasets" / f"{args.dataset}.json"`) and the output path
(`_DATA_DIR / dataset / ...`). A value like `--dataset ../../etc/passwd` would attempt to read/write
outside the intended directories. Given this is a locally-invoked dev CLI (via `make generate`) with
no untrusted/remote input path, this is low risk in the current threat model, but worth a
`str.isidentifier()`-style guard if this script is ever wrapped by anything accepting external input
(e.g. a future Airflow DAG parameter).

**Fix:** Validate `args.dataset` against an allow-list pattern (e.g. `^[a-zA-Z0-9_-]+$`) before use.

---

_Reviewed: 2026-08-28T22:13:23Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
