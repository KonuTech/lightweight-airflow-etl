# Phase 3: CSV Processing Engine - Pattern Map

**Mapped:** 2026-08-29
**Files analyzed:** 15 (10 new source modules, 1 config-loader mod, 1 generator mod, 2 DDL files, 1 new package `__init__`)
**Analogs found:** 15 / 15 (11 direct Tier-A/Tier-B analogs in the reference repo, 4 in-repo analogs)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|--------------------|------|-----------|-----------------|---------------|
| `packages/csv-processor/src/csv_processor/detect/dialect.py` | utility (detector) | transform | `/home/user/projects/airflow-platform/.../detect/dialect.py` | exact (Tier A, vendor verbatim + 1 import swap) |
| `packages/csv-processor/src/csv_processor/detect/encoding.py` | utility (detector) | transform | `/home/user/projects/airflow-platform/.../detect/encoding.py` | exact (Tier A, vendor verbatim + 1 import swap) |
| `packages/csv-processor/src/csv_processor/detect/header.py` | utility (detector) | transform | `/home/user/projects/airflow-platform/.../detect/header.py` | exact (Tier A, vendor verbatim + 1 import swap) |
| `packages/csv-processor/src/csv_processor/detect/filename.py` | utility (detector) | transform | `/home/user/projects/airflow-platform/.../detect/filename.py` | exact (Tier A, vendor for parity, no caller) |
| `packages/csv-processor/src/csv_processor/detect/schema.py` | utility (detector) | transform | `/home/user/projects/airflow-platform/.../detect/schema.py` | exact (Tier A, vendor for parity, no caller) |
| `packages/csv-processor/src/csv_processor/detect/__init__.py` | config/barrel | — | `/home/user/projects/airflow-platform/.../detect/__init__.py` | exact (empty re-export marker) |
| `packages/csv-processor/src/csv_processor/errors.py` | utility (exceptions) | — | `packages/csv-processor/src/csv_processor/config/errors.py` | exact (in-repo local-exception convention) |
| `packages/csv-processor/src/csv_processor/compression.py` | service | streaming/file-I/O | `/home/user/projects/airflow-platform/.../compression.py` | partial — streaming-open mechanics port, detection logic is new code (see Shared Patterns note) |
| `packages/csv-processor/src/csv_processor/source.py` | service (orchestrator) | streaming | `/home/user/projects/airflow-platform/.../source.py` (Tier B, read-only) + `packages/csv-processor/src/csv_processor/config/loader.py` (in-repo error-wrapping convention) | role-match (algorithm sequence only, not portable file) |
| `packages/csv-processor/src/csv_processor/normalize.py` | utility (transform) | transform | `/home/user/projects/airflow-platform/.../dataplat/normalize/dates.py` (Tier B, strptime algorithm) | role-match (algorithm only) |
| `packages/csv-processor/src/csv_processor/validate.py` | service (validation) | transform | `/home/user/projects/airflow-platform/.../dataplat/validate/*` (Tier B, algorithm only — check-order/priority is new per D-13/D-14) | partial (structure is new; no direct precision/scale precedent — see Research Pattern 3) |
| `packages/csv-processor/src/csv_processor/engine.py` | service (generator/orchestrator) | batch/streaming | `packages/csv-processor/src/csv_processor/config/loader.py` (error-wrapping + docstring convention) + Research Pattern 5 (`itertools.batched`) | role-match (new code, no direct file analog) |
| `tests/unit/test_detect_dialect.py`, `test_detect_encoding.py`, `test_detect_header.py`, `test_compression.py`, `test_structural_validation.py`, `test_type_validation.py`, `test_normalize.py`, `test_engine_chunks.py`, `test_no_airflow_import.py` | test | request-response (pytest) | `tests/unit/test_config_models.py` (in-repo pytest style) + `tests/unit/test_corpus_bounded_memory.py` (RLIMIT_AS/subprocess pattern for `test_engine_chunks.py`'s bounded-memory case) | exact (in-repo test conventions) |
| `docker/oracle/init/04_widen_invalid_columns.sql` | migration | batch | `docker/oracle/init/02_customers.sql`, `03_orders.sql` | exact (same DDL author/convention, Phase 1) |
| `generator/generate_csv.py` (add `--compress` flag) | utility (CLI) | file-I/O | itself (modification, not new file) | exact (extend existing argparse CLI) |

## Pattern Assignments

### `packages/csv-processor/src/csv_processor/detect/dialect.py` (utility, transform)

**Analog:** `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/dialect.py`

**Vendoring instruction:** copy the file verbatim (200 lines), then apply exactly one substitution:

```python
# line 53 — reference repo:
from dataplat.errors import CsvDialectDetectionError
# replace with a local class of the identical name in
# packages/csv-processor/src/csv_processor/errors.py:
from csv_processor.errors import CsvDialectDetectionError
```

Everything else (the `clevercsv.Detector().detect()` call, the `SimpleDialect('', '', '')`
degenerate-result guard, the `None`-return guard, `.to_csv_dialect()` conversion) is pure and
Postgres/S3/Vault/K8s-free — copy unchanged.

---

### `packages/csv-processor/src/csv_processor/detect/encoding.py` (utility, transform)

**Analog:** `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/encoding.py`

**Vendoring instruction:** copy verbatim (291 lines), one substitution:

```python
# line 67 — reference repo:
from dataplat.errors import EncodingDetectionError
# replace with:
from csv_processor.errors import EncodingDetectionError
```

**Critical dependency note (from RESEARCH.md Summary/Pitfall 2):** this file's
`_best_corroborating_match` near-tie logic structurally requires **both** `charset-normalizer`
*and* `chardet` — do not drop `chardet` even though this project's own prior STACK.md guidance
said to avoid it. Install both.

BOM-first check (D-26), then `charset-normalizer` + `chardet` corroboration with alias
canonicalization via `codecs.lookup`, then the near-tie epsilon logic and the no-BOM
wide-encoding confidence ceiling — all copy unchanged.

---

### `packages/csv-processor/src/csv_processor/detect/header.py` (utility, transform)

**Analog:** `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/header.py`

**Vendoring instruction:** copy verbatim (402 lines), one substitution:

```python
# line 55 — reference repo:
from dataplat.errors import FileInspectionError
# replace with:
from csv_processor.errors import FileInspectionError
```

Multi-gate scoring (non-empty, non-numeric, modal-field-count match), duplicate-name rejection
(exact and case-variant), footer/repeated-header detection — copy unchanged. Note per D-27: this
project has no metadata-preamble/footer fixture requirement today; vendor for parity but this
phase's `source.py`/`engine.py` only need the header-name-match subset (D-21/D-22 — order-
independent, case-sensitive exact match) driven directly off `config.columns`, not this module's
full preamble/footer scoring — still call this module's `detect_header` for its structural
signal, per D-25's "auto-detect and cross-check" requirement.

---

### `packages/csv-processor/src/csv_processor/detect/filename.py`, `schema.py` (utility, transform)

**Analog:** `/home/user/projects/airflow-platform/packages/csv-processor/.../detect/filename.py` (417
lines), `.../detect/schema.py` (344 lines)

**Vendoring instruction:** copy verbatim, one substitution each:

```python
# filename.py line 55:
from dataplat.errors import FilenameParsingError  →  from csv_processor.errors import FilenameParsingError
# filename.py line 58 (deferred import inside a function):
from dataplat.config.model import FilenameMaskConfig  →  local equivalent or inline type, see D-27 note
```
```python
# schema.py: locate its single dataplat.errors import (same substitution pattern) — not printed
# above since it was not directly quoted this session, but the file's own docstring/import block
# follows the identical single-import-swap shape as the other four Tier-A modules.
```

Per D-27, vendor for completeness/parity only — neither module has a caller in this
config-driven design (file pattern and schema are both already fully declared in `config.json`).
Do not wire either into `source.py`/`engine.py`.

---

### `packages/csv-processor/src/csv_processor/errors.py` (utility, exceptions — new local hierarchy)

**Analog:** `packages/csv-processor/src/csv_processor/config/errors.py` (in-repo, Phase 2)

**Convention to follow** (full file, 27 lines):

```python
class ConfigurationError(Exception):
    """..."""
    def __init__(self, message: str, *, context: dict) -> None:
        super().__init__(message)
        self.context = context
```

Apply the identical shape to every new exception this phase needs: `StructuralValidationError`,
`DetectionMismatchError` (D-16 — same base or sibling per Claude's Discretion), plus one class per
vendored `detect/*` module's `dataplat.errors` import name (`CsvDialectDetectionError`,
`EncodingDetectionError`, `FileInspectionError`, `FilenameParsingError`, and whatever `schema.py`
imports) — every one a plain `Exception` subclass, zero coupling to `dataplat`, `context: dict`
keyword-only constructor, matching this project's own already-established local-exception
convention (not the reference repo's).

---

### `packages/csv-processor/src/csv_processor/compression.py` (service, streaming/file-I/O)

**Analog:** `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/compression.py` (385 lines) — **partial port only**, per RESEARCH.md Pitfall 1.

**Reuse (streaming-open mechanics, port with S3→local swap):**

```python
# reference repo lines 1-24 (docstring) establishes the split this project must replicate:
# .gz -> gzip.GzipFile(fileobj=io.BufferedReader(stream)) — true single-pass stream
# .zip -> zipfile.ZipFile needs its central directory (file's end) before opening any member,
#         so buffer the *compressed* archive bytes into io.BytesIO first (not the decompressed
#         CSV content, never disk) — D-33 already scopes every archive to exactly one member.
```

Swap the reference's S3-backed open (`dataplat.storage.objectstore.open_text_stream`, line 36) for
plain `pathlib.Path.open("rb")` / `open(path, "rb")`. Reuse `_DecompressionBombGuard`'s
incremental cumulative-byte-ceiling pattern (function starting at line 88) as the
decompression-bomb mitigation named in RESEARCH.md's Security Domain table — keep an equivalent
ceiling even though detection itself is new code. `_BOUNDED_READ_CHUNK_BYTES = 65_536` (64 KiB)
and `_DEFAULT_MAX_DECOMPRESSED_BYTES = 512 * 1024 * 1024` are reasonable starting constants to
port as-is.

**Do NOT port `detect_compression()` (lines 67-83) as the detection function** — RESEARCH.md
Pitfall 1 confirms this reference function dispatches by **file extension**
(`_EXTENSION_COMPRESSION = {".gz": "gzip", ".zip": "zip"}`), which contradicts this project's
locked D-30 (magic-byte sniffing, pattern-agnostic). Write a **new** function:

```python
def detect_compression(sample: bytes) -> str | None:
    """Magic-byte sniff — D-30. New code, not a port (see 03-PATTERNS.md note)."""
    if sample[:2] == b"\x1f\x8b":
        return "gzip"
    if sample[:4] == b"PK\x03\x04":
        return "zip"
    return None
```

---

### `packages/csv-processor/src/csv_processor/source.py` (service orchestrator, streaming)

**Analog (Tier B — algorithm sequence only):** `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/source.py` (925 lines, wired into `dataplat`'s `Source`/`SchemaRepository`/`RecordChunk` — not portable). Read only for the *sequence*: detect compression → decode encoding → detect dialect → detect header → stream rows (RESEARCH.md Architecture Pattern 1).

**Error-wrapping convention to follow (in-repo analog):** `packages/csv-processor/src/csv_processor/config/loader.py` — never let a raw library exception escape; wrap every failure path in this phase's own local exception type:

```python
# packages/csv-processor/src/csv_processor/config/loader.py:44-58 pattern
try:
    ...
except (OSError, json.JSONDecodeError) as exc:
    raise ConfigurationError(
        f"could not read dataset config at {path}: {exc}",
        context={"path": str(path), "errors": [...]},
    ) from exc
```

Apply the same `raise X(...) from exc` + structured `context: dict` shape for
`StructuralValidationError`/`DetectionMismatchError` in `source.py`.

**Core sequence (research-verified, Pattern 1):**

```python
SAMPLE_BYTES = 65_536  # 64 KiB, matches encoding.py's own documented convention

def resolve_profile(raw_bytes_stream, config):
    sample = raw_bytes_stream.read(SAMPLE_BYTES)
    enc_detection = detect_encoding(sample, contract_encoding=None)  # None: always cross-check (D-25)
    decoded_sample = decode_strict(sample, enc_detection)
    dialect_detection = detect_dialect(decoded_sample, contract_delimiter=None)
    stdlib_dialect = to_stdlib_dialect(dialect_detection)
    # cross-check against config.csv (D-28: low-confidence defers to config)
    return enc_detection, stdlib_dialect
```

BOM must be stripped (`utf-8-sig` decode) before any of the above runs (D-26).

---

### `packages/csv-processor/src/csv_processor/normalize.py` (utility, transform)

**Analog (Tier B — algorithm only):** `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/normalize/dates.py` (478 lines, `_parse_plain_format` around lines 335-385) — strip all `StreamingStage`/DST/two-digit-year-pivot/spreadsheet-serial machinery (this project's `ColumnSpec` has no timezone/pivot/epoch fields).

**Adapted pattern (already verified live against this project's own configs/fixtures):**

```python
import datetime as dt

def parse_date_strict(raw: str, fmt: str, *, is_timestamp: bool) -> tuple[dt.date | dt.datetime | None, str | None]:
    try:
        parsed = dt.datetime.strptime(raw, fmt)
    except ValueError:
        code = "INVALID_TIMESTAMP_FORMAT" if is_timestamp else "INVALID_DATE_FORMAT"
        return None, code
    return (parsed if is_timestamp else parsed.date()), None
```

Decimal precision/scale — **no direct reference-repo precedent** (`dataplat/normalize/numeric.py`
validates `fixed_width`/scientific-notation, not `precision`/`scale`); this is this project's own
derivation, verified live against `orders.json`'s real `amount` column and corpus fixture 18:

```python
from decimal import Decimal, InvalidOperation

def parse_decimal_strict(raw: str, *, precision: int, scale: int) -> tuple[Decimal | None, str | None]:
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None, "TYPE_MISMATCH"
    sign, digits, exponent = value.as_tuple()
    if exponent >= 0:
        value_scale, value_precision = 0, len(digits) + exponent
    else:
        value_scale, value_precision = -exponent, max(len(digits), -exponent)
    if value_scale > scale or value_precision > precision:
        return None, "DECIMAL_PRECISION_EXCEEDED"
    return value, None
```

Never convert through `float()` (reference repo's own unconditional rule, `numeric.py` docstring).

---

### `packages/csv-processor/src/csv_processor/validate.py` (service, transform)

**Analog (Tier B — algorithm only, structure is new per D-13/D-14/D-15):** `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/validate/*` — read only for individual check algorithms (e.g. `pattern.py`'s regex checks); ignore all `StreamingStage`/`BarrierStage`/`RejectedRecord` scaffolding and every referential/uniqueness/volume-anomaly/completeness/circuit-breaker validator (out of scope per project CLAUDE.md and spec §28).

**Check-order pattern (new design, D-13/D-14/D-15 — no direct file precedent):**

```python
def validate_row(row: dict[str, str], config: DatasetConfig) -> tuple[str | None, str | None, str | None]:
    """Returns (error_code, error_message, error_column) or (None, None, None) if valid."""
    nullability_violations = []
    type_violations = []
    for col in config.columns:  # config.columns preserves declared order (D-15 tie-break)
        value = row[col.name]
        if not col.nullable and value == "":
            nullability_violations.append(col)
            continue
        if value == "" and col.nullable:
            continue
        if not _passes_type_check(value, col):
            type_violations.append(col)
    if nullability_violations:
        col = nullability_violations[0]
        return "NULL_VIOLATION", f"{col.name!r} is required but empty", col.name
    if type_violations:
        col = type_violations[0]
        return _type_error_code(col.type), f"{col.name!r} failed type check", col.name
    return None, None, None
```

Structural check (field count vs. header count) runs **before** this function and short-circuits
(D-13) — never call `validate_row` on a structurally-broken row.

---

### `packages/csv-processor/src/csv_processor/engine.py` (service, batch/streaming)

**Analog:** no direct file analog (new for this phase) — follow `packages/csv-processor/src/csv_processor/config/loader.py`'s docstring/error-wrapping convention for the public function's docstring shape, and RESEARCH.md's own verified `itertools.batched` pattern for chunking:

```python
import csv
import itertools

def read_chunks(text_stream, dialect, chunk_size: int):
    reader = csv.reader(text_stream, dialect=dialect)
    header = next(reader)  # already validated/matched before this point
    for batch in itertools.batched(reader, chunk_size):
        yield batch
```

Note: Python 3.12 pinned here — `itertools.batched` has **no** `strict=` kwarg until 3.13; do not
pass it. Also set `csv.field_size_limit(1_048_576)` explicitly (Research Pattern 6) rather than
relying on the unstated 131072 default.

`process_chunks(file_path, config) -> Iterator[tuple[list[dict], list[dict]]]` (D-11) is the
public surface this phase builds — Phase 4's `process()`/`ProcessingResult` wrapper is explicitly
out of scope (do not build it here).

---

### Tests — `tests/unit/test_*.py` (test, request-response/pytest)

**Analog for general pytest style:** `tests/unit/test_config_models.py` (in-repo, Phase 2) —
module docstring explaining scope/distinction from sibling test files, `from __future__ import
annotations`, plain `pytest`/model imports, one `_VALID_DATASET`-style fixture constant at module
scope, grouped test sections.

**Analog for `test_engine_chunks.py`'s bounded-memory assertion specifically:**
`tests/unit/test_corpus_bounded_memory.py` (in-repo, Phase 2) — the `RLIMIT_AS` subprocess
technique (streaming survives a hard address-space cap; a deliberately-broken `.readlines()`
variant dies under the identical cap as a negative control):

```python
_RLIMIT_AS_BYTES = 25_165_824  # 24 MiB

_STREAMING_SCRIPT = """
import resource, sys
resource.setrlimit(resource.RLIMIT_AS, (int(sys.argv[2]), int(sys.argv[2])))
with open(sys.argv[1], newline="") as handle:
    for _ in handle:
        pass
"""
```
Reuse this exact subprocess/`RLIMIT_AS` pattern, adapted to call `process_chunks()` instead of a
raw file iterator, and skip cleanly (`pytest.skip(...)`) if `resource`/`RLIMIT_AS` is unavailable
(non-POSIX) — never silently pass.

**Pitfall to encode directly in test design (RESEARCH.md Pitfall 3):** corpus fixtures
`23_embedded_newline_in_quoted_field`, `24_embedded_delimiter_in_quoted_field`,
`25_doubled_quote_escaping`, `27_oversized_field_value` (`byte_level_hard` category) do **not**
match either real dataset schema (`order_id, note`/`big_field` vs. `orders.json`'s real
`order_id, customer_id, order_date, amount`). Test these against the raw parsing primitives
(`csv.reader`/`detect_dialect`) or a fixture-scoped ad hoc `DatasetConfig`, never through
`process_chunks()` with the real `orders.json`/`customers.json` config expecting a
`SUCCESS`/valid-row outcome.

---

### `docker/oracle/init/04_widen_invalid_columns.sql` (migration, batch)

**Analog:** `docker/oracle/init/02_customers.sql`, `docker/oracle/init/03_orders.sql` (Phase 1 DDL,
same author/convention). Read these directly before writing the migration to get the exact current
column sizes (D-04 — keep `VARCHAR2(64)`/`VARCHAR2(255)` etc. as currently declared, do not widen
to `VARCHAR2(4000)` uniformly). Add `raw_line VARCHAR2(4000)` (or CLOB, D-06) to both `_INVALID`
tables. Per D-03, deliver both this new numbered init script (for a fresh
`docker compose down -v && up`) **and** the equivalent `ALTER TABLE ... MODIFY (...)` statements
run directly against the currently-running container (`docker compose exec sqlplus`) — follow
Phase 1's own D-06 precedent (see `01-02-SUMMARY.md` if present) for exactly how that dual-delivery
was structured there.

---

### `generator/generate_csv.py` (modify — add `--compress` flag)

**Analog:** itself (existing argparse-based CLI, Phase 2). Add a `--compress` flag that gzips the
generated output file after writing (D-32) — follow the file's own existing argparse
option-definition and post-write-hook conventions (read the file directly during implementation;
not excerpted here since it is a modification to an existing, already-known-shape file rather than
a new file needing an external analog).

## Shared Patterns

### Local-exception, never-let-library-exceptions-escape
**Source:** `packages/csv-processor/src/csv_processor/config/errors.py` +
`packages/csv-processor/src/csv_processor/config/loader.py`
**Apply to:** `errors.py`, `source.py`, `compression.py`, `engine.py` — every module that can fail
on malformed input wraps the failure in a local `Exception` subclass with a keyword-only
`context: dict`, always `raise X(...) from exc`.

### Tier-A vendoring = one-line import swap, nothing else
**Source:** confirmed by direct read of all four Tier-A `detect/*` modules with a `dataplat.errors`
import this session (`dialect.py:53`, `encoding.py:67`, `header.py:55`, `filename.py:55`)
**Apply to:** `detect/dialect.py`, `detect/encoding.py`, `detect/header.py`, `detect/filename.py`,
`detect/schema.py` — copy the file verbatim, swap exactly the one `from dataplat.errors import X`
line for `from csv_processor.errors import X` (same class name, new local definition in
`errors.py`). Do not "clean up" or restructure the rest of the file — the detection algorithms
themselves have zero Postgres/S3/Vault/K8s coupling and should be byte-identical to the reference
repo apart from that one import line.

### D-30 vs. `compression.py`'s actual approach — do not silently "port" compression detection
**Source:** RESEARCH.md Pitfall 1 (direct read of `/home/user/projects/airflow-platform/.../compression.py:67-83` this session)
**Apply to:** `compression.py` only — the file's *streaming-open* mechanics (gzip single-pass,
zip buffered-archive-bytes, `_DecompressionBombGuard`) are legitimate ports; the *detection*
function itself (`detect_compression`) is extension-based in the reference repo and must be
rewritten as magic-byte sniffing per this project's own locked D-30. Do not cite the reference
file as prior art for the detection function.

### Decimal-exact parsing, never `float()`
**Source:** `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/normalize/numeric.py` docstring ("this module never converts a parsed value through `float`")
**Apply to:** `normalize.py`'s decimal-type conversion path.

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `packages/csv-processor/src/csv_processor/validate.py`'s check-priority/tie-break logic (D-13/D-14/D-15) | service | transform | No reference-repo file implements "structural short-circuits, then exhaustive nullability-before-type with declared-column-order tie-break" — this is a fresh design per this phase's own locked decisions; only the individual per-type check *algorithms* (date/decimal) have precedent |
| `packages/csv-processor/src/csv_processor/compression.py`'s `detect_compression()` (magic-byte sniff) | utility | transform | Reference repo's same-named function is extension-based (see RESEARCH.md Pitfall 1) — new code, not a port; magic bytes (`1f8b` gzip, `504b0304` zip) verified live but no source file to copy from |
| `packages/csv-processor/src/csv_processor/normalize.py`'s decimal precision/scale check | utility | transform | RESEARCH.md Assumption/Pattern 3 — "this project's `precision`/`scale` check has no direct precedent to port" (`dataplat/normalize/numeric.py` validates a different concern); derived and verified live against real config/corpus values instead |

## Metadata

**Analog search scope:** `/home/user/projects/airflow-platform/packages/{csv-processor,dataplat}/src/**`, `/home/user/projects/lightweight-airflow-etl/{packages/csv-processor,tests,docker/oracle/init,generator}/**`
**Files scanned:** ~25 (reference repo detect/compression/source/normalize/validate modules + in-repo config/test/DDL files)
**Pattern extraction date:** 2026-08-29
</content>
