# Phase 3: CSV Processing Engine - Research

**Researched:** 2026-08-29
**Domain:** CSV detection/parsing/validation engine (Airflow-agnostic, chunked, Oracle-adjacent but no Oracle I/O in this phase)
**Confidence:** MEDIUM-HIGH (module algorithms verified by reading the actual reference-repo source this session; two of 03-CONTEXT.md's own "verified" claims were found to be incorrect on re-read — see Summary and Open Questions)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Invalid-Row Storage Shape**
- D-01: `customers_invalid`/`orders_invalid` (Phase 1 DDL) currently mirror the `_VALID` tables' native typed/NOT-NULL columns — but ENGINE-06 requires invalid rows to carry their **original field values**, and a malformed date string or empty required field cannot literally be inserted into a native NOT NULL/typed column as currently defined. Resolved: widen every data column on both `_INVALID` tables to **nullable VARCHAR2**, storing every original field as its raw string (never converted/typed). `error_code`/`error_message`/`source_file`/`row_number` are unaffected.
- D-02: The DDL migration (D-01) is executed **now, in Phase 3**, even though Phase 3 itself never inserts into Oracle.
- D-03: Migration delivery follows Phase 1's own precedent: **both** a new numbered init script (`docker/oracle/init/04_widen_invalid_columns.sql` or similar) **and** the same `ALTER TABLE` statements applied directly against the currently-running container.
- D-04: Widened VARCHAR2 columns **keep each column's current size** (`customer_id`/`order_id`/`country` stay `VARCHAR2(64)`, `name` stays `VARCHAR2(255)`, etc.) rather than uniformly widening to `VARCHAR2(4000)` — an oversized original value is its own distinct, worth-flagging error condition, not something to silently accommodate.
- D-05: A genuinely blank CSV cell stores as an **empty string**; SQL `NULL` is reserved for a column that's truly **absent** from the row (a ragged/short row missing that field entirely).
- D-06: Both `_INVALID` tables also get a **`raw_line`** column (`VARCHAR2(4000)`/CLOB) holding the entire original CSV line as one string, in addition to the per-column widened values.
- D-07: `row_number` = a **1-indexed count of every line after the header that reaches row-processing** — including blank lines (D-24 flags a blank line as its own invalid row).
- D-08: `source_file` records the **basename only** (e.g. `customers_20260829.csv`).

**Row Output Shape (Phase 3 ↔ Phase 4 interface)**
- D-09: An invalid row is a **dict**: `{col_name: original_string_value, ..., "error_code": ..., "error_message": ..., "source_file": ..., "row_number": ...}`.
- D-10: A valid row uses the **same dict shape** with typed Python values instead of strings (`datetime.date`, `Decimal`, `int`, `str`, `bool`), no `error_*` keys.
- D-11: The engine's chunk-processing function is a **lazy generator** — e.g. `process_chunks(file_path, config) -> Iterator[tuple[list[dict], list[dict]]]` (valid rows, invalid rows) per chunk. Only one chunk's rows are ever in memory at once.

**Error Code Taxonomy**
- D-12: `error_code` is **one specific code per failure type** (e.g. `MISSING_REQUIRED_COLUMN`, `DUPLICATE_COLUMN_NAME`, `WRONG_COLUMN_COUNT`, `NO_HEADER_ROW`, `TYPE_MISMATCH`, `NULL_VIOLATION`, `INVALID_DATE_FORMAT`, `DECIMAL_PRECISION_EXCEEDED`, etc.) — roughly one per structural/type/nullability check the engine actually runs.
- D-13: Check execution: a **structurally-broken row short-circuits** — its `error_code` is always the structural one; type/nullability checks are skipped for that row. Once a row **passes** structural validation, **all** type checks and **all** nullability checks run across every column, and `error_code` reports whichever violation is highest-priority among what was found (not simple first-check-wins).
- D-14: Within type + nullability: **nullability is checked before type**. If no nullability violation exists anywhere in the row, the first type violation found wins.
- D-15: When multiple columns share the **same kind** of violation, the reported column is the **first one in `config.json`'s declared `columns` order**.
- D-16: A detect-vs-config mismatch uses the **same `StructuralValidationError` exception type** as header-level structural failures, but gets its **own distinct `error_code` family/prefix** (e.g. `DETECT_*` vs `STRUCT_*`).

**Structural Failure Scope**
- D-17: **Header-level** mismatches (missing/extra/duplicate declared column, no header row at all) reject the **whole file** — zero rows processed. **Row-level** ragged rows (wrong field count on one specific data row, header otherwise fine) become **per-row invalid entries**.
- D-18: An extra/unrecognized column in the header is a **hard whole-file reject**.
- D-19: A duplicate column name in the header is the **same hard whole-file reject**.
- D-20: A completely **empty file** (zero bytes, no header at all) is a whole-file reject. A file with a **valid header but zero data rows** is **not an error** — processes successfully with `total=0/valid=0/invalid=0`.
- D-21: Header column **order is independent** of `config.json`'s declared `columns` order — columns are matched by **name**, not position.
- D-22: Header column-name matching is **case-sensitive, exact match required**.
- D-23: A whole-file structural reject is signaled at the Python level as a **plain exception** (e.g. `StructuralValidationError`) — `csv_processor` stays Airflow-agnostic per ENGINE-09.
- D-24: A blank line interspersed between data rows is **flagged as its own invalid row** (0 fields where N were expected), not silently skipped.

**Detection Cross-Check Strictness & Compressed Input**
- D-25: The engine **auto-detects** dialect/encoding/header via the vendored Tier-A modules and **cross-checks** the result against `config.json`'s declared values, rather than trusting config blindly.
- D-26: A UTF-8 **BOM is stripped** (`utf-8-sig` decode) **before any parsing**, always.
- D-27: All **5** of the reference repo's Tier-A `detect/*` modules are vendored (`dialect.py`, `encoding.py`, `header.py`, `filename.py`, `schema.py`) for completeness/parity, even though `filename.py`/`schema.py` have no current caller.
- D-28: A **low-confidence** detection disagreement with config **defers to config** as authoritative (no error raised) — only a **high-confidence** disagreement raises a detect-vs-config mismatch error.
- D-29 (major scope addition): The engine must read **compressed CSV input** (`.csv.gz`, `.zip`) transparently, via **streaming decompression** — never extract-to-a-temp-file first — using the already-vendored `compression.py`.
- D-30: Compressed-file detection is by **magic-byte sniffing** (gzip `0x1f 0x8b`, zip `PK\x03\x04`), **pattern-agnostic** — not by filename extension. *(See Open Questions — this session's direct read of `compression.py` found the reference module actually dispatches by extension, not magic bytes. D-30's own stated verification does not hold; treat the "match `compression.py`'s approach" framing as void and implement magic-byte sniffing as a fresh decision, not a port.)*
- D-31: `config.json`'s `file_pattern` is **widened** to match both plain and compressed variants, e.g. `"customers_*.csv*"`.
- D-32: Phase 2's business-row generator (`generate_csv.py`) gets a **`--compress`** flag.
- D-33: A **multi-entry zip** archive requires **exactly one** member; zero or more than one entry raises a structural error.

**Async / Optimization**
- D-34: **No** async/concurrent chunk reading in Phase 3 — the engine stays strictly **sequential**.

### Claude's Discretion
- Exact module/file layout within `packages/csv-processor/src/csv_processor/` (`detect/`, `source.py`, `normalize.py`/`validate.py`, `engine.py`) — ARCHITECTURE.md's directory sketch is a reasonable starting point.
- Detection's "bounded sample" size for dialect/encoding sniffing — **verified this session**: the reference repo's `detect/encoding.py` docstring documents a "~64 KiB" sample convention (comment in the module docstring: `tests/fixtures/corpus.yaml`-documented ~64 KiB convention), consistent with 03-CONTEXT.md's note. Use a fixed 64 KiB (65,536-byte) sample for dialect+encoding+header detection unless a concrete reason to differ surfaces during implementation.
- Exact exception class hierarchy (`StructuralValidationError`, a `DetectionMismatchError` sibling or subclass per D-16) and the full `error_code` enum member list.

### Deferred Ideas (OUT OF SCOPE)
- **Concurrent/async chunk reading for file I/O** — revisit only if Phase 6's benchmark shows CPU-bound row parsing/validation, not Oracle round-trips, is the actual limiter. Not built in this phase (D-34).
- **Writing compressed CSV output** — no current requirement or DAG step produces a CSV file as output at all.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ENGINE-01 | Validate CSV structure (column count / missing / unexpected columns) before validating anything else | `detect/header.py`'s scoring algorithm (read this session) + D-17..D-24's structural rule set; see Architecture Patterns Pattern 1 |
| ENGINE-02 | Validate each column's type (integer/decimal/date) per config schema | `datetime.strptime` strict-rejection pattern (from `dataplat/normalize/dates.py`, read this session) + Decimal precision/scale check (verified live this session, see Code Examples) |
| ENGINE-03 | Validate required (non-nullable) fields are non-empty | D-14's check-order (nullability before type); plain function, no library needed |
| ENGINE-04 | Explicitly convert each valid CSV string field to its configured Python/Oracle type | Same type-check functions produce the typed value on success (D-10) |
| ENGINE-05 | Invalid row doesn't stop processing of rest of file; valid/invalid split, both counted | Collect-don't-raise pattern (ARCHITECTURE.md Anti-Pattern 5), D-13 short-circuit rule |
| ENGINE-06 | Invalid row records `error_code`, `error_message`, `source_file`, `row_number` + original field values | D-09, D-12 error taxonomy |
| ENGINE-07 | CSV reading/validation processes rows in configurable chunks, bounded memory | `itertools.batched` (stdlib, Python 3.12, verified live this session) + D-11's generator API |
| ENGINE-09 | `csv_processor` has no Airflow import/dependency, unit-testable standalone | Package already has zero Airflow imports (Phase 1/2 precedent); this phase must not add any |
| TEST-01 | Unit tests cover config parsing, CSV parsing, type conversion, date validation, valid/invalid row handling, chunked processing | Validation Architecture section below maps each to a concrete test file/command |
</phase_requirements>

## Summary

Phase 3 builds `csv_processor`'s internal detect → parse → validate → normalize → split pipeline
as a lazy, chunked generator with zero Airflow dependency. The shape of the work is well-established
by prior research (ARCHITECTURE.md, PITFALLS.md) and by 03-CONTEXT.md's 34 locked decisions — this
research pass exists to verify the specific mechanics those decisions assume, by reading the actual
reference-repo source rather than trusting summaries.

Two verification findings materially change what the planner should write into tasks:

1. **D-30 is factually wrong about its own citation.** 03-CONTEXT.md's D-30 states compressed-file
   detection "[m]atches `compression.py`'s own actual detection approach (verified: reads magic
   bytes, not extension)." Reading `compression.py` directly this session
   (`/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/compression.py:41-85`)
   shows the opposite: `detect_compression(key)` dispatches purely on filename suffix
   (`_EXTENSION_COMPRESSION = {".gz": "gzip", ".zip": "zip"}`), and its own docstring says so
   explicitly: *"Extension-based dispatch (rather than magic-byte sniffing) is this plan's chosen,
   documented resolution... the simpler, sufficient choice for this platform's synthetic,
   well-formed corpus."* There is no magic-byte logic anywhere in that file or elsewhere in the
   vendored `csv_processor`/`dataplat` tree (grepped this session — zero hits for `0x1f`/`1f8b`/
   `PK\x03\x04`/`magic`). D-30 itself is still the user's locked decision (magic-byte sniffing,
   pattern-agnostic) and stays authoritative — but the planner must write it as a **new
   implementation**, not a Tier-A port, since there is no reference algorithm to vendor for this
   specific piece. See Open Questions and Code Examples for the concrete implementation.
2. **`detect/encoding.py`'s actual corroboration algorithm needs both `chardet` AND
   `charset-normalizer`**, contradicting this project's own prior STACK.md guidance ("What NOT to
   Use: `chardet`... Use Instead: `charset-normalizer`"). The vendored file's near-tie corroboration
   logic (`_best_corroborating_match`) structurally requires agreement between the two detectors —
   dropping `chardet` breaks the ported algorithm, not just a preference. Since D-25 locks in
   "auto-detects... via the vendored Tier-A modules," both libraries are required dependencies for
   this phase, not just `charset-normalizer` alone. `chardet` was never installed in this project
   before now.

**Primary recommendation:** Vendor `dialect.py`, `encoding.py`, `header.py` verbatim (swap the
single `dataplat.errors` import in each for a local exception of the same name — no other changes
needed); vendor `filename.py`/`schema.py` for parity per D-27 but do not wire them into the pipeline
(neither has a caller in this config-driven design); write compression detection and dispatch as new
code (magic-byte sniffing, `.gz`/`.zip` streaming) rather than as a port, since the reference file's
own approach differs from the locked decision; reimplement date/decimal parsing as small plain
functions following the reference's `strptime`/`Decimal` algorithms (not their `StreamingStage`
scaffolding); use `itertools.batched` over a single `csv.reader` for record-count chunking.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Config load/validate | `csv_processor.config` (already built, Phase 2) | — | Already validated once per run; this phase only reads the frozen `DatasetConfig` |
| Compression/encoding/dialect/header detection | `csv_processor.detect` + `csv_processor.compression` (new, this phase) | — | Runs once per file, before any row streaming; no Airflow, no Oracle |
| Row streaming + chunking | `csv_processor.source` (new, this phase) | — | Opens the (possibly decompressed) text stream, applies the resolved dialect, yields `itertools.batched` chunks of raw rows |
| Structural / type / nullability validation | `csv_processor.validate` (new, this phase) | — | Pure functions over one row + `DatasetConfig`; never touches I/O or Oracle |
| Type conversion (string → Python type) | `csv_processor.normalize` (new, this phase) | — | Runs only after a row passes all validation; produces the typed dict (D-10) |
| Valid/invalid split + chunk generator | `csv_processor.engine` (partial, this phase — `process_chunks` only, not `process()`) | — | The `process_chunks` generator IS this phase's public surface; the full `process()`/`ProcessingResult` wrapper is explicitly Phase 4's ENGINE-08 |
| Oracle bulk load | Phase 4 | — | Out of scope this phase — do not build |
| Airflow DAG wiring | Phase 5 | — | Out of scope this phase — `csv_processor` must import nothing from `airflow.*` |

## Package Legitimacy Audit

Three new PyPI dependencies are needed this phase, none currently declared anywhere in this
project's `pyproject.toml` files (`clevercsv`, `charset-normalizer`, `chardet` — confirmed absent
via `grep` this session). All three are already-vetted by this project's own prior STACK.md research
pass (`clevercsv==0.8.5`, `charset-normalizer==3.5.1`) with one addition (`chardet`) discovered this
session as structurally required by the vendored `encoding.py` algorithm (see Summary).

**Version verification (`pip index versions`, run live this session):**

| Package | Latest on PyPI | STACK.md-pinned version | Notes |
|---------|----------------|--------------------------|-------|
| `clevercsv` | 0.8.5 | 0.8.5 | Matches — no drift since STACK.md's research pass |
| `charset-normalizer` | 3.5.1 | 3.5.1 | Matches |
| `chardet` | 7.6.0 | *(not previously pinned — new)* | `7.5.1` mentioned in `encoding.py`'s own docstring as the version it was verified against; `7.6.0` is current. Recommend pinning `chardet==7.6.0` unless a compatibility reason to pin `7.5.1` surfaces during implementation. |

| Package | Registry | Age (per legitimacy check) | Downloads | Source Repo | Verdict | Disposition |
|---------|----------|------|-----------|--------------|---------|-------------|
| `clevercsv` | PyPI | published 2026-05-11 (registry metadata; project itself is long-established — Alan Turing Institute-affiliated, 0.1.1 dates to 2019) | unknown (registry API didn't report a figure) | none reported by the check | `SUS` (`unknown-downloads`, `no-repository`) | **Flagged, not removed.** Reasons are a metadata-tool limitation, not evidence of a fake package — this exact package/version is already a locked project decision in STACK.md (verified there via Context7 + PyPI, cross-checked against the actual GitHub repo `alan-turing-institute/CleverCSV`, which the automated check's `repoUrl` field simply failed to surface). Planner should still add `checkpoint:human-verify` before install per protocol. |
| `charset-normalizer` | PyPI | published 2026-08-15 (registry metadata reflects a recent release; project is long-established, `>3.5.1` line has years of history) | unknown | none reported | `SUS` (`too-new`, `unknown-downloads`, `no-repository`) | **Flagged, not removed.** Same as above — already locked in STACK.md, well-known MIT-licensed successor to `chardet` in most modern tooling (`pip`, `requests` itself use it). `checkpoint:human-verify` before install. |
| `chardet` | PyPI | published 2026-08-14 | unknown | `github.com/chardet/chardet` (reported) | `SUS` (`too-new`, `unknown-downloads`) | **Flagged, not removed.** Has a real, canonical source repo reported by the check itself. New to this project (not previously pinned) — required only because the vendored `encoding.py` algorithm structurally needs it (see Summary). `checkpoint:human-verify` before install. |

**Packages removed due to `[SLOP]` verdict:** none.
**Packages flagged as suspicious `[SUS]`:** `clevercsv`, `charset-normalizer`, `chardet` — all three flagged purely on registry-metadata-completeness signals (`unknown-downloads`/`no-repository`/`too-new`), not on any evidence of typosquatting or malicious content. All three are well-known, actively-maintained libraries already used identically by this project's own reference repo and already vetted once in this project's own STACK.md research. The planner must still add a `checkpoint:human-verify` task before each `uv add`, per protocol — but this is a process gate, not a signal to substitute or drop any of the three.

No `postinstall` scripts detected for any of the three (not applicable to PyPI in the same sense as npm; no setup.py-based build-time network calls were found in a source skim of the vendored `encoding.py`/`dialect.py` usage).

## Architecture Patterns

### System Architecture Diagram

```
raw CSV file (or .csv.gz / .zip) on local filesystem
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│ csv_processor.compression.detect_compression(path)           │  magic-byte sniff:
│   → None | "gzip" | "zip"                                    │  0x1f 0x8b | PK\x03\x04
└─────────────────────────────────────────────────────────────┘
        │ open_compressed_stream() — streaming, never temp-file
        ▼
┌─────────────────────────────────────────────────────────────┐
│ ONE bounded sample read (~64 KiB) from the (decompressed)    │
│ stream, used for ALL of the following — never re-read per    │
│ chunk (ARCHITECTURE.md Anti-Pattern 2):                      │
│   csv_processor.detect.encoding.detect_encoding(sample)      │──▶ BOM check first (D-26),
│   csv_processor.detect.dialect.detect_dialect(decoded)       │    then charset-normalizer +
│   csv_processor.detect.header.detect_header(rows)            │    chardet corroboration
└─────────────────────────────────────────────────────────────┘
        │ cross-check detected profile against config.json (D-25/D-28)
        │   low-confidence disagreement → defer to config (no error)
        │   high-confidence disagreement → DetectionMismatchError (whole-file reject, D-16)
        ▼
┌─────────────────────────────────────────────────────────────┐
│ csv_processor.source: open full stream with resolved dialect,│
│ csv.reader(..., dialect=resolved), header-name-matched to    │
│ config.columns (D-21/D-22)                                   │
│   header mismatch (missing/extra/dup/no-header) → whole-file │
│   StructuralValidationError, ZERO rows processed (D-17/D-18/ │
│   D-19/D-20)                                                 │
└─────────────────────────────────────────────────────────────┘
        │ itertools.batched(reader, chunk_size)  — record-count chunks (D-11, ENGINE-07)
        ▼
┌─────────────────────────────────────────────────────────────┐
│ per chunk, per row:                                          │
│  1. structural row check (field count == header count)       │
│     fail → invalid row, error_code=WRONG_COLUMN_COUNT,       │
│            SKIP type/nullability (D-13 short-circuit)        │
│  2. pass → run ALL nullability checks, then ALL type checks  │
│     (D-14 order) across every column                         │
│     any violation → invalid row, highest-priority error_code │
│     (D-15 tie-break: first column in config.columns order)   │
│  3. no violation → normalize (typed conversion, D-10)         │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
  yield (valid_rows: list[dict], invalid_rows: list[dict])   ← one tuple per chunk (D-11)
        │
        ▼
  Phase 4's process() loop: executemany() + discard chunk, pull next
```

### Recommended Project Structure

```
packages/csv-processor/src/csv_processor/
├── config/                  # existing (Phase 2) — untouched this phase
│   ├── models.py
│   ├── loader.py
│   └── errors.py
├── detect/                  # NEW this phase — Tier A, vendored + import-fixed
│   ├── __init__.py
│   ├── dialect.py            # vendored verbatim + import swap
│   ├── encoding.py           # vendored verbatim + import swap
│   ├── header.py              # vendored verbatim + import swap
│   ├── filename.py            # vendored per D-27, no caller (parity only)
│   └── schema.py               # vendored per D-27, no caller (parity only)
├── compression.py            # NEW code (not a port — see Summary/Open Questions):
│                              # magic-byte sniff + streaming gzip/zip open
├── source.py                 # NEW — Tier B rewrite: open, detect-once, chunked read
├── normalize.py               # NEW — Tier B: per-type string→Python conversion functions
├── validate.py                 # NEW — Tier B: structural/type/nullability checks
├── errors.py                    # NEW — local exception hierarchy (StructuralValidationError,
│                                 #        DetectionMismatchError, plus one per detect/* module's
│                                 #        vendored exception name)
└── engine.py                     # NEW (this phase's slice only) — process_chunks() generator;
                                    # full process()/ProcessingResult wrapper is Phase 4 (ENGINE-08)
```

### Pattern 1: Detect once, cross-check against config, never re-detect per chunk

**What:** Detection (compression, encoding, dialect, header) runs exactly once, from one bounded
sample near the start of `process_chunks`, before the chunk-iterator is ever constructed. Every
subsequent chunk reuses the resolved profile.

**When to use:** Always, for this phase's `source.py` orchestrator.

**Example (encoding + dialect, adapted from the vendored modules' verified call shape):**
```python
# Source: /home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/
#         detect/encoding.py (read verbatim this session) + detect/dialect.py
from csv_processor.detect.encoding import detect_encoding, decode_strict
from csv_processor.detect.dialect import detect_dialect, to_stdlib_dialect

SAMPLE_BYTES = 65_536  # 64 KiB, matches encoding.py's own documented convention

def resolve_profile(raw_bytes_stream, config):
    sample = raw_bytes_stream.read(SAMPLE_BYTES)
    enc_detection = detect_encoding(sample, contract_encoding=None)  # None: always cross-check (D-25)
    decoded_sample = decode_strict(sample, enc_detection)
    dialect_detection = detect_dialect(decoded_sample, contract_delimiter=None)
    stdlib_dialect = to_stdlib_dialect(dialect_detection)
    # ... cross-check enc_detection/dialect_detection against config.csv (D-28: low-confidence defers)
    return enc_detection, stdlib_dialect
```

### Pattern 2: Structural check short-circuits; type/nullability run exhaustively (D-13/D-14)

**What:** Per row: check field count first. On mismatch, emit `WRONG_COLUMN_COUNT` and stop —
never attempt type/nullability against misaligned fields. On a structurally-sound row, run every
nullability check and every type check across every column (never stop at the first check), then
select the reported `error_code` by priority (nullability beats type, D-14) and by column order
(D-15) when multiple columns share the same violation kind.

**Example:**
```python
def validate_row(row: dict[str, str], config: DatasetConfig) -> tuple[str | None, str | None, str | None]:
    """Returns (error_code, error_message, error_column) or (None, None, None) if valid."""
    nullability_violations = []
    type_violations = []
    for col in config.columns:  # config.columns preserves declared order (D-15 tie-break)
        value = row[col.name]
        if not col.nullable and value == "":
            nullability_violations.append(col)
            continue  # a null violation makes type-checking this column meaningless
        if value == "" and col.nullable:
            continue  # empty + nullable is valid, never a type check target
        if not _passes_type_check(value, col):
            type_violations.append(col)
    if nullability_violations:
        col = nullability_violations[0]  # first in declared order (D-15)
        return "NULL_VIOLATION", f"{col.name!r} is required but empty", col.name
    if type_violations:
        col = type_violations[0]
        return _type_error_code(col.type), f"{col.name!r} failed type check", col.name
    return None, None, None
```

### Pattern 3: Decimal precision/scale check via `Decimal.as_tuple()` (verified live this session)

**What:** `ColumnSpec.precision`/`scale` (total significant digits / digits after the decimal point)
is verified against a parsed `Decimal` using `as_tuple()`'s `(sign, digits, exponent)` — never a
string-length heuristic, which breaks on values with trailing zeros stripped or leading zeros.

**Verified live** (`python3` REPL, this session) against `orders.json`'s real `amount` column
(`precision=12, scale=2`, confirmed by reading
`/home/user/projects/lightweight-airflow-etl/configs/datasets/orders.json:29-30`) and corpus
fixture 18's exact value (`"100.999"`, confirmed by reading
`/home/user/projects/lightweight-airflow-etl/tests/fixtures/corpus.yaml:259`):

```python
from decimal import Decimal, InvalidOperation

def parse_decimal_strict(raw: str, *, precision: int, scale: int) -> tuple[Decimal | None, str | None]:
    """Returns (value, error_code). error_code is None on success."""
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

# Verified this session:
# parse_decimal_strict("100.999", precision=12, scale=2)
#   -> as_tuple() = (0, (1,0,0,9,9,9), -3); value_scale=3, value_precision=6
#   -> 3 > 2 (declared scale)  => ("None", "DECIMAL_PRECISION_EXCEEDED")  -- matches fixture 18 exactly
```

**Reference algorithm origin (Tier B — algorithm only, not the file):**
`/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/normalize/numeric.py` (read
this session) uses `Decimal` parsing but validates against `fixed_width`/scientific-notation
concerns, not `precision`/`scale` directly — this project's `precision`/`scale` check has **no
direct precedent to port** in the reference repo (its `ColumnContract` shape differs); the
`as_tuple()` formula above is this session's own verified derivation, not a ported algorithm.
Confidence: HIGH (derived and verified live against the actual corpus fixture and actual config
values, not assumed).

### Pattern 4: Strict `strptime` date/timestamp rejection (Tier B algorithm, adapted)

**What:** Parse with `datetime.strptime(value, column.format)` inside a `try`/`except ValueError`;
never fall back to `dateutil.parser.parse` or any format-guessing. A `ValueError` is the row's
`INVALID_DATE_FORMAT`/`INVALID_TIMESTAMP_FORMAT` signal.

**Example (adapted from `dataplat/normalize/dates.py`'s `_parse_plain_format`, read this session —
stripped of the `StreamingStage`/DST/spreadsheet-serial machinery this project's schema does not
declare; `customers.json`/`orders.json` only ever set a plain `format` string, confirmed by reading
both config files this session):**
```python
# Source algorithm: /home/user/projects/airflow-platform/packages/dataplat/src/dataplat/
#                    normalize/dates.py:335-385 (read this session; StreamingStage/timezone/
#                    two-digit-year/spreadsheet-serial machinery removed — this project's
#                    ColumnSpec has no timezone/pivot/epoch fields to drive them)
import datetime as dt

def parse_date_strict(raw: str, fmt: str, *, is_timestamp: bool) -> tuple[dt.date | dt.datetime | None, str | None]:
    try:
        parsed = dt.datetime.strptime(raw, fmt)
    except ValueError:
        code = "INVALID_TIMESTAMP_FORMAT" if is_timestamp else "INVALID_DATE_FORMAT"
        return None, code
    return (parsed if is_timestamp else parsed.date()), None
```

Applies directly to corpus fixtures 19/20 (`31/02/2026` against `%Y-%m-%d`; `2026-01-01
00:00:00` against `%Y-%m-%dT%H:%M:%S%z` — both confirmed by reading
`tests/fixtures/corpus.yaml:263-290` this session), both of which are pure `ValueError` cases under
this simplified function — no DST/serial/pivot handling is needed because `customers.json`'s
`birth_date`/`event_ts` and `orders.json`'s `order_date` (confirmed by reading both config files)
declare only a plain `format` string, never a timezone or spreadsheet-epoch field (which do not
even exist on this project's `ColumnSpec`, confirmed by reading
`packages/csv-processor/src/csv_processor/config/models.py:26-64` this session).

### Pattern 5: Record-count chunking via `itertools.batched`

**What:** Chunk by record ordinal, never byte/line offset (ARCHITECTURE.md Anti-Pattern 3).
`itertools.batched` is stdlib as of Python 3.12 — **confirmed available** via live `help()` call
this session against the pinned interpreter (`python3 --version` → `Python 3.12.3`, matching
`pyproject.toml`'s `requires-python = ">=3.12"`, confirmed by reading
`/home/user/projects/lightweight-airflow-etl/pyproject.toml:8` this session).

```python
# Source: stdlib itertools docs, signature verified live this session:
#   itertools.batched(iterable, n) -> yields tuples of length n, last batch may be shorter
import csv
import itertools

def read_chunks(text_stream, dialect, chunk_size: int):
    reader = csv.reader(text_stream, dialect=dialect)
    header = next(reader)  # already validated/matched before this point
    for batch in itertools.batched(reader, chunk_size):
        yield batch  # tuple[tuple[str, ...], ...] — never materialized beyond one batch
```

Note: `itertools.batched` gained a `strict=` keyword in Python 3.13; this project's pinned 3.12
does not have it — do not pass `strict=` (would raise `TypeError` at 3.12, confirmed by inspecting
the live 3.12.3 `help()` signature this session, which shows only `batched(iterable, n)`).

### Pattern 6: `csv.field_size_limit` set explicitly, not left at the Python default

**What:** Python's default `csv.field_size_limit()` is **131072 (128 KiB)** — confirmed live this
session (`python3 -c "import csv; print(csv.field_size_limit())"` → `131072`). This comfortably
covers corpus fixture 27's 10,001-character field (confirmed by reading
`tests/fixtures/corpus.yaml:379-391` this session — see Open Questions for what this fixture
actually needs to prove). ARCHITECTURE.md's Anti-Pattern 4 still recommends setting the limit
**explicitly** (not relying on the unstated default) so an unterminated-quote runaway field fails
predictably rather than silently inheriting whatever the interpreter's default happens to be.

```python
import csv
csv.field_size_limit(1_048_576)  # 1 MiB, explicit — matches ARCHITECTURE.md's own recommendation
```

### Anti-Patterns to Avoid

- **Re-detecting encoding/dialect/header per chunk** (ARCHITECTURE.md Anti-Pattern 2) — wasteful and
  can produce inconsistent results chunk-to-chunk.
- **Chunking by byte/line offset instead of record ordinal** (ARCHITECTURE.md Anti-Pattern 3) — an
  embedded newline inside a quoted field makes offset-based splitting corrupt rows.
- **Wrapping each row in the config's Pydantic model to drive validation** (ARCHITECTURE.md
  Anti-Pattern 5) — Pydantic raises on the first invalid field; this fights the
  collect-every-violation model D-13/D-14 require.
- **Treating fixture 27's threshold as a VARCHAR2-column-size check.** There is no config field
  (`ColumnSpec` has no `max_length`) carrying an oversized-value threshold, and the fixture's own
  header (`order_id, big_field`) does not match either dataset's real schema — see Open Questions.
  Do not invent a `VALUE_TOO_LONG` error_code against an undeclared limit; if D-04's "oversized
  original value" concern needs a real check, it needs a new config field first (flag for
  discussion, don't silently build it).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| CSV dialect (delimiter/quoting) sniffing | A custom delimiter-frequency heuristic | Vendored `detect/dialect.py` (`clevercsv.Detector().detect()`) | `csv.Sniffer` (stdlib) is documented to raise on genuinely single-column files and misidentify delimiters inside quoted fields — the vendored module's own docstring names this as the reason `clevercsv` was chosen over both alternatives |
| Encoding detection | A single-detector (`chardet`-only or `charset-normalizer`-only) guess | Vendored `detect/encoding.py`'s two-detector corroboration (BOM check first, then charset-normalizer + chardet agreement) | A lone probabilistic detector's "confident" wrong guess produces silent mojibake (PITFALLS.md Pitfall 5); corroboration between two independently-designed detectors is the vendored module's actual verified mitigation |
| Header/preamble/footer detection | A "first non-empty row is the header" heuristic | Vendored `detect/header.py`'s multi-gate scoring (non-empty, non-numeric, modal-field-count-match) | Handles metadata preambles and footer rows the naive approach misses; not needed by this project's fixed two-dataset schema today, but the module is vendored anyway per D-27 |
| Date/timestamp parsing | `dateutil.parser.parse` or format-guessing | `datetime.strptime(value, column.format)` in a `try`/`except ValueError` | STACK.md explicitly excludes `dateutil` from the validator path — the whole point is catching malformed dates, not accepting "close enough" ones |
| Decimal parsing | `float()` then round | `decimal.Decimal(raw)` | `float` introduces binary rounding error on monetary values; `Decimal` is exact — confirmed as the reference repo's own unconditional rule ("this module never converts a parsed value through `float`", `dataplat/normalize/numeric.py` docstring, read this session) |
| Streaming decompression | `shutil.unpack_archive` / extract-to-temp-file, then read | `gzip.GzipFile(fileobj=...)` for `.gz` (true single-pass stream); `zipfile.ZipFile` over a buffered-in-memory archive for `.zip` (ZIP's central directory lives at the file's end — genuinely cannot stream-open without either seeking or buffering, per the vendored `compression.py`'s own docstring, read this session) | D-29's explicit "never extract-to-a-temp-file" requirement; the vendored module's own D-22a precedent already solved the ZIP structural constraint this way |
| Chunked reading | Manual byte-offset seeking or `\n`-counting | `itertools.batched(csv.reader(...), chunk_size)` | ARCHITECTURE.md Anti-Pattern 3 — embedded newlines inside quoted fields make offset-based chunking corrupt rows; `csv.reader` is the sole row-boundary authority |

**Key insight:** every "don't hand-roll" item in this table already has a working, tested
implementation one directory tree away — this phase's job is disciplined vendoring/adaptation (Tier
A) and small, targeted reimplementation of specific algorithms (Tier B), not fresh design, with the
one confirmed exception of compression-kind detection (see Open Questions).

## Common Pitfalls

### Pitfall 1: Trusting D-30's own "verified" claim about `compression.py` without re-reading the file

**What goes wrong:** 03-CONTEXT.md's D-30 asserts magic-byte sniffing "[m]atches `compression.py`'s
own actual detection approach (verified: reads magic bytes, not extension)." A planner or executor
who takes this at face value and tries to vendor `detect_compression()` verbatim will get
extension-based dispatch instead, silently contradicting the user's actual locked decision (D-30
itself: magic-byte, pattern-agnostic).

**Why it happens:** A "verified" tag in a prior discussion doesn't survive re-reading the source; the
discussion-time verification appears to have checked a different signal (perhaps the docstring's
mention of "magic-byte sniffing" as a rejected alternative, misread as the chosen approach) than
what the code actually does.

**How to avoid:** The planner must write compression-kind detection as new code implementing D-30's
actual requirement (magic bytes `0x1f 0x8b` for gzip, `PK\x03\x04` for zip — both confirmed live
this session via `gzip`/`zipfile` module round-trip), not as a vendoring task, and should not cite
`compression.py` as prior art for the detection function itself (only for the streaming-open
mechanics around it, which are still valid to port).

**Warning signs:** A task description that says "vendor `detect_compression`" or "port compression
detection from `compression.py`" — that function does not do what D-30 needs.

### Pitfall 2: `encoding.py`'s corroboration algorithm silently breaking if `chardet` is dropped

**What goes wrong:** This project's own STACK.md explicitly advises against installing `chardet`
("Avoid: `chardet`... Use Instead: `charset-normalizer`"). If a planner/executor follows that
guidance literally while vendoring `detect/encoding.py` verbatim, the module's `chd = chardet.detect(sample)`
call and `_best_corroborating_match` corroboration logic will `ImportError` or need to be gutted,
which is a bigger change than a 1-2-line import swap — no longer "vendor unchanged."

**Why it happens:** STACK.md's guidance predates this session's direct read of `encoding.py`'s
actual algorithm; the general "prefer `charset-normalizer`" advice is sound for greenfield code but
doesn't apply once the decision is "vendor this specific file's algorithm."

**How to avoid:** Add `chardet` as a dependency alongside `clevercsv`/`charset-normalizer` for this
phase. Vendor `encoding.py` unchanged (just the import swap).

**Warning signs:** `ModuleNotFoundError: No module named 'chardet'` when importing the vendored
`detect/encoding.py`, or a "simplified" version of the file that silently drops the near-tie
corroboration logic (breaks the module's own documented `06_windows1250.csv`-style near-tie case).

### Pitfall 3: `byte_level_hard` corpus fixtures 23/24/25/27 cannot pass full structural validation against either real dataset config

**What goes wrong:** Fixtures `23_embedded_newline_in_quoted_field`, `24_embedded_delimiter_in_quoted_field`,
`25_doubled_quote_escaping`, and `27_oversized_field_value` (all read this session,
`tests/fixtures/corpus.yaml:328-391`) declare a 2-column header `["order_id", "note"]` (or
`["order_id", "big_field"]`) — neither matches `orders.json`'s real declared columns
(`order_id, customer_id, order_date, amount`, confirmed by reading
`configs/datasets/orders.json:4-32` this session). Under D-17/D-18/D-21, this header would trip
**both** "missing declared column" (`customer_id`/`order_date`/`amount` absent) and "extra
unexpected column" (`note`/`big_field` present) — a hard whole-file `INVALID_FILE` reject before
any row is ever parsed for its embedded-newline/delimiter/quote-escaping content. A test written to
assert "this fixture's row parses correctly and its `note` field equals `'line one\nline two'`" run
through the **full** `process_chunks(file, orders_config)` path will instead get a
`StructuralValidationError` and never reach the byte-level assertion the fixture exists to prove.

**Why it happens:** These fixtures were authored in Phase 2 (`02-04-PLAN.md Task 3`) to exercise
RFC-4180 byte-level parsing mechanics in isolation — genuinely orthogonal to schema-matching. Phase
2's own corpus.yaml header comment confirms `expect:` is free-text prose, "never a fixed
`error_code`, which is Phase 3/ENGINE-06's vocabulary to define" — i.e., these fixtures were never
designed with this phase's structural-reject rules in mind.

**How to avoid:** Test fixtures 23/24/25/27 directly against the parsing primitives
(`csv.reader`/`detect_dialect`) or against a fixture-scoped ad hoc `DatasetConfig` (columns =
`order_id, note`) rather than against `customers.json`/`orders.json`. Do **not** route them through
`process_chunks` with the real dataset configs and expect a `SUCCESS`/valid-row outcome — flag this
explicitly in the phase's own test plan (see Validation Architecture) so TEST-01's fixture coverage
doesn't silently assert the wrong thing.

**Warning signs:** A unit test for byte-level parsing that asserts `total=1, valid=1, invalid=0`
against fixture 23/24/25/27 run through the real `orders.json` config — should instead assert
`StructuralValidationError` is raised (which proves nothing about embedded-newline handling) or use
a fixture-local config.

### Pitfall 4: `precision`/`scale` violations need `Decimal.as_tuple()`, not string length

**What goes wrong:** A naive check like `len(raw.split(".")[-1]) > scale` breaks on integer-valued
decimals (`"100"` has no `.` at all), values with a leading `+`/`-` sign, or a value like `"100.10"`
where `Decimal` would report only 4 total significant digits but the naive string-length check
reports 6 characters after stripping the sign.

**How to avoid:** Use the verified `as_tuple()` formula in Code Examples Pattern 3 — parse first,
then derive precision/scale from the `Decimal`'s own internal representation, not from the original
string's characters.

**Warning signs:** A decimal validation test that passes on `"100.999"` (fixture 18) but fails or
gives a wrong `error_code` on an edge case like `"100"` (no fractional part) or `"-45.10"` (signed,
trailing zero).

## Code Examples

See Architecture Patterns above (Patterns 1–6) for the primary verified code examples — each is
tagged with its exact source and what was verified live this session, rather than duplicated here.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| `cx_Oracle` | `oracledb` (python-oracledb), thin mode | Already resolved project-wide (PROJECT.md, STACK.md) | Not this phase's concern (no Oracle I/O here), but do not import `oracledb` anywhere in this phase's code — that would violate ENGINE-09 |
| `chardet`-only encoding detection | `charset-normalizer` + `chardet` corroboration (this specific vendored algorithm) | N/A — reference-repo-specific design, not an industry-wide shift | This phase must install both, contradicting the project's own general-purpose STACK.md guidance for this one vendored file (see Pitfall 2) |

**Deprecated/outdated:** Nothing else in this phase's scope has moved since the project's own prior
research passes (`STACK.md`, `ARCHITECTURE.md`, `PITFALLS.md`, all dated 2026-08-28, one day before
this research). No stack drift found this session beyond the two findings in the Summary.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The 64 KiB bounded-sample size is appropriate for this project's corpus (customers/orders schemas, small fixture files) even though it was originally sized for a different, larger reference corpus | Claude's Discretion / Pattern 1 | Low — if detection accuracy suffers on a genuinely large real file, the sample size is a one-line constant to tune; no structural rework needed |
| A2 | `chardet==7.6.0` (current PyPI latest) is compatible with the vendored `encoding.py` algorithm, which was verified by its own authors against `chardet==7.5.1` | Package Legitimacy Audit | Low-Medium — `chardet`'s public `detect()` API has been stable for years; a minor-version bump is unlikely to change `cd["encoding"]`'s shape, but this should be smoke-tested against the corpus's encoding-tagged fixtures during implementation, not assumed to "just work" |
| A3 | No config field currently exists to drive an "oversized value" check implied by D-04's framing ("an oversized original value is its own distinct, worth-flagging error condition") — this research found no `max_length`/similar field on `ColumnSpec` and no fixture that cleanly exercises it against a real dataset schema | Anti-Patterns to Avoid / Pitfall 3 | Medium — if the planner assumes this check exists and is testable against corpus fixture 27, tasks will target an untestable requirement; flagged as an Open Question for discussion, not silently built |
| A4 | D-16's `DetectionMismatchError` and D-23's `StructuralValidationError` are two distinct Python exception classes sharing a common base (per D-16's "same exception type... but distinct error_code family"), rather than one class with a discriminator field | Architecture Patterns | Low — Claude's Discretion already covers "exact exception class hierarchy"; either shape satisfies D-16's literal wording, but the planner should pick one explicitly rather than leave it ambiguous across tasks |

**If this table is empty:** N/A — see rows above.

## Open Questions

1. **Does D-30's "magic-byte sniffing" decision still stand, given its own citation was wrong?**
   - What we know: The user's actual locked decision (D-30's operative text) is magic-byte
     sniffing, pattern-agnostic. Its supporting citation ("matches `compression.py`'s own actual
     detection approach") is verifiably false — the reference file uses extension-based dispatch
     (read directly this session, see Summary).
   - What's unclear: Whether the user would have made the same decision knowing the reference
     approach was actually simpler (extension-based). The rationale offered elsewhere in
     03-CONTEXT.md (D-29's "modern processing solutions are not extracting contents of compressed
     files") is about streaming vs. extract-to-temp-file, not about extension vs. magic-byte
     detection — so D-30's own independent rationale doesn't obviously hinge on the false citation.
   - Recommendation: Treat D-30 as still locked (magic-byte sniffing) since it's the user's own
     explicit choice, not solely derived from the citation — but the planner should surface this
     discrepancy in the plan's own notes so it's a visible, acknowledged decision, not a silent
     "we ported this" claim. Implementation is straightforward either way (magic-byte read of the
     first 2-4 bytes, both verified live this session: gzip `1f 8b`, zip `50 4b 03 04`).

2. **What does corpus fixture 27 (`oversized_field_value`) actually validate, and against what
   config?**
   - What we know: Its header (`order_id, big_field`) doesn't match either real dataset schema
     (Pitfall 3). Its 10,001-character field is well within Python's default `csv.field_size_limit`
     (131,072, confirmed live). D-04 frames "oversized original value" as a distinct error
     condition, but no `ColumnSpec` field carries a size threshold to check against.
   - What's unclear: Whether this fixture is purely a parser-robustness smoke test (proves the CSV
     reader doesn't choke on a large-but-legitimate field, independent of any dataset schema) or
     whether it's meant to exercise a not-yet-designed "value exceeds declared size" validation rule
     tied to the widened `_INVALID` VARCHAR2 column sizes (D-04).
   - Recommendation: Default to treating it as a parser-robustness test only (assert successful
     parse, not an error_code) unless the planner/user decides a real oversized-value check is
     in scope — if so, that needs a new `ColumnSpec` field (e.g. `max_length`) added first, which
     is itself a config-contract change outside this phase's stated boundary ("this phase builds the
     internal detect → parse → validate → normalize → split pipeline," not new config schema).

3. **Exact `error_code` enum member list and exception class hierarchy** — explicitly left to
   Claude's Discretion in 03-CONTEXT.md; this research surfaces every error_code named in D-12/D-16
   plus the corpus's own fixture-implied codes (`WRONG_COLUMN_COUNT`, `MISSING_REQUIRED_COLUMN`,
   `DUPLICATE_COLUMN_NAME`, `NO_HEADER_ROW`, `TYPE_MISMATCH`, `NULL_VIOLATION`,
   `INVALID_DATE_FORMAT`, `INVALID_TIMESTAMP_FORMAT`, `DECIMAL_PRECISION_EXCEEDED`, plus a
   `DETECT_*`-prefixed family for D-16's detection-mismatch case) — the planner should finalize this
   list as a concrete `enum.StrEnum`/`Literal` in `errors.py` or `validate.py` rather than leaving it
   implicit in scattered string literals.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| Python 3.12 | Entire engine, `itertools.batched` (ENGINE-07) | ✓ | 3.12.3 (confirmed live) | — |
| `clevercsv` | `detect/dialect.py` | ✗ (not yet installed) | latest on PyPI: 0.8.5 | none needed — install per Package Legitimacy Audit |
| `charset-normalizer` | `detect/encoding.py` | ✗ (not yet installed) | latest on PyPI: 3.5.1 | none needed — install |
| `chardet` | `detect/encoding.py` (corroboration, see Pitfall 2) | ✗ (not yet installed) | latest on PyPI: 7.6.0 | none needed — install |
| `gzip`, `zipfile`, `csv`, `itertools`, `decimal`, `datetime` (stdlib) | Compression, chunking, type conversion | ✓ | stdlib, bundled with 3.12.3 | — |
| Oracle Database Free / `oracledb` | Not required this phase (ENGINE-09: no Oracle I/O here) | N/A | — | — |
| Airflow | Not required this phase (ENGINE-09: must not import) | N/A | — | — |

**Missing dependencies with no fallback:** none — all three new PyPI packages are straightforward
installs with no platform-specific build requirements (pure-Python or has prebuilt wheels for
Python 3.12 per their existing use in this project's own reference repo).

**Missing dependencies with fallback:** none.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest 9.1.1 (confirmed pinned in root `pyproject.toml`, read this session) |
| Config file | root `pyproject.toml` `[tool.pytest.ini_options]` — `testpaths = ["tests"]`, `pythonpath = ["."]` (confirmed by reading this session) |
| Quick run command | `pytest tests/unit -x -q` |
| Full suite command | `pytest tests/unit -q` (no integration/e2e tests exist yet — those are Phase 4/6) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|--------------------|-------------|
| ENGINE-01 | Structural validation runs before type/nullability; header mismatches reject whole file | unit | `pytest tests/unit/test_structural_validation.py -x` | ❌ Wave 0 |
| ENGINE-02 | Type validation (int/decimal/date) per config schema | unit | `pytest tests/unit/test_type_validation.py -x` | ❌ Wave 0 |
| ENGINE-03 | Required-field nullability check | unit | `pytest tests/unit/test_type_validation.py::test_nullability -x` | ❌ Wave 0 |
| ENGINE-04 | Explicit type conversion (string → Python type) | unit | `pytest tests/unit/test_normalize.py -x` | ❌ Wave 0 |
| ENGINE-05 | Invalid row doesn't halt file; valid/invalid split + accurate counts | unit | `pytest tests/unit/test_engine_chunks.py -x` | ❌ Wave 0 |
| ENGINE-06 | Invalid row carries error_code/message/source_file/row_number + original values | unit | `pytest tests/unit/test_engine_chunks.py::test_invalid_row_shape -x` | ❌ Wave 0 |
| ENGINE-07 | Chunked, bounded-memory processing; detect-once-per-file | unit | `pytest tests/unit/test_engine_chunks.py::test_chunking -x` (peak-memory assertion mirrors `tests/unit/test_corpus_bounded_memory.py`'s existing RLIMIT_AS pattern, confirmed by reading that file's approach this session) | ❌ Wave 0 |
| ENGINE-09 | No Airflow import anywhere in `csv_processor` | unit | `pytest tests/unit/test_no_airflow_import.py -x` (grep-based or `ast`-based import scan) | ❌ Wave 0 |
| TEST-01 | Full coverage: config/CSV/type/date/valid-invalid/chunked | unit | `pytest tests/unit -q` | ❌ Wave 0 (config parsing already covered by existing `test_config_loader.py`/`test_config_models.py`) |

### Sampling Rate
- **Per task commit:** `pytest tests/unit -x -q` (fast subset touching the changed module)
- **Per wave merge:** `pytest tests/unit -q` (full unit suite)
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/unit/test_detect_dialect.py`, `test_detect_encoding.py`, `test_detect_header.py` — one
      per vendored Tier-A module, using corpus fixtures 1-8 (`dialect_encoding` category)
- [ ] `tests/unit/test_compression.py` — magic-byte sniffing + streaming open, using corpus fixtures
      28-30 (`large_compressed` category) plus the new `--compress`-generated fixtures (D-32)
- [ ] `tests/unit/test_structural_validation.py` — corpus fixtures 9-16 (`structural` category)
- [ ] `tests/unit/test_type_validation.py` — corpus fixtures 17-22 (`type_nullability` category)
- [ ] `tests/unit/test_normalize.py` — type conversion, including the `Decimal.as_tuple()`
      precision/scale check (Pattern 3) and the strict-`strptime` date check (Pattern 4)
- [ ] `tests/unit/test_engine_chunks.py` — `process_chunks()` generator: chunk boundaries, bounded
      memory (reuse `test_corpus_bounded_memory.py`'s RLIMIT_AS pattern), row_number counting across
      chunks (D-07)
- [ ] `tests/unit/test_no_airflow_import.py` — ENGINE-09 enforcement
- [ ] Note on fixtures 23/24/25/27 (`byte_level_hard`): write these against parsing primitives or a
      fixture-local ad hoc config, not against `customers.json`/`orders.json` (see Pitfall 3) —
      the Wave 0 test files above should account for this rather than discovering it mid-implementation
- [ ] Framework install: none needed — pytest already installed and configured

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|----------------|---------|-------------------|
| V2 Authentication | no | Out of scope — this phase has no auth surface |
| V3 Session Management | no | N/A |
| V4 Access Control | no | N/A |
| V5 Input Validation | yes | Exactly this phase's core job — structural/type/nullability checks over untrusted CSV content; `csv.field_size_limit` set explicitly (Pattern 6); `Decimal`/`strptime` used for exact parsing (never `eval`/format-guessing) |
| V6 Cryptography | no | N/A |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|-----------------------|
| Decompression bomb (a small `.gz`/`.zip` expanding to unbounded size) | Denial of Service | Vendored `compression.py`'s own `_DecompressionBombGuard` pattern (read this session) enforces a cumulative decompressed-byte ceiling via bounded-chunk reads — this project's port should keep an equivalent ceiling even though it's writing new detection code (Open Question 1), since the streaming-open mechanics around detection are still worth reusing |
| Unterminated-quote runaway field (a single malformed row consuming unbounded memory via `csv.reader`) | Denial of Service | Explicit `csv.field_size_limit` (Pattern 6) rather than the unstated interpreter default |
| Zip-bomb via nested/multi-entry archive | Denial of Service | D-33's "exactly one member" rule already closes this — a multi-entry or zero-entry archive is a structural reject, never silently iterated |
| CSV injection into a downstream spreadsheet tool (a cell value starting with `=`/`+`/`-`/`@`) | Tampering (of a downstream consumer, not this system) | Out of scope for this phase — this project's output is Oracle rows via `executemany()` bind variables (Phase 4), never a re-exported CSV/spreadsheet a human might open in Excel; noted here only so Phase 4/6 don't accidentally reintroduce a CSV-export path without considering it |

## Sources

### Primary (HIGH confidence — read directly this session)
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/dialect.py` — dialect detection algorithm, `clevercsv` usage, degenerate-result handling
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/encoding.py` — BOM check, charset-normalizer/chardet corroboration algorithm, 64 KiB sample convention
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/header.py` — header/preamble/footer scoring gates, duplicate-name rejection
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/filename.py` — confirmed no `dataplat.errors` import relevant to compression; confirmed unrelated to compression detection (filename-mask parsing only)
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/compression.py` — confirmed **extension-based** dispatch (contradicts D-30's citation, see Summary/Open Questions); streaming-open mechanics (gzip single-pass, zip buffered-archive-bytes) still valid to reuse
- `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/normalize/dates.py` — strict `strptime` rejection algorithm (Tier B, adapted in Pattern 4)
- `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/normalize/numeric.py` — `Decimal`-exact parsing discipline (never `float`); confirmed no direct precision/scale precedent (this project's own check derived fresh, Pattern 3)
- `/home/user/projects/lightweight-airflow-etl/packages/csv-processor/src/csv_processor/config/models.py` — actual `ColumnSpec`/`DatasetConfig` shape (no `max_length` field, confirmed — Open Question 2)
- `/home/user/projects/lightweight-airflow-etl/packages/csv-processor/src/csv_processor/config/loader.py`, `config/errors.py` — existing local-exception convention (`Exception` subclass, `(message, *, context: dict)`) this phase's own `errors.py` should follow
- `/home/user/projects/lightweight-airflow-etl/configs/datasets/customers.json`, `orders.json` — real per-dataset schema (no timezone/spreadsheet-epoch/max_length fields)
- `/home/user/projects/lightweight-airflow-etl/tests/fixtures/corpus.yaml` — all 30 fixtures' `expect:` prose, cross-checked against dataset configs (found the fixture 23/24/25/27 schema mismatch, Pitfall 3)
- `/home/user/projects/lightweight-airflow-etl/docker/oracle/init/02_customers.sql`, `03_orders.sql` — actual current DDL D-01 must migrate
- `/home/user/projects/lightweight-airflow-etl/pyproject.toml`, `packages/csv-processor/pyproject.toml` — confirmed Python 3.12 pin, confirmed `clevercsv`/`charset-normalizer`/`chardet` not yet declared as deps
- Live verification this session: `python3 --version` (3.12.3), `itertools.batched` signature, `csv.field_size_limit()` default (131072), gzip/zip magic bytes (`1f8b`/`504b0304`), `Decimal.as_tuple()` precision/scale formula against real corpus/config values, `pip index versions` for `clevercsv`/`charset-normalizer`/`chardet`

### Secondary (MEDIUM confidence)
- `.planning/research/ARCHITECTURE.md`, `.planning/research/FEATURES.md`, `.planning/research/PITFALLS.md`, `.planning/research/STACK.md` — prior project-level research (2026-08-28), cross-checked against this session's direct file reads; STACK.md's `chardet` exclusion found to conflict with the vendored algorithm's actual needs (Pitfall 2)

### Tertiary (LOW confidence)
- `gsd-tools query package-legitimacy check` verdicts for `clevercsv`/`charset-normalizer`/`chardet` (all `SUS` on metadata-completeness signals only, not on any malicious-content signal) — treated as a process gate (`checkpoint:human-verify`), not as evidence against using these already-vetted packages

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — every package version verified live against PyPI this session; the one gap (`chardet` missing from STACK.md) was discovered and resolved by reading the actual algorithm, not assumed
- Architecture: HIGH — module structure and detect-once/chunk-by-record patterns come directly from ARCHITECTURE.md (already-settled project research) plus this session's direct reads of the actual reference-repo algorithms
- Pitfalls: HIGH — two of the four pitfalls in this document were discovered by directly falsifying a prior "verified" claim (D-30) and a prior stack recommendation (chardet exclusion) against the actual source this session, not carried over unchecked

**Research date:** 2026-08-29
**Valid until:** 30 days (stable domain — stdlib CSV/decimal/datetime/gzip/zipfile APIs and the vendored reference-repo files are not expected to change; re-verify package versions if implementation is delayed past this window)

---
*Phase: 3-CSV Processing Engine*
*Researched: 2026-08-29*
