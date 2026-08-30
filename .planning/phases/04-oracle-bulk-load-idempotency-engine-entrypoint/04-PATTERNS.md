# Phase 4: Oracle Bulk Load, Idempotency & Engine Entrypoint - Pattern Map

**Mapped:** 2026-08-29
**Files analyzed:** 6 (3 new source modules, 3 new test files/dirs)
**Analogs found:** 6 / 6 (all role-match or exact; no reference-repo analog needed — this
phase's code has no dataplat/reference-repo Tier-A/B counterpart, per CLAUDE.md's reuse rules
this phase's DB integration is genuinely new code)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|--------------------|------|-----------|-----------------|---------------|
| `packages/csv-processor/src/csv_processor/models.py` | model | request-response (data contract) | `packages/csv-processor/src/csv_processor/config/models.py` | role-match (Pydantic model conventions) |
| `packages/csv-processor/src/csv_processor/load.py` | service (DB loader) | CRUD (bulk insert + idempotency check) | `scripts/verify_environment.py` (connection pattern) + `packages/csv-processor/src/csv_processor/config/loader.py` (error-wrapping convention) | role-match |
| `packages/csv-processor/src/csv_processor/engine.py` (extend, add `process()`) | service (orchestrator/entrypoint) | request-response | `packages/csv-processor/src/csv_processor/engine.py`'s own existing `process_chunks()` (same file) | exact (same file, same module conventions) |
| `tests/integration/conftest.py` (new) | test (fixture) | request-response | `scripts/verify_environment.py`'s `main()` connection setup | role-match |
| `tests/integration/test_load_oracle.py` (new) | test | CRUD | `tests/unit/test_engine_chunks.py` (structure/style) + `tests/test_verify_environment.py` (Oracle-adjacent test style) | role-match |
| `tests/unit/test_engine_process.py` (new) | test | request-response | `tests/unit/test_config_loader.py` (exception-mapping test style) | role-match |

## Pattern Assignments

### `packages/csv-processor/src/csv_processor/models.py` (model)

**Analog:** `packages/csv-processor/src/csv_processor/config/models.py`

**Module docstring / frozen-model convention** (lines 1-24, config/models.py):
```python
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

class ColumnSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str
    type: _COLUMN_TYPES
    ...
```
Copy this exact `model_config = ConfigDict(extra="forbid", frozen=True)` convention for
`ProcessingResult` — every model in this codebase is frozen and rejects unknown keys.
`Status` should be a `str, Enum` per RESEARCH.md's `models.py` code example (already
fully drafted there — copy verbatim, it cites `REQUIREMENTS.md`'s 7-member enum, not
invented).

**Error/context shape convention** (mirrors `config/errors.py` lines 11-28):
```python
class ConfigurationError(Exception):
    def __init__(self, message: str, *, context: dict) -> None:
        super().__init__(message)
        self.context = context
```
`ProcessingResult` itself doesn't need this (it's a success-path return value, not an
exception), but any new exception this phase introduces (none currently planned — existing
`StructuralValidationError`/`ConfigurationError` are reused per D-08) must follow this same
`context: dict` keyword-only constructor shape if one is needed.

---

### `packages/csv-processor/src/csv_processor/load.py` (service, CRUD)

**Analog 1 — connection pattern:** `scripts/verify_environment.py`

**Imports + lazy-import convention** (lines 16-31):
```python
from __future__ import annotations

import http.client
...
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred to a type-checking-only import (WR-03): the module-level `import
    # oracledb` made loading this file for testing verify_airflow_auth() (which
    # has no Oracle dependency) require the oracledb driver to be installed.
    import oracledb

ORACLE_DSN = "localhost:1521/FREEPDB1"
ORACLE_USER = "admin"
ORACLE_PASSWORD = "admin"
```
**IMPORTANT DEVIATION (per RESEARCH.md Pitfall 6 / Assumption A2):** do NOT copy the
hardcoded `ORACLE_USER = "admin"` literal into `load.py` — read via
`os.environ.get("ORACLE_USER", "admin")`-style env-var-first fallbacks instead. The
*connection call shape* below is still the right analog to copy verbatim:

**Connection pattern** (lines 178-181, `main()`):
```python
def main() -> int:
    import oracledb  # WR-03: lazy import -- only main() needs the Oracle driver.

    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN)
    try:
        cursor = conn.cursor()
        ...
    finally:
        conn.close()
```
`load.py` extends this with explicit `commit()`/`rollback()` (RESEARCH.md Pattern 1) since
`verify_environment.py` is read-only and never writes.

**Reusable verification helpers** (lines 52-115) — `verify_tables()`, `verify_columns()`,
`verify_widened_invalid_columns()` are explicitly designed to be imported by this phase's
integration tests (per the module's own docstring, line 13: "Phase 4's Oracle integration
tests reuse them"). Import these directly into `tests/integration/conftest.py` /
`test_load_oracle.py` rather than re-deriving column-existence assertions.

**Analog 2 — error-wrapping / single-exception-type convention:**
`packages/csv-processor/src/csv_processor/config/loader.py`

**Try/except → single exception type re-raise pattern** (lines 61-81):
```python
try:
    defaults_text = defaults_path.read_text(encoding="utf-8") if defaults_path.exists() else ""
    defaults = _parse_json_text(defaults_text)
    dataset = _parse_json_text(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    raise ConfigurationError(
        f"could not read dataset config at {path}: {exc}",
        context={"path": str(path), "errors": [...]},
    ) from exc
```
`load.py` should follow this same "catch broad driver exceptions, translate to one project
type, always attach `context`/`from exc`" shape when translating `oracledb.Error` into
whatever internal signal `engine.process()` needs to map to `DATABASE_ERROR` (RESEARCH.md's
own Pattern 4 / exception-to-status table already specifies the exact `oracledb.IntegrityError`
+ `full_code == "ORA-00001"` check to layer on top of this convention).

**Checksum helper — reimplement, do not import (Pitfall 5):**
Reference algorithm at `tools/corpus/digests.py:37-50`:
```python
def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()
```
Copy this exact 5-line chunked-read shape into a local `csv_processor.load._sha256_file()`
(or a new small `csv_processor.checksum` module) — never `from tools.corpus.digests import
sha256_file` (breaks the Airflow-container install boundary, per Pitfall 5).

**Bulk-insert / setinputsizes / idempotency-race patterns:** RESEARCH.md's own Pattern 1-4
code examples (lines 236-340 of 04-RESEARCH.md) are the primary source here — they are
already grounded in the real DDL (`01_ingestion_metadata.sql`, `02_customers.sql`,
`03_orders.sql`, `04_widen_invalid_columns.sql`, all read this session) and in
`python-oracledb`'s official docs. Use them directly; no closer in-repo analog exists since
this is the first Oracle-writing code in the project.

**Column/table shape ground truth** (build `VALID_INSERT_SQL`/`INVALID_INSERT_SQL` from
these, not by hand-typing): `docker/oracle/init/02_customers.sql` (customers_valid:
customer_id, name, country, birth_date, event_ts, signup_country, ingested_at;
customers_invalid: same + error_code, error_message, source_file, row_number, +
raw_line from `04_widen_invalid_columns.sql`); `docker/oracle/init/03_orders.sql` (orders_valid:
order_id, customer_id, order_date, amount, ingested_at; orders_invalid: same +
error_code/error_message/source_file/row_number/raw_line); `docker/oracle/init/01_ingestion_metadata.sql`
(ingestion_metadata: dataset, file_name, checksum, processed_at, total_rows, valid_rows,
invalid_rows, status — `UNIQUE(dataset, checksum)` at line 21 is the idempotency backstop).

---

### `packages/csv-processor/src/csv_processor/engine.py` — extend with `process()`

**Analog:** the file's own existing `process_chunks()` (lines 1-122, already read in full).

**Module docstring convention to extend** (lines 1-12):
```python
"""``process_chunks()`` -- Phase 3's public generator surface (D-11).

The full ``process()``/``ProcessingResult`` wrapper (status codes, Oracle
loading) is explicitly Phase 4's job (ENGINE-08) -- this module only builds
the chunked valid/invalid row split...
"""
```
Update this docstring when adding `process()` — it already documents that `process()`
belongs in this exact file.

**Chunk-consumption pattern to follow directly** (lines 27-59, signature + detect-once
setup):
```python
def process_chunks(
    file_path: Path, config: DatasetConfig
) -> Iterator[tuple[list[dict[str, object]], list[dict[str, object]]]]:
    ...
    source_file = file_path.name  # D-08 -- basename only
    text_stream, paired_rows, header = source.prepare_source(file_path, config)
    try:
        ...
        for batch in itertools.batched(paired_rows, config.processing.chunk_size):
            ...
            yield valid_rows, invalid_rows
    finally:
        text_stream.close()
```
`process()` wraps a `for (valid_rows, invalid_rows) in process_chunks(file_path, config):`
loop feeding `load.py`'s `executemany()` calls per RESEARCH.md's architecture diagram
(lines 160-205 of 04-RESEARCH.md) — reuses `process_chunks()` unmodified, no adapter.

**Exception-hierarchy reuse** (from `errors.py`, lines 83-91): `StructuralValidationError`
propagates from `process_chunks()`/`source.prepare_source()` on a whole-file reject —
`process()` is the sole catcher, translating it to `Status.INVALID_FILE` (D-08/D-23,
already the file's own documented contract).

**Config-load exception reuse:** `csv_processor.config.errors.ConfigurationError` (already
shown above) is caught by `process()` and translated to `Status.CONFIGURATION_ERROR` —
`csv_processor.config.loader.load_config()` is the call site, reused unmodified.

---

### `tests/integration/conftest.py` + `tests/integration/test_load_oracle.py` (test, CRUD)

**Analog 1 — Oracle connection/credential pattern:** `scripts/verify_environment.py` lines
33-35 (`ORACLE_DSN`/`ORACLE_USER`/`ORACLE_PASSWORD`) and lines 181 (`oracledb.connect(...)`)
— reuse directly for a pytest fixture; also import `verify_tables`/`verify_columns` (lines
52-82) for post-insert assertions rather than writing new `ALL_TAB_COLUMNS` queries.

**Analog 2 — test file structure/style:** `tests/unit/test_engine_chunks.py` (chunk-boundary
assertions) and `tests/test_verify_environment.py` (lines 1-30, module-docstring + fixture
style, mocking conventions via `unittest.mock`). Follow the existing test suite's use of
`unittest.TestCase`-free plain pytest functions if `test_engine_chunks.py`/
`test_config_loader.py` already use plain pytest style (check during implementation) —
match whichever convention the majority of `tests/unit/*.py` use for consistency.

**Per-test isolation pattern (new, no direct in-repo analog):** truncate/delete
`<DATASET>_VALID`/`_INVALID`/`ingestion_metadata` rows between tests — these tables are
`INTERVAL`-partitioned but plain `DELETE` still works (RESEARCH.md Wave-0-gaps note, line
597-599 of 04-RESEARCH.md). No existing fixture in the repo does this yet; write it fresh
following pytest's standard fixture teardown convention.

---

### `tests/unit/test_engine_process.py` (test, request-response)

**Analog:** `tests/unit/test_config_loader.py` (exact file not read this session, but is the
closest same-role/same-flow analog per file listing — config-loading exception-mapping
tests). Follow its exception-to-outcome assertion style for the four non-DB status paths
(`CONFIGURATION_ERROR`, `FILE_NOT_FOUND`, `INVALID_FILE`) — stub/mock `load.py`'s Oracle
calls entirely per RESEARCH.md's Wave 0 gap note (line 601-603): no real Oracle needed for
these three paths.

## Shared Patterns

### Frozen, extra-forbid Pydantic models
**Source:** `packages/csv-processor/src/csv_processor/config/models.py` (every class, e.g.
lines 26-36, 67-73, 100-105, 121-126, 129-137)
**Apply to:** `models.py`'s `ProcessingResult` — always `model_config = ConfigDict(extra="forbid", frozen=True)` unless there's a documented reason to deviate (there isn't one here).

### Single-exception-type translation with `context: dict`
**Source:** `packages/csv-processor/src/csv_processor/config/errors.py:11-28` and
`packages/csv-processor/src/csv_processor/errors.py:35-49`
```python
class ConfigurationError(Exception):
    def __init__(self, message: str, *, context: dict) -> None:
        super().__init__(message)
        self.context = context
```
**Apply to:** any new exception type this phase introduces (RESEARCH.md's design currently
implies none are strictly new — `oracledb.Error`/`IntegrityError` are caught and mapped
directly to `Status` values inside `process()`, not wrapped in a new project exception type —
but if the planner decides a `DatabaseLoadError` wrapper is cleaner than inline mapping,
this is the shape to use).

### Oracle connection setup (env-var-first, not hardcoded)
**Source:** `scripts/verify_environment.py:33-35, 178-181` (connection shape) — **but see
Pitfall 6**: do not copy the hardcoded literals verbatim into `load.py`. Use
`os.environ.get("ORACLE_USER", "admin")` / `os.environ.get("ORACLE_PASSWORD", "admin")` /
`os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")` instead, per INFRA-03 and
RESEARCH.md's Assumption A2. Test fixtures (`tests/integration/conftest.py`) may continue to
mirror `verify_environment.py`'s hardcoded dev-credential literals since they're dev-only,
non-shipped code — but `load.py` (used by the eventual Airflow DAG in Phase 5) should not.

### Chunked file read (no full-file buffering)
**Source:** `tools/corpus/digests.py:37-50` (`sha256_file`) — reimplement locally in
`csv_processor.load`, never import across the `tools/` package boundary (Pitfall 5).

## No Analog Found

None — every file in this phase has at least a role-match analog in the existing codebase or
a fully-specified code example in `04-RESEARCH.md` (which itself cites official
`python-oracledb` docs plus verbatim-read DDL, not invented patterns).

## Metadata

**Analog search scope:** `packages/csv-processor/src/csv_processor/` (all modules),
`scripts/verify_environment.py`, `tools/corpus/digests.py`, `docker/oracle/init/*.sql`,
`tests/` (unit + `test_verify_environment.py`)
**Files scanned:** 14 (engine.py, errors.py, config/models.py, config/loader.py,
config/errors.py, verify_environment.py, digests.py, 4 SQL DDL files, plus test-file listing)
**Pattern extraction date:** 2026-08-29
