# Phase 5: Airflow DAG Wiring & Deferrable File-Wait - Pattern Map

**Mapped:** 2026-08-29
**Files analyzed:** 7 (new) + 1 (modified)
**Analogs found:** 5 / 8 (no true Airflow-DAG analog exists in this repo yet — this is the first
DAG file. Analogs are drawn from the closest same-*role*-shape files in `csv_processor` and
`scripts/`.)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|-----------------|---------------|
| `airflow/dags/csv_ingest.py` | orchestrator/DAG (no true analog role in repo) | request-response (triggered) + event-driven (deferred sensor) | `packages/csv-processor/src/csv_processor/engine.py` (sequence-orchestration shape) + Code Examples in `05-RESEARCH.md` | partial (cross-repo, no in-repo DAG exists) |
| `airflow/dags/_common/__init__.py` | package init | — | `packages/csv-processor/src/csv_processor/config/__init__.py` | role-match |
| `airflow/dags/_common/paths.py` | utility (pure function, no Airflow import) | transform | `packages/csv-processor/src/csv_processor/config/loader.py` | role-match (error-wrapping/docstring convention) |
| `airflow/dags/_common/constants.py` (optional, e.g. `_DEFAULTS_PATH`) | config | — | `packages/csv-processor/src/csv_processor/config/loader.py` module constants | role-match |
| `tests/unit/test_dag_paths.py` (or similar, testing `_common/paths.py`) | test | transform | `tests/unit/test_config_loader.py` | exact (pure-function unit test, same style) |
| `tests/unit/test_no_airflow_import.py` (MODIFY — extend scope note only, not required to touch) | test | — | itself (existing) | exact — confirms `csv_processor` stays Airflow-free; DAG code is deliberately exempt |
| `docker-compose.yml` (MODIFY — add `ORACLE_DSN`/`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD` to `airflow-common-env`) | config | — | itself, `AIRFLOW_CONN_ORACLE_DEFAULT` line already in same block | exact |
| `scripts/trigger_dag.sh` or docs snippet (optional, Claude's discretion — REST trigger example) | utility (shell) | request-response | `scripts/verify_environment.py` (auth-flow section, lines ~37-146) | exact |

## Pattern Assignments

### `airflow/dags/csv_ingest.py` (DAG definition, request-response + deferred event-driven)

**No in-repo DAG analog exists** — this is the first file under `airflow/dags/`. Use
`05-RESEARCH.md`'s "Full DAG skeleton" (Code Examples section, verbatim below) as the primary
pattern source, cross-checked against this repo's own integration points (`engine.py`,
`config/loader.py`, `models.py`) for exact signatures.

**Imports pattern** (from RESEARCH.md Code Examples, verified import paths):
```python
from __future__ import annotations

from pathlib import Path

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Param, dag, get_current_context, task

from csv_processor.config.loader import load_config
from csv_processor.config.models import DatasetConfig
from csv_processor.engine import process
from csv_processor.models import ProcessingResult
```
Note: `airflow.sdk` (not `airflow.models`/`airflow.decorators`) is Airflow 3.3.1's stable import
surface — see RESEARCH.md "State of the Art" table. `FileSensor` stays under
`airflow.providers.standard.sensors.filesystem` (not re-exported through `airflow.sdk`).

**`load_config` integration — exact real signature** (`packages/csv-processor/src/csv_processor/config/loader.py:39`):
```python
def load_config(path: Path, *, defaults_path: Path) -> DatasetConfig:
```
`defaults_path` is keyword-only and required — RESEARCH.md Pitfall 4 flags `ARCHITECTURE.md`'s
one-argument sketch as wrong; always call
`load_config(Path(config_path), defaults_path=Path("configs/defaults.json"))`.

**`ConfigurationError` pattern to catch in `load_config_task`** (`packages/csv-processor/src/csv_processor/config/errors.py`):
```python
class ConfigurationError(Exception):
    def __init__(self, message: str, *, context: dict) -> None:
        super().__init__(message)
        self.context = context
```
Catch this specific type in `load_config_task` and translate to a `CONFIGURATION_ERROR`-shaped
result dict per D-03/D-08 — never let it propagate as an unhandled Airflow task failure for a
"known bad config" case (though note this is a genuinely new decision point since no `Status`
model instance exists yet at this point in the pipeline — the task can construct a minimal
dict-shaped early exit, exact shape is planner's call per D-08).

**`process()` entrypoint — never raises, exact signature** (`packages/csv-processor/src/csv_processor/engine.py`):
```python
def process(file_path: Path, config: DatasetConfig) -> ProcessingResult:
    """... never raises; every exception this function's own sequence can
    produce is caught and translated into a status instead."""
```
`process_csv_task` must call this exactly once, with no `try/except` around it that re-raises on
any `Status` member (Pitfall 5 / D-03) — always `return result.model_dump(mode="json")`.

**`ProcessingResult` / `Status` exact shape** (`packages/csv-processor/src/csv_processor/models.py`):
```python
class Status(str, Enum):
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_INVALID_ROWS = "SUCCESS_WITH_INVALID_ROWS"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    INVALID_FILE = "INVALID_FILE"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    PROCESSING_ERROR = "PROCESSING_ERROR"

class ProcessingResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: Status
    dataset: str
    file_name: str
    total_rows: int
    valid_rows: int
    invalid_rows: int
    duration_seconds: float
    checksum: str | None = None
```
`report_result_task`'s log line must reference exactly these field names (`dataset`, `file_name`,
`status`, `total_rows`, `valid_rows`, `invalid_rows`, `duration_seconds`) — see RESEARCH.md Code
Examples for the exact `logger.info(...)` call shape to copy.

**Core DAG structure / FileSensor + re-glob pattern** — copy verbatim from RESEARCH.md's "Full DAG
skeleton" (lines 601-690 of `05-RESEARCH.md`), which is this phase's primary source-of-truth code
example since no in-repo DAG exists to copy from instead. Key excerpt (Pitfall 1's fix):
```python
wait_for_file = FileSensor(
    task_id="wait_for_file",
    fs_conn_id="fs_default",
    filepath=(
        "{{ params.dataset }}/"
        "{{ ti.xcom_pull(task_ids='load_config_task')['file_pattern'] }}"
    ),
    deferrable=True,
    poke_interval=10,
)

@task
def process_csv_task(config_dict: dict) -> dict:
    ctx = get_current_context()
    dataset = ctx["params"]["dataset"]
    config = DatasetConfig.model_validate(config_dict)
    candidates = sorted((_DATA_ROOT / dataset).glob(config.file_pattern))
    file_path = candidates[0]
    result: ProcessingResult = process(file_path, config)
    return result.model_dump(mode="json")
```
**Do not** read `wait_for_file.output` as a file path (Pitfall 1) — `FileSensor.poke()` returns a
bare `bool`.

**Error handling pattern:** None of `csv_ingest.py`'s `@task` bodies should wrap `process()` in a
try/except that re-raises (D-03). The only place a real `try/except` belongs is around
`load_config(...)` in `load_config_task`, catching `ConfigurationError` specifically (mirrors
`csv_processor.config.loader`'s own "wrap every failure behind one exception type" discipline,
applied one layer up).

---

### `airflow/dags/_common/paths.py` (utility, transform, no Airflow import)

**Analog:** `packages/csv-processor/src/csv_processor/config/loader.py` (docstring/error-handling
convention — pure function, explicit `Args`/`Returns`/`Raises` docstring style used throughout this
codebase).

**Convention to copy** (module-level docstring + function docstring shape, from `loader.py:1-15,39-58`):
```python
"""<one-line purpose>.

<why this exists / what it's ported from or modeled after, if anything>.
"""

from __future__ import annotations

from pathlib import Path


def resolve_matched_file(base_dir: Path, file_pattern: str) -> Path:
    """<Args/Returns/Raises docstring, same style as loader.py>."""
    candidates = sorted(base_dir.glob(file_pattern))
    ...
```
Must contain **zero** `airflow` imports — `tests/unit/test_no_airflow_import.py` only scans
`packages/csv-processor/src/csv_processor`, so `_common/paths.py` is not itself enforced by that
test, but RESEARCH.md's "Recommended Project Structure" explicitly calls for it to stay
"plain, unit-testable, no Airflow import" so it can be tested without spinning up Airflow at all —
follow the same discipline as a matter of consistency, not because the existing test enforces it.

---

### `docker-compose.yml` (MODIFY — config, D-Pattern 4 fix)

**Analog:** the file's own existing `AIRFLOW_CONN_ORACLE_DEFAULT` line inside `airflow-common-env`
(same block, same interpolation convention).

**Exact current block to extend** (docker-compose.yml, inside `x-airflow-common.environment`):
```yaml
    AIRFLOW_CONN_ORACLE_DEFAULT: "oracle://${ORACLE_APP_USER:-admin}:${ORACLE_APP_USER_PASSWORD:-admin}@oracle:1521/?service_name=FREEPDB1&encoding=UTF-8&threaded=False&events=False"
```

**Addition required** (Pattern 4 / Pitfall 2 in RESEARCH.md — verified gap, zero existing matches
for these three names as container env vars):
```yaml
    ORACLE_DSN: "oracle:1521/FREEPDB1"
    ORACLE_APP_USER: "${ORACLE_APP_USER:-admin}"
    ORACLE_APP_USER_PASSWORD: "${ORACLE_APP_USER_PASSWORD:-admin}"
```
Consumed by `packages/csv-processor/src/csv_processor/load.py`:
```python
def oracle_dsn() -> str:
    return os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")

def oracle_user() -> str:
    return os.environ.get("ORACLE_APP_USER", "admin")

def oracle_password() -> str:
    return os.environ.get("ORACLE_APP_USER_PASSWORD", "admin")
```
Without this fix, every `process_csv_task` run inside the Airflow containers falls back to
`localhost:1521/FREEPDB1`, which does not resolve to the `oracle` compose service from inside
`airflow-scheduler`/`airflow-apiserver` — every run returns `Status.DATABASE_ERROR`.

---

### `tests/unit/test_dag_paths.py` (test, transform)

**Analog:** `tests/unit/test_config_loader.py` — closest existing unit test for a pure,
Airflow-free function (`load_config`) with explicit success/failure-path cases. Read this file at
implementation time to copy its `pytest` fixture/parametrize style (not excerpted here since exact
line ranges depend on the planner's chosen `_common/paths.py` function signatures).

**Analog for "no Airflow import" discipline test style:** `tests/unit/test_no_airflow_import.py`
(full file read above) — AST-based scanner pattern, self-tested against a synthetic positive
before trusting it against the real tree. If the planner wants a parallel guarantee for
`airflow/dags/_common/`, this is the file to model a second, `_common`-scoped variant after
(optional — not required by any locked decision, but consistent with the existing convention of
proving a scanner has real detection power before trusting it).

---

## Shared Patterns

### Pydantic model round-trip through XCom
**Source:** `packages/csv-processor/src/csv_processor/config/models.py` (`DatasetConfig`, same
`ConfigDict(extra="forbid", frozen=True)` convention as `models.py`'s `ProcessingResult`) and
`packages/csv-processor/src/csv_processor/models.py` (`ProcessingResult`).
**Apply to:** every `@task` boundary in `csv_ingest.py` that crosses an XCom hop —
`load_config_task` returns `config.model_dump(mode="json")`; `process_csv_task` returns
`result.model_dump(mode="json")`; the receiving task always does
`DatasetConfig.model_validate(config_dict)` / reads `result_dict[...]` directly (never
re-instantiates `ProcessingResult` unless a field needs typed access — plain dict passthrough is
fine and is what `load_results_task` does per D-02).
```python
# packages/csv-processor/src/csv_processor/models.py
model_config = ConfigDict(extra="forbid", frozen=True)
```

### Environment-variable-first config (Oracle credentials)
**Source:** `packages/csv-processor/src/csv_processor/load.py` (`oracle_dsn()`, `oracle_user()`,
`oracle_password()` — quoted above).
**Apply to:** `docker-compose.yml`'s `ORACLE_DSN`/`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD`
addition — the DAG code itself never reads these directly (they're consumed inside
`csv_processor.load`, called transitively via `engine.process()`), but the fix must match these
exact three names.

### Exception-to-status translation discipline
**Source:** `packages/csv-processor/src/csv_processor/config/loader.py` (wraps
`pydantic.ValidationError`/`OSError`/`json.JSONDecodeError` behind `ConfigurationError`) and
`packages/csv-processor/src/csv_processor/engine.py` (wraps everything behind a closed `Status`
enum, "never raises").
**Apply to:** `load_config_task` (catch `ConfigurationError` specifically, one layer up from where
`loader.py` already does its own wrapping) and the overall rule that `process_csv_task` never
re-raises for a modeled `Status` (D-03) — the same "collect and translate, don't let the caller
guess exception types" philosophy applied consistently top-to-bottom through this stack.

### REST API trigger auth flow (documentation/manual-testing only, not DAG code)
**Source:** `scripts/verify_environment.py` (lines ~37-39, ~134-146 — `AIRFLOW_AUTH_TOKEN_URL`,
`AIRFLOW_USER`, `AIRFLOW_PASSWORD`, the `POST /auth/token` call shape).
**Apply to:** any `scripts/trigger_dag.sh` or README snippet demonstrating DAG-02's HTTP trigger —
reuse the exact `/auth/token` → `Authorization: Bearer` flow already proven in this repo, do not
invent a different auth call shape.
```python
AIRFLOW_AUTH_TOKEN_URL = "http://localhost:8080/auth/token"
AIRFLOW_USER = "admin"
AIRFLOW_PASSWORD = "admin"
```

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `airflow/dags/csv_ingest.py` (DAG structure itself — `@dag`/`@task`/`FileSensor` wiring) | orchestrator | request-response + event-driven | No Airflow DAG file exists anywhere in this repo yet (`airflow/dags/.gitkeep` is the only prior content) — RESEARCH.md's Code Examples section is the closest available source, sourced from Context7-verified Airflow 3.x docs plus this repo's own verified integration signatures, not from an in-repo analog. |
| `airflow/dags/_common/xcom.py` (optional thin wrapper, if planner adds it) | utility | transform | Optional per RESEARCH.md's "Recommended Project Structure" — no locked requirement forces its existence; if added, follows the same `config/loader.py` docstring convention noted above, no separate analog needed. |

## Metadata

**Analog search scope:** `packages/csv-processor/src/csv_processor/` (all modules), `scripts/`,
`tests/unit/`, `tests/integration/`, `docker-compose.yml`, `airflow/dags/` (confirmed empty except
`.gitkeep`).
**Files scanned:** ~25 source files, ~20 test files, `docker-compose.yml`, `scripts/verify_environment.py`.
**Pattern extraction date:** 2026-08-29
