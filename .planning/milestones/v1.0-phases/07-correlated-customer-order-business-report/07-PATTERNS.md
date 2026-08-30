# Phase 7: Correlated Customer-Order Business Report - Pattern Map

**Mapped:** 2026-08-30
**Files analyzed:** 10
**Analogs found:** 10 / 10

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `generator/generate_csv.py` (add `generate_correlated_datasets()`, `zipf_weighted_sample()`, `structured_id()`, refactor `generate_rows()`) | utility / generator | transform (in-process, no I/O) | itself — `generate_rows()` (lines 153-184), `format_decimal()` (60-68), `output_path()` (240-243) | exact (existing file, extend in place) |
| `generator/generate_csv.py` `main()`/CLI (`--correlated` mode, D-22) | config / CLI | batch | itself — `build_parser()`/`main()` (214-268) | exact |
| `docker/oracle/init/05_correlation_constraints.sql` (new, D-13/D-14/D-15) | migration | batch (DDL) | `docker/oracle/init/04_widen_invalid_columns.sql` (whole file) and `02_customers.sql`/`03_orders.sql` for table/column names | exact — same numbered-init-script convention, same `ALTER SESSION`/`ALTER TABLE` idioms |
| `airflow/dags/report_ready.py` (new, D-26) | route / DAG | event-driven (poll + build) | `airflow/dags/csv_ingest.py` (whole file, esp. lines 29-141: `@dag`, `@task`, deferrable sensor wiring, `report_result_task` logging shape) | role-match — same `@dag`/`@task` TaskFlow conventions, different sensor (custom vs. `FileSensor`) |
| `airflow/dags/_common/oracle_partition_trigger.py` (new, D-28/D-29) | middleware / trigger | event-driven (async poll) | No existing analog in this codebase (no custom `BaseTrigger` exists yet) — use RESEARCH.md Pattern 3 code example (Context7-verified) as primary source; `packages/csv-processor/src/csv_processor/load.py`'s `oracle_dsn()`/`oracle_user()`/`oracle_password()` (lines 65-88) for credential-reading convention | partial — credential pattern reused from `load.py`, trigger shape has no in-repo precedent |
| `tests/unit/test_generate_csv.py` (extend: correlation/Zipf/determinism assertions; modify: standalone-orders tests at lines 195-196, 220, 229, 242, 270) | test | transform | itself — `test_generate_rows_is_deterministic_for_same_seed*` pattern (lines ~186-242) | exact |
| `tests/e2e/test_correlated_report_e2e.py` (new, D-09/D-10) | test | request-response (live HTTP+DB) | `tests/e2e/test_csv_ingest_e2e.py` (whole file) | exact — explicit template per D-10/RESEARCH.md |
| `scripts/regenerate_readme_summary.py` (modify `_run_ingestion()`/loop, D-21/D-23) | service / script | file-I/O + request-response | itself — `_run_ingestion()` (151-208), `_load_sibling_module()` (112-128) | exact (existing file, adapt in place) |
| `Makefile` `generate` target (D-22) | config | batch | itself — line 24, and `verify-phase6`/`verify-evidence` targets (lines 82-91) for how new phase-gate targets get added | exact |
| `docs/benchmark.md` (D-20, re-measured figure) | doc | batch | itself — existing 182.85× figure; `benchmark/run_benchmark.py`/`benchmark/naive_loader.py` (harness, unchanged) | exact |

## Pattern Assignments

### `generator/generate_csv.py` — `generate_correlated_datasets()`, `zipf_weighted_sample()`, `structured_id()` (utility, transform)

**Analog:** itself (existing functions in the same file)

**Imports pattern** (lines 18-30) — already-established style, extend, don't restructure:
```python
from __future__ import annotations

import argparse
import csv
import gzip
import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from csv_processor.config import ColumnSpec, DatasetConfig, load_config
from faker import Faker
```
Add `import hashlib` for D-06/D-07's seed-hash derivation (mirrors `csv_processor.load.sha256_file`'s own hashing discipline, `packages/csv-processor/src/csv_processor/load.py:44-62`).

**RNG/Faker seeding discipline to preserve** (lines 163-165, `generate_rows()`):
```python
fake = Faker()
Faker.seed(seed)
rng = random.Random(seed)
```
Two separate randomness streams (Faker for string realism, `random.Random` for everything else) must never interleave unpredictably — this is the exact discipline the module docstring (lines 1-16) describes and that D-05/Pitfall 1 require the new correlated function to respect explicitly (decide once whether `rng`/`fake` become injectable params threaded across both dataset calls, or stay internally constructed per call — see RESEARCH.md Pitfall 1, must be resolved in PLAN.md).

**Core value-generation dispatch pattern** (lines 80-109, `_valid_value()`) — the shape every new per-column helper (e.g. structured ID assignment overriding the `customer_id`/`order_id` columns) should follow: a type-dispatch `if column.type == ...` chain, each branch calling `rng`/`fake` and returning a `str`, with a final `raise ValueError` for unsupported types:
```python
def _valid_value(fake: Faker, rng: random.Random, column: ColumnSpec) -> str:
    if column.type == "string":
        return _fake_string_value(fake, column)
    ...
    msg = f"column {column.name!r}: unsupported column type {column.type!r}"
    raise ValueError(msg)
```

**Deterministic formatting discipline** (lines 60-68, `format_decimal()`) — exact `Decimal`-based formatting, never `str(float(...))`; the same discipline applies to `structured_id()`'s zero-padding (`f"{sequence:0{width}d}"`, never a computed/approximate width).

**Result dataclass shape to extend/reuse** (lines 140-150):
```python
@dataclass(frozen=True)
class GeneratedCsv:
    header: list[str]
    rows: list[list[str]]
    categories: list[str | None]
```
`generate_correlated_datasets()` should return something composing two `GeneratedCsv` instances (one per dataset) — do not flatten into a new ad hoc shape; existing callers (tests, `write_csv()`) already know how to consume `GeneratedCsv`.

**Error-raising convention for D-04** (mirrors lines 88-90, 95-97, 102-104, 108-109 — every internal precondition failure raises `ValueError` with an f-string message, never a silent fallback):
```python
if not valid_customer_pool:
    msg = "cannot generate correlated orders: valid-customer pool is empty"
    raise ValueError(msg)
```

**Output-path convention to mirror for staging (D-24)** (lines 240-243):
```python
def output_path(dataset: str, *, today: date | None = None) -> Path:
    """`./data/<dataset>/<dataset>_<YYYYMMDD>.csv` (D-06/D-07)."""
    day = today or date.today()
    return _DATA_DIR / dataset / f"{dataset}_{day:%Y%m%d}.csv"
```
A new `staging_path(dataset, filename)` helper should follow the identical `_DATA_DIR / dataset / ...` construction, adding a `.staging` path segment, then `Path.rename()` into `output_path()`'s target (D-24).

---

### `docker/oracle/init/05_correlation_constraints.sql` (migration, batch DDL)

**Analog:** `docker/oracle/init/04_widen_invalid_columns.sql` (whole file, 49 lines) + `02_customers.sql`/`03_orders.sql` for column names

**Mandatory session-setup preamble** (verbatim in every init script, `04_widen_invalid_columns.sql` lines 12-16, `02_customers.sql` lines 5-9):
```sql
-- Every init script mounted under /container-entrypoint-initdb.d runs once, on first boot,
-- via a bare `/ as sysdba` connection that lands in CDB$ROOT as SYS -- NOT in FREEPDB1 as ADMIN.
-- These two ALTER SESSION statements are mandatory, first, every time, no exceptions.
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;
```

**File-header comment discipline** (`04_widen_invalid_columns.sql` lines 1-10) — explain the *why*, cite the driving decisions (D-13/D-14/D-15/D-18 here), and note what is explicitly NOT touched (here: never `customers_invalid`/`orders_invalid`, per D-18):
```sql
-- <One-line what>.
--
-- <Why this is needed, which REQ/decision drives it>.
-- <Explicit scope note: table X is out of scope because Y>.
```

**DDL statement shape** — `ALTER TABLE ... ADD CONSTRAINT`/`CREATE INDEX`/`CREATE OR REPLACE TRIGGER`, one statement block per concern, matching RESEARCH.md's verified-shape example (Code Examples section) and this file's own column names from `02_customers.sql`/`03_orders.sql` (`customer_id VARCHAR2(64)`, `order_id VARCHAR2(64)`).

**Trigger scope note to include** (per RESEARCH.md Pitfall 3): add an explicit SQL comment stating the cross-table lookup (`orders_valid` trigger querying `customers_valid`) is safe and does not trigger `ORA-04091`, matching this project's habit of flagging non-bugs so future maintainers don't "fix" them (see `04_widen_invalid_columns.sql` lines 22-26's `ORA-01451` explanation as the template for this kind of inline gotcha note).

---

### `airflow/dags/report_ready.py` (new DAG, event-driven)

**Analog:** `airflow/dags/csv_ingest.py` (whole file)

**Imports pattern** (lines 15-26):
```python
from __future__ import annotations

import logging

from _common import paths, reporting
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import Param, dag, get_current_context, task
from csv_processor.config.errors import ConfigurationError
from csv_processor.config.loader import load_config
from csv_processor.config.models import DatasetConfig
from csv_processor.engine import process
from csv_processor.models import ProcessingResult, Status
```
Replace `FileSensor` import with `from _common.oracle_partition_trigger import OraclePartitionReadyTrigger`; `report_ready.py` needs no `dataset`/`config_path` params (D-29's sensor is dataset-agnostic — polls both).

**`@dag` decorator + no-schedule convention** (lines 29-37) — this project's DAGs are always manually/API-triggered (per D-29's own reasoning):
```python
@dag(
    dag_id="csv_ingest",
    schedule=None,
    catchup=False,
    params={...},
)
def csv_ingest() -> None:
```
`report_ready.py` should mirror `schedule=None, catchup=False`; likely no `params` needed (D-28's sensor takes no runtime conf).

**Deferrable-sensor task wiring** (lines 84-94, `FileSensor` construction) — this is the *shape* the new custom sensor class replaces `FileSensor` with; `ReportReadySensor` (RESEARCH.md Pattern 3) plugs in identically as a plain task object in the `@dag`-decorated function body, wired via `>>`.

**Thin logging-only report task pattern** (lines 118-141, `report_result_task`) — the new DAG's report-build/log task should follow this exact shape: no Slack/email, `logging.getLogger("airflow.task").info(...)`, `trigger_rule="none_failed_min_one_success"` if there's a branch, else a plain `@task`:
```python
@task(trigger_rule="none_failed_min_one_success")
def report_result_task() -> None:
    ctx = get_current_context()
    ti = ctx["ti"]
    outcome = ti.xcom_pull(task_ids="load_results_task") or ti.xcom_pull(task_ids="load_config_task")
    logging.getLogger("airflow.task").info(reporting.format_summary_log(outcome))
```
For D-26's report content, reuse `_BUSINESS_REPORT_SQL` (mirrored verbatim from `scripts/verify_evidence.sql`/`scripts/regenerate_readme_summary.py` lines 97-109) — never re-author that SQL a third time.

**Task-graph wiring convention** (lines 139-141) — plain `>>` operators at the bottom of the `@dag`-decorated function, after every task/sensor object has been constructed:
```python
route >> wait_for_file >> result_dict
route >> report
final_result_dict >> report
```

---

### `airflow/dags/_common/oracle_partition_trigger.py` (new custom trigger, event-driven)

**Analog:** No in-repo precedent for a custom `BaseTrigger`. Primary source: RESEARCH.md Pattern 3's Context7-verified code example (reproduced below). Credential-reading convention borrowed from `packages/csv-processor/src/csv_processor/load.py`.

**Credential-reading pattern to reuse** (`load.py` lines 65-88 — env-var-first, `admin`/`admin` dev fallback, exact same three functions this new module should call):
```python
def oracle_dsn() -> str:
    return os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")

def oracle_user() -> str:
    return os.environ.get("ORACLE_APP_USER", "admin")

def oracle_password() -> str:
    return os.environ.get("ORACLE_APP_USER_PASSWORD", "admin")
```
Import these directly from `csv_processor.load` rather than re-deriving env var names (matches this project's "never re-author the same logic twice" discipline, Phase 6 D-04/D-09, restated in CONTEXT.md's Established Patterns).

**Trigger/sensor pair shape** (RESEARCH.md Pattern 3, Context7-verified against `apache/airflow` `deferring.rst`):
```python
from airflow.sdk import BaseSensorOperator, Context
from _common.oracle_partition_trigger import OraclePartitionReadyTrigger

class ReportReadySensor(BaseSensorOperator):
    def execute(self, context: Context) -> None:
        self.defer(
            trigger=OraclePartitionReadyTrigger(poke_interval=30),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict | None = None) -> None:
        return
```
```python
import asyncio
import oracledb
from airflow.triggers.base import BaseTrigger, TriggerEvent

class OraclePartitionReadyTrigger(BaseTrigger):
    def __init__(self, poke_interval: float = 30.0) -> None:
        super().__init__()
        self.poke_interval = poke_interval

    def serialize(self):
        return (
            "_common.oracle_partition_trigger.OraclePartitionReadyTrigger",
            {"poke_interval": self.poke_interval},
        )

    async def run(self):
        query = (
            "SELECT COUNT(DISTINCT dataset) FROM ingestion_metadata "
            "WHERE dataset IN ('customers', 'orders') "
            "AND TRUNC(processed_at) = TRUNC(SYSDATE)"
        )
        while True:
            connection = await oracledb.connect_async(
                user=..., password=..., dsn=...
            )
            try:
                cursor = connection.cursor()
                await cursor.execute(query)
                (count,) = await cursor.fetchone()
            finally:
                await connection.close()
            if count == 2:
                yield TriggerEvent({"status": "ready"})
                return
            await asyncio.sleep(self.poke_interval)
```
Must use `oracledb.connect_async()`, never blocking `connect()` — the triggerer's shared asyncio event loop would otherwise stall for every other deferred task project-wide.

---

### `tests/unit/test_generate_csv.py` (extend + modify, transform)

**Analog:** itself — existing determinism-test template

**Existing determinism-test shape to mirror for new correlation assertions**:
```python
def test_generate_rows_is_deterministic_for_same_seed(...):
    first = generate_csv.generate_rows(orders_config, rows=50, invalid_ratio=0.2, seed=42)
    second = generate_csv.generate_rows(orders_config, rows=50, invalid_ratio=0.2, seed=42)
    assert first == second
```
New tests (`test_orders_customer_id_is_subset_of_valid_customer_pool`, `test_orders_customer_id_sampling_is_zipf_weighted`, `test_correlated_generation_is_deterministic_for_same_seed`) follow this exact call/assert shape — see RESEARCH.md's "Code Examples" section for the full illustrative bodies, already written against this project's own `GeneratedCsv.rows`/`.categories` fields.

**Exact call sites requiring modification** (RESEARCH.md Pitfall 2, confirmed via `grep`):
- Line 195-196: `test_generate_rows_is_deterministic_for_same_seed` (orders variant) — calls `generate_rows(orders_config, ...)` standalone; must be updated once orders generation requires a customer pool.
- Line 220: standalone orders row-count test.
- Line 229: standalone orders invalid-ratio test.
- Line 242: standalone orders category test.
- Line 270: standalone orders header test.

No test currently asserts specific `customer_id` *content* (only header shape via `assert header == [...]` at lines 186, 222, 270, 326) — the ID-format change (D-06/D-08) itself is safe; only the standalone-orders-without-a-pool call sites need updating to go through `generate_correlated_datasets()` instead.

---

### `tests/e2e/test_correlated_report_e2e.py` (new file, request-response live)

**Analog:** `tests/e2e/test_csv_ingest_e2e.py` (whole file, explicit D-10 template)

**Sibling-module-loading convention** (lines 36-52) — must be copied verbatim, including the `sys.modules` registration-before-`exec_module` ordering (required for `GeneratedCsv`'s postponed-annotation forward-ref resolution):
```python
_DAG_POLLING_PATH = _REPO_ROOT / "scripts" / "dag_polling.py"
_DAG_POLLING_SPEC = importlib.util.spec_from_file_location("dag_polling", _DAG_POLLING_PATH)
assert _DAG_POLLING_SPEC is not None and _DAG_POLLING_SPEC.loader is not None
dag_polling = importlib.util.module_from_spec(_DAG_POLLING_SPEC)
_DAG_POLLING_SPEC.loader.exec_module(dag_polling)

_GENERATE_CSV_PATH = _REPO_ROOT / "generator" / "generate_csv.py"
_GENERATE_CSV_SPEC = importlib.util.spec_from_file_location("generate_csv", _GENERATE_CSV_PATH)
assert _GENERATE_CSV_SPEC is not None and _GENERATE_CSV_SPEC.loader is not None
generate_csv = importlib.util.module_from_spec(_GENERATE_CSV_SPEC)
sys.modules["generate_csv"] = generate_csv
_GENERATE_CSV_SPEC.loader.exec_module(generate_csv)
```

**Stale-fixture-clearing precondition** (lines 55-71, `_clear_existing_customers_fixtures()`) — same pattern needed for both `customers` and `orders` dirs before triggering, since a stale file would make the sensor match immediately and never defer.

**Poll-then-assert-then-act ordering** (lines 74-128, the whole test body) — this exact five-step sequence is the D-25 template:
1. Trigger with target file confirmed absent (`dag_polling.trigger_dag(...)`).
2. Poll until `wait_for_file` reaches `"deferred"` (`dag_polling.wait_for_task_state(..., timeout=180.0)` — use the same generous CI-cold-start timeout, with the same inline comment explaining why 180s not the 60s default).
3. Only then write the fixture (staging + rename per D-24, replacing this file's direct `write_csv()` call).
4. Poll to completion (`dag_polling.wait_for_dag_run_result(...)`).
5. Assert real Oracle rows via `oracle_cursor` — never `result["status"]` alone (lines 130-145's `SELECT COUNT(*) ... WHERE customer_id IN (...)` shape).

**D-12 backdating addition** (new, not in the analog) — per RESEARCH.md Pitfall 4, call `csv_processor.load.insert_rows()` directly with an extended `columns` list including `"ingested_at"` and a past-date value in the row dicts, bypassing `engine.process()`'s normal flow for this one step only:
```python
load.insert_rows(
    cursor,
    table="orders_valid",
    columns=[*config.columns_as_names, "ingested_at"],
    rows=[{**row, "ingested_at": some_past_date} for row in backdated_rows],
)
```

**`oracle_cursor` fixture** — confirm exact fixture name/location via `tests/e2e/conftest.py` (not read this session; verify during planning) before assuming it's importable as shown in the analog.

---

### `scripts/regenerate_readme_summary.py` — `_run_ingestion()` loop (modify, D-21/D-23)

**Analog:** itself

**Sibling-module-loading convention** (lines 112-132) — already identical to the e2e test's; when `generate_correlated_datasets()` is adopted, load it the same way (no new pattern needed, `generate_csv` module already loaded here).

**Existing per-dataset loop to replace** (lines 340-344, `main()`):
```python
for dataset in _DATASETS:
    ingestion_results[dataset] = _run_ingestion(dataset)
```
Per D-21/D-23, this becomes: call `generate_csv.generate_correlated_datasets(customers_config, orders_config, seed=...)` once (outside the loop) to get both `GeneratedCsv` results, THEN loop `_DATASETS` to stage/rename/trigger/wait each dataset independently — preserves `_DATASETS = ("customers", "orders")`'s existing ordering (already customers-then-orders, matches D-23's requirement with no reordering needed).

**Never-touch-README-until-success discipline** (lines 340-366, whole `main()`) — must be preserved unchanged: every step builds in-memory data first, `README.md` is written exactly once at the very end, only after `RegenerationError` has had every chance to fire.

**SQL-mirroring discipline** (lines 73-109, `_ROW_COUNT_SQL`/`_BUSINESS_REPORT_SQL` comments) — explicit "never re-authored independently" comment style; the new report DAG's report-task SQL must match this exact business-report query text, sourced from the same place (`scripts/verify_evidence.sql`), never a fourth independent copy.

---

### `Makefile` — `generate` target (modify, D-22)

**Analog:** itself (line 24) + `verify-phase6`/`verify-evidence` targets (lines 82-91) for target-definition conventions

**Current target to replace**:
```makefile
generate:          ## Generate deterministic business-row CSV fixtures for every dataset (D-16f)
	uv run python generator/generate_csv.py --dataset customers && uv run python generator/generate_csv.py --dataset orders
```
Per D-22, becomes either `uv run python generator/generate_csv.py --correlated` (new flag) or a small new orchestrator script invocation — either way, ONE subprocess call, not two independent ones (Claude's Discretion which).

**Target comment-authority convention** (every target has a one-line `##` comment citing the driving decision, e.g. `(D-16f)`, `(D-13)`) — the modified `generate` target's comment should cite D-21/D-22.

**`.PHONY` line** (line 1) — remember to add any new target names (e.g. if a new `verify-phase7` gate target is added per project convention, following `verify-phase6`'s exact shape at lines 87-91) to the `.PHONY:` declaration.

## Shared Patterns

### Never re-author the same SQL/logic twice
**Source:** `scripts/verify_evidence.sql` + `scripts/regenerate_readme_summary.py` lines 73-109 (explicit code comment: "mirrored verbatim... never re-authored independently")
**Apply to:** `airflow/dags/report_ready.py`'s report-building task (must reuse the same business-report SQL text), `generator/generate_csv.py`'s `generate_correlated_datasets()` (single source of correlation logic per D-21, called from `make generate`, `regenerate_readme_summary.py`, and the new e2e test's fixture setup — never three independent implementations)

### Env-var-first Oracle credentials with `admin`/`admin` dev fallback
**Source:** `packages/csv-processor/src/csv_processor/load.py` lines 65-88
```python
def oracle_dsn() -> str:
    return os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")

def oracle_user() -> str:
    return os.environ.get("ORACLE_APP_USER", "admin")

def oracle_password() -> str:
    return os.environ.get("ORACLE_APP_USER_PASSWORD", "admin")
```
**Apply to:** `airflow/dags/_common/oracle_partition_trigger.py` (import these functions directly, don't re-derive env var names)

### `is_safe_identifier()` defense-in-depth for any dynamic SQL identifier
**Source:** `packages/csv-processor/src/csv_processor/load.py` lines 106-140 (`insert_rows()`)
**Apply to:** Not directly needed by this phase's new code (RESEARCH.md's Security Domain section confirms the new trigger DDL is static and the new sensor's polling query is a static parameterless `SELECT` — no new identifier-interpolation surface is introduced), but any executor writing new dynamic-SQL code in this phase should check this function first before hand-rolling identifier validation.

### Deferrable-sensor task wiring (`self.defer(...)`)
**Source:** `airflow/dags/csv_ingest.py` lines 84-94 (`FileSensor(deferrable=True, ...)`)
**Apply to:** `airflow/dags/report_ready.py`'s `ReportReadySensor` — same "poll without blocking a worker slot" shape, different trigger implementation underneath (custom `OraclePartitionReadyTrigger` vs. built-in `FileSensor`).

### Numbered init-script convention for Oracle DDL
**Source:** `docker/oracle/init/01_ingestion_metadata.sql` → `04_widen_invalid_columns.sql` (file naming + mandatory `ALTER SESSION` preamble)
**Apply to:** `docker/oracle/init/05_correlation_constraints.sql` — next number in sequence, same preamble, same header-comment discipline explaining why/scope.

### Poll-then-assert-then-act ordering (Pitfall 4 discipline)
**Source:** `tests/e2e/test_csv_ingest_e2e.py` lines 74-128, `scripts/regenerate_readme_summary.py` lines 151-208 (`_run_ingestion()`)
**Apply to:** `tests/e2e/test_correlated_report_e2e.py` and any D-24/D-25 staging-write-then-rename logic — never write/rename the target file before the sensor is confirmed `deferred`.

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `airflow/dags/_common/oracle_partition_trigger.py` (`OraclePartitionReadyTrigger` class specifically) | middleware / trigger | event-driven (async poll) | No custom `BaseTrigger` subclass exists anywhere in this codebase yet — `csv_ingest.py` only uses the built-in `FileSensor(deferrable=True)`. RESEARCH.md's Pattern 3 (Context7-verified against `apache/airflow` docs) is the authoritative source instead of an in-repo analog. |

## Metadata

**Analog search scope:** `generator/`, `docker/oracle/init/`, `airflow/dags/`, `packages/csv-processor/src/csv_processor/`, `scripts/`, `tests/unit/`, `tests/e2e/`, `Makefile`, `docs/`
**Files scanned:** `generate_csv.py`, `02_customers.sql`, `03_orders.sql`, `04_widen_invalid_columns.sql`, `csv_ingest.py`, `load.py`, `dag_polling.py`, `regenerate_readme_summary.py`, `Makefile`, `test_csv_ingest_e2e.py`, `test_generate_csv.py` (grep), `.github/workflows/ci.yml` (grep)
**Pattern extraction date:** 2026-08-30
