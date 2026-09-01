# Phase 9: Hourly Orchestrator DAG (`csv_generate_schedule`) - Pattern Map

**Mapped:** 2026-09-01
**Files analyzed:** 4 (1 new DAG, 1 new test file, 1 modified Makefile, 1 modified doc)
**Analogs found:** 4 / 4

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|--------------------|------|-----------|-----------------|----------------|
| `airflow/dags/csv_generate_schedule.py` | route (TaskFlow DAG) | event-driven (scheduled) + request-response (chain-trigger) | `airflow/dags/csv_ingest.py` (primary) + `airflow/dags/report_ready.py` (secondary, for direct-Oracle-query task shape) | exact (structural convention), role-match (no prior chain-triggering DAG exists in this repo) |
| `airflow/dags/_common/generate_schedule_helpers.py` *(new, Claude's-discretion module for pure-Python testable helpers — see rationale below)* | utility | transform | `airflow/dags/_common/paths.py` + `airflow/dags/_common/reporting.py` | exact |
| `tests/unit/dags/test_generate_schedule_helpers.py` | test | transform | `tests/unit/dags/test_report_result_format.py` + `tests/unit/dags/test_dag_helpers.py` | exact |
| `Makefile` (new `verify-phase9` target) | config | batch | `Makefile`'s `verify-phase5` target (lines 64-75) | exact |
| `docs/airflow-dag.md` (new section) | doc | — | existing `## csv_ingest DAG` / `## report_ready DAG` sections | exact |

**Note on the helpers module:** RESEARCH.md's Wave 0 Gaps explicitly call for extracting pure,
independently-testable functions (seed derivation, summary-line formatter, retention date-parsing)
"rather than inlining string-building in the task body" — mirroring `_common/reporting.py`'s
`format_summary_log()` and `_common/paths.py`'s pattern of zero-Airflow-import helper modules. This
PATTERNS.md assumes the planner will create a new `_common/generate_schedule_helpers.py` (or split
across `_common/reporting.py`/a new module — exact split is a planner call) so
`test_generate_schedule_helpers.py` has plain functions to import, exactly like
`test_report_result_format.py` imports `format_summary_log` and `test_dag_helpers.py` imports
`resolve_matched_file`. If the planner instead inlines these as closures inside `@task` functions,
the analog mapping below still applies to the *logic*, just not literally import-testable the same
way — inlining would contradict the "Wave 0 Gaps" testability requirement RESEARCH.md itself flags.

## Pattern Assignments

### `airflow/dags/csv_generate_schedule.py` (route, event-driven + request-response)

**Primary analog:** `airflow/dags/csv_ingest.py` (full file read; 144 lines)
**Secondary analog:** `airflow/dags/report_ready.py` (full file read; 69 lines) — for the
direct-Oracle-query task shape (`summary_task`) and the "no `conf=`" trigger contract.

**Module docstring pattern** (`csv_ingest.py` lines 1-13):
```python
"""The one, config-driven ``csv_ingest`` DAG (D-01/DAG-01/DAG-05).

Fully parameterized by runtime ``conf`` (``dataset`` name + ``config_path``) --
never one DAG per dataset. Delegates the entire detect->parse->validate->
normalize->chunk->load(Oracle) sequence to ``csv_processor.engine.process()``
(D-02/D-03/D-12) -- this file and ``_common/`` contain no CSV/Oracle logic of
their own, only thin wiring plus input-validation (T-05-01/T-05-02).

Task graph: ``load_config_task`` -> ``route_after_config`` -> either
``wait_for_file`` (deferrable ``FileSensor``, D-04) -> ``process_csv_task`` ->
``load_results_task`` -> ``report_result_task``, or straight to
``report_result_task`` on a CONFIGURATION_ERROR early exit (D-08).
"""
```
Follow this convention exactly for `csv_generate_schedule.py`: a docstring naming the requirement
IDs it satisfies (SCHED-01..08, SCHED-10, D-01..D-19) and spelling out the task graph in prose
before any code — this is the established documentation style for every DAG file in this repo.

**Imports pattern** (`csv_ingest.py` lines 15-26):
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
`csv_generate_schedule.py`'s own import block (per RESEARCH.md Code Examples §1-5) should follow
the exact same shape/ordering convention (`from __future__ import annotations` first, stdlib next,
then `_common`, then `airflow.*`, then in-repo packages):
```python
from __future__ import annotations

import logging
import subprocess
import sys
from datetime import timedelta

from _common import paths  # DATA_ROOT, same convention as csv_ingest.py
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from csv_processor import load  # get_connection(), same as report_ready.py
```

**`@dag(...)` decorator pattern** (`csv_ingest.py` lines 29-37):
```python
@dag(
    dag_id="csv_ingest",
    schedule=None,
    catchup=False,
    params={
        "dataset": Param("customers", type="string", enum=["customers", "orders"]),
        "config_path": Param("configs/datasets/customers.json", type="string"),
    },
)
def csv_ingest() -> None:
```
`csv_generate_schedule` copies this decorator shape, swapping `schedule=None` for `schedule="@hourly"`,
adding `max_active_runs=1` (SCHED-04) and `dagrun_timeout=timedelta(minutes=45)` (D-10), and using
`Param(100, type="integer", minimum=1)` / `Param(0.1, type="number", minimum=0.0, maximum=1.0)` for
`rows`/`invalid_ratio` (D-01/D-02, SCHED-08) — exact syntax already verified against the pinned
`airflow.sdk.definitions.param.Param` source in RESEARCH.md Code Examples §1/§4.

**`get_current_context()` runtime-param access pattern** (`csv_ingest.py` lines 49-51):
```python
ctx = get_current_context()
dataset = ctx["params"]["dataset"]
config_path = ctx["params"]["config_path"]
```
Direct template for `generate_task`'s `ctx["dag_run"].logical_date` (D-04) and
`ctx["params"]["rows"]` / `ctx["params"]["invalid_ratio"]` (D-01/D-02) access — same
`get_current_context()` call, same subscript style, zero new API surface.

**Domain-failure "return a status dict, don't raise" pattern** (`csv_ingest.py` lines 53-68):
```python
try:
    paths.validate_dataset(dataset)
    resolved_path = paths.resolve_safe_config_path(config_path)
    config = load_config(resolved_path, defaults_path=paths.DEFAULTS_PATH)
except (ValueError, ConfigurationError):
    return {
        "status": Status.CONFIGURATION_ERROR.value,
        ...
    }
return config.model_dump(mode="json")
```
This project's established convention is "domain failures return a structured result, only
genuinely unexpected errors raise." `generate_task` (subprocess `check=True`) deliberately departs
from this — RESEARCH.md Code Examples §2 explicitly notes letting `subprocess.CalledProcessError`
propagate is correct here (D-09's `retries=0`, "fail loudly" instinct) — but `retention_task`
follows the opposite, catch-everything-and-log convention (see Code Examples §5 in RESEARCH.md,
mirrored below in Shared Patterns).

**Trigger-task chaining / `>>` composition pattern** (`csv_ingest.py` lines 139-141):
```python
route >> wait_for_file >> result_dict
route >> report
final_result_dict >> report
```
`csv_generate_schedule`'s linear chain is simpler (no branching) — mirrors RESEARCH.md Code
Examples §3's:
```python
generate_task() >> trigger_customers >> trigger_orders >> trigger_report_ready
```
then `trigger_report_ready >> summary_task() >> retention_task()`.

**Direct-Oracle-query task pattern** (`report_ready.py` lines 51-64, the closest analog for
`summary_task` since `csv_ingest.py` never queries Oracle directly — it delegates to
`csv_processor.engine.process()`):
```python
@task
def build_report_task() -> None:
    """Query and log the business report (D-27: logs only, matching
    ``csv_ingest.py``'s ``report_result_task`` shape -- no Slack/email)."""
    connection = load.get_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(_BUSINESS_REPORT_SQL)
        rows = cursor.fetchall()
    finally:
        connection.close()
    logging.getLogger("airflow.task").info(
        "report_ready business report (%d rows): %s", len(rows), rows
    )
```
`get_connection()` signature (`packages/csv-processor/src/csv_processor/load.py` line 84-88):
```python
def get_connection() -> oracledb.Connection:
    """Open one real Oracle connection using this project's env-var-first
    credentials (falls back to the single documented dev credential pair,
    ``admin``/``admin``, per INFRA-03)."""
    return oracledb.connect(user=oracle_user(), password=oracle_password(), dsn=oracle_dsn())
```
`summary_task` (D-12/D-13/D-14) copies this exact `connection = load.get_connection(); try: ...
finally: connection.close()` shape, swapping the query for the per-dataset `ingestion_metadata`
latest-row lookup (RESEARCH.md Code Examples §4 has the full query + loop).

**Trigger_rule convention** (`csv_ingest.py` line 126, `report_result_task`):
```python
@task(trigger_rule="none_failed_min_one_success")
def report_result_task() -> None:
```
`summary_task` and `retention_task` reuse `trigger_rule="none_failed_min_one_success"` — the same
"reachable via either the success path or an acceptable early-exit path" convention, already
confirmed as the correct choice in RESEARCH.md Code Examples §4/§5.

**No custom exception class / minimal-import style:** confirmed by RESEARCH.md ("this repo has
zero existing `AirflowException` usage anywhere") — `csv_ingest.py`/`report_ready.py` both let
underlying exceptions (`ConfigurationError`, `ValueError`, Oracle driver errors) propagate directly
or convert to a status dict; neither file imports or raises `AirflowException`. `generate_task`
follows the same minimal-import style: let `subprocess.CalledProcessError` propagate unwrapped.

---

### `airflow/dags/_common/generate_schedule_helpers.py` (utility, transform) — new pure-function module

**Analog:** `airflow/dags/_common/paths.py` (full file, 94 lines) and
`airflow/dags/_common/reporting.py` (full file, 36 lines)

**Module docstring / zero-Airflow-import convention** (`_common/paths.py` lines 1-9):
```python
"""Path/dataset-safety helpers for ``csv_ingest.py`` (D-05, DAG-02, T-05-01).

Plain, unit-testable functions with zero Airflow import (05-RESEARCH.md
"Validation Architecture" Wave 0 Gaps) -- mirrors
``csv_processor.config.loader``'s docstring/error-wrapping convention, but
this module is DAG-side, not part of ``csv_processor`` itself (this codebase's
established "Airflow imports only ever live under airflow/dags/" boundary,
ENGINE-09's spirit extended informally to the whole repo).
"""

from __future__ import annotations

from pathlib import Path
```
Every new pure helper this phase needs (seed derivation, summary-line formatter, retention
date-parse-and-filter) should live in a module shaped exactly like this: `from __future__ import
annotations`, zero `airflow.*` import, a docstring naming which task(s) it backs and which
requirement IDs it satisfies.

**Constant-at-module-level convention** (`_common/paths.py` lines 15-24):
```python
DATA_ROOT = Path("/opt/airflow/data")
CONFIGS_ROOT = Path("/opt/airflow/configs")
DEFAULTS_PATH = CONFIGS_ROOT / "defaults.json"
DATASETS_CONFIG_ROOT = CONFIGS_ROOT / "datasets"

ALLOWED_DATASETS = frozenset({"customers", "orders"})
```
`retention_task`'s helper module reuses `_common.paths.DATA_ROOT` directly rather than redefining
its own `_DATA_ROOT` constant (RESEARCH.md's Code Examples §5 defines a local `_DATA_ROOT` — the
planner should prefer importing `paths.DATA_ROOT` instead, since it is already the established
single source of truth for this exact path and avoids a second constant drifting out of sync).

**Pure-function signature/docstring convention** (`_common/paths.py` lines 27-42):
```python
def resolve_matched_file(base_dir: Path, file_pattern: str) -> Path | None:
    """Glob ``base_dir`` for ``file_pattern`` and return the sorted-first match.
    ...
    Args:
        base_dir: Directory to search (e.g. ``/opt/airflow/data/customers``).
        file_pattern: A shell-style glob, e.g. ``"customers_*.csv*"``.

    Returns:
        The sorted-first matching path, or ``None`` if nothing matches.
    """
    candidates = sorted(base_dir.glob(file_pattern))
    return candidates[0] if candidates else None
```
Direct template for a `retention_task` helper like:
```python
def files_older_than(base_dir: Path, dataset: str, cutoff: datetime) -> list[Path]:
    """Return dated CSV/CSV.GZ paths under base_dir older than cutoff."""
    ...
```

**`format_summary_log()` — plain formatter, single f-string return** (`_common/reporting.py`
lines 11-35, full function):
```python
def format_summary_log(result: dict[str, object]) -> str:
    """Format ``result`` (a ``ProcessingResult``-shaped dict) as one summary log line.
    ...
    """
    return (
        f"dataset={result['dataset']} "
        f"file={result['file_name']} "
        f"status={result['status']} "
        f"total={result['total_rows']} "
        f"valid={result['valid_rows']} "
        f"invalid={result['invalid_rows']} "
        f"duration={result['duration_seconds']:.2f}s"
    )
```
Exact template for D-12/D-13/D-14's new `format_cascade_summary()` helper (SCHED-07) — same
"single f-string built from a dict/args, returned not logged" shape; `summary_task` itself then
just calls `logging.getLogger("airflow.task").info(format_cascade_summary(...))`, mirroring
`csv_ingest.py`'s `report_result_task` (line 135):
```python
logging.getLogger("airflow.task").info(reporting.format_summary_log(outcome))
```

**Raise-ValueError-with-f-string-message convention** (`_common/paths.py` lines 59-61):
```python
if dataset not in ALLOWED_DATASETS:
    msg = f"unknown dataset {dataset!r}; must be one of {sorted(ALLOWED_DATASETS)}"
    raise ValueError(msg)
```
The `msg = f"..."; raise ValueError(msg)` two-line form (never `raise ValueError(f"...")` inline)
is this codebase's consistent style — reuse it in any helper that validates input and raises.

---

### `tests/unit/dags/test_generate_schedule_helpers.py` (test, transform)

**Analog A:** `tests/unit/dags/test_report_result_format.py` (full file, 38 lines) — formatter-test shape
**Analog B:** `tests/unit/dags/test_dag_helpers.py` (full file, 51 lines) — parametrized pure-function-test shape
**Analog C:** `tests/unit/dags/conftest.py` (full file, 37 lines) — shared fixture

**`sys.path` bootstrap for importing `_common.*` from tests** (`test_report_result_format.py`
lines 1-14, identical pattern in `test_dag_helpers.py` lines 1-18):
```python
"""Tests for ``_common.reporting.format_summary_log`` (DAG-04).

Proves the exact field list/format ``report_result_task`` logs.
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "airflow" / "dags"))

from _common.reporting import format_summary_log  # noqa: E402
```
`test_generate_schedule_helpers.py` MUST use this exact bootstrap (`_common` is only importable
via `airflow/dags` on `sys.path`, not via the project's normal `pythonpath = ["."]` in
`pyproject.toml` — `tests/unit/dags/conftest.py`'s own docstring explains why, lines 1-9) before
importing whatever new helper module holds the seed/summary/retention functions, e.g.:
```python
from _common.generate_schedule_helpers import (
    derive_seed,
    format_cascade_summary,
    files_older_than,
)  # noqa: E402
```

**Plain assertion-per-field test shape** (`test_report_result_format.py` lines 17-37, full test):
```python
def test_format_summary_log_contains_all_required_fields() -> None:
    result = {
        "status": "SUCCESS",
        "dataset": "customers",
        ...
    }

    line = format_summary_log(result)

    assert "dataset=customers" in line
    assert "file=customers_20260829.csv" in line
    ...
```
Direct template for `test_summary_format` (SCHED-07's required test per RESEARCH.md's Phase
Requirements → Test Map).

**Parametrized dual-dataset test shape + `tmp_path` fixture** (`test_dag_helpers.py` lines
35-50, full test):
```python
@pytest.mark.parametrize(
    ("dataset", "filename"),
    [
        ("customers", "customers_20990101.csv"),
        ("orders", "orders_20990101.csv.gz"),
    ],
)
def test_resolve_matched_file_works_for_both_dataset_patterns(
    tmp_path: Path, dataset_configs, dataset: str, filename: str
) -> None:
    (tmp_path / filename).write_text("data", encoding="utf-8")

    result = resolve_matched_file(tmp_path, dataset_configs[dataset].file_pattern)

    assert result is not None
    assert result.name == filename
```
Direct template for `test_retention_deletes_old_files`/`test_retention_never_raises` — use
`tmp_path` to create fake dated `.csv`/`.csv.gz` files (old and recent), call the retention helper,
assert old ones are gone and recent ones remain; for `test_retention_never_raises`, create a file
that raises on `unlink()`/parse (e.g. monkeypatch or an unparseable filename) and assert the
function returns normally rather than raising — mirrors D-18's "log and skip" contract from
RESEARCH.md Code Examples §5.

**`conftest.py`'s shared `dataset_configs` fixture** (full file):
```python
"""Shared fixtures for ``tests/unit/dags/`` -- Phase 5's DAG-side pure-Python
helper tests (05-01-PLAN.md Task 2).
...
"""
from __future__ import annotations

from pathlib import Path

import pytest
from csv_processor.config.loader import load_config

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"
_DEFAULTS_PATH = _CONFIGS_DIR / "defaults.json"


@pytest.fixture
def dataset_configs():
    """Both real, validated dataset configs..."""
    return {
        "customers": load_config(
            _CONFIGS_DIR / "datasets" / "customers.json", defaults_path=_DEFAULTS_PATH
        ),
        "orders": load_config(
            _CONFIGS_DIR / "datasets" / "orders.json", defaults_path=_DEFAULTS_PATH
        ),
    }
```
Already exists and is shared across `tests/unit/dags/*` — no changes needed; reuse the
`dataset_configs` fixture as-is if any new test needs a real, validated dataset config (unlikely
needed for Phase 9's pure seed/summary/retention helpers, but available).

**Seed-varies-by-hour test:** no direct existing analog (new behavior) — closest structural analog
is `tests/unit/test_generate_csv.py`'s `test_generate_rows_is_deterministic_for_same_seed` (lines
56-60), inverted: assert two different `logical_date` values (different hours) produce different
seeds, and the same `logical_date` produces the same seed on retry (D-04's exact contract):
```python
def test_generate_rows_is_deterministic_for_same_seed(customers_config) -> None:
    first = generate_csv.generate_rows(customers_config, rows=50, invalid_ratio=0.2, seed=42)
    second = generate_csv.generate_rows(customers_config, rows=50, invalid_ratio=0.2, seed=42)

    assert first.header == second.header
```

---

### `Makefile` (config, batch) — new `verify-phase9` target

**Analog:** `verify-phase5` target, `Makefile` lines 64-75 (full target, plus its preceding
explanatory comment lines 59-63):
```makefile
# requires `make up` first, same as verify-phase4. The DagBag structure check
# uses BundleDagBag (not the plain airflow.models.DagBag) -- 05-01-SUMMARY.md's
# own recorded deviation found that plain DagBag never adds the dags folder to
# sys.path, so csv_ingest.py's `from _common import paths, reporting` fails
# under it even though it imports cleanly under Airflow's real dag-processor.
verify-phase5:     ## Phase 5's own combined local gate: unit suite + live DagBag structure check (requires `make up` first)
	uv run pytest tests/unit/ -x
	docker compose exec -T airflow-scheduler python -c "\
from pathlib import Path; \
from airflow.dag_processing.dagbag import BundleDagBag; \
b = BundleDagBag(bundle_path=Path('/opt/airflow/dags'), dag_folder='/opt/airflow/dags'); \
assert not b.import_errors, b.import_errors; \
dag = b.dags['csv_ingest']; \
required = {'load_config_task','wait_for_file','process_csv_task','load_results_task','report_result_task'}; \
assert required.issubset(set(dag.task_ids)), dag.task_ids; \
assert dag.get_task('wait_for_file').deferrable is True; \
print('DAGBAG_OK')"
```
`verify-phase9` copies this exact shape (unit suite line, then a `docker compose exec -T
airflow-scheduler python -c "..."` `BundleDagBag` structural check), swapping `dag_ids['csv_ingest']`
for `dag_ids['csv_generate_schedule']`, the `required` task-id set for
`{'generate_task','trigger_customers','trigger_orders','trigger_report_ready','summary_task',
'retention_task'}`, and adding the extra `max_active_runs`/`deferrable`/`fail_when_dag_is_paused`
assertions RESEARCH.md's Code Examples §6 already spells out verbatim. Also add `verify-phase9` to
the `.PHONY:` line (`Makefile` line 1) alongside the existing `verify-phase2 verify-phase3
verify-phase4 verify-phase5 ... verify-phase8` list.

---

### `docs/airflow-dag.md` (doc)

**Analog:** existing `## csv_ingest DAG` (lines 12-157) and `## report_ready DAG` (lines 158-189)
sections in the same file.

**Section shape to mirror** (`## report_ready DAG`, lines 158-189, full section):
```markdown
## `report_ready` DAG

`airflow/dags/report_ready.py` (D-26) is dataset-agnostic -- it takes no runtime `conf` and is
triggered on demand or run on a schedule, independently of any `csv_ingest` run.

### Task Graph

\`\`\`
wait_for_both_datasets -> build_report_task
\`\`\`

- `wait_for_both_datasets` -- a `ReportReadySensor` ...
- `build_report_task` -- a plain `@task` that opens a normal (blocking) Oracle connection ...

### Live Verification Evidence

...
```
Add a new `## csv_generate_schedule DAG` section following this exact shape: one-paragraph intro
(what it does, `@hourly`/`catchup=False`/`max_active_runs=1`), a fenced `### Task Graph` ASCII
diagram (`generate_task -> trigger_customers -> trigger_orders -> trigger_report_ready ->
summary_task -> retention_task`), a bullet per task explaining its role (same terse style as the
`csv_ingest`/`report_ready` bullet lists), and a `### Live Verification Evidence` subsection once a
real triggered run has been observed through the Airflow UI (per RESEARCH.md's mandated
live-verification step) — mirroring the `#### DAG-03:`/`#### DAG-05:` evidence-block style already
used for `csv_ingest` (lines 71-157).

## Shared Patterns

### Auth/Guard
Not applicable — no HTTP-facing surface in this phase's new DAG. `scripts/trigger_dag.sh`'s
`/auth/token` flow (`docs/airflow-dag.md` lines 42-46) is only relevant if the planner adds a
trigger-script convenience wrapper for `csv_generate_schedule`, which is not required by
CONTEXT.md/RESEARCH.md (the DAG is `@hourly`-scheduled, not manually triggered via script).

### Error Handling
Two distinct, deliberately different conventions apply to different tasks in this same DAG —
**do not conflate them:**
1. **"Fail loudly" (default):** `generate_task`, the three `TriggerDagRunOperator` tasks, and
   `summary_task` let underlying exceptions propagate unwrapped (no `try/except` swallowing) —
   `subprocess.run(..., check=True)` raises `CalledProcessError` on nonzero exit; Oracle driver
   errors in `summary_task` propagate. Matches `csv_ingest.py`/`report_ready.py`'s zero
   `AirflowException` usage and D-09's `retries=0` "fail loudly" instinct.
2. **"Best-effort, never raise" (retention only):** `retention_task` wraps each file's
   processing in `try: ... except Exception: logger.warning(..., exc_info=True)` per D-18 — this
   is the one deliberate exception to convention #1 in this phase, justified explicitly in
   RESEARCH.md's Code Examples §5 commentary (a housekeeping failure must never block the next
   hourly cycle).

### Validation
DAG `Param` JSON-Schema validation (`type="integer", minimum=1` / `type="number", minimum=0.0,
maximum=1.0`) is Airflow's own built-in mechanism, already used by `csv_ingest.py`'s
`Param(..., type="string", enum=[...])` (lines 34-35) — no custom validation library needed. For
defense-in-depth beyond the `Param` schema (mirroring `_common/paths.py`'s
`validate_dataset()`/`resolve_safe_config_path()` two-layer convention, lines 45-93), none is
required in Phase 9: unlike `csv_ingest.py`'s runtime `conf`-supplied `dataset`/`config_path`
(externally triggerable, untrusted), `csv_generate_schedule`'s trigger `conf=` payloads are
hardcoded dict literals in the DAG source itself (RESEARCH.md's Security Domain table confirms "no
new access-control surface... `conf=` payloads are fixed, hardcoded dicts").

### Logging
Every task in this DAG logs via `logging.getLogger("airflow.task")`, never Slack/email — the
project-wide convention confirmed by both `csv_ingest.py` line 135 and `report_ready.py` lines
62-64. `summary_task` and `retention_task` both continue this convention (`.info(...)` for the
cascade summary line, `.info(...)`/`.warning(...)` for retention deletions/skips).

### Oracle Connection Handling
`connection = load.get_connection(); try: cursor = connection.cursor(); cursor.execute(...); ...
finally: connection.close()` — used identically in `report_ready.py`'s `build_report_task` (lines
55-61) and is the exact shape `summary_task` should copy (looping the same `try/finally` body once
per dataset, or opening one connection and looping inner `cursor.execute()` calls — either is
consistent with existing usage since `report_ready.py` only ever runs one query per connection
today).

## No Analog Found

None — every file this phase touches has at least one exact structural analog already in the
repository (this phase is explicitly "wiring, not new engineering," per RESEARCH.md's own framing).

## Metadata

**Analog search scope:** `airflow/dags/` (all files), `airflow/dags/_common/` (all files),
`tests/unit/dags/` (all files), `tests/unit/test_generate_csv.py`, `Makefile`, `docs/airflow-dag.md`,
`packages/csv-processor/src/csv_processor/load.py` (`get_connection()`), `generator/generate_csv.py`
(`output_path()`, `build_parser()`, `write_staged()` — for filename/CLI-contract reference, not a
structural analog since this phase never modifies `generate_csv.py`).
**Files scanned:** 12 read in full (`csv_ingest.py`, `report_ready.py`, `_common/paths.py`,
`_common/reporting.py`, `generator/generate_csv.py`, `tests/unit/dags/conftest.py`,
`tests/unit/dags/test_dag_helpers.py`, `tests/unit/dags/test_report_result_format.py`,
`tests/unit/test_generate_csv.py` (partial, lines 1-60), `Makefile` (targeted range, lines 59-118),
`docs/airflow-dag.md` (full)); plus `load.py`'s `get_connection()` (targeted grep+read).
**Pattern extraction date:** 2026-09-01
