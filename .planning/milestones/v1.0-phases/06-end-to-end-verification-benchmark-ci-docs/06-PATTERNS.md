# Phase 6: End-to-End Verification, Benchmark, CI & Docs - Pattern Map

**Mapped:** 2026-08-29
**Files analyzed:** 17
**Analogs found:** 14 / 17

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `tests/e2e/conftest.py` | test (fixtures) | request-response | `tests/integration/conftest.py` | role-match |
| `tests/e2e/test_csv_ingest_e2e.py` | test | request-response / event-driven | `scripts/trigger_dag.sh` + `tests/integration/test_engine_process_oracle.py` | role-match |
| `benchmark/naive_loader.py` | utility (benchmark write path) | batch/CRUD | `packages/csv-processor/src/csv_processor/load.py` (`insert_rows`) | exact (inverse pattern) |
| `benchmark/run_benchmark.py` | utility (orchestrator) | batch/transform | `generator/generate_csv.py` (CLI/argparse shape) + `csv_processor/engine.py` (`process_chunks` consumer) | role-match |
| `scripts/verify_evidence.sql` | config/script (SQL) | request-response (read-only query) | `docker/oracle/init/*.sql` (schema/DDL, not query, but only SQL analog) | partial |
| `.github/workflows/ci.yml` | config (CI) | event-driven | none in repo (new capability) | no analog — use RESEARCH.md Pattern 3 |
| `.github/workflows/readme-summary.yml` | config (CI) | event-driven | none in repo (new capability) | no analog — use RESEARCH.md Pattern 4 |
| `pyproject.toml` (mypy/ruff sections) | config | n/a | existing `[tool.pytest.ini_options]`/`[tool.uv.*]` blocks in same file | exact |
| `Makefile` (`verify-phase6`, `benchmark`, `lint`, `verify-evidence` targets) | config | n/a | `Makefile`'s existing `verify-phase4`/`verify-phase5` targets | exact |
| `README.md` (Executive Summary section + links rewrite) | doc | n/a | `README.md` itself (existing "Getting Started"/Q&A structure) | exact |
| `docs/architecture.md` | doc | n/a | `docs/airflow-dag.md`, `docs/environment.md` | role-match |
| `docs/configuration.md` | doc | n/a | `docs/environment.md` | role-match |
| `docs/csv-engine.md` | doc | n/a | `docs/airflow-dag.md` | role-match |
| `docs/oracle.md` | doc | n/a | `docs/environment.md` | role-match |
| `docs/development.md` | doc | n/a | `docs/airflow-dag.md` ("Live Verification Evidence" section shape) | role-match |
| `docs/benchmark.md` | doc (generated data doc) | n/a | none — new artifact type | no analog — use RESEARCH.md D-05 shape |
| `airflow/dags/csv_ingest.py` (type-annotation fixes for D-14) | DAG/controller | request-response | itself (existing file, mechanical edit only) | exact |

## Pattern Assignments

### `tests/e2e/conftest.py` (test fixtures, request-response)

**Analog:** `tests/integration/conftest.py`

**Imports pattern** (lines 1-17):
```python
from __future__ import annotations

from typing import Iterator

import oracledb
import pytest

from csv_processor import load
```

**Core fixture pattern** (lines 20-30):
```python
@pytest.fixture
def oracle_cursor() -> Iterator[oracledb.Cursor]:
    """A real Oracle cursor -- commits and closes the connection on teardown."""
    connection = load.get_connection()
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        connection.commit()
        cursor.close()
        connection.close()
```

**Cleanup-before-test pattern** (lines 33-47) — reuse this shape for an e2e-scoped
`clean_customers_tables`/`clean_orders_tables` autouse fixture so repeated e2e runs
don't collide with `UNIQUE(dataset, checksum)`:
```python
@pytest.fixture(autouse=True)
def clean_customers_tables(oracle_cursor: oracledb.Cursor) -> None:
    oracle_cursor.execute("DELETE FROM customers_valid")
    oracle_cursor.execute("DELETE FROM customers_invalid")
    oracle_cursor.execute("DELETE FROM ingestion_metadata WHERE dataset = 'customers'")
    oracle_cursor.connection.commit()
```

Add a companion fixture for asserting the DAG stack is reachable before any e2e test
runs (new — no analog; poll `${AIRFLOW_BASE_URL}/api/v2/monitor/health` per
`docker-compose.yml`'s own `airflow-apiserver` healthcheck at line 119, bounded
timeout, fail loudly if never healthy).

---

### `tests/e2e/test_csv_ingest_e2e.py` (test, request-response + event-driven)

**Analog A:** `scripts/trigger_dag.sh` (the REST trigger flow, to be ported to Python
`requests`/`httpx` or shelled out via `subprocess.run`)

**Auth + trigger pattern** (trigger_dag.sh lines 34-58):
```bash
JWT_TOKEN=$(curl -s -X POST "${AIRFLOW_AUTH_TOKEN_URL}" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"${AIRFLOW_USER}\", \"password\": \"${AIRFLOW_PASSWORD}\"}" \
  | jq -r '.access_token')

DAG_RUN_ID=$(curl -s -X POST "${AIRFLOW_TRIGGER_URL}" \
  -H "Authorization: Bearer ${JWT_TOKEN}" -H "Content-Type: application/json" \
  -d "{\"conf\": {\"dataset\": \"${DATASET}\", \"config_path\": \"${CONFIG_PATH}\"}, \"logical_date\": null}" \
  | jq -r '.dag_run_id')
```
Reuse verbatim via `subprocess.run(["scripts/trigger_dag.sh", dataset, config_path], capture_output=True, text=True, check=True).stdout.strip()` — do not re-derive the auth flow in Python (RESEARCH.md "Don't Hand-Roll").

**Deferred-poll pattern** (RESEARCH.md Code Example 2, from `docs/airflow-dag.md`'s
already-proven live-verification curl):
```bash
curl -s -H "Authorization: Bearer ${JWT_TOKEN}" \
  "${AIRFLOW_BASE_URL}/api/v2/dags/csv_ingest/dagRuns/${DAG_RUN_ID}/taskInstances/wait_for_file" \
  | jq -r '.state'
# Poll until "deferred" (bounded timeout) BEFORE writing the fixture file — Pitfall 4.
```

**Analog B:** `tests/integration/test_engine_process_oracle.py` — pytest structure/
assertion style for real-Oracle row-count checks (read this file directly during
planning for the exact `oracle_cursor.execute(...)` / `fetchone()` assertion idiom
used elsewhere in this repo, so the e2e test's Oracle assertions match established
style rather than inventing a new one).

**Error handling / imports convention:** follow `tests/integration/conftest.py`'s
`from __future__ import annotations` + typed fixture signatures.

---

### `benchmark/naive_loader.py` (utility, batch/CRUD — write path only)

**Analog:** `packages/csv-processor/src/csv_processor/load.py::insert_rows`

**Imports pattern** (load.py lines 19-27):
```python
from __future__ import annotations

import os
from pathlib import Path

import oracledb

from csv_processor.config.models import is_safe_identifier
```

**Inverse-of-bulk pattern to implement** (RESEARCH.md Code Example 3, load.py lines
125-139 as the thing being deliberately NOT done — `naive_loader.py` must do the
opposite, one `cursor.execute()` per row, never `executemany()`):
```python
def run_naive(file_path, config, cursor):
    columns = [c.name for c in config.columns]
    sql = f"INSERT INTO {config.oracle.valid_table} ({', '.join(columns)}) " \
          f"VALUES ({', '.join(f':{c}' for c in columns)})"
    for valid_rows, invalid_rows in process_chunks(file_path, config):
        for row in valid_rows:  # D-01: genuinely one execute() per row
            cursor.execute(sql, row)
```
Reuse `load.py`'s `is_safe_identifier()` check pattern (lines 128-134) before
interpolating `table`/`columns` into the naive SQL string, and reuse
`load.get_connection()`/`oracle_dsn()`/`oracle_user()`/`oracle_password()` (lines
59-82) verbatim for connecting — do not re-derive Oracle credentials handling in the
new `benchmark/` package.

---

### `benchmark/run_benchmark.py` (utility, orchestrator)

**Analog A:** `generator/generate_csv.py` — CLI/argparse shape and module docstring
convention.

**Imports/CLI pattern** (generate_csv.py lines 1-34):
```python
from __future__ import annotations

import argparse
...
from pathlib import Path

from csv_processor.config import ColumnSpec, DatasetConfig, load_config

_REPO_ROOT = Path(__file__).resolve().parent.parent
```

**Analog B:** `packages/csv-processor/src/csv_processor/engine.py::process_chunks` —
the exact generator both benchmark write-paths must consume identically (RESEARCH.md
Pattern 2 / Code Example 3):
```python
from csv_processor.engine import process_chunks
from csv_processor import load

def run_bulk(file_path, config, cursor):
    for valid_rows, invalid_rows in process_chunks(file_path, config):
        load.insert_rows(cursor, table=config.oracle.valid_table,
                          columns=[c.name for c in config.columns], rows=valid_rows)
```

**Peak-memory measurement** (RESEARCH.md Pattern 2, stdlib `resource`, run naive/bulk
as two separate subprocess invocations — `ru_maxrss` is process-lifetime, not
resettable):
```python
import resource

def run_and_measure(fn):
    result = fn()
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return result, peak_rss_kb
```

**Error handling:** follow `engine.py::process`'s try/except/finally shape (lines
199-329) for connection lifecycle (open once, close in `finally`) — not required to
replicate the full status-translation logic, just the connection-hygiene pattern.

---

### `scripts/verify_evidence.sql` (SQL script, read-only query)

**Analog:** `docker/oracle/init/02_customers.sql` / `03_orders.sql` — table/column
names to join against (`customers.customer_id`, `customers.country`,
`orders.customer_id`, `orders.order_date`, `orders.amount`) — read these two files
directly during planning for the exact column names/types (this pattern-mapping pass
did not re-read their full DDL; the schema names are already fixed by Phase 1/PROJECT.md
and referenced in CONTEXT.md D-10).

**No close analog for the query itself** (first read-only reporting query in the
project) — construct per D-10's literal spec: join on `customer_id`, group by
`(customers.country, TRUNC(orders.order_date, 'MM'))`, aggregate `COUNT(*)`,
`SUM(amount)`, `AVG(amount)`.

---

### `.github/workflows/ci.yml` and `.github/workflows/readme-summary.yml`

**No analog in this repo** (first GitHub Actions workflow files). Use RESEARCH.md's
own fully-worked YAML in Pattern 3 (Oracle+e2e job reusing `docker-compose.yml`
unmodified) and Pattern 4 (README auto-commit via `stefanzweifel/git-auto-commit-action@v7.2.0`,
default `GITHUB_TOKEN`) verbatim as the starting shape — both already cite this
project's own `docker-compose.yml`/`docs/environment.md` "First-Clone Setup Gaps"
section for the `.env`/`simple_auth_manager_passwords.json.generated` recreation
steps (README.md lines 14-19 — copy this exact two-file bootstrap into the CI step).

---

### `pyproject.toml` mypy/ruff additions

**Analog:** existing `[tool.pytest.ini_options]`/`[tool.uv.sources]`/`[tool.uv.workspace]`
blocks in the same file (lines 30-49) — follow the same flat-TOML-table style, comment
convention (explaining *why*, citing pitfalls/decisions by ID) already used throughout
this file.

**Pattern to add** (RESEARCH.md Code Example 4):
```toml
[tool.mypy]
plugins = ["pydantic.mypy"]
```
Also add `uv add --dev "ruff==0.16.5" "mypy==2.3.1"` to `[dependency-groups] dev`
(currently only `"pytest==9.1.1"`, pyproject.toml line 27).

---

### `Makefile` new targets (`verify-phase6`, `benchmark`, `lint`, `verify-evidence`)

**Analog:** `Makefile`'s existing `verify-phase4`/`verify-phase5` targets (lines
47-49, 57-68) — same "requires `make up` first" comment convention, same
`uv run pytest tests/unit/ -x` first line, phase-specific live check appended after.

**Direct excerpt to mirror structurally:**
```makefile
verify-phase4:     ## Phase 4's own combined local gate: unit + real-Oracle integration suites (requires `make up` first)
	uv run pytest tests/unit/ -x
	uv run pytest tests/integration/ -x
```
`verify-phase6` should follow this exact shape: `uv run pytest tests/unit/ -x` first,
then `uv run pytest tests/e2e/ -x`, then `make lint`/`make benchmark`/`make verify-evidence`
per CONTEXT.md's Claude's-Discretion note. Also update the `.PHONY` line (Makefile
line 1) to add the new target names, matching the existing pattern of listing every
target there.

---

### `README.md` (Executive Summary + doc-links rewrite)

**Analog:** `README.md` itself — its existing "Getting Started" section (lines 7-33)
already establishes the "one fenced bash block + prose + link-out to docs/*.md" style
D-16 asks the rest of the README to follow; its "Notes & Q&A" section (lines 35+)
shows the established heading/prose voice to match when writing the new topic docs.

**Marker-based regenerated-section pattern** (RESEARCH.md Open Question 1
recommendation — plain HTML comment pair, no analog needed, simplest mechanism):
```html
<!-- EXEC-SUMMARY:START -->
...regenerated content...
<!-- EXEC-SUMMARY:END -->
```

---

### `docs/architecture.md`, `docs/configuration.md`, `docs/csv-engine.md`, `docs/oracle.md`, `docs/development.md`

**Analog:** `docs/airflow-dag.md` and `docs/environment.md` (both already exist,
read directly during planning for exact heading structure/voice — this pass
confirmed their existence via `find` but did not re-read their full bodies since
their role is purely structural precedent, already summarized in CONTEXT.md/RESEARCH.md
canonical_refs). Established convention per RESEARCH.md Pitfall 7: `docs/development.md`
must copy `.github/workflows/ci.yml`'s exact `run:` commands verbatim, never paraphrase.

---

### `docs/benchmark.md`

**No analog** — first committed benchmark-results doc. Shape fixed by CONTEXT.md D-05:
comparison table (rows/sec, peak memory, Oracle load time as rows; naive vs.
chunked/bulk as columns), explicit speedup ratio, run metadata, raw per-chunk timing
breakdown for the chunked run.

---

### `airflow/dags/csv_ingest.py` (D-14 type-annotation fixes)

**Analog:** itself — mechanical edit only. RESEARCH.md Pitfall 3 flags the exact
lines needing fixes: `-> dict`/`config_dict: dict` at lines 42, 75, 99, 121 should
become `dict[str, object]`, matching `csv_processor/engine.py`'s own
`list[dict[str, object]]` convention (engine.py line 35) already established
elsewhere in this codebase — read `airflow/dags/csv_ingest.py` directly during
planning to get the exact surrounding signatures before editing.

## Shared Patterns

### Oracle connection lifecycle
**Source:** `packages/csv-processor/src/csv_processor/load.py::get_connection` (lines
78-82) + `oracle_dsn()`/`oracle_user()`/`oracle_password()` (lines 59-76)
**Apply to:** `tests/e2e/conftest.py`, `benchmark/naive_loader.py`,
`benchmark/run_benchmark.py`, `scripts/verify_evidence.sql`'s Python runner (if any)
```python
def get_connection() -> oracledb.Connection:
    return oracledb.connect(user=oracle_user(), password=oracle_password(), dsn=oracle_dsn())
```
Never hardcode `admin`/`admin` directly in new files — always go through these three
env-var-first functions (`ORACLE_DSN`, `ORACLE_APP_USER`, `ORACLE_APP_USER_PASSWORD`).

### Airflow REST auth + trigger flow
**Source:** `scripts/trigger_dag.sh` (lines 34-58)
**Apply to:** `tests/e2e/test_csv_ingest_e2e.py`, `.github/workflows/readme-summary.yml`'s
ingestion-trigger step
Reuse the script directly via subprocess rather than re-implementing the `/auth/token`
→ `Bearer` → `POST .../dagRuns` flow in Python or a new bash script.

### `process_chunks()` as the single shared parse path
**Source:** `packages/csv-processor/src/csv_processor/engine.py` (lines 33-128)
**Apply to:** `benchmark/run_benchmark.py`, `benchmark/naive_loader.py` (both call
this, never re-implement CSV parsing)

### Makefile target composition ("unit suite first, then phase-specific live check")
**Source:** `Makefile` lines 47-49 (`verify-phase4`), 57-68 (`verify-phase5`)
**Apply to:** new `verify-phase6` target

### docstring/comment convention (cite decision IDs, explain "why" not just "what")
**Source:** every existing module in `packages/csv-processor/src/csv_processor/`
(e.g. `load.py` lines 1-17, `engine.py` lines 1-15) and `docker-compose.yml`'s own
inline comments (lines 11-53) explaining each Phase-5-discovered env var gap
**Apply to:** all new files this phase — CI YAML comments, `benchmark/` module
docstrings, `scripts/verify_evidence.sql` header comment should all cite the CONTEXT.md
decision ID (D-01, D-08, D-10, etc.) they implement, matching this repo's established
practice of self-documenting *why* a line exists, not just what it does.

## No Analog Found

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `.github/workflows/ci.yml` | config | event-driven | First GitHub Actions workflow in this repo — use RESEARCH.md Pattern 3 verbatim |
| `.github/workflows/readme-summary.yml` | config | event-driven | First auto-commit workflow — use RESEARCH.md Pattern 4 verbatim |
| `docs/benchmark.md` | doc (data artifact) | n/a | First committed benchmark-results doc — shape fixed by CONTEXT.md D-05, no prior committed benchmark output to copy from |
| `scripts/verify_evidence.sql` (query body itself, not the DDL it joins against) | script | request-response | First read-only reporting/business-report query in this repo — construct per D-10's literal spec |

## Metadata

**Analog search scope:** repo root (excluding `.venv/`, `.git/`, `.planning/`,
`__pycache__/`) — `scripts/`, `tests/`, `packages/csv-processor/src/csv_processor/`,
`Makefile`, `pyproject.toml`, `docker-compose.yml`, `README.md`, `docs/`,
`generator/generate_csv.py`, `airflow/dags/`
**Files scanned:** ~20 directly read; full repo tree enumerated via `find`
**Pattern extraction date:** 2026-08-29
