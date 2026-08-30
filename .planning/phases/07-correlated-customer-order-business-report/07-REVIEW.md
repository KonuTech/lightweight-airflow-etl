---
phase: 07-correlated-customer-order-business-report
reviewed: 2026-08-30T10:45:49Z
depth: standard
files_reviewed: 15
files_reviewed_list:
  - Makefile
  - README.md
  - airflow/dags/_common/oracle_partition_trigger.py
  - airflow/dags/report_ready.py
  - docker-compose.yml
  - docker/oracle/init/05_correlation_constraints.sql
  - docs/benchmark.md
  - generator/generate_csv.py
  - scripts/dag_polling.py
  - scripts/regenerate_readme_summary.py
  - tests/e2e/test_correlated_report_e2e.py
  - tests/e2e/test_report_ready_dag.py
  - tests/integration/test_correlation_constraints.py
  - tests/unit/test_generate_csv.py
  - tests/unit/test_oracle_partition_trigger.py
findings:
  critical: 1
  warning: 6
  info: 1
  total: 8
status: issues_found
---

# Phase 07: Code Review Report

**Reviewed:** 2026-08-30T10:45:49Z
**Depth:** standard
**Files Reviewed:** 15
**Status:** issues_found

## Summary

This phase adds correlated customer/order fixture generation, a DB-level correlation safety net
(PK/index/BEFORE INSERT trigger), a new `report_ready` DAG with a custom deferrable Oracle-polling
trigger/sensor, and a substantial e2e/integration/unit test suite. The implementation is generally
careful — determinism, staging+atomic-rename, idempotency, and the "whole batch fails" DB
constraint are all correctly reasoned through and covered by real (non-mocked) integration/e2e
tests.

The most significant finding is that `OraclePartitionReadyTrigger.run()` — the only mechanism this
phase provides for "wait until both datasets are ready" — has no exception handling around its
Oracle calls. Any transient Oracle error (a blip during a container restart, a momentary
connection-limit condition, etc.) crashes the deferred trigger outright and permanently fails the
`report_ready` DAG's sensor task, with no retry/backoff, despite the whole point of this component
being a long-running, unattended poll loop. Several other robustness/quality gaps are documented
below (no sensor timeout, unchecked trigger event payload, duplicated magic numbers, duplicated SQL
across four locations with no consistency check, a missing `--rows` validation, and an arithmetic
error in `docs/benchmark.md`'s reported percentage-improvement figure).

## Critical Issues

### CR-01: `OraclePartitionReadyTrigger.run()` has no error handling — any transient Oracle failure crashes the deferred sensor

**File:** `airflow/dags/_common/oracle_partition_trigger.py:111-125`
**Issue:** The trigger's poll loop calls `oracledb.connect_async()`, `cursor.execute()`, and
`cursor.fetchone()` on every iteration with zero exception handling:

```python
async def run(self) -> AsyncIterator[Any]:
    while True:
        connection = await oracledb.connect_async(
            user=oracle_user(), password=oracle_password(), dsn=oracle_dsn()
        )
        try:
            cursor = connection.cursor()
            await cursor.execute(_POLL_QUERY)
            (count,) = await cursor.fetchone()
        finally:
            await connection.close()
        if count >= _BOTH_DATASETS_PRESENT:
            yield TriggerEvent({"status": "ready"})
            return
        await asyncio.sleep(self.poke_interval)
```

`ReportReadySensor` defers to this trigger and never occupies a worker slot while waiting — the
entire premise of D-26/D-28 is that this loop runs, unattended, for as long as it takes both
datasets to land. Any single transient failure (Oracle mid-restart, a dropped connection, a brief
`ORA-12170`/connection-timeout, a momentary max-sessions condition) propagates out of the async
generator uncaught. Airflow's triggerer marks the trigger — and therefore the deferred sensor task
— as permanently failed. There is no retry, no backoff, and no distinction between "Oracle is
genuinely down" and "give it a moment and try again." A developer or CI job would have to fully
re-trigger the `report_ready` DAG run to recover, even though the underlying condition (data not
yet arrived) hadn't actually changed. This is a real production-shaped reliability gap in the one
component this phase exists to deliver.

**Fix:** Wrap the per-iteration Oracle work in a `try/except`, log the failure, and continue polling
(with a bound on consecutive failures so a genuinely-broken DB doesn't retry forever):

```python
async def run(self) -> AsyncIterator[Any]:
    consecutive_failures = 0
    while True:
        try:
            connection = await oracledb.connect_async(
                user=oracle_user(), password=oracle_password(), dsn=oracle_dsn()
            )
            try:
                cursor = connection.cursor()
                await cursor.execute(_POLL_QUERY)
                (count,) = await cursor.fetchone()
            finally:
                await connection.close()
        except oracledb.Error:
            consecutive_failures += 1
            if consecutive_failures > _MAX_CONSECUTIVE_FAILURES:
                raise
            _LOGGER.warning("OraclePartitionReadyTrigger poll failed, retrying", exc_info=True)
            await asyncio.sleep(self.poke_interval)
            continue
        consecutive_failures = 0
        if count >= _BOTH_DATASETS_PRESENT:
            yield TriggerEvent({"status": "ready"})
            return
        await asyncio.sleep(self.poke_interval)
```

## Warnings

### WR-01: `ReportReadySensor` has no execution timeout — waits indefinitely

**File:** `airflow/dags/report_ready.py:49`
**Issue:** `ReportReadySensor(task_id="wait_for_both_datasets")` is created with no `timeout`. If
`orders` (or `customers`) never ingests for the current wall-clock day, the deferred trigger polls
forever (every 30s) with no upper bound, unlike Airflow's conventional sensor `timeout=` contract.
Combined with CR-01's lack of failure handling, a DAG run that never resolves also never fails
loudly — it just silently occupies a slot in the triggerer's event loop indefinitely.
**Fix:** Pass an explicit `timeout` (and consider `soft_fail=True` if a missed report should not be
treated as a hard failure), e.g. `ReportReadySensor(task_id="wait_for_both_datasets", timeout=60 * 60 * 6)`.

### WR-02: `execute_complete()` ignores the trigger event payload entirely

**File:** `airflow/dags/_common/oracle_partition_trigger.py:138-139`
**Issue:**

```python
def execute_complete(self, context: object, event: dict[str, Any] | None = None) -> None:
    return
```

This unconditionally returns success regardless of what `event` actually contains. Today the
trigger only ever emits `{"status": "ready"}`, so this happens to be harmless — but it means the
callback performs no validation of the contract it depends on. If the trigger is ever extended to
emit an error/timeout event (a natural next step once CR-01 is fixed), this callback would
silently treat it as success rather than raising.
**Fix:** At minimum assert on the expected shape:
```python
def execute_complete(self, context: object, event: dict[str, Any] | None = None) -> None:
    if event is None or event.get("status") != "ready":
        msg = f"OraclePartitionReadyTrigger fired with unexpected event: {event!r}"
        raise ValueError(msg)
```

### WR-03: `poke_interval` magic number duplicated in the same file; the constructor's default is unreachable

**File:** `airflow/dags/_common/oracle_partition_trigger.py:101,133-134`
**Issue:** `OraclePartitionReadyTrigger.__init__` declares `poke_interval: float = 30.0` as its
default, but the only production caller, `ReportReadySensor.execute()`, always passes an explicit
literal:
```python
self.defer(
    trigger=OraclePartitionReadyTrigger(poke_interval=30),
    method_name="execute_complete",
)
```
The default value is therefore dead in production, and the two literals (`30.0` vs `30`) can drift
independently if only one is ever changed — a maintainer editing the sensor's poll cadence has no
signal that the constructor default silently disagrees.
**Fix:** Define a single module-level constant (e.g. `_DEFAULT_POKE_INTERVAL_SECONDS = 30.0`) and
reference it from both the constructor default and `ReportReadySensor.execute()`.

### WR-04: The business-report SQL is duplicated across four locations with no automated consistency check

**File:** `airflow/dags/report_ready.py:28-40`, `scripts/regenerate_readme_summary.py:98-110`
**Issue:** `_BUSINESS_REPORT_SQL` is hand-copied verbatim into `report_ready.py`,
`regenerate_readme_summary.py`, `scripts/verify_evidence.sql`, and (a variant without `FETCH FIRST`)
`tests/e2e/test_correlated_report_e2e.py`. Every copy carries a comment insisting it must "never be
re-authored" — but nothing enforces that beyond developer discipline; a future edit to one copy (a
column rename, a `GROUP BY` change) can silently diverge from the others with no test failing to
catch it, since each file's own tests only validate against its own embedded copy of the string.
**Fix:** Extract the SQL to a single shared constant (e.g. `airflow/dags/_common/reporting.py`, which
this phase already touches for other reporting helpers) and import it from `report_ready.py`,
`regenerate_readme_summary.py`, and the e2e test, so a change in one place is guaranteed to reach
all three call sites.

### WR-05: `generator/generate_csv.py --rows` has no validation — negative values are silently accepted and produce empty output

**File:** `generator/generate_csv.py:377`
**Issue:** `--invalid-ratio` is validated via `_ratio_type` (raising `argparse.ArgumentTypeError`
outside `[0.0, 1.0]`), but `--rows` has no equivalent check:
```python
parser.add_argument("--rows", type=int, default=100, help="Number of data rows to generate.")
```
Passing `--rows -5` doesn't error — `range(-5)` is empty, so `generate_rows()` silently returns zero
rows (only a header line gets written) instead of failing loudly. A user who mistypes a negative
value gets a confusingly "successful" run with an unexpectedly empty fixture rather than a clear
error message.
**Fix:** Add a small validator mirroring `_ratio_type`:
```python
def _rows_type(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        msg = f"--rows must be >= 0, got {parsed}"
        raise argparse.ArgumentTypeError(msg)
    return parsed
```
and use `type=_rows_type` for `--rows`.

### WR-06: `docs/benchmark.md`'s "6,741% improvement" figure is arithmetically wrong

**File:** `docs/benchmark.md:78`
**Issue:**
```
The chunked/bulk `executemany()` path is **~67.41× faster** (a **6,741% improvement**) than the
```
A ratio of 67.41× corresponds to a **percent improvement** of `(67.41 - 1) * 100 ≈ 6,641%`, not
6,741%. The document appears to have computed `67.41 * 100` directly (which is "6,741% of the
original value", i.e. what the new value *is* relative to the baseline) rather than subtracting the
baseline 100% first to get the actual improvement. The `×` figure itself (line 75-76,
`184206.91 / 2732.81 = 67.41×`) is correct; only the derived percentage in the prose is off by
~100 points.
**Fix:** Correct the prose to `(a ~6,641% improvement)`, or drop the percentage framing entirely and
rely solely on the already-correct `×` figure to avoid this class of off-by-100-points error
recurring on a future re-run of the benchmark.

## Info

### IN-01: `PYTHONPATH=/opt/airflow/dags` is set for every Airflow container, though only the triggerer needs it

**File:** `docker-compose.yml:80`
**Issue:** The accompanying comment explains this env var exists solely so the **triggerer**
process can `importlib.import_module()` a deferred trigger's classpath
(`_common.oracle_partition_trigger...`), since (unlike DAG parsing) the triggerer never gets
`/opt/airflow/dags` added to `sys.path` automatically. As written, though, the var lives in the
shared `x-airflow-common-env` anchor, so it's also injected into `airflow-apiserver`,
`airflow-scheduler`, `airflow-dag-processor`, and `airflow-init` — none of which need it, per the
comment's own reasoning. This is harmless today (no name collisions exist under `airflow/dags/`)
but broadens the blast radius of any future accidental top-level module name collision (e.g. a
future `dags/typing.py` or `dags/json.py` shadowing a stdlib/Airflow-internal module) to every
container instead of just the one that actually requires the path.
**Fix:** Move `PYTHONPATH: "/opt/airflow/dags"` out of `x-airflow-common-env` and set it only on the
`airflow-triggerer` service's own `environment:` block.

---

_Reviewed: 2026-08-30T10:45:49Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
