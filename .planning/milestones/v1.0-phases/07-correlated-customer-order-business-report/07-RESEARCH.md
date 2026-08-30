# Phase 7: Correlated Customer-Order Business Report - Research

**Researched:** 2026-08-30
**Domain:** Deterministic correlated fixture generation, Oracle DDL/PL-SQL constraints, Airflow 3.3.1 custom deferrable sensors
**Confidence:** MEDIUM-HIGH (core mechanisms verified against official docs and this project's own code; a few sequencing details are architecture decisions, not facts, and are flagged as such)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** `orders.customer_id` samples ONLY from customer rows that will land in
  `customers_valid` (not the full generated pool) — guarantees every non-corrupted order can join.
- **D-02:** Sampling is **with replacement** — one customer can have multiple orders.
- **D-03:** Sampling is **Zipf-weighted** (rank customers by pool order, weight ∝ 1/rank) — a few
  "power customers" get disproportionately more orders. Non-default choice (uniform was the
  recommended default).
- **D-04:** If the valid-customer pool is empty when generating orders, **raise an error
  immediately** rather than falling back to independent random generation.
- **D-05:** Given the same `--seed`, the full correlated dataset must be **fully deterministic and
  byte-identical** across runs. The pool-sampling/Zipf RNG draws from the same seeded
  `random.Random(seed)` instance already used elsewhere in `generate_csv.py` — no new entropy
  source.
- **D-06:** `customer_id` moves to a structured ID format: `CUST-{seed_hash}-00001` (prefix +
  seed-derived component + zero-padded sequential number).
- **D-07:** The seed-derived component is load-bearing for correctness, not cosmetic: Oracle tables
  accumulate across runs (no truncate/upsert), so resetting numbering to 1 every run would
  reintroduce duplicate `customer_id` values already in `customers_valid`, fanning out the JOIN.
  Reversibility: costly (changes ID shape until tables are reset, see D-19).
- **D-08:** `order_id` (not part of the correlation bug) is *also* given the same structured
  treatment for consistency: `ORD-{seed_hash}-00001`.
- **D-09:** The required regression test is **both** a fast unit test AND a live e2e test.
  Unit test: `orders.customer_id ⊆` valid-customers pool + Zipf-weighting property + determinism.
  Live e2e test: ingests both datasets for real and asserts the actual Oracle JOIN returns ≥1 row.
- **D-10:** The live e2e test lives in a **new, separate test file** (not appended to
  `tests/e2e/test_csv_ingest_e2e.py`).
- **D-11:** The new e2e test is wired into the **required, blocking `oracle-e2e` CI check**.
- **D-12:** The e2e test explicitly **backdates some inserted rows across multiple days**
  (bypassing `ingested_at`'s `SYSDATE` default just for this test) to prove the JOIN/report
  aggregates correctly across partition boundaries.
- **D-13:** Add a plain (non-unique) **index on `customer_id`** in both `customers_valid` and
  `orders_valid`.
- **D-14 (non-default):** Add an actual **PRIMARY KEY constraint** — `PRIMARY KEY (customer_id)` on
  `customers_valid`, `PRIMARY KEY (order_id)` on `orders_valid`. This is a DB-DDL-layer decision,
  distinct from PROJECT.md's existing "no uniqueness validation" decision (application-level CSV
  engine validators). Reversibility: one-way — requires a DDL migration and, practically, a
  `make reset` since existing data may already violate the constraint.
- **D-15 (non-default):** Add an **INSERT trigger on `orders_valid`** that validates the inserted
  `customer_id` exists in `customers_valid`, as a DB-level safety net on top of the Python-side
  correlation (D-01–D-05) — not a replacement for it.
- **D-16:** On trigger violation, the **whole batch/chunk fails** (matches Oracle's default
  `executemany()` behavior, no `batcherrors`-style partial-success handling).
- **D-17:** Existing daily interval partitioning is **already sufficient** — no new partitioning
  DDL needed.
- **D-18:** PK / index / trigger apply **only to `customers_valid`/`orders_valid`** — NOT to
  `customers_invalid`/`orders_invalid`.
- **D-19:** Applying the new DDL to an existing dev environment requires **`make reset` + `make
  up`** (full wipe so init scripts run fresh).
- **D-20:** Re-run and update `docs/benchmark.md`'s existing 182.85× speedup figure given the added
  trigger overhead.
- **D-21:** Correlation logic is **centralized in one shared function** (e.g.
  `generate_correlated_datasets(...)` in `generator/generate_csv.py`) that every call site uses —
  `make generate`, `scripts/regenerate_readme_summary.py`'s loop, and the new e2e test's fixture
  setup.
- **D-22:** `make generate` changes to a **new combined CLI mode** (e.g. `--correlated`, or a small
  new orchestrator) that generates both datasets together in one process. Reversibility: costly
  (changes the CLI's public interface).
- **D-23:** Generate both correlated CSVs first (via D-21's shared function), THEN
  trigger/drop each into its own dataset's watched directory.
- **D-24:** File-drop mechanism: write to a **staging path** (e.g. `data/<dataset>/.staging/
  <filename>`, same bind-mounted volume), then **atomically `Path.rename()`** into the watched
  path.
- **D-25:** This mechanism is proven against the **real, live Airflow stack** — not mocked.
- **D-26 (major scope addition, non-default):** Add a **new Airflow DAG** that senses when both
  `customers_valid` and `orders_valid` have data for the current (real) partition, then
  builds/logs the business report. Reversibility: reversible (new DAG file only).
- **D-27:** The new DAG runs **alongside** `regenerate_readme_summary.py`'s CI-triggered path, not
  replacing it.
- **D-28:** Detection mechanism: a **deferrable sensor task polls Oracle directly** (queries
  `ingestion_metadata` for both `customers`/`orders` having a row for the current partition).
- **D-29 (grounded via Context7/Airflow docs):** The sensor determines "today's partition" via the
  **real wall-clock date directly** (`TRUNC(ingested_at) = TRUNC(SYSDATE)` in Oracle, or Pendulum
  `now()` in Python) — NOT via `logical_date`/`data_interval` arithmetic, since this project's DAGs
  are always manually/API-triggered.
- **D-30:** REQUIREMENTS.md gets **new REQ IDs** during planning rather than being force-fit against
  the existing `DOC-01`/`TEST-03`.

### Claude's Discretion

- Exact `--seed`-derived hash/component used in `CUST-{seed_hash}-00001` (short hex digest, base36
  encoding, etc.).
- Exact zero-padding width for the sequential ID suffix.
- Exact new REQ ID naming/numbering (D-30) — follow REQUIREMENTS.md's existing convention.
- Exact shape of the new report DAG's task graph (single sensor + single report task vs. more
  granular breakdown).
- Whether the new combined CLI mode (D-22) is a new flag on `generate_csv.py` or a small separate
  orchestrator script.

### Deferred Ideas (OUT OF SCOPE)

- **Broad PL/SQL migration** — "move as much data logic and data processing to Oracle PL/SQL from
  Python, whenever possible." Explicitly deferred as its own future architectural direction;
  contradicts this project's shipped "thin Python engine" architecture.
- **Airflow UI logs not showing at :8080** — unrelated live operational bug, deferred to a separate
  investigation (e.g. `/gsd-debug`) after Phase 7.

</user_constraints>

<phase_requirements>
## Phase Requirements

No REQ IDs were provided by the orchestrator — D-30 explicitly defers exact ID assignment to
planning. The table below is a **candidate mapping only** (Claude's discretion per D-30), following
REQUIREMENTS.md's existing `<AREA>-<NN>` convention. The planner should treat these as a starting
point, not a locked contract.

| Candidate ID | Description | Research Support |
|----|-------------|------------------|
| DATA-01 | `orders.customer_id` sampled with replacement, Zipf-weighted, from the `customers_valid`-bound pool, fully deterministic per seed (D-01–D-05) | See "Zipf-Weighted Deterministic Sampling" pattern below; `random.choices()` verified against Python 3 official docs |
| DATA-02 | `customer_id`/`order_id` move to seed-derived structured IDs, collision-safe across accumulating runs (D-06–D-08) | See "Seed-Derived Structured IDs" pattern; VARCHAR2(64) width confirmed via `docker/oracle/init/02_customers.sql`/`03_orders.sql` |
| GEN-02 | Correlation logic centralized in one shared function used by every call site; `make generate` becomes one combined invocation (D-21, D-22) | See "Pitfall 1: RNG-instance reuse across `generate_rows()` calls" — this is the load-bearing refactor risk |
| DB-01 | PK + non-unique index on `customer_id`/`order_id` in `_valid` tables only (D-13, D-14, D-18) | DDL pattern below; existing `docker/oracle/init/` numbering convention confirmed (`01`→`04`, new file is `05_*.sql`) |
| DB-02 | `orders_valid` BEFORE INSERT trigger validating `customer_id` exists in `customers_valid`; whole-batch-fails on violation (D-15, D-16) | Verified via python-oracledb official docs: default `executemany()` (no `batcherrors=True`) rolls back the whole transaction on any row error — matches D-16 exactly |
| TEST-05 | Unit test: pool-subset, Zipf-weighting, and determinism assertions for the shared correlation function (D-09) | See "Code Examples" below |
| TEST-06 | Live e2e test (new file): real ingestion of both datasets, JOIN returns ≥1 row, multi-day backdated partitions, wired into required `oracle-e2e` CI (D-09–D-12) | `.github/workflows/ci.yml`'s `oracle-e2e` job runs `pytest tests/e2e/ -x` (whole dir) — no CI config change needed for a new file in that dir |
| INFRA-04 | Staging-dir + atomic `Path.rename()` file-drop mechanism, proven live (D-23–D-25) | See "Staging + Atomic Rename" pattern; confirmed same-filesystem (native WSL2 path, not `/mnt/c`) |
| DAG-06 | New report-sensing DAG with a custom Oracle-polling deferrable sensor/trigger, wall-clock-date based (D-26–D-29) | Verified: `apache-airflow-providers-oracle==4.6.2` has **no** sensor or deferrable operator at all (only `SQLExecuteQueryOperator`) — a custom `BaseTrigger` is required, not optional |
| BENCH-01 | Re-run and update `docs/benchmark.md`'s 182.85× figure given trigger overhead (D-20) | `benchmark/naive_loader.py`/`benchmark/run_benchmark.py` reusable as-is; only the schema underneath changed |
| DOC-02 | README Executive Summary business-report table reflects genuine non-empty results; `docs/oracle.md`/`docs/csv-engine.md` corrected if stale (Success Criterion 4) | `scripts/regenerate_readme_summary.py` requires no code change — it already mirrors `verify_evidence.sql` verbatim and will show real rows once DATA-01 lands |

</phase_requirements>

## Summary

This phase's real complexity is not the headline bug (disjoint Faker word pools) but the fact that
fixing it correctly requires touching **five independent layers that currently assume mutual
independence**: the generator's RNG/Faker sequencing, the CLI's public interface, Oracle DDL
(constraints that didn't exist before), a wholly new Airflow trigger class (because the Oracle
provider ships none), and three already-passing test suites whose fixtures currently rely on
`orders_valid` never seeing a real FK relationship.

The single most important non-obvious finding from this research: **`generate_rows()`'s current
signature is incompatible with D-05's determinism requirement once orders sampling must draw from
the SAME RNG stream a preceding customers-generation call already advanced.** Today `generate_rows()`
creates its own fresh `random.Random(seed)` and `Faker.seed(seed)` internally, every call, from
scratch. If `generate_correlated_datasets()` simply calls `generate_rows(customers_config, ...,
seed)` then separately `generate_rows(orders_config, ..., seed)`, both calls re-seed to the *same*
starting state — which is still technically deterministic (same seed → same output, satisfying the
literal words of D-05) but does NOT mean "the orders RNG continues from where customers left off."
Whether D-05 intends "continue the same live RNG object" (requiring a signature change: `rng`/`fake`
become injectable parameters, not internally constructed) or "reuse the same seed value each time"
(requiring no signature change) is an architecture decision the planner must make explicit, because
it changes `generate_rows()`'s public contract and several existing tests call it directly with
today's four-argument signature (`config, rows, invalid_ratio, seed`). See Pitfall 1.

The second major finding: **`apache-airflow-providers-oracle==4.6.2` (already pinned in this
project) provides no sensor and no deferrable operator whatsoever** — confirmed against the
official provider docs' operators page (only `SQLExecuteQueryOperator` is listed) and against
Airflow's own "Supported Deferrable Operators" reference (Oracle is absent). D-28's custom
Oracle-polling sensor is not an implementation convenience — it is the *only* way to get this
capability; there is no existing operator to wrap or subclass.

The third: **Oracle's `executemany()` default behavior (no `batcherrors=True`) already does exactly
what D-16 assumes** — a single row-level trigger exception aborts the entire batch/transaction,
verified against python-oracledb's own official batch-statement documentation. No special handling
is needed in `csv_processor.load.insert_rows()` to get D-16's "whole batch fails" behavior; it is
already the default and the function already lets `oracledb.DatabaseError` propagate uncaught.

**Primary recommendation:** Treat this phase as an ordered dependency chain, not five parallel
workstreams: (1) decide and implement the RNG-continuation contract for the shared correlation
function first — everything else (ID format, Zipf sampling, e2e tests) depends on its exact
determinism semantics; (2) land the Oracle DDL (PK/index/trigger) behind a `make reset`-gated
migration script, since it is a one-way, existing-data-breaking change; (3) build the custom
`BaseTrigger`/sensor last, using `oracledb.connect_async()` (already available in the pinned
`oracledb==4.0.2`, thin mode) to avoid blocking the triggerer's asyncio event loop.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Correlated ID pool generation / Zipf sampling | Application (generator, host-side Python) | — | Pure in-process data generation, no I/O; must run before any file lands in a watched dir (D-23) |
| Structured ID format (seed-hash derivation) | Application (generator) | — | Deterministic string construction, no DB round trip needed |
| FK-existence enforcement | Database / Storage (Oracle trigger) | Application (generator, primary correlation guarantee) | D-15 explicit: DB trigger is a *safety net*, not the source of correctness — Python still generates the correlation before Oracle ever sees the rows |
| PK / index enforcement | Database / Storage | — | DDL-only concern, orthogonal to the CSV engine's own application-level validators (PROJECT.md's existing "no uniqueness validation" scope is about `csv_processor`, not Oracle DDL) |
| Staging + atomic file drop | Application (generator, host-side) | Filesystem / Storage (shared bind mount) | Both generator (host) and Airflow (container) read/write the same bind-mounted directory tree — no network boundary, so atomicity is a plain POSIX `rename(2)` guarantee, not a distributed-systems problem |
| Report-sensing (poll for both datasets' partition data) | API / Backend (Airflow triggerer, custom `BaseTrigger`) | Database / Storage (Oracle, source of truth polled) | D-29: polls Oracle directly rather than trusting Airflow's own cross-DAG run-tracking state — Oracle is the tier that owns "has data arrived," Airflow only owns "when do we check" |
| Business report build/log | API / Backend (Airflow DAG task) | Database / Storage (query source) | New DAG's report task is a thin SQL-query-and-log step, same shape as the existing `regenerate_readme_summary.py`'s evidence-query pattern — no new query logic, reuse `scripts/verify_evidence.sql`'s SQL text |
| Benchmark re-measurement | Application (benchmark harness) | Database / Storage (target) | No architectural change — same harness, same schema-under-test, now with the trigger overhead included |

## Standard Stack

### Core

No new external packages are introduced by this phase. Every capability is achievable with what is
already pinned in this project.

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `random` (stdlib) | Python 3.12 | Zipf-weighted sampling with replacement via `random.Random.choices(population, weights=..., k=...)` | Official stdlib API, documented since Python 3.6; already the project's own randomness primitive (`generate_csv.py`'s existing `rng = random.Random(seed)`) [CITED: docs.python.org/3/library/random.html] |
| `hashlib` (stdlib) | Python 3.12 | Seed→short-hash derivation for structured IDs (`CUST-{seed_hash}-00001`) | Already used identically in this project for file checksums (`csv_processor.load.sha256_file`) — same discipline, no new dependency |
| `oracledb` | `4.0.2` (already pinned) | `connect_async()`/`AsyncConnection` for the new custom Oracle-polling trigger's `async def run()` | [VERIFIED: python-oracledb official docs via Context7] confirms thin-mode asyncio support (`oracledb.connect_async`, `await cursor.execute(...)`) — required so the trigger does not block the triggerer's shared asyncio event loop; already pinned in `docker/airflow/Dockerfile` line 12, no version bump needed (async support has existed since python-oracledb 2.x, well below the pinned 4.0.2) |
| `airflow.triggers.base.BaseTrigger` | Airflow 3.3.1 (already pinned) | Base class for the new custom deferrable Oracle-polling sensor | [VERIFIED: apache/airflow official docs via Context7] `deferring.rst`'s "Create a Custom Trigger" pattern — `__init__`, `serialize()`, `async def run()` yielding `TriggerEvent` |
| `airflow.sdk.BaseSensorOperator` | Airflow 3.3.1 (already pinned) | The sensor operator that calls `self.defer(trigger=..., method_name="execute_complete")` | [VERIFIED: apache/airflow official docs via Context7] "Implementing a Simple Deferrable Sensor" — same shape as this project's existing `FileSensor(deferrable=True)` usage in `airflow/dags/csv_ingest.py` |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Custom `BaseTrigger` for Oracle polling | `apache-airflow-providers-oracle`'s built-in sensor | **Does not exist.** [CITED: airflow.apache.org/docs/apache-airflow-providers-oracle/stable/operators.html] — the Oracle provider ships exactly one operator (`SQLExecuteQueryOperator`), no sensor, no deferrable class of any kind. [CITED: airflow.apache.org/docs/apache-airflow-providers/core-extensions/deferrable-operator-ref.html] confirms Oracle is entirely absent from Airflow's own "Supported Deferrable Operators" reference. |
| `random.choices()` for Zipf weighting | `numpy.random.zipf()` | numpy's Zipf sampler draws from the theoretical Zipf *distribution* (unbounded integer support, needs post-hoc clamping/mapping to a finite customer pool) and would be a new dependency this project doesn't otherwise need; `random.choices()` with manually computed `weight[i] = 1/(rank)` against the *actual* finite customer-ID population is simpler, matches D-03's literal wording ("rank customers by pool order, weight ∝ 1/rank"), and reuses the exact RNG instance already in scope — no new package |
| `hashlib.sha256(...).hexdigest()[:N]` for seed hash | Base36 encoding of the raw seed int | Both are "Claude's Discretion" per CONTEXT.md. A hex slice is simpler (no custom base-N encoder needed) and Python has no stdlib base36 function — would require hand-rolling one. Hex digest is the lower-complexity choice for a purely cosmetic uniqueness token. |
| DB trigger validating existence via `SELECT COUNT(*)` | A `NOT NULL` foreign key constraint (`REFERENCES customers_valid(customer_id)`) | A real `FOREIGN KEY` constraint is the more idiomatic Oracle mechanism for "must exist in another table" and would need no trigger at all — **flag this to the planner as a simpler alternative to D-15's trigger**, since D-15/D-16 describe building a hand-rolled existence check that a native constraint already provides. This may be a case where the locked decision (trigger) should be revisited, or where the trigger is genuinely wanted for a reason (e.g. custom error message) not captured in CONTEXT.md — the planner should surface this tradeoff explicitly rather than silently building the more complex option. |

**Installation:** None required — every capability uses already-pinned dependencies.

**Version verification:** `oracledb==4.0.2` and `apache-airflow-providers-oracle==4.6.2` confirmed
already installed via `docker/airflow/Dockerfile` (read this session, lines 12-15). No new `npm
view`/`pip index versions` check applies since no new package is introduced.

## Package Legitimacy Audit

**No new external packages are introduced by this phase.** Every capability (Zipf sampling,
seed-hash derivation, Oracle async polling, custom Airflow trigger) uses libraries already pinned
in `docker/airflow/Dockerfile` (`oracledb==4.0.2`, `apache-airflow-providers-oracle==4.6.2`,
`apache-airflow-providers-standard==1.17.0`) or the Python standard library (`random`, `hashlib`,
`asyncio`). The Package Legitimacy Gate is not applicable — no `npm view`/`pip index versions`/
registry check was run since there is nothing new to check.

**Packages removed due to [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none

## Architecture Patterns

### System Architecture Diagram

```
generator/generate_csv.py (host, WSL2 native FS)
  │
  │ 1. generate_correlated_datasets(customers_cfg, orders_cfg, seed)
  │    ┌─────────────────────────────────────────────────────────┐
  │    │ ONE random.Random(seed) instance, ONE Faker(seed)        │
  │    │  a) generate customers rows (structured CUST-… ids)      │
  │    │  b) extract valid-customer-id pool (rows NOT destined    │
  │    │     for customers_invalid per applicable_categories())   │
  │    │  c) Zipf-weight the pool (weight ∝ 1/rank)               │
  │    │  d) rng.choices(pool, weights=zipf_weights, k=n_orders)  │
  │    │  e) generate orders rows using sampled customer_ids +    │
  │    │     structured ORD-… ids                                │
  │    └─────────────────────────────────────────────────────────┘
  │
  │ 2. write both CSVs to data/<dataset>/.staging/<file>  (D-24)
  │
  ▼
Path.rename(staging_path, watched_path)   ── atomic, same bind mount, same FS (D-24/D-25)
  │
  ▼
data/customers/customers_*.csv   data/orders/orders_*.csv   (host, bind-mounted read-only-ish
                                                               into Airflow containers)
  │                                    │
  ▼                                    ▼
csv_ingest DAG (existing, unchanged) triggered per dataset via REST API (dag_polling.py)
  wait_for_file (deferrable FileSensor, non-recursive glob — .staging/ subdir invisible to it)
  → process_csv_task → csv_processor.engine.process() → load.insert_rows()
                                                            │
                                                            ▼
                                          customers_valid / orders_valid (Oracle)
                                          ┌──────────────────────────────────┐
                                          │ orders_valid: BEFORE INSERT       │
                                          │ FOR EACH ROW trigger validates    │
                                          │ :NEW.customer_id EXISTS in        │
                                          │ customers_valid (D-15/D-16 —      │
                                          │ safety net, batch fails on        │
                                          │ violation via default executemany)│
                                          └──────────────────────────────────┘
                                                            │
                          ┌─────────────────────────────────┴──────────────────────┐
                          ▼                                                        ▼
        scripts/verify_evidence.sql / regenerate_readme_summary.py     NEW report-sensing DAG
        (existing, CI-triggered on push to main — D-27, untouched)     (D-26)
                                                                          custom BaseTrigger polls
                                                                          Oracle every N seconds via
                                                                          oracledb.connect_async():
                                                                          SELECT EXISTS(...) FROM
                                                                          ingestion_metadata WHERE
                                                                          dataset IN ('customers',
                                                                          'orders') AND
                                                                          TRUNC(processed_at) =
                                                                          TRUNC(SYSDATE) [and both
                                                                          present] → TriggerEvent
                                                                          → build/log report (D-28/D-29)
```

### Recommended Project Structure

No new top-level directories. New files fit the existing layout:

```
generator/
├── generate_csv.py          # gains generate_correlated_datasets() (D-21), structured-ID helpers
docker/oracle/init/
├── 05_correlation_constraints.sql   # new — PK/index/trigger (D-13/D-14/D-15), follows 01-04 numbering
airflow/dags/
├── csv_ingest.py             # unchanged
├── report_ready.py           # new — D-26's sensing DAG (name is Claude's Discretion)
├── _common/
│   └── oracle_partition_trigger.py   # new — the custom BaseTrigger (D-28), kept in _common/
│                                       #   alongside the existing FileSensor pattern's home
tests/unit/
├── test_generate_csv.py      # extended with correlation/Zipf/determinism assertions (D-09)
tests/e2e/
├── test_csv_ingest_e2e.py    # UNCHANGED (D-10)
├── test_correlated_report_e2e.py   # new, separate file (D-09/D-10)
```

### Pattern 1: Zipf-Weighted Deterministic Sampling With Replacement

**What:** Rank the valid-customer-ID pool in generation order, assign `weight[i] = 1 / (i + 1)`,
then draw `k` order IDs with replacement using the project's existing seeded RNG instance.

**When to use:** Exactly D-01–D-05's ID-pool composition requirement.

**Example:**
```python
# Source: Python 3 official docs (docs.python.org/3/library/random.html#random.choices),
# confirmed this session via WebFetch.
def zipf_weighted_sample(
    rng: random.Random, pool: list[str], k: int
) -> list[str]:
    """D-03: weight ∝ 1/rank, rank = pool order (1-indexed). D-02: with replacement."""
    weights = [1.0 / (rank + 1) for rank in range(len(pool))]
    return rng.choices(pool, weights=weights, k=k)
```

`random.choices()` is a plain method call on an existing `random.Random` instance — it draws from
that instance's own internal state and advances it, exactly like every other call already made
against `rng` in `generate_csv.py` (`rng.randint`, `rng.choice`, `rng.sample`). No new entropy
source, no global `random` module state touched (matches D-05's "no new entropy source" and the
project's established two-stream discipline of never letting Faker's and `random.Random`'s draws
interleave unpredictably).

### Pattern 2: Seed-Derived Structured IDs

**What:** `CUST-{8-hex-char sha256 of seed}-{zero-padded sequence}`.

**Example:**
```python
import hashlib

def seed_component(seed: int, length: int = 8) -> str:
    """Deterministic, seed-derived, short -- mirrors the project's own
    sha256_file() discipline in csv_processor.load (hash, then truncate)."""
    return hashlib.sha256(str(seed).encode("utf-8")).hexdigest()[:length]

def structured_id(prefix: str, seed: int, sequence: int, width: int) -> str:
    return f"{prefix}-{seed_component(seed)}-{sequence:0{width}d}"

# structured_id("CUST", 20260101, 1, width=5) -> "CUST-<8hex>-00001"
```

**Length arithmetic (verified against actual DDL this session):**
`docker/oracle/init/02_customers.sql:12` — `customer_id VARCHAR2(64) NOT NULL` — and
`docker/oracle/init/03_orders.sql:12-13` — `order_id VARCHAR2(64) NOT NULL, customer_id
VARCHAR2(64) NOT NULL`. Worst case at 8-hex-char hash + 6-digit sequence (covers row counts up to
999,999, well above this project's ~100K-row benchmark ceiling): `"CUST-"` (5) + 8 + `"-"` (1) + 6
= 20 characters — well inside `VARCHAR2(64)`. No length pitfall regardless of exact hash-length
choice made under Claude's Discretion.

### Pattern 3: Custom Deferrable Trigger Polling Oracle (No Existing Operator to Reuse)

**What:** A `BaseTrigger` subclass whose `async def run()` polls Oracle directly using
`oracledb.connect_async()` and yields exactly one `TriggerEvent` once both datasets have a row for
today's wall-clock partition.

**Verified need:** [CITED: airflow.apache.org/docs/apache-airflow-providers-oracle/stable/operators.html]
— the Oracle provider's entire operator surface is `SQLExecuteQueryOperator`; no sensor, no
`BaseTrigger` subclass, nothing deferrable. [CITED: airflow.apache.org/docs/apache-airflow-providers/
core-extensions/deferrable-operator-ref.html] confirms Oracle does not appear anywhere in Airflow's
official list of providers with deferrable support. This is not an implementation shortcut — it is
the only path.

**Why `connect_async()`, not the plain synchronous `connect()`:** The triggerer runs a single
shared asyncio event loop serving every deferred task across every DAG. A blocking `oracledb.connect()`
/ `cursor.execute()` call inside `async def run()` would stall that entire event loop for every other
deferred trigger, not just this one. [VERIFIED: python-oracledb official docs via Context7] confirm
thin-mode `oracledb.connect_async()`/`AsyncConnection`/`await cursor.execute(...)` is supported and
is the documented pattern for asyncio integration.

**Example (sensor half, verified pattern shape via Context7 apache/airflow docs):**
```python
# Source: airflow-core/docs/authoring-and-scheduling/deferring.rst
# ("Implementing a Simple Deferrable Sensor" + "Create a Custom Trigger"), Context7 /apache/airflow
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

**Example (trigger half — polling loop shape):**
```python
# Source: pattern shape from Context7 /apache/airflow "Create a Custom Trigger"
# + python-oracledb asyncio docs (Context7 /oracle/python-oracledb).
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
        # D-29: real wall-clock date, never logical_date/data_interval.
        query = (
            "SELECT COUNT(DISTINCT dataset) FROM ingestion_metadata "
            "WHERE dataset IN ('customers', 'orders') "
            "AND TRUNC(processed_at) = TRUNC(SYSDATE)"
        )
        while True:
            connection = await oracledb.connect_async(
                user=..., password=..., dsn=...  # same env-var-first pattern as csv_processor.load
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

### Pattern 4: Staging + Atomic Rename

**What:** Write the generated CSV to `data/<dataset>/.staging/<filename>`, then
`Path.rename(staging_path, watched_path)`.

**Verified atomicity conditions (this project's specific environment, read this session):**
- The working directory (`/home/user/projects/lightweight-airflow-etl`) is a **native WSL2 Linux
  filesystem path**, not a Windows-host `/mnt/c/...` path. [CITED: docker.com "Docker Desktop: WSL 2
  Best Practices"] — bind mounts of a WSL2-native directory share the same kernel/VFS cache between
  the WSL2 distro and the `docker-desktop` utility VM; this avoids the separate,
  documented-slower/less-atomicity-guaranteed cross-boundary case of mounting a `/mnt/c/...` path.
- `docker-compose.yml:74` mounts `./data:/opt/airflow/data` as a **single** bind mount — confirmed
  by reading the file this session. Both `data/<dataset>/.staging/` and `data/<dataset>/` live
  **inside that same single mounted tree**, so the rename never crosses a mount-point boundary —
  it is a same-filesystem `rename(2)` on both the host side (where the generator writes) and the
  container side (where Airflow's `FileSensor` reads), because they are, at the OS level, the exact
  same inode-backed directory.
- `FileSensor`'s `filepath` glob (`customers_*.csv*` / `orders_*.csv*`, no `**`) is **non-recursive
  by default** — [CITED: airflow.apache.org providers-standard FileSensor docs] "the `recursive`
  parameter, when set to `True`, enables recursive directory matching (`**`)" — confirms the sensor
  will not descend into a `.staging/` subdirectory unless the DAG explicitly opts in, which it does
  not (verified: `airflow/dags/csv_ingest.py`'s `FileSensor(...)` call, read this session, sets no
  `recursive` kwarg).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Zipf-like weighted sampling | A custom cumulative-distribution sampler | `random.Random.choices(population, weights=..., k=...)` | Stdlib since Python 3.6, handles weight normalization and edge cases (zero weights, single-element pools) correctly; the project's own RNG instance already in scope |
| Detecting "does row exist in another table" at insert time | A Python-side pre-check query before every `executemany()` call | The Oracle `BEFORE INSERT` trigger (D-15) — already the locked decision | A trigger is DB-native, always enforced regardless of which code path inserts, and doesn't add a round-trip to the hot bulk-insert path; **but see the Alternatives Considered table above** — a native `FOREIGN KEY` constraint would achieve the same guarantee with even less hand-rolled code than a trigger, and is worth flagging to the planner as a simpler option than what's locked |
| Async DB polling loop primitives | Home-rolled retry/backoff/connection-pool logic for the new trigger | `oracledb.connect_async()` + a plain `asyncio.sleep(poke_interval)` loop, matching the existing `TimeDeltaTrigger`/`FileSensor` deferred-trigger shape this project already runs | Airflow's own trigger contract (`async def run()` yielding `TriggerEvent`) already provides serialization, resumption, and triggerer-side scheduling — reinventing any of that inside the trigger body would duplicate framework responsibility |
| Base36/base62 seed-hash encoding | A hand-rolled base-N encoder | `hashlib.sha256(...).hexdigest()[:N]` (Claude's Discretion, simplest option) | Python has no stdlib base36 encoder; a hex slice needs zero new code and is already the project's established hashing discipline (`csv_processor.load.sha256_file`) |

**Key insight:** every "hard" piece of this phase (weighted sampling, async polling, batch-failure
semantics) already has a documented, in-scope, no-new-dependency answer. The actual risk in this
phase is not missing library support — it's the RNG-continuation sequencing decision (Pitfall 1)
and the existing-test breakage from changing `generate_rows()`'s public contract (Pitfall 2).

## Runtime State Inventory

This phase is not a rename/refactor/migration phase in the "change a name everywhere" sense, but it
does introduce schema-breaking DDL and a public CLI-interface change, so the same discipline
applies:

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | Existing `customers_valid`/`orders_valid` rows accumulated by every prior phase's `make generate`/e2e-test/README-regeneration run carry the OLD Faker-word `customer_id` format, disjoint between datasets. These rows will **violate** the new PK constraint (D-14) if duplicates exist, and will simply never match the new trigger's existence check for any NEW `orders_valid` insert referencing an old-style ID. | D-19 already mandates `make reset` (full volume wipe) before this phase's new DDL is applied — this is the correct, already-locked resolution. No partial migration of old rows is needed or possible (old IDs are meaningless once the format changes). |
| Live service config | `.github/workflows/ci.yml`'s `oracle-e2e` job always starts from a **fresh** `docker compose up -d --wait` against ephemeral CI runner storage (no persisted Oracle volume across CI runs) — confirmed by reading the workflow this session (`docker compose down -v` in the `Tear down` step, `if: always()`). CI is unaffected by D-19's "existing dev environment" concern. | None — CI already gets a fresh DB every run. |
| OS-registered state | None found — this phase adds no OS-level task scheduling, no new cron/launchd/systemd units. | None. |
| Secrets/env vars | None — no new credential or env var name is introduced; the new trigger reuses `ORACLE_DSN`/`ORACLE_APP_USER`/`ORACLE_APP_USER_PASSWORD` (already read this session in `csv_processor/load.py`). | None. |
| Build artifacts | None found — no new installed package, no compiled artifact. | None. |

**Canonical question answered:** after every file in the repo is updated, the only runtime state
still carrying the "old shape" is the Oracle data volume itself (old-format `customer_id`/`order_id`
rows) — and D-19 already requires wiping it via `make reset`. Nothing else in this project's runtime
surface (CI, secrets, OS registrations, build artifacts) references the old ID format by name.

## Common Pitfalls

### Pitfall 1: RNG-Instance Continuation Across `generate_rows()` Calls Is Ambiguous

**What goes wrong:** `generate_correlated_datasets()` is built by calling today's `generate_rows()`
twice (once per dataset) with the same `seed` — appearing to satisfy D-05 ("deterministic given the
same seed") while NOT actually continuing a single live RNG stream across the customers→orders
boundary. Each call resets `random.Random(seed)` and `Faker.seed(seed)` to the identical starting
state.

**Why it happens:** `generate_rows()` (read this session, `generator/generate_csv.py:153-184`)
constructs `fake = Faker(); Faker.seed(seed); rng = random.Random(seed)` **internally**, every call —
there is currently no way to pass in an already-advanced RNG/Faker instance from outside.

**How to avoid:** Before writing `generate_correlated_datasets()`, the planner must explicitly
decide (and record as a plan-level decision, since CONTEXT.md doesn't disambiguate this) whether:
(a) `generate_rows()` gets refactored to accept optional `rng`/`fake` parameters (constructed once,
externally, and threaded through both the customers and orders generation calls) — this is the
literal reading of D-05's "the pool-sampling/Zipf RNG draws from the same seeded `random.Random(seed)`
**instance**"; or (b) each dataset keeps its own freshly-seeded `random.Random(seed)`, which is
still fully deterministic and byte-identical per D-05's outer requirement, just not a single
continuous stream. Both satisfy D-05's literal determinism requirement; they produce different byte
output and have different refactor scope. This is the single highest-leverage design decision in
this phase's plan.

**Warning signs:** A plan that treats `generate_correlated_datasets()` as "just call
`generate_rows()` twice, pass the pool to the second call" without addressing this ambiguity will
under-specify a load-bearing detail the executor will have to guess at.

### Pitfall 2: Existing Tests Call `generate_rows()` With Today's Signature, Standalone, Per Dataset

**What goes wrong:** `tests/unit/test_generate_csv.py` (read this session) has multiple tests that
call `generate_csv.generate_rows(orders_config, rows=N, invalid_ratio=X, seed=Y)` **completely
standalone**, with no customers dependency (e.g. `test_generate_rows_is_deterministic_for_same_seed_orders`
at line 194-196). Once `orders.customer_id` generation requires a valid-customer pool as an input
(D-01), these tests can no longer pass unmodified — there is no pool to sample from when
`generate_rows()` is called for `orders_config` alone.

**Why it happens:** D-22 explicitly acknowledges this ("orders can no longer be generated by a
fully independent CLI call") but the existing unit test suite was written before this constraint
existed and directly exercises the now-invalid standalone-orders code path.

**How to avoid:** The plan must include an explicit task to update/replace these specific existing
tests (not just add new ones) — `grep -n "generate_rows(orders_config" tests/unit/test_generate_csv.py`
finds the exact call sites to update. No test currently asserts specific Faker-word *content* for
`customer_id` (only header shape — verified via `grep -n "customer_id" tests/unit/test_generate_csv.py`
this session, matches found only assert `header == [...]`), so the ID *format* change (D-06/D-08)
itself is safe; it's specifically the standalone-orders-generation-determinism tests that need
updating.

**Warning signs:** `verify-phase3`/`verify-phase2`'s existing `pytest tests/unit/ -x` gate (still run
by every later phase's `verify-phaseN` target per the Makefile) will fail immediately if this is
missed — a fast, loud signal, not a silent regression.

### Pitfall 3: A Trigger That Queries `customers_valid` Inside `orders_valid`'s INSERT Path Can Mutate the Same Table Being Inserted Into (ORA-04091)

**What goes wrong:** Oracle famously raises `ORA-04091: table is mutating, trigger/function may not
see it` when a row-level trigger on table A tries to `SELECT`/query table A itself mid-INSERT. This
is NOT the case here (the trigger queries `customers_valid`, a *different* table, from an
`orders_valid` trigger) — but it is a well-known Oracle gotcha closely adjacent to this exact
pattern, and worth an explicit negative-space note: **querying a different table (customers_valid)
from an orders_valid row trigger is safe and does not trigger ORA-04091**; that error only fires
when a trigger queries the *same* table it is defined on.

**Why it happens (as a documented pitfall, even though it doesn't apply here):** Trigger authors
frequently conflate "querying another table" with "querying the table under mutation" and either
over-engineer around a problem that doesn't exist, or (worse) hit the real version of this bug if a
future change adds cross-references between `customers_valid` and `orders_valid` in either
direction.

**How to avoid:** No mitigation needed for D-15 as scoped (cross-table lookup only). Flag explicitly
in the trigger's own SQL comment (matching this project's existing DDL-commenting discipline, e.g.
`02_customers.sql`'s header comments) so a future maintainer doesn't "fix" a non-bug.

**Warning signs:** None expected for D-15 as scoped; would only surface if a future decision adds a
trigger that queries its own table.

### Pitfall 4: `ingested_at`'s `SYSDATE` Default Cannot Be Bypassed By Omission — It Must Be Explicitly Included in the INSERT Column List

**What goes wrong:** D-12 requires the new e2e test to backdate some rows across multiple partition
days. A naive attempt to do this via `csv_processor.load.insert_rows()` (which builds its column
list from `config.columns` — verified this session, `load.py:142`, and `config.columns` never
includes `ingested_at`) will always let Oracle's `DEFAULT SYSDATE` apply, silently landing every row
in today's partition regardless of test intent.

**Why it happens:** Oracle's `DEFAULT` clause only activates when a column is *omitted* from the
INSERT's column list — `config.columns` (customer_id, name, country, birth_date, event_ts,
signup_country / order_id, customer_id, order_date, amount) never includes `ingested_at`, so every
existing insert path always gets the default.

**How to avoid:** `insert_rows()` is already generic on its `columns`/`rows` parameters (verified
this session — it does not hardcode `config.columns` internally, the caller supplies the list). The
new e2e test can call `load.insert_rows(cursor, table="orders_valid", columns=[...config columns...,
"ingested_at"], rows=[{...row..., "ingested_at": some_past_date}, ...])` directly, bypassing
`csv_processor.engine.process()`'s normal flow entirely for this one backdating step — no change to
`insert_rows()` itself is needed, just a test-local call with an extended column list.

**Warning signs:** A backdated-row test whose Oracle query never shows the expected historical
partition is the direct symptom.

## Code Examples

### Unit test shape for D-09's correlation properties

```python
# Illustrative shape only -- not verified against a specific existing test file
# (no such test exists yet; this phase creates it), but follows this project's
# own established assertion style (see tests/unit/test_generate_csv.py's
# test_generate_rows_is_deterministic_for_same_seed as the template).

def test_orders_customer_id_is_subset_of_valid_customer_pool(...) -> None:
    result = generate_correlated_datasets(customers_config, orders_config, rows=..., seed=42)
    valid_customer_ids = {
        row[customer_id_index]
        for row, category in zip(result.customers.rows, result.customers.categories, strict=True)
        if category is None  # None == valid row, per GeneratedCsv's own convention
    }
    order_customer_ids = {row[customer_id_index] for row in result.orders.rows}
    assert order_customer_ids <= valid_customer_ids

def test_orders_customer_id_sampling_is_zipf_weighted(...) -> None:
    result = generate_correlated_datasets(customers_config, orders_config, rows=..., seed=42)
    counts = Counter(row[customer_id_index] for row in result.orders.rows)
    # top-ranked customer should appear noticeably more than the median --
    # exact threshold is a plan-level decision, not asserted here.
    assert counts.most_common(1)[0][1] > statistics.median(counts.values())

def test_correlated_generation_is_deterministic_for_same_seed(...) -> None:
    first = generate_correlated_datasets(customers_config, orders_config, rows=..., seed=42)
    second = generate_correlated_datasets(customers_config, orders_config, rows=..., seed=42)
    assert first == second  # byte-identical, D-05
```

### DDL shape for D-13/D-14/D-15 (new `05_correlation_constraints.sql`)

```sql
-- Illustrative shape only -- exact syntax should be re-verified against a live
-- Oracle 23ai Free container during implementation (this session verified the
-- general CREATE TRIGGER / RAISE_APPLICATION_ERROR pattern via WebSearch
-- against community Oracle documentation, not Oracle's own official docs
-- directly -- tag this block [CITED: community sources], not [VERIFIED]).
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;

ALTER TABLE customers_valid ADD CONSTRAINT pk_customers_valid PRIMARY KEY (customer_id);
CREATE INDEX ix_customers_valid_customer_id ON customers_valid (customer_id);

ALTER TABLE orders_valid ADD CONSTRAINT pk_orders_valid PRIMARY KEY (order_id);
CREATE INDEX ix_orders_valid_customer_id ON orders_valid (customer_id);

CREATE OR REPLACE TRIGGER trg_orders_valid_customer_exists
BEFORE INSERT ON orders_valid
FOR EACH ROW
DECLARE
  v_exists NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_exists
  FROM customers_valid
  WHERE customer_id = :NEW.customer_id;
  IF v_exists = 0 THEN
    RAISE_APPLICATION_ERROR(-20001, 'orders_valid.customer_id not found in customers_valid: ' || :NEW.customer_id);
  END IF;
END;
/
```

Note: `PRIMARY KEY` on `customer_id` in `customers_valid` implicitly creates a unique index; D-13's
separate plain index request is for `customer_id` in `orders_valid` (the FK side, not the PK side)
and, per D-13's literal wording, "in both" — the planner should confirm whether a *second*,
redundant index on `customers_valid.customer_id` is actually wanted given the PK already creates one,
or whether D-13 is satisfied by the PK's own implicit index on the `customers_valid` side and only
needs an explicit new index on the `orders_valid` side.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `orders.customer_id` = independent Faker word, zero relationship to `customers.customer_id` | Zipf-weighted sample with replacement from the `customers_valid`-bound pool, seed-deterministic | This phase | `scripts/verify_evidence.sql`'s JOIN goes from permanently empty to genuinely populated — the actual bug this phase exists to fix |
| Airflow 1.x/2.x `execution_date`/2.2+ `data_interval_start`/`data_interval_end` as "the trigger date" | Airflow 3's manually-triggered DAG runs do not guarantee `data_interval` derives from `logical_date` at all | Airflow 3.0 (this project pins 3.3.1) | D-29's sensor must use `SYSDATE`/`Pendulum.now()` directly, not interval arithmetic — [VERIFIED via Context7 apache/airflow docs this session] |
| `apache-airflow-providers-oracle`'s pre-2.x versions occasionally had experimental sensor classes in some community forks | Current stable (`4.6.2`, pinned here) ships operator-only, no sensor at all | Ongoing (confirmed current as of this research) | Removes any temptation to search for/import a nonexistent built-in Oracle sensor — must build custom |

**Deprecated/outdated:** None directly relevant — this project's stack (Airflow 3.3.1, Oracle Free
23.26.2, python-oracledb 4.0.2) is already current as of Phase 6's own research.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The exact PL/SQL trigger syntax shown in "Code Examples" (`RAISE_APPLICATION_ERROR`, `:NEW.customer_id`, `FOR EACH ROW`) will compile as-shown against Oracle Database Free 23.26.2 specifically — this was verified via general WebSearch results (community Oracle documentation sources, e.g. mkyong.com, Oracle blog posts) rather than Oracle's own official 23ai SQL Language Reference or a live compile against this project's actual container | Code Examples, Pattern 3 (DDL shape) | Low-medium — this is bog-standard, decades-stable Oracle trigger syntax unlikely to have changed; but the planner should have the executor confirm compilation against the live container (`docker compose exec oracle sqlplus ...`) before considering the DDL task done, not just trust this research |
| A2 | Zero-padding width and exact hash length for structured IDs are left fully to Claude's Discretion per CONTEXT.md — this research recommends 8 hex chars / 6-digit sequence as a safe default but this is a design choice, not a verified fact | Pattern 2 | Low — any reasonable choice within `VARCHAR2(64)`'s headroom works; the only real constraint (length) was independently verified against the actual DDL |
| A3 | D-13's "plain (non-unique) index on customer_id in both customers_valid and orders_valid" is interpreted as potentially redundant on the `customers_valid` side (where `customer_id` is already the PK, which Oracle indexes implicitly) — this research flags the redundancy but does not resolve it; CONTEXT.md's literal wording says "both" | Code Examples, DDL note | Low — worst case is one harmless redundant index; the planner should make the call explicit rather than silently either adding or skipping it |
| A4 | The FOREIGN KEY-constraint alternative to D-15's trigger (see "Alternatives Considered") is presented as a simpler option worth surfacing, but D-15/D-16 are locked decisions from CONTEXT.md, not open questions — this research does not recommend overriding them, only flagging the tradeoff for the planner/user to consciously accept or revisit | Standard Stack (Alternatives Considered) | Low — informational only; does not block planning either way |

## Open Questions

1. **Does D-05's "same seeded `random.Random(seed)` instance" mean object-identity continuation
   across the customers→orders generation boundary, or same-seed-value reuse?**
   - What we know: Both readings satisfy D-05's outer requirement (same seed → byte-identical
     output across runs). They produce different actual output and require different refactor
     scope to `generate_rows()`'s signature.
   - What's unclear: CONTEXT.md's exact wording doesn't disambiguate this, and no existing test or
     code path currently threads an RNG instance across two `generate_rows()` calls.
   - Recommendation: The planner should make this decision explicit in PLAN.md as its own numbered
     decision (not silently pick one), since it's the single highest-leverage design choice in this
     phase and affects `generate_rows()`'s public contract, which multiple existing tests call
     directly.

2. **Should the new report-sensing DAG (D-26) query `ingestion_metadata` (as this research assumed
   for the trigger's polling SQL) or directly `COUNT(*)` against `customers_valid`/`orders_valid`
   for today's partition?**
   - What we know: D-28 says "queries `ingestion_metadata` for both `customers` and `orders` having
     a row for the current partition" — `ingestion_metadata` is NOT itself partitioned (verified
     this session, `01_ingestion_metadata.sql` has no `PARTITION BY` clause) and its own
     `processed_at` column (not `ingested_at`) is the timestamp to check against `TRUNC(SYSDATE)`.
   - What's unclear: whether "has data for the current partition" means "has an `ingestion_metadata`
     row processed today" (simpler, matches D-28's literal wording) vs. "has a row in
     `customers_valid`/`orders_valid` whose own `ingested_at` falls in today's partition" (more
     directly checks the actual partitioned data tables, but requires two separate queries against
     differently-partitioned tables).
   - Recommendation: Follow D-28's literal wording (`ingestion_metadata`) since it's simpler, is not
     itself partitioned (no interval-partition edge cases to reason about), and already carries a
     `dataset` column making the "both present" check a single `COUNT(DISTINCT dataset)` query, as
     shown in the Pattern 3 code example above.

3. **Is a second, PK-redundant index wanted on `customers_valid.customer_id` per D-13's literal
   "both" wording?**
   - See Assumption A3 above — same open question, cross-referenced here for visibility.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Oracle Database Free (via `docker-compose.yml`) | All DDL/trigger work (D-13–D-19), correlation e2e test (D-09/D-10) | ✓ (pinned) | `gvenzl/oracle-free:23.26.2-faststart` (verified this session, `docker-compose.yml:101`) | — |
| `python-oracledb` thin mode async support | Custom trigger's `connect_async()` (D-28) | ✓ (pinned) | `4.0.2` (verified via Dockerfile) — asyncio support present since 2.x, well below this pin | — |
| Apache Airflow 3.3.1 triggerer service | New custom `BaseTrigger` (D-26–D-29) | ✓ (already running as a `docker-compose.yml` service, `airflow-triggerer`) | `3.3.1-python3.12` (verified via `docker/airflow/Dockerfile:1`) | — |
| `apache-airflow-providers-oracle` sensor/deferrable operator | Would have satisfied D-28 without custom code | ✗ (confirmed absent, see Standard Stack) | `4.6.2` (pinned, operator-only) | Custom `BaseTrigger` (already the locked plan, D-26) |
| GitHub Actions runner disk headroom | `oracle-e2e` CI job running the new e2e test alongside the existing one | ✓ (already handled — Phase 6's `docs/environment.md`/06-RESEARCH.md already documents the `slim-faststart` fallback if disk pressure appears) | — | Switch Oracle image tag to `23.26.2-slim-faststart` if needed (already documented, not newly introduced by this phase) |

**Missing dependencies with no fallback:** none.

**Missing dependencies with fallback:** the Oracle-provider deferrable sensor gap has its fallback
already built into D-26's own scope (a custom `BaseTrigger`) — not an external blocker.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (already configured, `pyproject.toml`) |
| Config file | `pyproject.toml` (`pythonpath = ["."]`, verified referenced in prior-phase decisions in STATE.md) |
| Quick run command | `uv run pytest tests/unit/ -x` |
| Full suite command | `uv run pytest tests/unit/ -x && uv run pytest tests/integration/ -x && uv run pytest tests/e2e/ -x` (mirrors `verify-phase6`'s shape in `Makefile`) |

### Phase Requirements → Test Map

| Req ID (candidate) | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| DATA-01 | `orders.customer_id ⊆` valid-customers pool | unit | `pytest tests/unit/test_generate_csv.py -k customer_id_is_subset -x` | ❌ Wave 0 |
| DATA-01 | Zipf-weighting property (skewed distribution) | unit | `pytest tests/unit/test_generate_csv.py -k zipf_weighted -x` | ❌ Wave 0 |
| DATA-01 | Determinism (same seed → identical assignment) | unit | `pytest tests/unit/test_generate_csv.py -k correlated_generation_is_deterministic -x` | ❌ Wave 0 |
| DATA-01/DB-02 | Real Oracle JOIN returns ≥1 row after live ingestion | e2e | `pytest tests/e2e/test_correlated_report_e2e.py -x` | ❌ Wave 0 (new file, D-10) |
| TEST-06 | Multi-day backdated partitions, report aggregates correctly across boundaries | e2e | Same file as above, additional assertion (D-12) | ❌ Wave 0 |
| DB-01 | PK/index DDL applies cleanly, existence enforced | integration (or covered by e2e's live ingestion) | Either a new `tests/integration/` case against the fresh schema, or folded into TEST-06's e2e | ❌ Wave 0 |
| GEN-02 | Existing standalone-orders determinism tests updated to the new call shape | unit | `pytest tests/unit/test_generate_csv.py -k orders -x` (existing tests, modified) | ✅ exists (needs modification, Pitfall 2) |
| BENCH-01 | `docs/benchmark.md` re-measured against new trigger-bearing schema | manual-only (benchmark run, not asserted in CI) | `uv run python -m benchmark.run_benchmark --mode bulk --rows 100000` | ✅ harness exists, doc needs regeneration |

### Sampling Rate

- **Per task commit:** `uv run pytest tests/unit/ -x`
- **Per wave merge:** `uv run pytest tests/unit/ -x && uv run pytest tests/e2e/ -x` (mirrors `verify-phase6`)
- **Phase gate:** Full suite green (`verify-phase6`-style: unit + e2e + lint + `verify-evidence`)
  before `/gsd-verify-work`, plus a live `make reset && make up` cycle to prove the new DDL applies
  cleanly to a genuinely fresh environment (D-19).

### Wave 0 Gaps

- [ ] `tests/e2e/test_correlated_report_e2e.py` — new file, covers DATA-01/TEST-06/D-09-D-12
- [ ] `tests/unit/test_generate_csv.py` extensions — correlation/Zipf/determinism assertions for
      `generate_correlated_datasets()`
- [ ] `tests/unit/test_generate_csv.py` modifications — existing standalone-orders determinism tests
      (Pitfall 2) must be updated to the new correlated call shape, or they will fail once orders
      generation requires a customer pool
- [ ] Framework install: none — pytest already fully configured

*(No framework installation gap — this project's test infrastructure already covers unit/
integration/e2e tiers; only new test *files*/cases are needed, not new tooling.)*

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | No auth surface changes in this phase |
| V3 Session Management | No | No session surface changes |
| V4 Access Control | No | No new access-control boundary introduced |
| V5 Input Validation | Yes | Already-established `is_safe_identifier()` allowlist in `csv_processor.config.models` (verified this session, referenced from `load.py`) continues to guard every table/column name interpolated into SQL text — the new trigger's SQL is static DDL (no runtime string interpolation of user input), and the new sensor's polling query is a static, parameterless `SELECT` — no new injection surface |
| V6 Cryptography | Marginal | `hashlib.sha256()` for the seed-derived ID component is NOT a security/cryptographic use — it's a deterministic uniqueness token, not a secret or integrity guarantee. No cryptographic requirement applies; flagged only to confirm this is correctly understood as non-security use of a hash function, not an ASVS V6 gap |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| SQL injection via dynamically-built identifier lists | Tampering | Already mitigated project-wide via `is_safe_identifier()` (verified this session in `load.py`/`benchmark/naive_loader.py`) — this phase's new code (trigger DDL, sensor query) uses no dynamic identifier interpolation at all, so the existing mitigation's scope doesn't even need to extend to cover it |
| Denial of service via unbounded trigger polling | Denial of Service (of the triggerer's shared event loop) | The custom `BaseTrigger`'s `async def run()` must use `oracledb.connect_async()` (non-blocking) rather than the synchronous `oracledb.connect()` — see Pattern 3's explicit rationale; a blocking call inside an async trigger would starve every other deferred task sharing the triggerer's event loop, a real availability risk specific to this phase's new code, not a generic OWASP category but a documented Airflow-specific hazard |
| Overly broad Oracle error messages leaking schema details | Information Disclosure (low severity, local-dev-only scope) | The trigger's `RAISE_APPLICATION_ERROR` message includes the offending `customer_id` value — acceptable here since this project's entire scope is local-dev-only (INFRA-03's already-established `admin`/`admin` local-dev-only credential model), not a production system with external attackers |

## Sources

### Primary (HIGH confidence)

- Context7 `/oracle/python-oracledb` — `executemany()` batch error handling (`batcherrors=True` vs.
  default rollback-whole-transaction behavior), `connect_async()`/`AsyncConnection`/asyncio
  integration patterns
- Context7 `/apache/airflow` — "Create a Custom Trigger" (`BaseTrigger`/`TriggerEvent`),
  "Implementing a Simple Deferrable Sensor" (`BaseSensorOperator`/`self.defer`), Airflow 3
  `logical_date`/`data_interval` manual-trigger behavior (`upgrading_to_airflow3.rst`)
- This project's own source, read directly this session: `generator/generate_csv.py`,
  `configs/datasets/{customers,orders}.json`, `docker/oracle/init/{01,02,03,04}_*.sql`,
  `packages/csv-processor/src/csv_processor/load.py`, `airflow/dags/csv_ingest.py`,
  `docker/airflow/Dockerfile`, `scripts/verify_evidence.sql`, `scripts/dag_polling.py`,
  `scripts/regenerate_readme_summary.py`, `Makefile`, `docker-compose.yml`,
  `tests/e2e/test_csv_ingest_e2e.py`, `tests/integration/test_load_oracle.py`,
  `tests/integration/test_engine_process_oracle.py`, `tests/unit/test_generate_csv.py`,
  `docs/benchmark.md`, `benchmark/naive_loader.py`, `.github/workflows/ci.yml`,
  `.planning/config.json`

### Secondary (MEDIUM confidence)

- `docs.python.org/3/library/random.html#random.choices` — fetched directly this session
  (WebFetch), official Python documentation
- `airflow.apache.org/docs/apache-airflow-providers-oracle/stable/operators.html` — fetched
  directly this session, confirms Oracle provider's operator-only (no sensor) surface
- `airflow.apache.org/docs/apache-airflow-providers/core-extensions/deferrable-operator-ref.html` —
  fetched directly this session, confirms Oracle's absence from the deferrable-operator reference
- `airflow.apache.org/docs/apache-airflow-providers-standard/.../filesystem/index.html` (via
  WebSearch summary) — `FileSensor`'s `recursive` parameter default/glob behavior
- `docker.com` "Docker Desktop: WSL 2 Best Practices" (via WebSearch summary) — WSL2-native bind
  mount VFS-sharing behavior

### Tertiary (LOW confidence)

- Community Oracle trigger syntax examples (mkyong.com, O'Reilly archived chapters, via
  WebSearch) — general `BEFORE INSERT ... FOR EACH ROW ... RAISE_APPLICATION_ERROR` shape; NOT
  Oracle's own official 23ai documentation. Flagged as Assumption A1 — recommend a live compile
  check during implementation before treating the DDL as done.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new packages, every capability verified against official docs or this
  project's own already-pinned versions
- Architecture (RNG sequencing, custom trigger necessity): HIGH on "custom trigger is required" and
  "executemany() default matches D-16"; MEDIUM on the exact RNG-continuation semantics, which is
  correctly flagged as an open architecture decision rather than a researched fact
- Pitfalls: HIGH — Pitfall 1/2 found by direct code-reading (not inference), Pitfall 4 confirmed via
  reading `load.py`'s actual `insert_rows()` signature this session
- Oracle trigger DDL syntax specifics: MEDIUM — pattern shape is standard and stable, but not cross-
  checked against Oracle's own official 23ai reference docs (community sources only)

**Research date:** 2026-08-30
**Valid until:** 30 days (this project's pinned versions — Airflow 3.3.1, oracledb 4.0.2, Oracle
Free 23.26.2 — are stable/pinned and unlikely to drift within that window; the Oracle-provider
"no deferrable sensor" finding should be re-checked if `apache-airflow-providers-oracle` is ever
bumped past `4.6.2` during implementation)
