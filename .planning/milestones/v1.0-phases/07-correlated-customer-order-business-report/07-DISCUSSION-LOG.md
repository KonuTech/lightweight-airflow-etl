# Phase 7: Correlated Customer-Order Business Report - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-30
**Phase:** 07-correlated-customer-order-business-report
**Areas discussed:** ID pool composition, Regression test type, customer_id/order_id realism,
Call-site consistency, Indexing/PK/Partitioning/Triggers (PL/SQL scope), Sequencing/FileSensor,
Report DAG design, Requirements traceability, DDL scope, Benchmark re-verification, Airflow logs
bug (deferred)

---

## ID Pool Composition

| Option | Description | Selected |
|--------|-------------|----------|
| Valid-rows-only pool | Sample only from customer rows that pass validation | ✓ |
| Full generated pool | Sample from all generated customer rows, valid + invalid | |

| Option | Description | Selected |
|--------|-------------|----------|
| With replacement | A customer can have multiple orders | ✓ |
| Without replacement | Each order maps to a unique customer | |

| Option | Description | Selected |
|--------|-------------|----------|
| Raise an error on empty pool | Fail loudly if no valid customer_id exists | ✓ |
| Fall back to independent random IDs | Reintroduce old uncorrelated behavior with a warning | |

| Option | Description | Selected |
|--------|-------------|----------|
| Uniform random sampling | Every customer equally likely | |
| Weighted (Zipf) distribution | Power-customer realism | ✓ |

| Option | Description | Selected |
|--------|-------------|----------|
| Zipf distribution | weight[i] ∝ 1/rank | ✓ |
| Fixed 80/20 split | Two-group split | |

| Option | Description | Selected |
|--------|-------------|----------|
| Fully deterministic given seed | Same seed → byte-identical correlated dataset | ✓ |
| Allow run-to-run variance | Not deterministic | |

**User's choice:** Valid-only pool, with-replacement, Zipf-weighted, error-on-empty-pool, fully
deterministic given seed.
**Notes:** User pushed for depth here across 7 follow-up rounds; converged once asked "what
specifically is still unclear" and answered "Nothing — all set, move on."

---

## Regression Test Type

| Option | Description | Selected |
|--------|-------------|----------|
| Both unit + live e2e | Fast subset-membership unit test + real Oracle JOIN e2e test | ✓ |
| Unit test only | No live proof | |

| Option | Description | Selected |
|--------|-------------|----------|
| New separate e2e test file | Keeps existing test_csv_ingest_e2e.py focused | ✓ |
| Extend existing file | Mixes two proof concerns | |

| Option | Description | Selected |
|--------|-------------|----------|
| Wired into required CI | Blocks merge on regression | ✓ |
| Local-only, not in CI | Regression could merge undetected | |

| Option | Description | Selected |
|--------|-------------|----------|
| Assert subset-membership + Zipf-weighting + determinism | Broader unit-test coverage | ✓ |
| Subset-membership only | Narrower coverage | |

**User's choice:** Both unit + e2e, new separate e2e file, wired into required `oracle-e2e` CI,
broader unit-test assertions.
**Notes:** None additional.

---

## customer_id / order_id Realism

| Option | Description | Selected |
|--------|-------------|----------|
| Structured ID format | e.g. CUST-00001 | ✓ |
| Keep Faker word style | Minimal diff | |

| Option | Description | Selected |
|--------|-------------|----------|
| CUST-00001 style | Prefix + zero-padded sequential number | ✓ |
| Plain zero-padded number | No prefix | |

| Option | Description | Selected |
|--------|-------------|----------|
| Also fix order_id for consistency | ORD-00001 style | ✓ |
| Leave order_id untouched | Stay strictly in bug scope | |

| Option | Description | Selected |
|--------|-------------|----------|
| Run-unique prefix/suffix derived from seed | Avoids cross-run ID collisions | ✓ |
| Reset to 1 each run, accept risk | JOIN fan-out risk across runs | |

| Option | Description | Selected |
|--------|-------------|----------|
| Derived from --seed | Reuses existing entropy, stays deterministic | ✓ |
| Separate timestamp/run component | New non-determinism source | |

**User's choice:** CUST-{seed_hash}-00001 / ORD-{seed_hash}-00001, seed-derived uniqueness.
**Notes:** User independently identified the cross-run duplicate-ID / JOIN-fan-out risk before
Claude raised it as a question — this shaped D-07's design.

---

## Call-site Consistency

| Option | Description | Selected |
|--------|-------------|----------|
| Centralized shared function | One generate_correlated_datasets() used by all call sites | ✓ |
| Wire each call site independently | Risk of drift across sites | |

| Option | Description | Selected |
|--------|-------------|----------|
| New combined CLI mode | Single command generates both datasets together | ✓ |
| Keep two CLI calls, pool file handoff | File-based intermediate handoff | |

**User's choice:** Centralized shared function + new combined CLI mode (replaces the Makefile's
two independent `generate_csv.py` subprocess calls).
**Notes:** None additional.

---

## Partitioning / PK / Indexing / PL/SQL Scope

**Initial ask (via free text):** "partitioning and indexing of oracle tables and searching for a
file, putting a generated file to sensor."

Claude flagged partitioning/indexing as likely out of scope (a new capability, not the correlation
bug) and asked for clarification.

| Option | Description | Selected |
|--------|-------------|----------|
| New backlog idea | Defer, unrelated to this fix | |
| Relates to this fix | User wanted to clarify further | ✓ |

Follow-up clarification (multiSelect): Index customer_id for JOIN perf ✓, Sequencing
customers-then-orders generation vs FileSensor ✓, Partition tables by dataset/date ✓, plus
free-text addition: "Generating a file where sensor is looking for one. Also during CI on GitHub
Actions."

Claude pushed back specifically on table partitioning (new capability, unrelated to the JOIN bug):

| Option | Description | Selected |
|--------|-------------|----------|
| Defer partitioning to backlog | Keep phase focused | |
| Include it anyway | User confirmed, accepting scope | ✓ |

**Discovery during scouting:** `docker/oracle/init/02_customers.sql`/`03_orders.sql` already have
daily interval partitioning (Phase 1). Reframed into a multi-partition test-data requirement.

| Option | Description | Selected |
|--------|-------------|----------|
| No action needed — partitioning already exists | | |
| Something else I meant | | ✓ |

User clarified: "Make sure we have datarows for many partitions to test them in a JOIN and the
report."

| Option | Description | Selected |
|--------|-------------|----------|
| Test-only explicit backdated inserts | Self-contained multi-partition proof | ✓ |
| Rely on natural accumulation over time | No new test-only code path | |

User then added: "Whenever adding PK, indexing, partitions to generate customers and orders tables
start fresh so the final report will be clean" — surfaced a new PK question.

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, add PK constraint | Stronger DB-level uniqueness guarantee | ✓ |
| No, index only | Avoid ingestion-failure risk | |

User then added: "Yes, add PK, triggers, use Oracle PLSQL for processes, including the final
report" — Claude pushed back firmly (contradicts ROADMAP success criterion 1's literal
Python-at-generation-time wording; reopens Phase 6's shipped evidence-script work; project's core
architecture is explicitly a thin Python engine).

| Option | Description | Selected |
|--------|-------------|----------|
| Defer PL/SQL/triggers to backlog | Keep phase scoped | |
| Include it now, accept scope/timeline hit | User insisted | ✓ |

Claude asked what the triggers should concretely do:

| Option | Description | Selected |
|--------|-------------|----------|
| Triggers validate/audit on insert | Bounded, DB-level safety net | |
| Something else — let me describe it | | ✓ |

User: "Try to move as much of data logics and data processing to the Oracle PLSQL from Python.
Whenever possible." Claude declined to fold this in unbounded (no defined boundary, reverses
shipped architecture) and asked again:

| Option | Description | Selected |
|--------|-------------|----------|
| Keep Phase 7 scoped, note this as a future direction | Recorded as deferred idea | ✓ |
| Give me one specific, bounded PL/SQL task for this phase | | |

**User's final choice:** Existing partitioning is sufficient (no new DDL beyond D-13's index);
new regression test backdates rows across multiple days; PRIMARY KEY on customer_id/order_id;
plain index on both; a bounded INSERT trigger on `orders_valid` validating `customer_id` exists in
`customers_valid`; broader "move everything to PL/SQL" explicitly deferred as a future-direction
idea, not decided here.

Follow-up (whole-batch vs. batcherrors on trigger violation):

| Option | Description | Selected |
|--------|-------------|----------|
| Whole batch fails on any violation | Matches Oracle executemany() default; should be unreachable given Python-side correlation | ✓ |
| Per-row rejection via batcherrors | More resilient, more complexity | |

Follow-up (DDL scope — valid vs. invalid tables):

| Option | Description | Selected |
|--------|-------------|----------|
| Valid tables only | Invalid tables intentionally hold malformed rows | ✓ |
| Apply to invalid tables too | Would need special-casing | |

Follow-up (benchmark re-verification, raised by Claude given trigger overhead):

| Option | Description | Selected |
|--------|-------------|----------|
| Re-run and update docs/benchmark.md | Keeps documented numbers honest | ✓ |
| Leave as-is | Stale but roughly representative | |

---

## Sequencing customers-then-orders Generation vs. FileSensor (incl. CI)

| Option | Description | Selected |
|--------|-------------|----------|
| Generate both CSVs first, then trigger both DAGs | Keeps DAG runs independent/parallel-capable | ✓ |
| Fully sequential: wait for customers DAG to complete first | Stronger guarantee, slower, loses parallelism | |

User added: "we have to test few times a mechanism of files being moved to the directory where DAG
is picking up the files... generation of files and movement from directory A to B... have to be
different paths. Propose a live solution."

Claude proposed a concrete staging-dir + atomic-rename mechanism:

| Option | Description | Selected |
|--------|-------------|----------|
| Staging dir + atomic rename (same volume) | Race-free, no new infra | ✓ |
| Something else — a genuinely separate location | | |

User then confirmed (via free text) this must run against the live Airflow stack, and Claude
confirmed the new e2e test (already locked, wired into required CI) covers exactly this:

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, the live e2e test covers it | | ✓ |
| Something more — let me describe it | | |

**User's choice:** Generate both CSVs to a staging dir first, atomically rename into the watched
dir, trigger both DAGs independently; proven live via the required e2e test.

---

## New Report-Sensing DAG (major scope addition)

User (free text, during a "any remaining gray areas" check): "Are you aiming to have one DAG for
two tables ingestion and report. Or three separate DAGs? ... maybe we should have a DAG which
checks if data arrived to tables orders, customers on specific data partition. If yes, then create
the report." Also separately flagged: Airflow UI logs not showing at :8080 (handled below, out of
scope).

| Option | Description | Selected |
|--------|-------------|----------|
| Keep script-based report (existing regenerate_readme_summary.py) | No new orchestration | |
| Add a new report-generation DAG | Genuinely new capability | ✓ |

| Option | Description | Selected |
|--------|-------------|----------|
| Alongside, complementary to the existing CI script | Doesn't touch shipped Phase 6 work | ✓ |
| Replace the CI script entirely | Reopens Phase 6's shipped design | |

| Option | Description | Selected |
|--------|-------------|----------|
| Poll Oracle directly | Checks actual source of truth | ✓ |
| ExternalTaskSensor on both DAG runs | Fragile for ad hoc HTTP-triggered runs | |

User: "make sure that DAG scans for correct date... old version looks for date from date before,
modern from today. Check these two approaches and choose best." Claude consulted Context7
(`/apache/airflow`) and found Airflow 3's own docs: manual DAG runs don't guarantee
`data_interval` matches `logical_date`.

| Option | Description | Selected |
|--------|-------------|----------|
| Query real wall-clock date directly | Sidesteps logical_date/data_interval entirely | ✓ |
| Use logical_date from trigger context | Airflow docs say this isn't reliable for manual triggers | |

**User's choice:** New DAG, complementary to the CI script, Oracle-polling sensor, wall-clock date
detection (grounded via live Context7 lookup against Airflow 3.3.1 docs).

---

## Airflow UI Logs Bug (deferred, out of scope)

User (free text): "I noticed that in our Airflow on 8080 I can't see any logs. There is something
wrong with logs. Fix it."

| Option | Description | Selected |
|--------|-------------|----------|
| Handle separately, after this discussion | Keeps this discussion focused on Phase 7 | ✓ |
| Investigate right now, pause this discussion | | |

**User's choice:** Deferred — handle after Phase 7 via a separate investigation (e.g.
`/gsd-debug`), not folded into this phase.

---

## Requirements Traceability

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, add new REQ IDs during planning | Keeps traceability honest given real scope | ✓ |
| No, reuse existing DOC-01/TEST-03 | Understates actual scope | |

---

## Claude's Discretion

- Exact seed-derived hash/component format for structured IDs.
- Exact zero-padding width for sequential ID suffixes.
- Exact new REQ ID naming/numbering.
- Exact task-graph shape of the new report DAG.
- Whether the combined CLI mode is a new flag vs. a small separate orchestrator script.

## Deferred Ideas

- Broad PL/SQL migration ("move as much data logic and data processing to Oracle PL/SQL from
  Python, whenever possible") — recorded as a future architectural direction, not decided here.
- Airflow UI logs not showing at :8080 — unrelated live bug, to be investigated separately.
