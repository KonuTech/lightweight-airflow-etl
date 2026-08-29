# Phase 6: End-to-End Verification, Benchmark, CI & Docs - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-29
**Phase:** 6-End-to-End Verification, Benchmark, CI & Docs
**Areas discussed:** Benchmark design/dataset/scope, e2e test scope, CI gating/trigger, docs depth/
structure/content, lint scope, Executive Summary + business report (user-initiated), evidence
capture mechanism, Executive Summary regeneration model, benchmark report content, naive-baseline
code placement, CI loop guard, Executive Summary exact content

---

## Benchmark comparison design

| Option | Description | Selected |
|--------|-------------|----------|
| Real naive-loop baseline | Genuine row-by-row `execute()` loop as comparison | ✓ ("choose the most representative approach to test") |
| Vary chunk_size on existing code | `chunk_size=1` vs `5000` through the same `executemany()` path | |

**User's choice:** Real naive-loop baseline (D-01).
**Notes:** User's own phrasing interpreted as confirming the more representative option, reflected
back and implicitly confirmed by continuing.

---

## E2E test trigger & CI scope

| Option | Description | Selected |
|--------|-------------|----------|
| Local-only, live-stack required | pytest against an already-running docker-compose stack, not in CI | |
| CI-runnable via docker-compose services: | GitHub Actions spins up Oracle+Airflow, e2e runs in PR pipeline | ✓ |

**User's choice:** CI-runnable (D-06) — explicit scope expansion beyond CI-01's literal text.

## CI gating

| Option | Description | Selected |
|--------|-------------|----------|
| Required check | Oracle+e2e job blocks merge like lint/type/unit | ✓ |
| Non-blocking / allowed-to-fail | Reports status but doesn't block | |

**User's choice:** Required check (D-07).

## CI trigger

| Option | Description | Selected |
|--------|-------------|----------|
| Every PR | Matches CI-01's literal wording | ✓ |
| Main-branch only | Faster PR feedback, e2e only post-merge | |

**User's choice:** Every PR (D-07).

---

## Docs depth

| Option | Description | Selected |
|--------|-------------|----------|
| Full topic split | README + architecture/configuration/csv-engine/oracle/development docs | ✓ |
| Consolidated | README + one combined architecture doc + short development.md | |

**User's choice:** Full topic split (D-15).

## Benchmark output

| Option | Description | Selected |
|--------|-------------|----------|
| Committed report file | docs/benchmark.md with results | ✓ |
| Console output only | Nothing persisted | |

**User's choice:** Committed report file (D-05).

---

## Executive Summary + business report (user-initiated, not originally in gray-area list)

User's own words: *"We have to be able to see that DAG worked from catching up that the file shown
up, that event of showing up a file should start the DAG, DAG is reading a file using CSV engine and
insert data, either correct or incorrect into oracle tables, we have to be able to query tables to
get an evidences of working pipeline. Report evidenced to README.md in a top Executive Summary as
evidence of working lightweight airflow+oracle etl platform."*

Claude reflected this back as: e2e test proves file-appears → deferred-wake → read → correct/
incorrect Oracle rows; evidence via direct table queries; written up as a README Executive Summary.
**User confirmed:** "Yes." — then added: *"But also we have to be able to join our two tables:
customers and orders to get some sort of business report. Add is also to mentioned Executive
summary."*

**Locked:** D-08 (e2e proves full trigger chain), D-09 (evidence via direct Oracle queries), D-10
(customers⋈orders business report), D-11 (Executive Summary in README top).

## Business report shape

| Option | Description | Selected |
|--------|-------------|----------|
| You decide | Claude picks something simple/illustrative | |
| I'll specify | User describes columns/aggregation | ✓ — "Join both tables and aggregate any numeric metrics by region and date if possible. Also add a count. Something typical" |

**Note:** Neither table has a literal `region` column — `customers.country` used as the region proxy,
flagged explicitly to the user via CONTEXT.md rather than silently assumed (D-10).

---

## Evidence capture mechanism

| Option | Description | Selected |
|--------|-------------|----------|
| Committed script + Makefile target | scripts/verify_evidence.sql + make verify-evidence | ✓ |
| Documented manual sqlplus commands | Typed by hand each time | |

**User's choice:** Committed script + Makefile target (D-09).

## Executive Summary type

| Option | Description | Selected |
|--------|-------------|----------|
| Snapshot with "last verified" date | Captured output pasted in once, timestamped | |
| Live/regenerated | Numbers stay current via a regeneration mechanism | ✓ (non-default choice) |

**User's choice:** Live/regenerated (D-12).

## Regeneration trigger

| Option | Description | Selected |
|--------|-------------|----------|
| Local command, manually committed | Maintainer runs + commits on demand | |
| CI auto-commits on merge | Workflow pushes an automatic commit after every merge to main | ✓ (non-default choice) |

**User's choice:** CI auto-commits on merge (D-12/D-13) — needs a `[skip ci]` loop guard, confirmed
by user in a follow-up question.

---

## Benchmark dataset & run design

| Option | Description | Selected |
|--------|-------------|----------|
| Direct call, customers dataset | process()/load called directly, bypasses Airflow | ✓ |
| Through the real DAG | HTTP-triggers the actual csv_ingest DAG | |

**User's choice:** Direct call, customers dataset (D-02/D-03).

## Lint/type-check scope

| Option | Description | Selected |
|--------|-------------|----------|
| csv_processor + generator + tests only | Airflow DAGs excluded from mypy-strict | |
| Everything, including DAGs | Whole repo under mypy/ruff | ✓ (non-default choice) |

**User's choice:** Everything, including DAGs (D-14).

## README structure

| Option | Description | Selected |
|--------|-------------|----------|
| Self-contained walkthrough | Every command inline in README | |
| Summary + links | Overview + links into docs/ for command sequences | ✓ |

**User's choice:** Summary + links (D-16).

## development.md content

| Option | Description | Selected |
|--------|-------------|----------|
| Local dev workflow | Tests, Oracle reset, fixture regen, lint/type-check | ✓ |
| Architecture/contribution notes | Code layout, adding a dataset, conventions | ✓ |
| CI/troubleshooting | What CI runs, debugging failing checks | ✓ |

**User's choice:** All three (D-17).

---

## Benchmark report content

| Option | Description | Selected |
|--------|-------------|----------|
| Side-by-side comparison table | Naive vs chunked/bulk, all 3 metrics | ✓ |
| Speedup ratio / % improvement | Explicit "Nx faster" line | ✓ |
| Run metadata | Row count, dataset, machine specs, date | ✓ |
| Raw per-chunk timing breakdown | Per-chunk timing for the chunked run | ✓ |

**User's choice:** All four (D-05).

## Naive baseline code placement

| Option | Description | Selected |
|--------|-------------|----------|
| benchmark/ directory | New top-level dir, separate from packages/csv-processor/ | ✓ |
| Inside tests/, marked clearly | tests/benchmark/ with naive loader as local helper | |

**User's choice:** benchmark/ directory (D-04).

---

## CI loop guard

| Option | Description | Selected |
|--------|-------------|----------|
| [skip ci] convention | Standard GitHub Actions loop-breaking pattern | ✓ |
| Something else | User describes an alternative | |

**User's choice:** [skip ci] convention (D-13).

## Executive Summary exact content

| Option | Description | Selected |
|--------|-------------|----------|
| Total rows processed per dataset | Valid/invalid counts from latest run | ✓ |
| Deferred-wake proof line | Timestamp proving non-blocking file-wait | ✓ |
| Business report (top N rows) | customers⋈orders aggregation, top 10 | ✓ |

**User's choice:** All three (D-11).

---

## Claude's Discretion

- Exact peak-memory measurement method (tracemalloc/resource.getrusage/psutil/`/usr/bin/time -v`).
- Exact GitHub Actions workflow YAML structure (job names, matrix, uv caching).
- Exact SQL content of `scripts/verify_evidence.sql` and README marker syntax.
- Whether the deferred-wake proof comes from the same e2e test or a separate check.
- Exact composition of the `verify-phase6` Makefile target.

## Deferred Ideas

None — all scope expansions (Executive Summary, business report) stayed within Phase 6's own
TEST-03/DOC-01 mandate rather than introducing a new capability requiring its own phase.
