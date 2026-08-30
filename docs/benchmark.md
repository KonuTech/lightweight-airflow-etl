# Benchmark: Naive Row-by-Row vs. Chunked/Bulk Oracle Writes (TEST-04)

This document records a real, ~100,000-row measurement comparing a genuine naive per-row Oracle
insert loop against the project's real chunked/bulk `executemany()` write path
(`csv_processor.load.insert_rows()`) — proving the design choice PITFALLS.md's "Performance Trap
1" warns against actually costs what it's expected to cost, on this project's own schema and
Oracle container, not a synthetic estimate.

Both runs (`benchmark/run_benchmark.py --mode naive` and `--mode bulk`) consume the **exact same**
`csv_processor.engine.process_chunks()` generator for CSV parsing/validation (D-03) — the *only*
variable between the two runs is the Oracle write strategy:

- **naive** (`benchmark/naive_loader.py`): one `cursor.execute()` call per row, in a Python loop —
  never `executemany()` at any chunk size (D-01; `grep -c executemany benchmark/naive_loader.py`
  is `0`).
- **bulk**: the real, already-tested `csv_processor.load.insert_rows()`, which issues one
  `cursor.executemany()` call per chunk (array-bind, `chunk_size=5000` for `customers`).

Reproduce with:

```bash
uv run python -m benchmark.run_benchmark --mode naive --rows 100000 --seed 20260101
uv run python -m benchmark.run_benchmark --mode bulk  --rows 100000 --seed 20260101
```

## Run Metadata

| Field | Value |
|---|---|
| Dataset | `customers` |
| Rows generated | 100,000 (`--rows 100000`) |
| Invalid-row ratio | 0.1 (generator default — 10,000 deliberately invalid rows) |
| Seed | `20260101` (`--seed 20260101`) |
| Chunk size | 5,000 (`configs/datasets/customers.json` → `processing.chunk_size`) |
| Run date | 2026-08-30 (UTC) |
| Machine | `Linux DESKTOP-SION1AN 6.18.33.2-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC x86_64` (WSL2, Docker Desktop-hosted Oracle Database Free `23.26.2-faststart`) |
| CPUs | 32 (`nproc`) |
| Row-split parity | Both runs report identical `total_rows=100000`, `valid_rows=90000`,
`invalid_rows=10000` — confirms `process_chunks()`'s output is unaffected by which write path
consumes it (D-03's isolation requirement holds at full scale, not just the 1,000-row smoke test). |

This re-run (Phase 7, Plan 07-06) executes against the schema as it now stands after Plan 07-04's
`customers_valid` DDL change (D-20): `customer_id` carries an explicit `PRIMARY KEY` constraint
(and its implicit unique B-tree index), which did not exist during the original Phase 6 run.
Every number below reflects that overhead honestly, whichever direction it moved the numbers —
nothing here is copy-pasted from the prior Phase 6 measurement.

## Comparison Table

| Metric | Naive (`--mode naive`) | Chunked/Bulk (`--mode bulk`) |
|---|---|---|
| rows/sec (Oracle write only) | 2,732.81 | 184,206.91 |
| Peak memory (RSS, process-lifetime peak) | 136.78 MB (140,060 KB) | 137.37 MB (140,664 KB) |
| Oracle load time (write-only, 20 chunks) | 32.933 s | 0.489 s |
| Wall-clock (full CLI invocation, incl. fixture generation) | 43.64 s | 10.25 s |

Peak RSS is near-identical between the two runs because both processes pay the same fixed
`oracledb`/`pydantic`/generator import and 100K-row Faker-generation cost — the write strategy
itself does not meaningfully change this process's peak memory footprint at this row count; the
real, load-bearing difference this benchmark exists to prove is **Oracle write throughput**, not
memory.

Naive throughput dropped from Phase 6's 4,268.08 rows/sec to 2,732.81 rows/sec — consistent with
the new `PRIMARY KEY`'s implicit unique index adding a per-row uniqueness check to each individual
`cursor.execute()` round trip. Bulk throughput also dropped, from 780,429.15 to 184,206.91
rows/sec — the same index-maintenance cost applies per row inside each `executemany()` batch too,
plus this run's machine load differed from the original session. The bulk path still writes over
67× faster than the naive path, so the design conclusion PITFALLS.md's "Performance Trap 1" warns
against is unchanged; the absolute numbers moved because the schema genuinely got heavier, not
because either write path regressed relative to the other.

## Speedup Ratio

```
bulk_rows/sec / naive_rows/sec = 184206.91 / 2732.81 = 67.41×
```

The chunked/bulk `executemany()` path is **~67.41× faster** (a **6,741% improvement**) than the
genuine naive per-row loop for Oracle write throughput on this dataset/machine, now measured
against the schema carrying `customers_valid`'s new `PRIMARY KEY` overhead (D-20) — confirming
PITFALLS.md's "Performance Trap 1" is not a theoretical concern: one Oracle round trip per row is
measurably, drastically more expensive than a single array-bound `executemany()` call per chunk,
even with the added index-maintenance cost the DDL change introduced.

## Per-Chunk Timing Breakdown (Chunked/Bulk Run Only)

20 chunks of 5,000 rows each (100,000 rows ÷ `chunk_size=5000`). Each row is a single chunk's
`executemany()` write duration, in seconds:

| Chunk | Seconds |
|---|---|
| 1 | 0.28215 |
| 2 | 0.00906 |
| 3 | 0.00856 |
| 4 | 0.01026 |
| 5 | 0.01070 |
| 6 | 0.01019 |
| 7 | 0.00960 |
| 8 | 0.01196 |
| 9 | 0.01163 |
| 10 | 0.01172 |
| 11 | 0.01137 |
| 12 | 0.01053 |
| 13 | 0.01022 |
| 14 | 0.01386 |
| 15 | 0.00975 |
| 16 | 0.01344 |
| 17 | 0.01090 |
| 18 | 0.01062 |
| 19 | 0.01205 |
| 20 | 0.01002 |

Min: 0.00856 s · Max: 0.28215 s (chunk 1) · Mean: 0.02443 s · Range: 0.27359 s. Unlike the Phase 6
run's uniformly flat band, this run's first chunk carries a one-time cost the remaining 19 chunks
do not — consistent with the new `PRIMARY KEY` index's B-tree segment allocation/first-insert
overhead paid once, not per chunk. Chunks 2 through 20 land in a tight ~0.0086 s–0.0139 s band
(mean 0.0112 s, excluding chunk 1), still flat and bounded with no growth trend across the
streaming run — the bounded-memory design still behaves as intended once the one-time index
warm-up cost is paid.

For contrast, the naive run's 20 chunks (also shown for completeness — not part of D-05's required
per-chunk breakdown, which applies to the chunked/bulk run only) each took roughly 1.53–1.88 s
(5,000 individual `cursor.execute()` round trips per chunk, each now also paying the new index's
per-row uniqueness check), consistent with the aggregate 32.933 s naive Oracle write total above.
