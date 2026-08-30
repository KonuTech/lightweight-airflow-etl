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
| Run date | 2026-08-29 (UTC) |
| Machine | `Linux DESKTOP-SION1AN 6.18.33.2-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC x86_64` (WSL2, Docker Desktop-hosted Oracle Database Free `23.26.2-faststart`) |
| CPUs | 32 (`nproc`) |
| Row-split parity | Both runs report identical `total_rows=100000`, `valid_rows=90000`,
`invalid_rows=10000` — confirms `process_chunks()`'s output is unaffected by which write path
consumes it (D-03's isolation requirement holds at full scale, not just the 1,000-row smoke test). |

## Comparison Table

| Metric | Naive (`--mode naive`) | Chunked/Bulk (`--mode bulk`) |
|---|---|---|
| rows/sec (Oracle write only) | 4,268.08 | 780,429.15 |
| Peak memory (RSS, process-lifetime peak) | 130.09 MB (133,216 KB) | 130.61 MB (133,744 KB) |
| Oracle load time (write-only, 20 chunks) | 21.087 s | 0.115 s |
| Wall-clock (full CLI invocation, incl. fixture generation) | 28.040 s | 7.336 s |

Peak RSS is near-identical between the two runs because both processes pay the same fixed
`oracledb`/`pydantic`/generator import and 100K-row Faker-generation cost — the write strategy
itself does not meaningfully change this process's peak memory footprint at this row count; the
real, load-bearing difference this benchmark exists to prove is **Oracle write throughput**, not
memory.

## Speedup Ratio

```
bulk_rows/sec / naive_rows/sec = 780429.15 / 4268.08 = 182.85×
```

The chunked/bulk `executemany()` path is **~182.85× faster** (an **18,285% improvement**) than the
genuine naive per-row loop for Oracle write throughput on this dataset/machine — confirming
PITFALLS.md's "Performance Trap 1" is not a theoretical concern: one Oracle round trip per row is
measurably, drastically more expensive than a single array-bound `executemany()` call per chunk.

## Per-Chunk Timing Breakdown (Chunked/Bulk Run Only)

20 chunks of 5,000 rows each (100,000 rows ÷ `chunk_size=5000`). Each row is a single chunk's
`executemany()` write duration, in seconds — flat and tightly bounded across chunks (no growth
trend), demonstrating the streaming, bounded-memory design actually behaves as intended rather
than degrading as more chunks are processed:

| Chunk | Seconds |
|---|---|
| 1 | 0.00543 |
| 2 | 0.00501 |
| 3 | 0.00558 |
| 4 | 0.00556 |
| 5 | 0.00609 |
| 6 | 0.00587 |
| 7 | 0.00633 |
| 8 | 0.00540 |
| 9 | 0.00484 |
| 10 | 0.00510 |
| 11 | 0.00721 |
| 12 | 0.00715 |
| 13 | 0.00592 |
| 14 | 0.00569 |
| 15 | 0.00597 |
| 16 | 0.00559 |
| 17 | 0.00572 |
| 18 | 0.00570 |
| 19 | 0.00545 |
| 20 | 0.00570 |

Min: 0.00484 s · Max: 0.00721 s · Mean: 0.00577 s · Range: 0.00237 s — all 20 chunks land within a
narrow ~2.4 ms band, no chunk grows meaningfully slower than an earlier one.

For contrast, the naive run's 20 chunks (also shown for completeness — not part of D-05's required
per-chunk breakdown, which applies to the chunked/bulk run only) each took roughly ~1.0–1.1 s
(5,000 individual `cursor.execute()` round trips per chunk), consistent with the aggregate 21.087 s
naive Oracle write total above.
