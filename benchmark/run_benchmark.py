"""CLI benchmark harness (TEST-04) -- proves the chunked/bulk ``executemany()``
Oracle-write design is measurably faster than a genuine naive per-row
``cursor.execute()`` loop, both driven by the exact same
``csv_processor.engine.process_chunks()`` parse pass (D-01/D-03). Only the
Oracle write strategy varies between ``--mode naive`` and ``--mode bulk``.

Run naive and bulk as two SEPARATE subprocess invocations
(``uv run python -m benchmark.run_benchmark --mode naive ...`` /
``--mode bulk ...``) -- ``resource.getrusage(...).ru_maxrss`` is a
process-lifetime peak, not resettable mid-process, so two calls inside one
long-running script would contaminate the second run's peak-memory number
with the first run's already-freed-but-counted allocations (06-RESEARCH.md
Pattern 2).

Prints one ``BENCHMARK_JSON <json>`` line to stdout -- the machine-parseable
summary ``docs/benchmark.md``'s own generation step (Task 2) captures and
parses from each subprocess's output.
"""

from __future__ import annotations

import argparse
import json
import resource
import time
from pathlib import Path

from csv_processor import load
from csv_processor.config import load_config
from csv_processor.config.models import DatasetConfig, is_safe_identifier
from csv_processor.engine import process_chunks
from oracledb import Cursor

from benchmark.naive_loader import run_naive
from generator.generate_csv import generate_rows, write_csv

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"
_BENCHMARK_DATA_DIR = _REPO_ROOT / "data" / "benchmark"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark naive per-row vs. chunked/bulk Oracle writes over the "
            "identical csv_processor.engine.process_chunks() parse pass (TEST-04)."
        )
    )
    parser.add_argument("--dataset", default="customers", help="Dataset name (D-02: 'customers').")
    parser.add_argument(
        "--rows", type=int, required=True, help="Number of data rows to generate/process."
    )
    parser.add_argument(
        "--invalid-ratio",
        type=float,
        default=0.1,
        help=(
            "Fraction of generated rows that are deliberately invalid "
            "(matches generate_csv.py's own default)."
        ),
    )
    parser.add_argument(
        "--seed", type=int, default=20260101, help="Seed for the deterministic fixture generator."
    )
    parser.add_argument(
        "--mode",
        choices=("naive", "bulk"),
        required=True,
        help="Which Oracle write path to exercise.",
    )
    return parser


def _fixture_path(dataset: str, rows: int, seed: int) -> Path:
    """A benchmark-only fixture path, distinct from ``generator/generate_csv.py``'s
    own ``data/<dataset>/<dataset>_<date>.csv`` convention -- never clobbers a
    real day-stamped fixture file, and is byte-identical across the naive/bulk
    subprocess invocations for the same ``(dataset, rows, seed)``.
    """
    return _BENCHMARK_DATA_DIR / f"{dataset}_{rows}_{seed}.csv"


def _delete_target_tables(cursor: Cursor, config: DatasetConfig) -> None:
    """Clean ``valid_table``/``invalid_table`` before timing starts (not
    counted in Oracle load time) so naive/bulk runs -- and repeated runs of
    either -- start from a comparable, empty table state.
    """
    for table in (config.oracle.valid_table, config.oracle.invalid_table):
        if not is_safe_identifier(table):  # defense-in-depth, mirrors load.insert_rows()
            msg = f"unsafe SQL identifier for table name: {table!r}"
            raise ValueError(msg)
        cursor.execute(f"DELETE FROM {table}")
    cursor.connection.commit()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    config = load_config(
        _CONFIGS_DIR / "datasets" / f"{args.dataset}.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )

    fixture_path = _fixture_path(args.dataset, args.rows, args.seed)
    generated = generate_rows(config, args.rows, args.invalid_ratio, args.seed)
    write_csv(generated, config, fixture_path)

    connection = load.get_connection()
    cursor = connection.cursor()
    try:
        _delete_target_tables(cursor, config)

        total_rows = valid_rows = invalid_rows = 0
        chunk_timings: list[float] = []
        oracle_write_seconds = 0.0

        # Both modes consume the SAME process_chunks() generator (D-03) --
        # only the write call inside the loop body differs.
        for chunk_valid, chunk_invalid in process_chunks(fixture_path, config):
            chunk_start = time.monotonic()
            if args.mode == "naive":
                run_naive(cursor, config, chunk_valid)
            else:
                load.insert_rows(
                    cursor,
                    table=config.oracle.valid_table,
                    columns=[column.name for column in config.columns],
                    rows=chunk_valid,
                )
            chunk_elapsed = time.monotonic() - chunk_start
            chunk_timings.append(chunk_elapsed)
            oracle_write_seconds += chunk_elapsed

            total_rows += len(chunk_valid) + len(chunk_invalid)
            valid_rows += len(chunk_valid)
            invalid_rows += len(chunk_invalid)

        connection.commit()
    finally:
        cursor.close()
        connection.close()

    # ru_maxrss is a process-lifetime peak -- read once at process exit
    # (06-RESEARCH.md Pattern 2), never mid-process/reset between modes.
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rows_per_sec = valid_rows / oracle_write_seconds if oracle_write_seconds > 0 else 0.0

    summary = {
        "mode": args.mode,
        "dataset": args.dataset,
        "rows": args.rows,
        "seed": args.seed,
        "total_rows": total_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "oracle_write_seconds": oracle_write_seconds,
        "rows_per_sec": rows_per_sec,
        "peak_rss_kb": peak_rss_kb,
        "chunk_timings_seconds": chunk_timings,
    }
    # Machine-parseable summary line -- Task 2's docs/benchmark.md generation
    # step greps for the "BENCHMARK_JSON " prefix and json.loads()s the rest.
    print(f"BENCHMARK_JSON {json.dumps(summary)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
