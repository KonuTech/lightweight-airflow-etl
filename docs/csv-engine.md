# CSV Engine: `csv_processor.engine.process()` / `process_chunks()`

This document covers the reusable, Airflow-agnostic CSV processing engine
(`packages/csv-processor/src/csv_processor/engine.py`) — its detect → parse → validate →
normalize → chunk sequence, the closed `Status` contract it returns, and the bounded-memory
chunking guarantee (ENGINE-07). See `docs/architecture.md` for where this engine sits in the
overall request path and `docs/oracle.md` for what happens to its output once it reaches Oracle.

## The Detect → Parse → Validate → Normalize → Chunk Sequence

`process_chunks(file_path, config)` is the engine's public generator (D-11) — it detects the
file's shape **once**, then streams `config.processing.chunk_size`-row chunks:

1. **Detect (once per file, not once per chunk)** — `source.prepare_source()` runs compression
   detection, encoding detection, CSV dialect sniffing, and header detection exactly once at the
   start of `process_chunks()`, before the first row is read. This is deliberate: re-detecting per
   chunk would be both wasteful and inconsistent (a mid-file dialect "re-sniff" could disagree with
   the file's actual, fixed dialect).
2. **Parse + structural short-circuit (per row)** — each raw row is compared against the detected
   header's field count *before* anything else runs. A field-count mismatch is the **only**
   structural check that always wins (D-13's short-circuit ordering): `validate.check_row()` is
   **never called** on a structurally malformed row, and its `error_code` is always
   `WRONG_COLUMN_COUNT`. A missing trailing field becomes `None` in that row's dict (distinguishing
   "structurally absent" from "present but empty string"), never `""`.
3. **Validate (per row, only if structurally sound)** — `validate.check_row()` runs the full
   nullability-then-type check across every configured column. Structural/type/nullability
   validation only — referential/uniqueness/volume-anomaly/completeness/circuit-breaker validation
   is explicitly out of scope for this project (see `PROJECT.md`'s Out of Scope section).
4. **Normalize (valid rows only)** — `validate.normalize_row()` converts a structurally- and
   type-valid row's string values into their target Python types before it's queued for Oracle.
5. **Chunk (streamed)** — rows accumulate into `(valid_rows, invalid_rows)` tuples of at most
   `config.processing.chunk_size` rows each, yielded one chunk at a time.

This ordering means a wrong-column-count row can never reach `validate.check_row` — a row that's
already known to be structurally broken doesn't get a spurious "field X failed type validation"
error layered on top of its real problem (`WRONG_COLUMN_COUNT`).

## The Bounded-Memory Chunking Guarantee (ENGINE-07)

`process_chunks()` uses `itertools.batched()` over a lazy row iterator — **only one chunk's rows
are ever held in memory at once**, regardless of the source file's total size. This is what lets
`benchmark/run_benchmark.py` process a ~100,000-row file with peak RSS around 130 MB (see
`docs/benchmark.md`) rather than scaling with file size. Detection (step 1 above) is the one
exception to "streamed" — it reads a bounded sample once, up front, never the whole file.

A chunk that ends up entirely valid still yields (e.g. `(valid_rows, [])`), and a file with a
valid header but zero data rows yields **no chunks at all** — this is not treated as an error
case (D-20), just an empty result.

## The 7 Closed `Status` Values

`csv_processor.models.Status` is a closed, 7-member enum (`packages/csv-processor/src/
csv_processor/models.py`) — copied verbatim from `REQUIREMENTS.md`, with no 8th member added for
this phase's idempotency short-circuit (a re-processed file returns the *original* recorded
status instead of inventing a new one):

| Status | Meaning |
|---|---|
| `SUCCESS` | Every row in the file was valid. |
| `SUCCESS_WITH_INVALID_ROWS` | The file processed to completion, but at least one row was structurally or type/nullability invalid — those rows landed in `<dataset>_invalid`, not a fatal error. |
| `FILE_NOT_FOUND` | `file_path` did not exist on disk when `process()` was called — checked before any Oracle connection is opened. |
| `INVALID_FILE` | A whole-file structural problem was detected (e.g. undetectable encoding/dialect) *before any row was processed* — the one case where `process_chunks()` itself raises (`StructuralValidationError`), rather than producing per-row invalid entries. |
| `CONFIGURATION_ERROR` | `config` failed re-validation at the top of `process()` — e.g. a stale runtime `conf` passed a config that no longer validates. Checked before any file I/O. |
| `DATABASE_ERROR` | An `oracledb.Error` occurred during the Oracle-writing phase (connection failure, a row-level constraint violation that isn't the idempotency race, etc.). |
| `PROCESSING_ERROR` | Any other unexpected exception during `process()`'s sequence — the catch-all, translated rather than left to propagate. |

None of these 7 statuses ever causes `process_csv_task` to fail the Airflow task (see
`PROJECT.md`'s Key Decisions table) — every run, success or domain failure, reaches
`report_result_task` so its "concise summary" logging always fires. Only a genuinely unexpected
exception that `process()` itself can't translate would fail the task, and `process()`'s own
`except Exception:` branch (translating to `PROCESSING_ERROR`) makes that vanishingly rare in
practice.

## `ProcessingResult`'s Fields

The single object `process()` returns (and the DAG round-trips through Airflow's XCom as
`result.model_dump(mode="json")`):

| Field | Type | Notes |
|---|---|---|
| `status` | `Status` | One of the 7 values above. |
| `dataset` | `str` | Echoed from `config.dataset`. |
| `file_name` | `str` | The processed file's basename (never the full path — D-08). |
| `total_rows` | `int` | `valid_rows + invalid_rows`. |
| `valid_rows` | `int` | Rows that passed validation and were inserted into `<dataset>_valid`. |
| `invalid_rows` | `int` | Rows that failed validation and were inserted into `<dataset>_invalid`. |
| `duration_seconds` | `float` | Real elapsed wall time (`time.monotonic()` delta), computed at every return path via one shared `_build_result()` helper so every status path's timing is measured consistently. |
| `checksum` | `str \| None` | The file's SHA-256 hex digest — `None` only on `FILE_NOT_FOUND`/`CONFIGURATION_ERROR` (checksum is never computed on those paths, since they short-circuit before file I/O). |

`ProcessingResult` is `frozen=True, extra="forbid"` (`csv_processor.models`), matching the rest of
this codebase's config-model convention — a typo'd field would fail at construction, not at some
later, harder-to-trace point in the DAG.

## Idempotency (LOAD-04)

Before running `process_chunks()` at all, `process()` checks `ingestion_metadata` for an existing
`(dataset, checksum)` record (`load.find_existing_ingestion()`) — a match returns the **original**
recorded `ProcessingResult`, verbatim, without touching the file a second time. See
`docs/oracle.md`'s "Idempotency" section for the full round trip, including the concurrent-writer
race-handling path.
