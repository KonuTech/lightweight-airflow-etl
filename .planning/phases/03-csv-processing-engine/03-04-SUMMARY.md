---
phase: 03-csv-processing-engine
plan: 04
subsystem: csv-processor
tags: [compression, gzip, zip, streaming, decompression-bomb, generator]

# Dependency graph
requires:
  - phase: 03-csv-processing-engine (plan 03)
    provides: "csv_processor.source.prepare_source()'s _open_raw_stream() seam, deliberately left as a plain open(file_path, 'rb') stub so this plan could fill it in without touching any other function's signature"
provides:
  - "csv_processor.compression.detect_compression(sample) -- magic-byte sniff (gzip 0x1f 0x8b, zip PK\\x03\\x04), pattern-agnostic (D-30)"
  - "csv_processor.compression.open_compressed_stream(file_path, *, compression, max_decompressed_bytes) -- true streaming gzip/zip open, never extract-to-temp-file (D-29)"
  - "source.py's _open_raw_stream() now compression-aware -- every other function in source.py/engine.py unchanged"
  - "generator/generate_csv.py --compress -- gzips generated output, deletes the plain .csv (D-32)"
affects: ["03-05", "04-oracle-bulk-load", "05-airflow-dag-wiring (file_pattern D-31 widening once its file-sensor is built)"]

# Actuals (#2632)
actuals:
  tokens: 8083
  tasks: 3
  commits: 3

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "detect_compression() is NEW code, not a Tier-A port -- the reference repo's own compression.py dispatches by filename extension, which contradicts this project's locked D-30 (magic-byte sniffing, pattern-agnostic); only the streaming-open mechanics and _DecompressionBombGuard were ported"
    - "_open_raw_stream() peeks 4 bytes then seeks back to 0 before deciding whether to return the same handle (uncompressed, zero extra open() calls) or delegate to open_compressed_stream() (compressed) -- the only change 03-04 makes to source.py; every other function keeps calling it by name"
    - "_DecompressionBombGuard loops _BOUNDED_READ_CHUNK_BYTES (64 KiB) reads and checks a cumulative ceiling after each one, so a caller's read(-1) ('read everything') can never materialize an entire decompression-bomb payload in one call before the ceiling check runs"
    - "A zip archive's compressed bytes (never the decompressed CSV, never disk) are buffered into io.BytesIO before zipfile.ZipFile can open it -- a structural ZIP-format requirement (central directory lives at the file's end), not a shortcut around streaming"

key-files:
  created:
    - packages/csv-processor/src/csv_processor/compression.py
    - tests/unit/test_compression.py
  modified:
    - packages/csv-processor/src/csv_processor/errors.py
    - packages/csv-processor/src/csv_processor/source.py
    - generator/generate_csv.py
    - tests/unit/test_generate_csv.py

key-decisions:
  - "D-30's own citation (compression.py's actual approach) is factually wrong per 03-RESEARCH.md Pitfall 1 -- the reference file dispatches by extension. D-30's operative decision (magic-byte sniffing) stands regardless, implemented as fresh code rather than a port, per 03-PATTERNS.md's explicit guidance."
  - "_DecompressionBombGuard's context dict uses this project's own error_code vocabulary key (not the reference repo's diagnostic_code), reusing csv_processor.errors.FileInspectionError -- matching the same class the vendored detect/header.py already raises, per errors.py's own documented convention."
  - "The gzip-wrapped-tracer proof of process_chunks() transparency was added to test_compression.py (not test_engine_chunks.py) since it directly exercises source.py's compression seam and test_compression.py was already an authorized plan artifact."

requirements-completed: [ENGINE-01, ENGINE-07]

coverage:
  - id: D1
    description: "detect_compression() classifies fixture 29 (gzip) and 30 (zip)'s real magic bytes correctly, returns None for fixture 28's plain CSV, and never inspects a filename/extension"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_compression.py::test_detect_compression_classifies_fixture_29_gzip_magic_bytes, ::test_detect_compression_classifies_fixture_30_zip_magic_bytes, ::test_detect_compression_returns_none_for_fixture_28_plain_csv, ::test_detect_compression_never_inspects_extension_or_filename"
        status: pass
    human_judgment: false
  - id: D2
    description: "open_compressed_stream() decompresses fixture 29/30's gzip/zip-wrapped content byte-exactly matching the inner uncompressed payload, and passes an uncompressed file through unchanged"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_compression.py::test_gzip_stream_decompresses_to_exact_inner_payload, ::test_zip_stream_decompresses_to_exact_inner_payload, ::test_uncompressed_passthrough_returns_identical_bytes"
        status: pass
    human_judgment: false
  - id: D3
    description: "A zip archive with 0 or 2 members raises FileInspectionError(CORRUPTED_ARCHIVE) before any member is opened; a corrupted zip raises the same error"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_compression.py::test_zip_archive_with_two_members_raises_before_opening_any_member, ::test_zip_archive_with_zero_members_raises, ::test_corrupted_zip_archive_raises_corrupted_archive"
        status: pass
    human_judgment: false
  - id: D4
    description: "A decompressed stream exceeding an artificially tiny max_decompressed_bytes ceiling raises FileInspectionError(DECOMPRESSION_BOMB_EXCEEDED) partway through read(), never silently truncating; a payload genuinely under the ceiling reads cleanly (negative control)"
    requirement: ENGINE-07
    verification:
      - kind: unit
        ref: "tests/unit/test_compression.py::test_decompression_bomb_ceiling_trips_on_oversized_gzip_stream, ::test_decompression_bomb_ceiling_allows_a_payload_under_the_limit"
        status: pass
    human_judgment: false
  - id: D5
    description: "process_chunks() reads a gzip-wrapped copy of 03-03's tracer fixture through the exact same entrypoint as plain CSV, yielding the identical (valid_rows, invalid_rows) shape"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "tests/unit/test_compression.py::test_process_chunks_reads_gzip_wrapped_tracer_fixture_transparently"
        status: pass
    human_judgment: false
  - id: D6
    description: "Every 03-03 test (test_engine_chunks.py, test_structural_validation.py) still passes unmodified, proving the uncompressed path is unaffected by wiring compression into _open_raw_stream()"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/test_engine_chunks.py tests/unit/test_structural_validation.py -q (25 passed)"
        status: pass
    human_judgment: false
  - id: D7
    description: "generate_csv.py --compress produces data/<dataset>/<dataset>_<date>.csv.gz and does NOT leave the plain .csv file behind; the .gz decompresses to a valid CSV matching the dataset's schema"
    requirement: ENGINE-07
    verification:
      - kind: unit
        ref: "tests/unit/test_generate_csv.py::test_compress_flag_produces_gz_file_and_removes_plain_csv, ::test_compress_flag_produces_valid_gzipped_csv_matching_schema"
        status: pass
    human_judgment: false

# Metrics
duration: 25min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 4: Compressed CSV Input Summary

**Wired transparent, true-streaming `.gz`/`.zip` input into `process_chunks()` via `source.py`'s already-established seam, and gave the generator its own `--compress` flag for a realistic-volume compressed fixture.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-08-29
- **Tasks:** 3/3 completed
- **Files modified:** 6 (2 created, 4 modified)

## Accomplishments

- Built `csv_processor.compression.detect_compression()` — magic-byte sniffing only (gzip `0x1f 0x8b`, zip `PK\x03\x04`), pattern-agnostic per D-30, written as fresh code rather than ported from the reference repo (whose own `detect_compression()` dispatches by extension, contradicting D-30 — 03-RESEARCH.md Pitfall 1)
- Built `csv_processor.compression.open_compressed_stream()` — true single-pass `.gz` streaming via `gzip.GzipFile`, and `.zip`'s buffered-archive-bytes exception (`zipfile.ZipFile` needs its central directory at the file's end before opening any member) — ported from the reference repo with its S3-backed open swapped for plain `Path.open("rb")`
- Ported `_DecompressionBombGuard` verbatim (apart from the exception type and `error_code`-vs-`diagnostic_code` key naming) — enforces a cumulative decompressed-byte ceiling via bounded 64 KiB incremental reads, proven with a real artificially-tiny-ceiling test that fails, plus a negative control that succeeds under the same ceiling
- D-33's "exactly one member" rule: a zip archive with 0 or 2+ members raises `FileInspectionError(CORRUPTED_ARCHIVE)` before any member is opened; a corrupted/truncated archive raises the same error
- Wired `source.py`'s `_open_raw_stream()` seam (03-03's deliberately-left stub): peeks 4 bytes, seeks back, and either returns the same handle (uncompressed — zero extra `open()` calls, byte-for-byte identical to 03-03's behavior) or delegates to `open_compressed_stream()`. No other function in `source.py`/`engine.py` changed.
- Added `generate_csv.py --compress`: gzips the just-written CSV via stdlib `gzip.open()`, then deletes the plain file (mirrors the `gzip` CLI's in-place-replace behavior, matches D-31's widened `file_pattern`)

## Task Commits

Each task was committed atomically:

1. **Task 1: `csv_processor.compression` — magic-byte detection + streaming gzip/zip open (D-29/D-30/D-33)** - `a46c1bb` (feat, tdd)
2. **Task 2: Wire compression into `source.py`'s `_open_raw_stream` seam** - `d7a9d7b` (feat)
3. **Task 3: `generate_csv.py --compress` flag (D-32)** - `4b6d1b4` (feat)

## Files Created/Modified

- `packages/csv-processor/src/csv_processor/compression.py` - `detect_compression()`, `open_compressed_stream()`, `_DecompressionBombGuard`, `_open_zip_stream()`
- `tests/unit/test_compression.py` - fixtures 28/29/30 materialized via `tools.corpus.generators`; magic-byte classification; byte-exact decompression; D-33 multi/zero-member zip rejects; decompression-bomb ceiling (positive + negative control); `process_chunks()` transparency proof against a gzip-wrapped tracer fixture
- `packages/csv-processor/src/csv_processor/errors.py` - added `CORRUPTED_ARCHIVE`, `DECOMPRESSION_BOMB_EXCEEDED` error_code constants
- `packages/csv-processor/src/csv_processor/source.py` - `_open_raw_stream()`'s body replaced with the peek/detect/delegate sequence; one new import (`compression`)
- `generator/generate_csv.py` - `--compress` flag, `main()`'s post-write gzip-and-delete branch
- `tests/unit/test_generate_csv.py` - `--compress` produces `.gz` and removes the plain file, decompresses to a schema-matching CSV, defaults to `False`

## Decisions Made

- Treated D-30 as still locked (magic-byte sniffing) despite its own citation being factually wrong about the reference repo's actual approach, per 03-RESEARCH.md's explicit recommendation and 03-PATTERNS.md's "do not silently port" guidance — implemented as new code, not a vendored port
- Placed the `process_chunks()`-through-gzip integration proof (task 2's own acceptance criterion) in `test_compression.py` rather than adding a new file, since `test_compression.py` was already an authorized plan artifact and the test is fundamentally about the compression seam

## Deviations from Plan

None — plan executed exactly as written. The one file-list nuance (adding the gzip-tracer integration test to the already-authorized `test_compression.py` rather than creating a new file) is a placement choice, not a scope change; every task's `<behavior>`/`<acceptance_criteria>` is satisfied unchanged.

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required. No new PyPI dependencies (gzip/zipfile/io are stdlib).

## Known Stubs

None. `compression.py` is fully wired into `source.py`'s real read path and exercised end-to-end against real corpus fixtures, synthetic edge cases, and a full `process_chunks()` integration test.

## Next Phase Readiness

`.gz`/`.zip` CSV input is now transparently readable through the exact same `process_chunks()` entrypoint as plain CSV, with zero architectural change to any other function in `source.py`/`engine.py`. 03-05 (bounded-memory/chunk-boundary/no-airflow-import proof) has this compression-aware `process_chunks()` to test against. Phase 5's future file-sensor will need D-31's widened `file_pattern` (e.g. `"customers_*.csv*"`) once built — not this plan's concern, but the config-side glob widening is still an open follow-up noted in 03-CONTEXT.md. No blockers for 03-05 or subsequent phases.

## Self-Check: PASSED

All 6 modified/created source/test files confirmed present on disk; all 3 task commit hashes (`a46c1bb`, `d7a9d7b`, `4b6d1b4`) confirmed in git log.
