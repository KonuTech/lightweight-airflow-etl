---
phase: 03-csv-processing-engine
plan: 05
subsystem: testing
tags: [pytest, rlimit-as, ast-scan, rfc4180, chunking, makefile]

# Dependency graph
requires:
  - phase: 03-csv-processing-engine (plan 03)
    provides: "csv_processor.engine.process_chunks() -- the D-11 lazy chunked generator this plan proves bounds and cross-chunk row_number continuity on"
  - phase: 03-csv-processing-engine (plan 04)
    provides: "csv_processor.compression's transparent gzip/zip input -- this plan's byte-level tests exercise the same process_chunks() entrypoint, uncompressed"
provides:
  - "tests/unit/test_engine_chunks.py additions: exact chunk-count/size assertions + literal 1..12 row_number sequence across chunk boundaries; an RLIMIT_AS/subprocess proof that process_chunks() streams the corpus's real ~60 MiB fixture under bounded memory while a buffering negative control dies under the identical cap"
  - "tests/unit/test_byte_level_hard.py -- corpus fixtures 23-27 (byte_level_hard category) proven via fixture-local ad hoc DatasetConfig instances, closing the one corpus category no prior Phase 3 plan exercised"
  - "tests/unit/test_no_airflow_import.py -- an AST-based, self-verifying scan enforcing ENGINE-09 across the whole csv_processor package tree"
  - "make verify-phase3 -- Phase 3's single combined local gate, matching verify-phase2's exact convention"
  - "A real bug fix in csv_processor.detect.dialect.detect_dialect(): a sample containing a raw NUL byte no longer crashes clevercsv's own parser uncaught -- folded into the module's existing declined-detection pattern"
affects: ["04-oracle-bulk-load", "06-ci-cd-and-docs (CI-01's future wiring of verify-phase3 into GitHub Actions)"]

# Actuals (#2632)
actuals:
  tokens: 6343
  tasks: 4
  commits: 4

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "RLIMIT_AS bounded-memory proofs against process_chunks() need a materially larger address-space cap than a raw line-iterator equivalent (100 MiB vs. test_corpus_bounded_memory.py's own 24 MiB) -- the gap is entirely import-time overhead from process_chunks()'s own dependency stack (pydantic-core's compiled extension, chardet's bundled probability-model tables), not a bounded-memory regression in process_chunks() itself; the empirically-verified cap and its rationale are documented inline in the test file's own header comment, mirroring test_corpus_bounded_memory.py's own honesty convention"
    - "Only invalid-row dicts carry row_number (D-09) -- proving a literal, gap-free row_number sequence across ALL rows in a multi-chunk file requires making every row in the proof fixture deliberately invalid, not a subset, since a valid row's typed dict has no row_number key to collect"
    - "clevercsv.Detector().detect() has a THIRD 'nothing usable' outcome beyond the two already documented in dialect.py's own docstring (degenerate SimpleDialect('','','') and a bare None return): it raises clevercsv.exceptions.Error outright on a sample containing a raw NUL byte -- folded into the same declined-detection return, never a crash, matching the file's own established idiom"

key-files:
  created:
    - tests/unit/test_byte_level_hard.py
    - tests/unit/test_no_airflow_import.py
  modified:
    - tests/unit/test_engine_chunks.py
    - Makefile
    - packages/csv-processor/src/csv_processor/detect/dialect.py

key-decisions:
  - "Chunk-boundary test makes ALL 12 rows deliberately invalid (empty required id) rather than 'a few', since only invalid rows carry row_number (D-09) -- proving the full literal 1..12 sequence with no gaps requires every row to be inspectable, not a sampled subset"
  - "Bounded-memory RLIMIT_AS cap raised to 100 MiB (104,857,600 bytes) from test_corpus_bounded_memory.py's own 24 MiB, empirically determined this session via a standalone probe script across 8 cap values -- the gap is process_chunks()'s own import-time overhead (pydantic-core, chardet's model tables), not a memory-boundedness regression; documented inline with the exact failure modes observed at each rejected cap"
  - "Fixture 26 (embedded_nul_byte) is tested via its own fixture-local ad hoc DatasetConfig for consistency with the rest of test_byte_level_hard.py, even though its header happens to match orders.json's real column set exactly and would not itself trip RESEARCH.md Pitfall 3's structural-reject trap -- documented explicitly in the module docstring so this isn't misread as another Pitfall-3 case"

patterns-established: []

requirements-completed: [ENGINE-01, ENGINE-04, ENGINE-05, ENGINE-07, ENGINE-09, TEST-01]

coverage:
  - id: D1
    description: "process_chunks() yields exactly 3 chunks (sizes [5,5,2]) for a 12-row file at chunk_size=5, with row_number forming the literal gap-free sequence 1..12 across all chunk boundaries"
    requirement: ENGINE-05
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_chunks.py::test_chunk_boundaries_and_cross_chunk_row_number_continuity"
        status: pass
    human_judgment: false
  - id: D2
    description: "process_chunks() streams the corpus's real ~60 MiB fixture 28 under a 100 MiB RLIMIT_AS address-space cap, while a buffering (list(process_chunks(...))) negative control dies under the identical cap -- empirically proving ENGINE-07's bounded-memory guarantee, not just via code inspection"
    requirement: ENGINE-07
    verification:
      - kind: unit
        ref: "tests/unit/test_engine_chunks.py::test_process_chunks_streaming_survives_the_rlimit_as_cap, ::test_process_chunks_buffering_dies_under_the_identical_rlimit_as_cap"
        status: pass
    human_judgment: false
  - id: D3
    description: "Corpus fixtures 23-27 (byte_level_hard) each parse through process_chunks() via a fixture-local ad hoc config, asserting the exact recovered field value: embedded newline, embedded delimiter, doubled-quote unescaping, an embedded NUL byte surviving as a literal '\\x00' character, and a 10,001-character field parsing unsplit"
    requirement: ENGINE-01
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/test_byte_level_hard.py -q (5 passed)"
        status: pass
    human_judgment: false
  - id: D4
    description: "No file under packages/csv-processor/src/csv_processor/ imports anything from the airflow namespace, verified by an AST-based scan proven against a synthetic import-airflow file and a negative control (not a scanner that vacuously always returns False)"
    requirement: ENGINE-09
    verification:
      - kind: unit
        ref: "uv run pytest tests/unit/test_no_airflow_import.py -q (4 passed)"
        status: pass
    human_judgment: false
  - id: D5
    description: "make verify-phase3 runs the complete Phase 3 unit test suite (175 tests) and exits 0 -- the single command confirming the whole csv_processor engine is correct"
    requirement: TEST-01
    verification:
      - kind: unit
        ref: "make verify-phase3 (175 passed)"
        status: pass
    human_judgment: false

# Metrics
duration: 40min
completed: 2026-08-29
status: complete
---

# Phase 3 Plan 5: Phase 3's Closing Test/Verification Gate Summary

**Proved chunk-boundary/cross-chunk row_number continuity and ENGINE-07's bounded-memory guarantee empirically against the real ~60 MiB corpus fixture, closed out the byte_level_hard corpus category (fixtures 23-27, the one category no prior Phase 3 plan exercised), added ENGINE-09's self-verifying no-Airflow-import enforcement, and wired `make verify-phase3` as Phase 3's single combined local gate.**

## Performance

- **Duration:** ~40 min
- **Completed:** 2026-08-29
- **Tasks:** 4/4 completed
- **Files modified:** 5 (2 created, 3 modified)

## Accomplishments

- Extended `tests/unit/test_engine_chunks.py` with a chunk-boundary test: a 12-row ad hoc fixture at `chunk_size=5` yields exactly 3 chunks with sizes `[5, 5, 2]`, and `row_number` forms the literal, gap-free sequence `1..12` across all 3 chunk boundaries (every row deliberately invalid so `row_number` — only present on invalid-row dicts, D-09 — is directly inspectable for the FULL sequence)
- Added an `RLIMIT_AS`/subprocess proof, reusing `test_corpus_bounded_memory.py`'s exact technique: `process_chunks()` streams the corpus's real ~62,915-row (~60 MiB) fixture 28 under a 100 MiB address-space cap while a `list(process_chunks(...))` buffering negative control dies under the identical cap — empirically proving ENGINE-07's bounded-memory guarantee, not just via code inspection
- Built `tests/unit/test_byte_level_hard.py` — the 5 `byte_level_hard` corpus fixtures (23-27), each run through a fixture-local ad hoc `DatasetConfig` per 03-RESEARCH.md Pitfall 3, asserting the exact recovered field value (embedded newline preserved as one field, embedded delimiter preserved as one field, doubled-quote unescaping, an embedded NUL byte surviving intact, a 10,001-character field parsing unsplit) — closing the corpus at all 30/30 fixtures exercised across this phase
- Built `tests/unit/test_no_airflow_import.py` — an AST-based `_imports_airflow()` scanner, proven against a synthetic `import airflow` file and a negative control before being trusted against the real `csv_processor` package tree (ENGINE-09)
- Added `make verify-phase3` to the Makefile, matching `verify-phase2`'s exact convention (a plain `uv run pytest tests/unit/ -x` — no new fixture-digest mechanism this phase)
- Full unit suite: 175 tests passing, zero regressions

## Task Commits

Each task was committed atomically:

1. **Task 1: Chunk-boundary, cross-chunk row_number, and bounded-memory proof** - `c5567cc` (test, tdd)
2. **Task 2: RFC-4180 byte-level edge cases — corpus fixtures 23-27 (byte_level_hard)** - `e64183d` (test, tdd)
3. **Task 3: ENGINE-09 enforcement — no Airflow import anywhere in csv_processor** - `82a07f1` (test, tdd)
4. **Task 4: make verify-phase3 — Phase 3's combined local gate** - `0a65ace` (feat)

## Files Created/Modified

- `tests/unit/test_engine_chunks.py` - chunk-boundary/row_number-continuity test, `large_fixture_path` self-materializing fixture, `_STREAMING_SCRIPT`/`_BUFFERING_SCRIPT` RLIMIT_AS proof
- `tests/unit/test_byte_level_hard.py` - corpus fixtures 23-27, fixture-local ad hoc `DatasetConfig` instances
- `tests/unit/test_no_airflow_import.py` - `_imports_airflow()` AST scanner + self-test + real-package-tree assertion
- `Makefile` - `verify-phase3` target + `.PHONY` entry
- `packages/csv-processor/src/csv_processor/detect/dialect.py` - `detect_dialect()` now catches `clevercsv.exceptions.Error` (a NUL-byte sample) and folds it into the existing declined-detection outcome (see Deviations)

## Decisions Made

- Made all 12 rows in the chunk-boundary fixture deliberately invalid (rather than "a few") since only invalid-row dicts carry `row_number` (D-09) — this is the only way to assert the complete, gap-free `1..12` sequence rather than a sampled subset
- Determined the bounded-memory `RLIMIT_AS` cap (100 MiB) empirically via a standalone probe script run outside the test suite, trying 8 cap values from 24 MiB up to 200 MiB and recording the exact failure mode at each rejected value (`pydantic_core`'s compiled extension failing to `mmap` itself below ~94 MiB; `chardet.models._decompress_tables` `MemoryError` at intermediate caps) — the chosen value and its rationale are documented in the test file's own header comment, not silently picked
- Fixture 26 uses its own fixture-local ad hoc config for consistency with the rest of `test_byte_level_hard.py`, even though (unlike fixtures 23/24/25/27) its header already matches `orders.json`'s real column set exactly and would not itself trip RESEARCH.md Pitfall 3's structural-reject trap — the module docstring says so explicitly to avoid overstating the Pitfall-3 finding

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `clevercsv.Detector().detect()` crashes uncaught on a sample containing a raw NUL byte**
- **Found during:** Task 2, writing the fixture 26 (`embedded_nul_byte`) test
- **Issue:** `csv_processor.detect.dialect.detect_dialect()` already handles two "nothing usable" outcomes from `clevercsv.Detector().detect()` (a degenerate `SimpleDialect('', '', '')` and a bare `None` return, both documented in the module's own docstring) but not a third: a sample containing a raw NUL (0x00) byte makes clevercsv's own C parser raise `clevercsv.exceptions.Error: line contains NULL byte` outright, uncaught. This crashed `source.py`'s PASS 1 detection (called unconditionally by `process_chunks()` before any row is read) for fixture 26's `customer_id` field, even though `csv.reader` — the actual PASS 2 real-read parser, and stdlib's own documented behavior — handles an embedded NUL byte in a field without raising at all. This directly blocked Task 2's own acceptance criterion ("Fixture 26's test asserts no exception is raised").
- **Fix:** Caught `clevercsv.exceptions.Error` around the `clevercsv.Detector().detect(sample)` call and folded it into the same `declined=True` return the module already uses for its other two "nothing usable" outcomes. `source.py`'s cross-check already treats `declined` as "defer to config" (D-28), and the real PASS 2 read never depends on this function's output for its own dialect (it uses `config.csv.delimiter` directly) — so this fix has zero effect on any already-passing test and is scoped to exactly the one new failure mode.
- **Files modified:** `packages/csv-processor/src/csv_processor/detect/dialect.py`
- **Verification:** `uv run pytest tests/unit/test_byte_level_hard.py -x -q` (was failing on fixture 26 before the fix, passes after); `uv run pytest tests/unit/ -q` — full 171-then-175-test suite green with zero regressions before and after.
- **Committed in:** `e64183d` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 real bug fix)
**Impact on plan:** The fix is required for fixture 26's own documented `<behavior>` ("parses without raising") to hold at all — without it, any real-world file whose 64 KiB detection sample happens to contain a NUL byte would crash `process_chunks()` outright before a single row is processed, which is a genuine ENGINE-01/ENGINE-09-adjacent correctness gap this plan's own test exists to catch. No other production code path is touched; every other Phase 3 test continues passing unchanged.

## Issues Encountered

None beyond the one deviation documented above.

## User Setup Required

None — no external service configuration required.

## Known Stubs

None. `detect/filename.py`/`detect/schema.py` remain unwired per 03-02's own D-27 parity decision (pre-existing, not introduced by this plan) — this plan's own new files (`test_byte_level_hard.py`, `test_no_airflow_import.py`) and modifications (`test_engine_chunks.py`, `Makefile`, `dialect.py`) are fully wired and exercised, with no placeholder/hardcoded-empty data paths.

## Next Phase Readiness

Phase 3 is now closed out: `make verify-phase3` is the single command confirming the whole `csv_processor` engine is correct (175 tests, 0 failures), and every one of the corpus's 30 fixtures has been exercised by some plan in this phase (1-8 by 03-02, 9-22 by 03-03, 23-27 by this plan, 28-30 by 03-04/this plan). `csv_processor.engine.process_chunks(file_path, config)` remains the complete, tested public surface Phase 4 (Oracle Bulk Load, Idempotency & Engine Entrypoint) builds its `process()`/`ProcessingResult` wrapper on top of — nothing in this plan changed that function's signature or behavior, only proved properties it already had. Phase 6's future CI wiring (CI-01) has a ready-made `make verify-phase3` target to invoke. No blockers for Phase 4.

## Self-Check: PASSED

All 5 created/modified files confirmed present on disk; all 4 task commit hashes (`c5567cc`, `e64183d`, `82a07f1`, `0a65ace`) confirmed in git log.
