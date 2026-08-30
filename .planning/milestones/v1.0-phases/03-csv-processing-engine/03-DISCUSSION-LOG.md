# Phase 3: CSV Processing Engine - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-29
**Phase:** 3-CSV Processing Engine
**Areas discussed:** Invalid-row storage shape, Error code taxonomy, Structural failure scope,
Detection cross-check strictness, Async/optimization

---

## Invalid-row storage shape

| Question | Selected answer |
|---|---|
| How should the DDL conflict (`_INVALID` tables mirror `_VALID`'s native typed/NOT-NULL columns) be resolved, given ENGINE-06 needs original field values? | Widen `_INVALID` columns to nullable VARCHAR2, storing every original field as its raw string |
| Who executes the DDL migration? | Phase 3 now, as prep |
| What shape should Phase 3's invalid-row Python output take? | dict per row: `{col_name: original_string_value, ..., error_code, error_message, source_file, row_number}` |
| Should VALID rows use the same dict-per-row shape with typed values? | Yes — `{col_name: typed_value, ...}` |
| How should the ALTER TABLE migration be delivered given Phase 1's init-script-only-runs-on-first-boot pattern? | Both: new numbered init script AND applied directly against the running container |
| What size should the widened VARCHAR2 columns use? | Keep each column's current size |
| Blank CSV field vs. missing column — empty string or SQL NULL? | Empty string for blank; NULL only if truly absent |
| Should the widened `_INVALID` tables also keep a `raw_line` column? | Yes, add `raw_line` as defense-in-depth |
| What does `row_number` count? | 1-indexed post-header row, initially "blanks excluded" — later revised (see below) |
| What should `source_file` record? | Basename only |

**Notes:** This area consumed 10 questions because a genuine architectural conflict was
discovered during codebase scouting (not a user prompt) — the Phase 1 `_INVALID` DDL uses native
typed/NOT-NULL columns that cannot literally hold a malformed/missing original value, directly
contradicting ENGINE-06. The `row_number` decision was revisited and finalized in the Structural
failure scope area below (blank lines DO consume a `row_number` after all — see that section).

---

## Error code taxonomy

| Question | Selected answer |
|---|---|
| How granular should `error_code` be? | One specific code per failure type |
| When a row fails multiple checks, which `error_code` wins? | Run all checks, report highest-priority violation |
| Clarification: do type/nullability checks still run against a structurally-broken row? | No — structural failure short-circuits; "run all checks" applies only once structural passes |
| Type vs. nullability priority across different columns? | Nullability first, then type |
| Tie-breaking across columns with the same kind of violation? | First column in `config.json`'s declared order |

**Notes:** Direct continuation of Phase 2's D-16d, which explicitly deferred the real
`error_code` vocabulary to this phase.

---

## Structural failure scope

| Question | Selected answer |
|---|---|
| Header-level mismatches vs. row-level ragged rows — whole-file reject or per-row? | Header-level = whole file; row-level = per-row |
| Extra/unrecognized header column? | Hard reject (whole file) |
| Duplicate column name in header? | Same hard reject |
| Empty file / header-only-zero-rows — how handled? | Empty file = whole-file reject; header-only = success with 0 rows |
| Must header column order match config's declared order? | No — order-independent, matched by name |
| Is header column-name matching case-sensitive? | Yes — case-sensitive, exact match |
| What Python-level signal does a whole-file reject produce? | Raise a plain exception (`StructuralValidationError`); Phase 4 translates to `INVALID_FILE` — confirmed via Context7 against Airflow's `AirflowFailException`/retry semantics |
| Blank line between data rows — silently skipped or flagged invalid? | Flagged as its own invalid row |
| Follow-up: does a flagged blank line consume a `row_number`? | Yes — revises the earlier "blanks excluded" framing; `row_number` is simply every post-header line reaching row processing |

**Notes:** The corpus's own `structural` fixture category (8 fixtures) directly informed this
area — each option was checked against a specific fixture's `expect:` reason where one existed.

---

## Detection cross-check strictness

| Question | Selected answer |
|---|---|
| Auto-detect + cross-check against config, or trust config outright? | Auto-detect + cross-check |
| BOM handling in the header? | Strip BOM before any parsing, always |
| Which of the 5 Tier-A `detect/*` modules does Phase 3 vendor? | All 5, for completeness |
| Should a low-confidence detection disagreement still raise an error? | No — defer to config below a confidence threshold |
| Where do detection-vs-config mismatches fit in the error taxonomy? | Same exception type, distinct `error_code` family |
| SCOPE ADDITION (user-initiated): should the engine support compressed CSV input? | Yes — streaming decompression, never extract-to-temp-file first |
| Should the engine also write compressed output? | Deferred — no current requirement/DAG step writes file output at all |
| How is a compressed file detected? | Magic-byte sniffing, pattern-agnostic |
| Follow-up: `file_pattern` doesn't match `.csv.gz` — how to fix? | Widen `file_pattern` to match both, e.g. `"customers_*.csv*"` |
| Should `generate_csv.py` get a `--compress` flag? | Yes |
| How should a multi-entry zip be handled? | Require exactly one entry, error otherwise |

**Notes:** The user introduced compressed-input support as new scope mid-discussion ("modern
processing solutions are not extracting contents of compressed [files]... it should be able to
read binaries like .csv.gz"), superseding Phase 2's D-16c framing that compressed fixtures existed
only to exercise `compression.py` with no real pipeline need. A follow-up gap (file_pattern glob
mismatch) was caught and resolved in the same area.

---

## Async / optimization

**User's proposal:** async I/O for the file read itself — "reading in many bulks instead of a
sequential read bulk by bulk."

**Claude's response:** Explained why this doesn't hold up for this project: CSV row boundaries
aren't safely discoverable in parallel (an embedded newline inside a quoted field means you can't
seek to arbitrary chunk offsets — ARCHITECTURE.md's own Anti-Pattern 3 warns against exactly this
approach); `asyncio` doesn't help a local mounted-volume read the way it helps network I/O (no real
I/O-wait to overlap, and Python has no true async local-disk I/O on this stack); the actual
bottleneck is Oracle round-trip count, already addressed by chunked bulk `executemany()`; and
concurrent/out-of-order chunk reads would conflict with the sequential `row_number`/tie-breaking
guarantees already locked in during this same discussion.

**User's decision:** Still wanted it explored — captured as a deferred idea (not built now), to
revisit only if Phase 6's benchmark (TEST-04) shows CPU-bound parsing (not Oracle round-trips) is
the actual limiter.

**Follow-up locked in:** the engine's chunk-processing function is a **lazy generator**
(`Iterator[tuple[list[dict], list[dict]]]`), so only one chunk's rows are ever in memory at once —
enforces ENGINE-07's bounded-memory guarantee at the API boundary itself.

---

## Claude's Discretion

- Exact module/file layout within `packages/csv-processor/src/csv_processor/` (`detect/`,
  `source.py`, `normalize.py`/`validate.py`, `engine.py`) — ARCHITECTURE.md's directory sketch is a
  reasonable starting point.
- Detection's "bounded sample" size for dialect/encoding sniffing — match the reference repo's own
  64 KiB precedent unless a concrete reason to differ surfaces.
- Exact exception class hierarchy and the full `error_code` enum member list — implementation
  detail once the naming pattern (one code per failure type, `DETECT_*` vs `STRUCT_*` families) is
  established.

## Deferred Ideas

- **Concurrent/async chunk reading for file I/O** — revisit only if Phase 6's benchmark shows
  CPU-bound parsing is the actual limiter, not Oracle round-trips.
- **Writing compressed CSV output** — no current requirement or DAG step produces file output at
  all; note for a future phase/milestone if a file-based export/archival step is ever added.
