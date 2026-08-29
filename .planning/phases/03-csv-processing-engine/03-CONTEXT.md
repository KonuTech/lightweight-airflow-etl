# Phase 3: CSV Processing Engine - Context

**Gathered:** 2026-08-29
**Status:** Ready for planning

<domain>
## Phase Boundary

Given a raw CSV file and a dataset's `config.json`, the `csv_processor` engine correctly separates
valid (type-converted) rows from invalid (error-tagged) rows, processing in bounded-memory chunks,
with zero Airflow dependency. This phase builds the internal detect → parse → validate → normalize
→ split pipeline; the full `process()` public entrypoint with `ProcessingResult`/status semantics
and Oracle loading is explicitly Phase 4's job (its own title is "Oracle Bulk Load, Idempotency &
Engine Entrypoint" — REQUIREMENTS.md maps ENGINE-08/LOAD-01..04 there, not here).

Requirements: ENGINE-01, ENGINE-02, ENGINE-03, ENGINE-04, ENGINE-05, ENGINE-06, ENGINE-07,
ENGINE-09, TEST-01. Depends on Phase 2 (config contract shape + generated fixtures/corpus to
validate against).

</domain>

<decisions>
## Implementation Decisions

### Invalid-Row Storage Shape (discovered DDL conflict)

- **D-01 (major discovery):** `customers_invalid`/`orders_invalid` (Phase 1 DDL) currently mirror
  the `_VALID` tables' native typed/NOT-NULL columns (`event_ts TIMESTAMP NOT NULL`,
  `order_id VARCHAR2 NOT NULL`, `amount NUMBER(12,2)`) — but ENGINE-06 requires invalid rows to
  carry their **original field values**, and a malformed date string or empty required field
  cannot literally be inserted into a native NOT NULL/typed column as currently defined. Resolved:
  widen every data column on both `_INVALID` tables to **nullable VARCHAR2**, storing every
  original field as its raw string (never converted/typed). `error_code`/`error_message`/
  `source_file`/`row_number` are unaffected. — **Reversibility:** costly — **rationale:** reverting
  means another DDL migration plus touching Phase 4's insert-binding code once it exists.
- **D-02:** The DDL migration (D-01) is executed **now, in Phase 3**, even though Phase 3 itself
  never inserts into Oracle (that's Phase 4/TEST-02) — fixes the schema gap as soon as discovered
  rather than carrying it forward.
- **D-03:** Migration delivery follows Phase 1's own precedent (D-06/01-02-SUMMARY.md): **both** a
  new numbered init script (`docker/oracle/init/04_widen_invalid_columns.sql` or similar, for a
  genuinely fresh `docker compose down -v && up`) **and** the same `ALTER TABLE` statements applied
  directly against the currently-running container (`docker compose exec sqlplus`), so the existing
  Phase-1-provisioned database doesn't need a full volume wipe.
- **D-04:** Widened VARCHAR2 columns **keep each column's current size** (`customer_id`/`order_id`/
  `country` stay `VARCHAR2(64)`, `name` stays `VARCHAR2(255)`, etc.) rather than uniformly widening
  to `VARCHAR2(4000)` — an oversized original value is its own distinct, worth-flagging error
  condition, not something to silently accommodate.
- **D-05:** A genuinely blank CSV cell stores as an **empty string**; SQL `NULL` is reserved for a
  column that's truly **absent** from the row (a ragged/short row missing that field entirely) —
  these are different failure signals worth distinguishing.
- **D-06:** Both `_INVALID` tables also get a **`raw_line`** column (`VARCHAR2(4000)`/CLOB) holding
  the entire original CSV line as one string, as defense-in-depth for the corpus's `byte_level_hard`
  category where a per-column split might itself be unreliable — in addition to (not instead of)
  the per-column widened values.
- **D-07:** `row_number` = a **1-indexed count of every line after the header that reaches
  row-processing** — including blank lines (D-24 below flags a blank line as its own invalid row,
  so it consumes a `row_number` like any other row; there is exactly one counting rule, not two).
- **D-08:** `source_file` records the **basename only** (e.g. `customers_20260829.csv`) — matches
  `config.json`'s `file_pattern` glob, stable across environments, and is what LOAD-03's idempotency
  check keys on anyway (filename + checksum + dataset).

### Row Output Shape (Phase 3 ↔ Phase 4 interface)

- **D-09:** An invalid row is a **dict**: `{col_name: original_string_value, ..., "error_code":
  ..., "error_message": ..., "source_file": ..., "row_number": ...}` — self-describing, matches the
  config's column-name-driven schema, easy to assert against by key in unit tests.
- **D-10:** A valid row uses the **same dict shape** with typed Python values instead of strings:
  `{col_name: typed_value, ...}` (`datetime.date`, `Decimal`, `int`, `str`, `bool`), no `error_*`
  keys — gives Phase 4's Oracle-loading code one consistent access pattern for both streams.
- **D-11:** The engine's chunk-processing function is a **lazy generator** — e.g.
  `process_chunks(file_path, config) -> Iterator[tuple[list[dict], list[dict]]]` (valid rows,
  invalid rows) per chunk. Only one chunk's rows are ever in memory at once, enforcing ENGINE-07's
  bounded-memory guarantee at the API boundary itself rather than trusting the caller to discard
  promptly. Phase 4's `process()` loops over it, bulk-inserting and discarding each chunk before
  pulling the next.

### Error Code Taxonomy (D-16d from Phase 2 explicitly deferred this to Phase 3)

- **D-12:** `error_code` is **one specific code per failure type** (e.g.
  `MISSING_REQUIRED_COLUMN`, `DUPLICATE_COLUMN_NAME`, `WRONG_COLUMN_COUNT`, `NO_HEADER_ROW`,
  `TYPE_MISMATCH`, `NULL_VIOLATION`, `INVALID_DATE_FORMAT`, `DECIMAL_PRECISION_EXCEEDED`, etc.) —
  roughly one per structural/type/nullability check the engine actually runs, not a handful of
  broad categories. Directly queryable/groupable in `_INVALID`, and each corpus fixture's `expect:`
  prose maps 1:1 to a real code.
- **D-13:** Check execution: a **structurally-broken row short-circuits** — its `error_code` is
  always the structural one; type/nullability checks are skipped for that row (checking misaligned
  fields would be meaningless, per FEATURES.md's own reasoning). Once a row **passes** structural
  validation, **all** type checks and **all** nullability checks run across every column, and
  `error_code` reports whichever violation is highest-priority among what was found (not simple
  first-check-wins).
- **D-14:** Within type + nullability (structural already passed): **nullability is checked before
  type**, matching ENGINE-03 being checked ahead of ENGINE-04 in the requirements' own ordering. If
  no nullability violation exists anywhere in the row, the first type violation found wins.
- **D-15:** When multiple columns share the **same kind** of violation (e.g. two different required
  columns are both empty), the reported column is the **first one in `config.json`'s declared
  `columns` order** — deterministic, matches the config's own authored order.
- **D-16:** A detect-vs-config mismatch (dialect/encoding/header disagreement — see D-25) uses the
  **same `StructuralValidationError` exception type** as header-level structural failures (same
  `INVALID_FILE` consequence downstream via Phase 4's `process()`), but gets its **own distinct
  `error_code` family/prefix** (e.g. `DETECT_*` vs `STRUCT_*`) so the two failure sources stay
  distinguishable in logs even though the Python control flow and Airflow-facing status match.

### Structural Failure Scope

- **D-17:** **Header-level** mismatches (missing/extra/duplicate declared column, no header row at
  all) reject the **whole file** — zero rows processed, maps to Phase 4's future `INVALID_FILE`
  status. **Row-level** ragged rows (wrong field count on one specific data row, header otherwise
  fine) become **per-row invalid entries**; the rest of the file keeps processing. Matches
  ENGINE-05's "one bad row never halts the rest" plus the distinct `INVALID_FILE` vs.
  `SUCCESS_WITH_INVALID_ROWS` status codes already in REQUIREMENTS.md.
- **D-18:** An extra/unrecognized column in the header (not declared in `config.json`'s schema) is
  a **hard whole-file reject** — consistent with CONFIG-02's fail-fast philosophy already applied
  to `config.json` itself, and catches a genuinely wrong file dropped into the wrong dataset's
  directory.
- **D-19:** A duplicate column name in the header is the **same hard whole-file reject** — the
  ambiguity of which occurrence owns the data makes any other treatment a guess.
- **D-20:** A completely **empty file** (zero bytes, no header at all) is a whole-file reject, same
  as "no header row." A file with a **valid header but zero data rows** is **not an error** —
  processes successfully with `total=0/valid=0/invalid=0` (matches the corpus's own `expect:`
  reason for its header-only fixture).
- **D-21:** Header column **order is independent** of `config.json`'s declared `columns` order —
  columns are matched by **name**, not position. More resilient to upstream reordering; only the
  set of names matters structurally.
- **D-22:** Header column-name matching is **case-sensitive, exact match required** against
  `config.json`'s declared names — simple, predictable, and `config.json` is the one place column
  names carry semantic meaning (Oracle's own identifiers are uppercase-unquoted per Phase 1 D-02,
  case-agnostic at the DB layer).
- **D-23:** A whole-file structural reject is signaled at the Python level as a **plain exception**
  (e.g. `StructuralValidationError`) — `csv_processor` stays Airflow-agnostic per ENGINE-09. Phase
  4's `process()` catches it and translates it into `ProcessingResult(status=INVALID_FILE)`, per
  ARCHITECTURE.md's own already-settled design ("`process()` owns all status/exception
  translation"). Confirmed via Context7 against Airflow's own exception model
  (`AirflowFailException` — fails a task while skipping remaining retries, vs. a plain exception
  which triggers normal retry policy): whether Phase 5's DAG layer later re-raises
  `AirflowFailException` for a permanent failure like `INVALID_FILE` is Phase 5's own decision, not
  locked here — just noting the natural fit.
- **D-24:** A blank line interspersed between data rows is **flagged as its own invalid row** (0
  fields where N were expected), not silently skipped — and (per D-07) it consumes a `row_number`
  in the same 1-indexed post-header counting scheme as every other row.

### Detection Cross-Check Strictness & Compressed Input

- **D-25:** The engine **auto-detects** dialect/encoding/header via the vendored Tier-A modules and
  **cross-checks** the result against `config.json`'s declared values, rather than trusting config
  blindly — directly addresses PITFALLS.md's Pitfall 5 (a wrong encoding guess can silently produce
  mojibake that slips into `_VALID` looking superficially plausible).
- **D-26:** A UTF-8 **BOM is stripped** (`utf-8-sig` decode) **before any parsing**, always — before
  dialect/header detection even runs. Matches PITFALLS.md's own explicit recommendation; prevents a
  BOM-prefixed header (`'\ufeffcustomer_id'`) from falsely triggering `MISSING_REQUIRED_COLUMN`.
- **D-27:** All **5** of the reference repo's Tier-A `detect/*` modules are vendored (`dialect.py`,
  `encoding.py`, `header.py`, `filename.py`, `schema.py`) for completeness/parity with PROJECT.md's
  original Tier-A list, even though `filename.py`/`schema.py` have no current caller in this
  config-driven design (file pattern and schema are both already fully declared in `config.json`).
- **D-28:** A **low-confidence** detection disagreement with config **defers to config** as
  authoritative (no error raised) — only a **high-confidence** disagreement raises a
  detect-vs-config mismatch error. Avoids false-positive whole-file rejects from detector noise on
  short/ambiguous files (PITFALLS.md: detectors are "least reliable on short files").
- **D-29 (major scope addition — user-initiated):** The engine must read **compressed CSV input**
  (`.csv.gz`, `.zip`) transparently, via **streaming decompression** — never extract-to-a-temp-file
  first — using the already-vendored `compression.py`. User's explicit rationale: "modern processing
  solutions are not extracting contents of compressed" files.
- **D-30:** Compressed-file detection is by **magic-byte sniffing** (gzip `0x1f 0x8b`, zip
  `PK\x03\x04`), **pattern-agnostic** — not by filename extension. Matches `compression.py`'s own
  actual detection approach (verified: reads magic bytes, not extension).
- **D-31:** `config.json`'s `file_pattern` (Phase 2 D-07) is **widened** to match both plain and
  compressed variants, e.g. `"customers_*.csv*"` — discovered as a follow-up: the original
  `"customers_*.csv"` glob does **not** match `"customers_*.csv.gz"`, so Phase 5's future
  file-arrival detection would never find a compressed file at all, regardless of magic-byte
  sniffing happening once a candidate file is opened. — **Reversibility:** costly —
  **rationale:** touches both this phase's config convention and Phase 5's future file-sensor
  pattern matching once built.
- **D-32:** Phase 2's business-row generator (`generate_csv.py`) gets a **`--compress`** flag —
  gzips the output after writing, giving Phase 3 (and Phase 6's benchmark) a realistic-volume
  compressed fixture to test/benchmark against, beyond the corpus's small synthetic
  `large_compressed` fixtures.
- **D-33:** A **multi-entry zip** archive requires **exactly one** member; zero or more than one
  entry raises a structural error (which member is the actual CSV data is ambiguous) — fail loudly
  rather than guessing.

### Async / Optimization

- **D-34:** **No** async/concurrent chunk reading in Phase 3 — the engine stays strictly
  **sequential**: open once, detect once, read/validate/split chunk-by-chunk in file order. Explored
  and rejected after discussion: CSV row boundaries aren't safely discoverable in parallel (an
  embedded newline inside a quoted field means you can't seek to arbitrary chunk offsets —
  ARCHITECTURE.md's own Anti-Pattern 3 warns against exactly this), `asyncio` doesn't help a local
  mounted-volume read the way it helps network I/O (no real I/O-wait to overlap; Python has no true
  async local-disk I/O on this stack), the actual bottleneck is Oracle round-trip count (already
  addressed by chunked bulk `executemany()`), and concurrent/out-of-order chunk reads would conflict
  with the sequential `row_number`/tie-breaking guarantees already locked in above (D-07, D-15).
  See Deferred Ideas below — captured for later, not dropped.

### Claude's Discretion

- Exact module/file layout within `packages/csv-processor/src/csv_processor/` (`detect/`,
  `source.py`, `normalize.py`/`validate.py`, `engine.py`) — ARCHITECTURE.md's directory sketch is a
  reasonable starting point, matches Phase 1's D-16-locked package path.
- Detection's "bounded sample" size for dialect/encoding sniffing — match the reference repo's own
  precedent (a fixed 64 KiB sample, per ARCHITECTURE.md's Anti-Pattern 2 discussion) unless a
  concrete reason to differ surfaces during implementation.
- Exact exception class hierarchy (`StructuralValidationError`, a `DetectionMismatchError` sibling
  or subclass per D-16) and the full `error_code` enum member list — implementation detail once the
  naming pattern (one code per failure type, `DETECT_*` vs `STRUCT_*` families) is established.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project-level requirements & decisions
- `.planning/PROJECT.md` — two-tier reuse decision, dependency-isolation constraint (never import
  `dataplat`), pinned tech decisions
- `.planning/REQUIREMENTS.md` — ENGINE-01..07/09, TEST-01 full text; ENGINE-08/LOAD-01..04 are
  explicitly Phase 4, not this phase — do not build the `ProcessingResult`/Oracle-loading wrapper
  here
- `.planning/ROADMAP.md` §Phase 3 — goal and success criteria for this phase
- `.planning/phases/02-config-contract-csv-generator/02-CONTEXT.md` — D-01/D-02/D-03 (CSV dialect
  fields), D-08/D-09/D-10/D-11 (schema notation: strptime formats, nullable/required split,
  decimal precision/scale, boolean type), D-13 (chunk_size is per-dataset config), D-16 family (the
  fixture corpus this phase's detection/validation logic must pass against), D-16d (explicitly
  deferred the `error_code` vocabulary to this phase — see D-12..D-16 above)
- `.planning/phases/01-environment-oracle-foundation/01-CONTEXT.md` — D-01 (Oracle schema shape),
  D-02 (uppercase unquoted identifiers), D-16 (locked repo layout)

### Actual schema/config this phase validates against (more authoritative than research sketches)
- `docker/oracle/init/02_customers.sql`, `docker/oracle/init/03_orders.sql` — the ACTUAL Oracle DDL
  this phase's D-01..D-08 migrate (widen `_INVALID` columns, add `raw_line`)
- `packages/csv-processor/src/csv_processor/config/models.py` — the ACTUAL Pydantic config contract
  (`ColumnSpec`, `CsvDialectConfig`, `OracleTargetSpec`, `ProcessingConfig`, `DatasetConfig`) Phase 3
  reads and validates CSV rows against
- `configs/datasets/customers.json`, `configs/datasets/orders.json` — real per-dataset config
  instances
- `tests/fixtures/corpus.yaml`, `tests/fixtures/CORPUS.sha256` — the 30-fixture, 5-category
  (`dialect_encoding`, `structural`, `type_nullability`, `byte_level_hard`, `large_compressed`)
  byte-exact corpus this phase's detection/parsing/validation must pass against; every fixture's
  `expect:` prose is the informal spec for a real `error_code` (or a "not an error" case)

### Research (produced before this discussion)
- `.planning/research/ARCHITECTURE.md` — module boundaries (`detect/`, `source.py`,
  `normalize.py`/`validate.py`, `engine.py`), the detect-once-per-file sequence, Anti-Pattern 2
  (detecting per chunk) and Anti-Pattern 3 (byte/line-offset chunking) — both directly inform D-25
  and D-34
- `.planning/research/FEATURES.md` — structural → type → nullability check-order rationale (behind
  D-13/D-14)
- `.planning/research/PITFALLS.md` §Pitfall 5 "BOM and locale-driven encoding surprises" — behind
  D-25/D-26

### Reference repo (read-only — never imported, never a dependency)
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/detect/{dialect,encoding,header,filename,schema}.py`
  — Tier-A vendoring source (D-27); each imports `from dataplat.errors import <SomeError>` — swap
  for a local exception class of the same name, detection logic itself is pure
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/compression.py` —
  Tier-A vendoring source (D-29/D-30); swap its S3-backed `open_text_stream` call for plain
  `open()`/`pathlib.Path.open()`
- `/home/user/projects/airflow-platform/packages/csv-processor/src/csv_processor/source.py` — Tier-B
  read-the-algorithm source for the detect → open → chunk sequence; not portable as a file (wired
  into `dataplat`'s `Source`/`SchemaRepository`/`RecordChunk`)
- `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/normalize/{dates,numeric,unicode,boolean_null}.py`,
  `/home/user/projects/airflow-platform/packages/dataplat/src/dataplat/validate/*` — Tier-B
  algorithm source for type-conversion/nullability logic (e.g. strict-`strptime` date rejection);
  reimplement the algorithm as plain functions, not the `StreamingStage`/`BarrierStage` scaffolding

### External docs (fetched live during this discussion)
- Apache Airflow docs via Context7 (`/apache/airflow/3.1.6`, `airflow-core/docs/core-concepts/tasks.rst`)
  — `AirflowFailException` vs. plain-exception retry semantics, behind D-23

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `packages/csv-processor/src/csv_processor/config/` — complete, tested Pydantic config contract
  (Phase 2) that this phase reads directly; `models.py`'s `ColumnSpec`/`CsvDialectConfig`/
  `OracleTargetSpec`/`ProcessingConfig` are the actual schema shape rows are validated against.
- `tools/corpus/` + `tests/fixtures/corpus.yaml` + `CORPUS.sha256` — the 30-fixture byte-exact
  corpus (Phase 2) this phase's detection/parsing must pass against.
- `generator/generate_csv.py` — Phase 2's business-row CSV generator; gets a `--compress` flag
  added per D-32.

### Established Patterns
- Phase 2's config validation style (Pydantic v2, `extra="forbid"`, `frozen=True`,
  `model_validator(mode="after")` cross-field checks, fail-loud with no silent defaults) — this
  phase's own exception classes should follow the same fail-loud philosophy.
- Phase 1's D-16-locked repo layout (`packages/csv-processor/src/csv_processor/`) — this phase adds
  `detect/`, `source.py`, `normalize.py`/`validate.py` (and possibly `engine.py`, though the full
  `process()` entrypoint is Phase 4's per REQUIREMENTS.md's ENGINE-08 mapping) inside it.
- ARCHITECTURE.md's already-settled module boundaries and detect-once-per-file sequence — this
  phase builds within that sketch, not around it.

### Integration Points
- Phase 4 (Oracle Bulk Load) consumes this phase's dict-per-row chunk generator (D-11) directly for
  `executemany()` binding, and reads the widened `_INVALID` DDL (D-01) this phase migrates.
- Phase 4's `process()` entrypoint catches this phase's `StructuralValidationError`/detect-mismatch
  exceptions (D-16, D-23) and translates them into `ProcessingResult` status codes.
- Phase 5 (DAG wiring) will need the widened `file_pattern` (D-31) once its file-wait sensor is
  built; may use `AirflowFailException` for permanent failures like `INVALID_FILE` (noted, not
  locked here).
- Phase 6's benchmark (TEST-04) is the trigger condition for revisiting the deferred async/
  concurrent-chunk idea (see Deferred Ideas).

</code_context>

<specifics>
## Specific Ideas

- User explicitly wants the engine to handle compressed CSV input transparently — streaming
  decompression, never extract-then-read — citing "modern processing solutions are not extracting
  contents of compressed" files. This drove D-29 through D-33 and was NOT anticipated by the
  original research (Phase 2's D-16c explicitly said compressed fixtures existed only to exercise
  `compression.py`, "even though no requirement in this project calls for compressed CSVs in its
  actual pipeline" — that framing is now superseded by this discussion).
- User raised, then dropped after technical discussion, concurrent/async reading of the file itself
  ("reading in many bulks instead of a sequential read bulk by bulk") — walked through why CSV row
  boundaries make this unsafe and why the real bottleneck is elsewhere (Oracle round-trips, not
  local-disk I/O wait); captured as a deferred idea (D-34) rather than built now.
- Discovered during codebase scouting, not user-initiated: `customers_invalid`/`orders_invalid`'s
  native typed/NOT-NULL columns structurally conflict with ENGINE-06's "original field values"
  requirement. This became the single largest decision thread (D-01 through D-08) — a real gap
  between Phase 1's already-provisioned DDL and Phase 3's actual requirement, not a hypothetical.

</specifics>

<deferred>
## Deferred Ideas

- **Concurrent/async chunk reading for file I/O** — revisit only if Phase 6's benchmark (TEST-04,
  row-by-row vs. chunked/bulk at ~100K rows) shows CPU-bound row parsing/validation — not Oracle
  round-trips — is the actual limiter. Not built in this phase (D-34).
- **Writing compressed CSV output** — no current requirement or DAG step (`load_config` →
  `wait_for_file` → `process_csv` → `load_results` → `report_result`) produces a CSV file as output
  at all; the pipeline's only output is Oracle rows via `executemany()`. Note for a future
  phase/milestone if a file-based export/archival step is ever added.

### Reviewed Todos (not folded)
None — `todo.match-phase` returned zero matches for Phase 3.

</deferred>

---

*Phase: 3-CSV Processing Engine*
*Context gathered: 2026-08-29*
