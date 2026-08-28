# Pitfalls Research

**Domain:** Lightweight local Airflow CSV→Oracle ETL platform (deferrable file-wait triggers,
`python-oracledb` bulk loading, CSV dialect/encoding detection, chunked processing, Docker
Desktop + WSL2 + Oracle Database Free)
**Researched:** 2026-08-28
**Confidence:** MEDIUM (official docs for Airflow trigger semantics and `python-oracledb` API
behavior; MEDIUM-verified for WSL2/Docker/CSV-sniffing community findings cross-checked across
multiple independent sources — see per-pitfall notes and Sources)

## Critical Pitfalls

### Pitfall 1: Blocking calls inside a Trigger's `run()` stall the entire triggerer, not just one task

**What goes wrong:**
A custom `BaseTrigger.run()` (e.g. the file-availability wait this project needs) is written with
a synchronous blocking call inside it — a sync DB query, `requests.get()`, `time.sleep()`, a plain
`os.path.exists()` loop without proper async wrapping, or any CPU-bound work. Airflow's triggerer
runs *all* deferred triggers as coroutines multiplexed on a **single shared asyncio event loop**
in one process. One blocking call anywhere doesn't just delay that one trigger — it freezes the
entire triggerer, so every other deferred task across every DAG stalls until the blocking call
returns. Airflow only logs a warning *after* the offending trigger finishes; there's no proactive
alarm while it's happening.

**Why it happens:**
Deferrable operators look like "just write an `async def run()`" from the docs, but it's easy to
call a synchronous library (e.g. `pathlib.Path.exists()` on a network/mounted filesystem, or any
non-async Oracle/HTTP client) inside that coroutine without realizing every `await` boundary must
actually yield control. Airflow's `time.sleep`-shaped file-polling intuition (carried over from
writing normal Sensors) transfers incorrectly to Triggers.

**How to avoid:**
Use `asyncio.sleep()` for polling delays, never `time.sleep()`. For filesystem checks, either use
`aiofiles`/`asyncio.to_thread(os.path.exists, path)` to offload the blocking syscall to a worker
thread, or accept that fast local-filesystem stats are cheap enough to not need it — but never put
network calls, a sync `python-oracledb` connection, or `requests` inside `run()`. Keep the trigger
minimal: check-and-yield-or-sleep, nothing else. Test the trigger by running two of them
concurrently locally and confirming both progress independently (no serialization between them).

**Warning signs:**
Triggerer logs show a task taking far longer than its expected poke interval to resume; multiple
unrelated deferred tasks all resume at nearly the same wall-clock time (a sign one blocked the
loop); triggerer CPU pinned at 100% on a single core while idle DAGs wait.

**Phase to address:**
The phase implementing the file-availability Deferrable Trigger — write it async-only from the
start, review the diff specifically for any non-`await`ed I/O before merging.

---

### Pitfall 2: `executemany()` batches with inconsistent per-column types raise cryptic DPY errors deep in a run

**What goes wrong:**
`cursor.executemany()` infers each bind column's Oracle type from the *first* row's Python type,
or from mixed list-of-lists rows. If a later chunk has a different type in the same column
position (commonly: an all-`None` first batch for a nullable column, followed by a batch with
actual integers/strings in that position — very likely here, since the CSV-generator produces
nullable fields), it raises `DPY-3013 "unsupported Python type ... for database type ..."`. Using
lists instead of tuples for row data can also raise `DPY-2010 "element is not the same data type
as previous elements"`. Both surface mid-run, potentially after several chunks have already
committed, leaving a partially-loaded file.

**Why it happens:**
Oracle bind-variable typing has no equivalent of dynamic per-value coercion the way SQLite or a
naive ORM might; `executemany()` needs consistent, explicit types per bind position across the
whole call. The bug is invisible in early testing because early test batches happen to have
homogeneous types, and only shows up on the CSV rows that actually exercise nulls or mixed values.

**How to avoid:**
Always call `cursor.setinputsizes(...)` before `executemany()`, explicitly declaring the Oracle
type for every bind position from the target table schema (driven by `config.json`'s type
definitions) rather than letting the driver infer from data. Normalize every row to a fixed tuple
shape (not list) with consistent Python types per column (e.g. always `datetime.date` or `None`
for date columns, never a string sometimes and a date object other times) *before* calling
`executemany()`. Chunk boundaries should never split so that dtype-inference could see an all-None
column in one chunk — with `setinputsizes()` this is moot, so prefer that fix over "arrange lucky
chunk boundaries."

**Warning signs:**
`DPY-3013`/`DPY-2010` appearing only on larger benchmark runs (~100K rows) or specifically on the
`orders` dataset (which has more nullable/decimal columns) but not on small dev fixtures — a strong
signal that type inference is silently relying on sample data rather than schema.

**Phase to address:**
The Oracle bulk-loading phase — bake `setinputsizes()` derivation from `config.json` schema into
the loader from the first implementation, not as a later fix.

---

### Pitfall 3: `batcherrors=True` does not auto-commit successful rows — the transaction is left open

**What goes wrong:**
To get the "collect-and-continue invalid rows" behavior this project wants at the DB layer (as a
backstop, even though invalid rows are mainly filtered before loading), a developer sets
`batcherrors=True` on `executemany()`. If any row in the batch errors, Oracle does **not**
auto-commit the successful rows in that batch — even with `connection.autocommit=True`. The
application must explicitly call `cursor.getbatcherrors()`, decide what to do, and then explicitly
`commit()` or `rollback()`. Skipping this leaves an open transaction holding locks, and a later
unrelated `commit()`/`rollback()` elsewhere in the DAG can silently discard or persist rows the
developer didn't intend.

**Why it happens:**
`batcherrors=True` reads like "errors won't stop the batch," which is true, but the commit-skip
side effect is a specific, easy-to-miss Oracle behavior that only shows up when a real error
occurs in a batch — which may not happen until real messy data arrives, well after development.

**How to avoid:**
Given the spec explicitly filters invalid rows *before* database loading (VALID/INVALID split
happens in the `csv_processor`, not at the DB layer), avoid relying on `batcherrors=True` as the
primary error-handling path at all — only valid, pre-validated rows should reach the `_VALID`
table's `executemany()` call, and it should raise on any DB error (schema mismatch, constraint
violation) since that indicates a bug in validation, not expected bad data. If `batcherrors=True`
is used anywhere (e.g. defensively), always pair it with an explicit `getbatcherrors()` check and
explicit `commit()`/`rollback()` immediately after every `executemany()` call — never assume
autocommit covers it.

**Warning signs:**
Row counts in the metadata/ingestion tracking table don't match what's actually visible in
`<DATASET>_VALID` after a run with any DB-level error; locks/blocking observed on the Oracle
container after a task that should have completed.

**Phase to address:**
The Oracle bulk-loading phase — decide explicitly whether `batcherrors` is used at all (recommend:
no, rely on pre-validation) and document the commit boundary per chunk.

---

### Pitfall 4: Full-file CSV load balloons memory 3-4x file size, defeating "chunked processing" requirement

**What goes wrong:**
Loading an entire CSV into memory at once (`pd.read_csv(path)` with no `chunksize`, or
`list(csv.reader(f))`) uses roughly 3-4x the on-disk file size in RAM once per-column dtype
inference and Python object materialization happen — a CSV that looks modest on disk can use far
more RAM than expected, and at the ~100K-row benchmark scale (or larger real files) this can push
a local Docker Desktop/WSL2 VM into memory pressure or OOM, especially since Oracle Database Free
and Airflow are already running as neighboring containers competing for the same VM memory budget.

**Why it happens:**
It's the path of least resistance during early development ("just read the whole file, it's a
small CSV") and works fine on tiny fixtures, so the memory cost is invisible until the benchmark
phase or a larger real file is tested — exactly the failure mode the project's own benchmark
requirement (row-by-row vs chunked/bulk) is designed to catch, but only if chunking is actually
implemented as a generator/iterator, not simulated by chunking an already-fully-loaded structure.

**How to avoid:**
Process the CSV as a stream from the start: iterate rows (or fixed-size row batches) directly off
the open file handle / `csv.reader`, never materialize the whole file into a list or DataFrame
first. If pandas is used anywhere, use `chunksize=` on `read_csv` and process/discard each chunk
before reading the next; specify `dtype=`/`usecols=` explicitly to cut inference cost. Keep the
"accumulate rows for one bulk-insert chunk" buffer bounded to the configured chunk size and flush
(insert + clear) at each boundary — never append processed rows into a single growing list across
the whole file before writing anywhere.

**Warning signs:**
Peak RSS memory during the benchmark test scales linearly (or worse) with file size instead of
staying flat; the container's memory usage graph in `docker stats` shows a single large step-up
that correlates with file read rather than a sawtooth pattern across chunk boundaries.

**Phase to address:**
The `csv_processor` core-engine phase (reading/parsing) and the benchmark phase — the generator/
chunked design must be the *only* code path (not an initial naive version "optimized later"),
since the benchmark's entire point is proving this design decision with real numbers.

---

### Pitfall 5: BOM and locale-driven encoding surprises break dialect/encoding detection on "clean-looking" files

**What goes wrong:**
CSV files exported from Excel on Windows default to the system locale encoding (commonly
Windows-1252 / cp1252) *unless* a UTF-8 BOM is present, in which case tools must strip and honor
it rather than treating the BOM bytes as file content. Heuristic encoding detectors
(chardet/charset-normalizer) are confidence-scored guesses, not guarantees — they are least
reliable on short files, files with few non-ASCII bytes, or mixed-language content, all of which
are realistic for a synthetic CSV generator that mostly emits ASCII with occasional edge-case rows
(e.g. names with accents to test the invalid-row path). A wrong encoding guess doesn't always
error — it can silently produce mojibake that looks superficially plausible and slips into the
"valid" table with corrupted string data.

**Why it happens:**
BOM-handling and encoding detection are easy to skip during development because ASCII-only test
fixtures never exercise the edge case; the failure only appears once real (or deliberately
generated) files include one byte outside the ASCII range or an actual BOM.

**How to avoid:**
Always check for a BOM explicitly first (`utf-8-sig` handling) before falling back to statistical
detection; don't rely on chardet/charset-normalizer as the sole signal — cross-check against a
UTF-8-strict decode attempt first, since UTF-8 decode failures are a clean, deterministic signal
that something else is going on, whereas a "confident" wrong guess is not. Since the CSV files
here are *generated by this project's own deterministic generator* (not sourced from arbitrary
external systems), it's reasonable to pin the generator's output encoding (e.g. UTF-8, optionally
with a BOM variant fixture) and treat encoding detection primarily as defensive code exercised
by dedicated edge-case fixtures, not as a load-bearing guess against unknown real-world files.

**Warning signs:**
Rows land in `_VALID` with garbled characters instead of being rejected; unit tests pass on ASCII
fixtures but a manually-crafted UTF-8-BOM or Windows-1252 fixture fails silently rather than
raising a clear detection/parse error.

**Phase to address:**
The `csv_processor` detection phase (vendoring `detect/encoding.py`/`detect/dialect.py` per the
two-tier reuse plan) — include BOM and non-ASCII fixtures in the phase's own test suite, don't
defer them to a later "edge case" pass.

---

### Pitfall 6: Docker Desktop + WSL2 lets the VM's memory grow unbounded, starving Oracle/Airflow containers or the host

**What goes wrong:**
WSL2 runs as a lightweight VM (`vmmem`) that dynamically grows its memory allocation and does
**not** release it back to Windows until an explicit `wsl --shutdown` — Oracle Database Free's own
memory footprint compounds this, and unmanaged growth has been observed consuming most/all host
RAM and spilling into the Windows pagefile, degrading the entire host (not just the containers).
Separately, Windows 11 24H2's WSL "mirrored" networking mode (on by default on updated hosts)
enables IPv6 preference inside WSL/containers, which has caused intermittent connection timeouts
to Oracle's listener when the client stack still assumes IPv4 — a networking gotcha that looks
like "Oracle is randomly unreachable" rather than a config issue.

**Why it happens:**
Neither of these is visible on first setup — they surface after hours/days of the WSL VM staying
up across a working session (memory) or after a Windows update silently flips the networking mode
default (connectivity). Both are host/platform-level, not something this project's code controls
directly, but they directly affect whether local development is usable.

**How to avoid:**
Pin explicit `memory=` and `processors=` limits in `%UserProfile%\.wslconfig` sized to the actual
docker-compose stack's documented CPU/RAM allocation (a requirement this project already commits
to), and periodically `wsl --shutdown` during long dev sessions if memory creeps. Document the
`.wslconfig` recommendation in the project's setup docs rather than assuming defaults are fine.
For networking, verify the DB connection uses/accepts IPv4 explicitly (or pin `networkingMode` in
`.wslconfig` to `NAT` if mirrored mode causes issues) and document this as a first troubleshooting
step if "Oracle unreachable" issues appear intermittently rather than consistently.

**Warning signs:**
Host machine (Windows) becomes sluggish system-wide during/after long docker-compose sessions;
`docker stats`/Task Manager shows `Vmmem` process memory far exceeding the docker-compose stack's
documented budget; Oracle connection errors that are intermittent (works, then times out, then
works again) rather than consistent (which would point to a real config/credentials issue).

**Phase to address:**
The docker-compose provisioning phase — document `.wslconfig` sizing and the IPv4/mirrored-mode
caveat directly in the project's environment setup docs, since this is exactly the kind of gotcha
that costs hours if undocumented and encountered cold.

---

## Technical Debt Patterns

Shortcuts that seem reasonable but create long-term problems.

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Skip `setinputsizes()`, let `executemany()` infer bind types from data | Less boilerplate per loader call | Cryptic `DPY-3013`/`DPY-2010` errors that only appear on real/larger data with nulls or mixed types; cursor-cache churn | Never — always derive from `config.json` schema |
| Use pandas `read_csv()` without `chunksize` "for now, chunk later" | Simpler first pass, fewer moving parts | The chunked-design requirement and benchmark comparison become retrofits instead of the actual architecture being measured | Never — this project's benchmark exists specifically to prove the chunked design; build it chunked from day one |
| Rely on `FileSensor` poke/reschedule mode instead of a real Deferrable Trigger, planning to "swap it for a Trigger later" | Faster to get a working DAG initially | Worker-slot exhaustion pattern never gets exercised/tested; the requirement explicitly calls for a Deferrable Trigger, so this is redoing a phase, not simplifying it | Never — build the Trigger directly, it is a named requirement |
| Hardcode Oracle Database Free's `latest` tag during early dev, pin "later" | Slightly less setup friction initially | Non-reproducible builds; CLAUDE.md and PROJECT.md already flag this as a resolved decision, so drifting from it re-opens a closed question | Never — pin from the first docker-compose commit |

## Integration Gotchas

Common mistakes when connecting to external services.

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|-------------------|
| Airflow REST API trigger (`POST /dags/{dag_id}/dagRuns`) | Assuming the `conf` payload passed at trigger time is validated before the DAG run starts | Validate `conf` (dataset + config path) inside the DAG's own first task via Pydantic v2, and fail fast with a clear `CONFIGURATION_ERROR` status rather than letting a bad path surface deep in processing |
| `python-oracledb` thin mode | Assuming thin mode supports every feature thick/`cx_Oracle` mode does (e.g. some advanced features require thick mode + Oracle Client libs) | Confirm thin mode covers `executemany()`, `setinputsizes()`, and the specific type bindings this project needs (it does for the documented core bulk-insert path) before committing to zero-Oracle-Client-install as a hard constraint |
| Oracle Database Free container readiness | Starting the Airflow DAG's first Oracle-dependent task immediately after `docker-compose up`, before Oracle has finished its (slow, multi-minute) first-boot database creation | Use `docker-compose` healthchecks against Oracle's listener/readiness, and don't assume "container running" means "database accepting connections" |
| Deferrable Trigger + Oracle "file arrived" check | Putting a live Oracle connection or query inside the Trigger's `run()` for readiness checks | Keep Triggers filesystem-only for the file-wait requirement; do Oracle-related readiness checks in a separate, ordinary (non-deferred) task, since Triggers must be cheaply serializable and shouldn't hold DB connections |

## Performance Traps

Patterns that work at small scale but fail as usage grows.

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Row-by-row Oracle inserts (single-row `execute()` in a loop) instead of `executemany()` | Works fine on small fixtures; DAG task duration scales far worse than linearly with row count | Always use `executemany()` with a configured `batch_size`, never per-row `execute()` | Becomes clearly visible at the project's own ~100K-row benchmark; this is exactly the comparison the benchmark is designed to surface |
| Unbounded `executemany()` batch size on very large files | Works on the 100K benchmark; large internal buffer allocation and memory spikes on much larger files | Use the `batch_size` parameter (e.g. 10K-200K range depending on row width) to bound per-call buffer size regardless of total file size | Any file large enough that one `executemany()` call would need to buffer the entire dataset at once |
| Encoding/dialect detection re-run per chunk instead of once per file | Negligible on small files; wasted CPU and possible inconsistent detection results across chunks of the same file on large files | Detect encoding/dialect/header once at file-open time, reuse the result for every subsequent chunk | Any file large enough that per-chunk re-detection is measurably slow, or where chunk boundaries could split a multi-byte encoding sequence and confuse an isolated per-chunk detector |

## Security Mistakes

Domain-specific security issues beyond general web security.

| Mistake | Risk | Prevention |
|---------|------|------------|
| Building Oracle `INSERT` SQL via string formatting/f-strings with CSV-derived values instead of bind variables | SQL injection via crafted CSV cell content; also defeats Oracle's cursor caching/plan reuse | Always use positional bind variables (`:1, :2, ...`) with `executemany()`/`setinputsizes()`, never string-interpolate row data into SQL text |
| Logging full row content (including invalid rows) at INFO level in Airflow task logs | Sensitive-looking data (even synthetic) ends up in persisted task logs; noisy logs hide real errors | Log row *counts*, error codes/messages, and file/row identifiers — not full row payloads — matching the `ProcessingResult` status-code model already specified |
| Committing a real Oracle password into `docker-compose.yml` or `config.json` checked into git | Credential leak in version control history | Use environment variables / `.env` (gitignored) for Oracle credentials, even in this local-only project, since the repo will be cloned fresh per the "reproducibly, from a fresh `git clone`" core-value statement |

## UX Pitfalls

Common user experience/DX mistakes in this domain (operator/developer experience, since there is
no end-user UI here beyond Airflow's own UI and REST API).

| Pitfall | User Impact | Better Approach |
|---------|-------------|-------------------|
| Generic `PROCESSING_ERROR` status for every failure mode | Developer/operator triggering via REST API can't tell a bad config from a missing file from a DB outage without digging into task logs | Use the full documented status vocabulary (`FILE_NOT_FOUND`, `INVALID_FILE`, `CONFIGURATION_ERROR`, `DATABASE_ERROR`, `PROCESSING_ERROR`) consistently, set as early and specifically as possible in the DAG |
| Silent success when 100% of rows are invalid | A run "succeeds" with zero rows loaded and no obvious signal something is wrong with the source file/config | Distinguish `SUCCESS` from `SUCCESS_WITH_INVALID_ROWS` clearly (already in requirements) and make the DAG's final report surface the invalid-row count prominently, not just in nested task logs |
| README/docs describing setup that assumes prior Oracle/Airflow expertise | "Clone-to-first-ingest" core value breaks for a new contributor if `.wslconfig` sizing, Oracle image pinning, and the REST-API trigger payload shape aren't spelled out concretely | Include copy-pasteable exact commands (curl for the REST trigger, exact `.wslconfig` snippet, exact `docker-compose up` sequence) in the README, not prose descriptions |

## "Looks Done But Isn't" Checklist

Things that appear complete but are missing critical pieces.

- [ ] **Deferrable file-wait Trigger:** Often "done" as a `FileSensor` in reschedule mode instead
      of a real custom `BaseTrigger` — verify it's an actual async `Trigger`/`TriggerEvent`
      implementation, and that its `run()` contains zero blocking calls (grep for `time.sleep`,
      sync `requests`, sync DB calls).
- [ ] **Chunked processing:** Often "done" by wrapping a full in-memory load with an artificial
      chunk loop after the fact — verify peak memory is actually flat across file sizes (this is
      precisely what the benchmark test should catch; don't let the benchmark be added after the
      chunking design, add it in the same phase to prove the design as it's built).
- [ ] **`executemany()` bulk load:** Often "done" without `setinputsizes()` — verify bind types are
      explicitly declared from schema, not inferred, and that a mixed-null/typed-value test row
      exists in the test suite to catch `DPY-3013`/`DPY-2010` before real data does.
- [ ] **Idempotency (filename + checksum + dataset):** Often "done" only for the happy path (file
      seen once) — verify the specific case of an Airflow task *retry* after a partial Oracle load
      (some rows already committed) doesn't double-insert; this needs an explicit test, not just
      "checksum lookup exists."
- [ ] **Oracle Database Free image pin:** Often left on `latest` "temporarily" during early dev and
      forgotten — verify the exact tag is in `docker-compose.yml` before considering the
      provisioning phase complete.
- [ ] **CSV encoding/BOM handling:** Often "done" against ASCII-only fixtures only — verify a
      UTF-8-BOM fixture and a non-ASCII (e.g. accented character) fixture both exist in the test
      suite and are explicitly asserted, not just "detection code exists."

## Recovery Strategies

When pitfalls occur despite prevention, how to recover.

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|-----------------|
| Trigger blocks the triggerer process | LOW | Fix the offending blocking call (wrap in `asyncio.to_thread` or replace with an async equivalent); no data damage, just a stalled schedule — restart the triggerer if needed |
| `DPY-3013`/`DPY-2010` mid-benchmark or mid-run | MEDIUM | Roll back the open transaction (`connection.rollback()`), add `setinputsizes()` derived from schema, normalize row tuples before rebinding, re-run from the last successfully-committed chunk (idempotency/checksum tracking should make this safe) |
| Partially-committed batch after `batcherrors=True` misuse | MEDIUM | Inspect `cursor.getbatcherrors()` retroactively if the connection is still open; otherwise use the ingestion metadata table's row counts vs actual `_VALID` table counts to detect the discrepancy, then delete/reload the affected file using the checksum-based idempotency key |
| Encoding misdetection let mojibake into `_VALID` | MEDIUM | Use the file checksum + ingestion metadata to identify the affected file/run, delete the loaded rows for that file, fix detection logic, re-trigger the DAG for that file |
| WSL2 vmmem memory exhaustion mid-session | LOW | `wsl --shutdown` then restart Docker Desktop; add/adjust `.wslconfig` memory cap so it doesn't recur |
| Oracle unreachable due to mirrored-networking IPv6 preference | LOW | Switch `.wslconfig` `networkingMode` to `NAT`, or force IPv4 in the Oracle connection string/DSN, restart WSL |

## Pitfall-to-Phase Mapping

How roadmap phases should address these pitfalls.

| Pitfall | Prevention Phase | Verification |
|---------|-------------------|--------------|
| Blocking calls inside Trigger `run()` | Deferrable file-wait Trigger phase | Code review specifically for non-`await`ed I/O; run two file-wait tasks concurrently and confirm independent progress |
| `executemany()` type-inference errors (`DPY-3013`/`DPY-2010`) | Oracle bulk-loading phase | Unit/integration test with a batch containing nulls followed by typed values in the same column position |
| `batcherrors` transaction left open | Oracle bulk-loading phase | Integration test asserting explicit commit/rollback occurs after every `executemany()` call, with no `batcherrors` reliance for the primary valid-row path |
| Full-file load memory blowup | `csv_processor` core engine + benchmark phase | Benchmark test asserts peak memory stays roughly flat as synthetic file size increases (not just "chunked code exists") |
| BOM/encoding misdetection | `csv_processor` detection phase (vendoring `detect/encoding.py`) | Dedicated UTF-8-BOM and non-ASCII fixtures in the phase's own test suite |
| WSL2/Docker Desktop memory and networking gotchas | docker-compose provisioning phase | `.wslconfig` recommendation documented in setup docs; README includes IPv4/mirrored-mode troubleshooting note |
| Oracle image tag drift (`latest`) | docker-compose provisioning phase | `docker-compose.yml` reviewed for an explicit pinned tag before phase sign-off |
| `FileSensor` used instead of real Trigger | Deferrable file-wait Trigger phase | Code review confirms a custom `BaseTrigger` subclass exists, not `FileSensor` in any mode |

## Sources

- [Deferrable Operators & Triggers — Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html) — HIGH (official docs, via Context7)
- [airflow.triggers.base source](https://github.com/apache/airflow/blob/main/airflow-core/docs/authoring-and-scheduling/deferring.rst) — HIGH (official source, via Context7)
- [Triggerer intermittent failure / single-loop blocking discussion — apache/airflow](https://github.com/apache/airflow/discussions/28303) and [Troubleshooting triggerer — Google Cloud Composer docs](https://docs.cloud.google.com/composer/docs/composer-2/troubleshooting-triggerer) — MEDIUM (community + vendor docs, cross-checked)
- [python-oracledb Batch Statement and Bulk Loading — official docs](https://python-oracledb.readthedocs.io/en/latest/user_guide/batch_statement.html) — HIGH (official docs, via Context7)
- [python-oracledb Bind Variables guide](https://python-oracledb.readthedocs.io/en/latest/user_guide/bind.html) — HIGH (official docs, via Context7)
- [DPY-3013 issue — oracle/python-oracledb #291](https://github.com/oracle/python-oracledb/issues/291) and [executemany batcherrors discussion #418](https://github.com/oracle/python-oracledb/discussions/418) — MEDIUM (project's own issue tracker, cross-checked against official docs)
- [CleverCSV Dialect Detection docs](https://clevercsv.readthedocs.io/en/latest/_changelog.html) and [CleverCSV dialect-detection methodology](https://github.com/alan-turing-institute/CleverCSVDemo/blob/master/CSV_dialect_detection_with_CleverCSV.md) — MEDIUM (official project docs + academic-affiliated demo repo)
- Pandas chunked-reading memory analysis: [Reducing Pandas memory usage #3: Reading in chunks](https://pythonspeed.com/articles/chunking-pandas/) — MEDIUM (well-known independent technical source)
- [WSL2 vmmem memory issue with Oracle containers — microsoft/WSL #7923](https://github.com/microsoft/WSL/issues/7923) and [WSL mirrored networking Oracle connection timeout — microsoft/WSL #12419](https://github.com/microsoft/WSL/issues/12419) — MEDIUM (primary-source issue reports, cross-checked against related `docker/for-win` memory issues)
- [Airflow Sensors — official docs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html) — HIGH (official docs)

---
*Pitfalls research for: Lightweight local Airflow CSV→Oracle ETL platform*
*Researched: 2026-08-28*
