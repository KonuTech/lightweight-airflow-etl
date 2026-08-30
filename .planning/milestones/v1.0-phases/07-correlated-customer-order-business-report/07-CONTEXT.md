# Phase 7: Correlated Customer-Order Business Report - Context

**Gathered:** 2026-08-30
**Status:** Ready for planning

<domain>
## Phase Boundary

Make the customers⋈orders business report actually work: `orders.customer_id` must be drawn from
a real pool of `customers.customer_id` values generated for the same fixture run (not
independently random), so `scripts/verify_evidence.sql`'s JOIN returns real rows with correct
aggregates instead of "no rows selected" (its status in this project's history to date). This
phase has grown substantially beyond a minimal data-generation fix through discussion — it now
also covers: DB-level PK/index/trigger enforcement on the `_valid` tables, a staging+atomic-rename
file-drop mechanism, unit + live e2e regression coverage wired into required CI, a new
report-sensing Airflow DAG, and a benchmark re-verification. It does NOT include a broader
migration of business logic from Python to PL/SQL (explicitly deferred, see Deferred Ideas) or the
unrelated Airflow UI logs-not-showing bug (also deferred — separate issue).

</domain>

<decisions>
## Implementation Decisions

### ID Pool Composition

- **D-01:** `orders.customer_id` samples ONLY from customer rows that will land in
  `customers_valid` (not the full generated pool, which would include rows customers-ingestion
  rejects for unrelated reasons like a bad `birth_date`) — guarantees every non-corrupted order can
  join, maximizes real matches for the business report.
- **D-02:** Sampling is **with replacement** — one customer can have multiple orders (realistic
  "one customer, many orders" business pattern), which also makes the business report's
  `COUNT`/`SUM`/`AVG` aggregates meaningful (multiple orders per customer, not 1:1 rows).
- **D-03:** Sampling is **Zipf-weighted** (rank customers by pool order, weight ∝ 1/rank) — a few
  "power customers" get disproportionately more orders, most get few/none. User explicitly chose
  this over uniform-random sampling (the recommended, simpler default) — non-default choice, own
  words: "Weighted distribution."
- **D-04:** If the valid-customer pool is empty when generating orders (e.g. `--rows 0` or
  `--invalid-ratio 1.0` for customers), **raise an error immediately** rather than falling back to
  independent random generation — silently degrading back to uncorrelated IDs is exactly the
  failure mode this phase exists to fix.
- **D-05:** Given the same `--seed`, the full correlated dataset (which customer_ids orders pick,
  the Zipf weighting) must be **fully deterministic and byte-identical** across runs — matches this
  project's existing generator guarantee (`Faker.seed(seed)` + separate `random.Random(seed)`,
  already-tested byte-identical CLI reruns). The pool-sampling/Zipf RNG draws from the same seeded
  `random.Random(seed)` instance already used elsewhere in `generate_csv.py` — no new entropy
  source.

### ID Realism (customer_id / order_id format)

- **D-06:** `customer_id` moves from a random Faker word (e.g. "apple") to a **structured ID
  format**: `CUST-{seed_hash}-00001` — prefix + a component derived from `--seed` (for run-to-run
  uniqueness, see D-07) + zero-padded sequential number. User explicitly chose this over minimal-diff
  (keep word-style, just correlate it).
- **D-07:** The seed-derived component matters for correctness, not just cosmetics: Oracle tables
  accumulate across runs (no truncate/upsert; uniqueness was previously explicitly out of scope per
  PROJECT.md). If numbering reset to 1 every run, every run after the first would reintroduce
  duplicate `customer_id` values already in `customers_valid`, causing the JOIN to **fan out**
  (one order row joining multiple same-ID customer rows from different runs) and inflate the
  business report's aggregate counts. Deriving the unique component from `--seed` avoids this
  without a new entropy source (see D-05 — different seeds are already used for every live/CI run
  per Phase 6's D-12 "run-unique-seeded fresh fixture").
  — **Reversibility:** costly — changing the ID scheme after data exists means old and new rows use
  different `customer_id` shapes until the tables are reset (see D-19).
- **D-08:** `order_id` (not part of the correlation bug — no other dataset joins on it) is *also*
  given the same structured treatment for consistency: `ORD-{seed_hash}-00001`. User's own words:
  "cheap consistency win... avoids a half-fixed-looking business report."

### Regression Test Strategy

- **D-09:** The required regression test (success criterion 3) is **both** a fast unit test AND a
  live e2e test — not either/or. Unit test: asserts `orders.customer_id ⊆` the valid-customers pool,
  plus the Zipf-weighting property (a few IDs appear noticeably more often) and determinism (same
  seed → identical assignment across two runs) — matches this project's existing test discipline
  (e.g. `test_generate_rows_is_deterministic_for_same_seed`). Live e2e test: ingests both datasets
  for real and asserts the actual Oracle JOIN returns ≥1 row.
- **D-10:** The live e2e test lives in a **new, separate test file** (not appended to
  `tests/e2e/test_csv_ingest_e2e.py`, which stays focused on its existing single-dataset
  deferred-wake proof) — keeps two different proof concerns (file-sensor deferral vs. cross-dataset
  JOIN correctness) apart.
- **D-11:** The new e2e test is wired into the **required, blocking `oracle-e2e` CI check**
  (Phase 6 D-06/D-07's established pattern: e2e proofs are required on every PR, not local-only) —
  a future regression that silently breaks correlation again fails PRs, not just gets noticed later
  when the README's report goes empty again.
- **D-12:** The e2e test explicitly **backdates some inserted rows across multiple days** (bypassing
  `ingested_at`'s `SYSDATE` default just for this test) to prove the JOIN/report aggregates
  correctly across partition boundaries — a single test run only ever lands rows in today's
  partition otherwise (interval-partitioned tables, see D-17).

### Database Schema Changes

- **D-13:** Add a plain (non-unique) **index on `customer_id`** in both `customers_valid` and
  `orders_valid` — supports the now-real JOIN workload.
- **D-14 (non-default choice):** Also add an actual **PRIMARY KEY constraint** — `PRIMARY KEY
  (customer_id)` on `customers_valid`, `PRIMARY KEY (order_id)` on `orders_valid`. User's own
  reasoning, confirmed explicitly: this is a DB-DDL-layer decision, distinct from PROJECT.md's
  existing "no uniqueness validation" decision (which was about the CSV engine's own *application-level*
  validators, not Oracle constraints). Catches any future generator regression as a load failure
  instead of silent bad data. — **Reversibility:** one-way — undoing requires a DDL migration and,
  practically, a `make reset` (see D-19) since existing data may already violate the constraint.
- **D-15 (non-default choice):** Add an **INSERT trigger on `orders_valid`** that validates the
  inserted `customer_id` exists in `customers_valid`, as a DB-level safety net on top of the
  Python-side correlation (D-01–D-05) — not a replacement for it (Python still generates the
  correlated CSV data before Oracle ever sees it; a trigger can't retroactively correlate at
  insert time, it can only validate).
- **D-16:** On trigger violation, the **whole batch/chunk fails** (matches Oracle's default
  `executemany()` behavior, no `batcherrors`-style partial-success handling) — this path should be
  unreachable in practice since Python-side correlation already guarantees every order's
  `customer_id` is real before the CSV is written; a trigger firing here means something is deeply
  wrong upstream and should fail loudly, not partially succeed.
- **D-17:** Existing daily interval partitioning (`PARTITION BY RANGE (ingested_at) INTERVAL
  (NUMTODSINTERVAL(1, 'DAY'))`, already in `docker/oracle/init/02_customers.sql` /
  `03_orders.sql` since Phase 1) is **already sufficient** — no new partitioning DDL needed.
  "Partition by dataset" doesn't apply either since customers/orders are already separate tables.
- **D-18:** PK / index / trigger apply **only to `customers_valid`/`orders_valid`** — NOT to
  `customers_invalid`/`orders_invalid`. The invalid tables intentionally hold malformed/rejected
  rows (including blank or duplicate `customer_id` from the `missing_required` invalid-row
  category by design); a uniqueness/existence constraint there would reject the very rows those
  tables exist to capture.
- **D-19:** Because `docker/oracle/init/*.sql` only runs on first container boot, applying the new
  DDL (D-14/D-15) to an existing dev environment requires **`make reset` (already exists: `docker
  compose down -v`) + `make up`** — a full wipe so all init scripts (including the new PK/index/
  trigger DDL) run fresh. This is also how "start fresh so the report is clean" (no old
  uncorrelated-Faker-word `customer_id` rows lingering from before this fix) gets satisfied — user
  explicitly flagged this as required, not optional.
- **D-20:** Given the added trigger overhead, **re-run and update `docs/benchmark.md`**'s existing
  182.85× chunked/bulk speedup figure (Phase 6) rather than leaving it stale — keeps documented
  performance numbers honest about the real, current schema.

### Generation Call-Site Consistency

- **D-21:** Correlation logic (generate customers → extract valid-ID pool → Zipf-weighted-sample
  for orders) is **centralized in one shared function** (e.g.
  `generate_correlated_datasets(customers_config, orders_config, ...)` in
  `generator/generate_csv.py`) that every call site uses — `make generate`, `scripts/
  regenerate_readme_summary.py`'s per-dataset loop, and the new e2e test's fixture setup — rather
  than each site re-deriving the pooling/weighting/seed-derivation algorithm independently and
  risking drift (echoes this project's own established discipline, e.g. D-04 in Phase 6's CONTEXT
  for `scripts/verify_evidence.sql`'s SQL never being re-authored twice).
- **D-22:** The Makefile's `make generate` target (currently two independent `generate_csv.py`
  subprocess invocations — one per dataset, per `Makefile` line 24) changes to a **new combined CLI
  mode** (e.g. `generate_csv.py --correlated`, or a small new orchestrator script) that generates
  both datasets together in one process via the shared function — replaces the two independent
  invocations, since orders can no longer be generated by a fully independent CLI call.
  — **Reversibility:** costly — changes the CLI's public interface/usage pattern that `make
  generate`, docs, and scripts depend on.

### File Sequencing vs. FileSensor (including CI)

- **D-23:** Generate both correlated CSVs first (via D-21's shared function), THEN trigger/drop
  each into its own dataset's watched directory — keeps the two DAG runs independent and
  parallel-capable (only the CSV *generation* step needs the customers-before-orders order, not the
  DAG triggering itself).
- **D-24:** Concrete file-drop mechanism: write the generated CSV to a **staging path** (e.g.
  `data/<dataset>/.staging/<filename>`, same bind-mounted volume as the watched dir per
  `docker-compose.yml`'s `./data:/opt/airflow/data` mount and `airflow/dags/csv_ingest.py`'s
  `/opt/airflow/data/{dataset}/`), then **atomically `Path.rename()`** it into the actual watched
  path. Same filesystem → atomic, race-free move; genuinely two different paths as requested.
  Repeatable/testable multiple times since staging files can be regenerated freely without
  touching the watched dir until the move.
- **D-25:** This mechanism is proven against the **real, live Airflow stack** — not mocked. The new
  e2e test (D-09–D-11) triggers the real DAG, waits for `wait_for_file` to reach `deferred`, does
  the staging-write + rename, and asserts the live sensor picks it up — exactly Pitfall 4's
  established ordering (poll-then-assert-then-act), now exercised for the correlated two-dataset
  case specifically.

### New Report-Sensing DAG (major scope addition — user-initiated)

- **D-26 (major scope addition — user-initiated, non-default choice):** Add a **new Airflow DAG**
  that senses when both `customers_valid` and `orders_valid` have data for the current (real)
  partition, then builds/logs the business report. User explicitly chose this over the recommended
  "keep report generation as the existing script" option — today there is exactly ONE generic
  `csv_ingest` DAG (Phase 5 D-01: param-driven per dataset, not one-per-dataset) and report
  generation lives entirely in `scripts/regenerate_readme_summary.py` (a CI-triggered script, not
  an Airflow DAG). This is genuinely new orchestration capability on top of everything else in this
  phase. — **Reversibility:** reversible — new DAG file, can be removed without affecting the
  existing `csv_ingest` DAG or the CI script.
- **D-27:** The new DAG runs **alongside** `regenerate_readme_summary.py`'s CI-triggered path, not
  replacing it — keeps Phase 6's already-shipped README automation (D-11/D-12/D-13) untouched and
  unrisked; the new DAG is an additional, live in-Airflow path.
- **D-28:** Detection mechanism: a **deferrable sensor task polls Oracle directly** (queries
  `ingestion_metadata` for both `customers` and `orders` having a row for the current partition) —
  same pattern spirit as the existing `wait_for_file` deferrable sensor, checks the actual source
  of truth (Oracle) rather than Airflow's own cross-DAG run-tracking state.
- **D-29 (grounded via Context7/Airflow docs during discussion):** The sensor determines "today's
  partition" by querying the **real wall-clock date directly** (`TRUNC(ingested_at) =
  TRUNC(SYSDATE)` in Oracle, or Pendulum `now()` in Python) — NOT via `logical_date`/
  `data_interval_start`/`data_interval_end` arithmetic. Confirmed against Apache Airflow's own
  docs (this project pins Airflow 3.3.1, `docker/airflow/Dockerfile`): "manual DAG runs do NOT
  guarantee that data_interval is derived from or equal to the logical_date" — and this project's
  DAGs are **always** manually/API-triggered (never scheduled), so `logical_date`/`data_interval`
  arithmetic isn't reliable here regardless of which Airflow generation (pre-2.2 `execution_date`
  vs. 2.2+ `data_interval`) is used as the mental model.

### Requirements Traceability

- **D-30:** Given how far this phase has grown beyond "fix a data bug" (ID correlation, PK/index/
  trigger DDL, staging/rename, new DAG, benchmark re-verification), REQUIREMENTS.md gets **new REQ
  IDs** during planning (e.g. `DATA-01` for ID correlation, `DB-01` for PK/index/trigger — exact
  IDs are the planner's call) rather than being force-fit against the existing, already-`Complete`
  `DOC-01`/`TEST-03` from Phase 6 — per the ROADMAP's own open question on this.

### Claude's Discretion

- Exact `--seed`-derived hash/component used in `CUST-{seed_hash}-00001` (short hex digest, base36
  encoding, etc.) — implementation detail, pick whichever is simplest and collision-resistant
  enough for the seed space this project actually uses.
- Exact zero-padding width for the sequential ID suffix — size to the row count, matches existing
  `format_decimal`-style discipline (exact formatting, never approximate).
- Exact new REQ ID naming/numbering (D-30) — follow REQUIREMENTS.md's existing ID convention.
- Exact shape of the new report DAG's task graph (single sensor + single report task vs. more
  granular breakdown) — implementation detail once D-26/D-28/D-29's behavior is locked.
- Whether the new combined CLI mode (D-22) is a new flag on `generate_csv.py` or a small separate
  orchestrator script — either satisfies D-21/D-22 as long as `make generate` ends up as one
  command using the shared correlation function.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (this phase's exact scope)
- `.planning/ROADMAP.md` §Phase 7 — goal, root cause, and the 4 literal success criteria. Success
  criteria are the floor; this discussion locked substantial scope beyond them (D-13 through D-29)
  — see D-30 on reconciling this with REQUIREMENTS.md.
- `.planning/REQUIREMENTS.md` — existing `DOC-01`/`TEST-03` full text and traceability table
  (`Phase 6 | Complete`); new REQ IDs get added here per D-30.
- `.planning/PROJECT.md` — "Referential integrity, uniqueness... explicitly excluded" (Out of
  Scope) is about the CSV engine's own application-level validators, NOT the new Oracle-level PK/
  trigger (D-14/D-15) — do not conflate the two when planning.

### Prior-phase decisions this phase builds on
- `.planning/phases/06-end-to-end-verification-benchmark-ci-docs/06-CONTEXT.md` — D-09 (evidence
  script, never re-authored SQL twice — directly informs D-21's "centralize, don't duplicate"
  rule), D-10 (business report shape: `customers.country` as region proxy × month-of-`order_date`,
  count + total/avg `amount` — unchanged by this phase), D-12/D-13 (live/CI-regenerated README,
  `[skip ci]` convention — D-27 builds alongside this, does not touch it), D-06/D-07 (required,
  blocking `oracle-e2e` CI check — D-11 extends this pattern).
- `.planning/phases/05-airflow-dag-wiring-deferrable-file-wait/05-CONTEXT.md` — D-01 (single
  generic `csv_ingest` DAG, param-driven, not one-per-dataset — the baseline D-26's new DAG adds
  to), D-05 (file-path convention `/opt/airflow/data/<dataset>/` — what D-24's staging/rename
  writes into).
- `.planning/phases/04-oracle-bulk-load-idempotency-engine-entrypoint/04-CONTEXT.md` — bulk-insert
  design (`executemany()` + array binding) that D-16's trigger-failure behavior must be compatible
  with.

### Actual code this phase integrates with (more authoritative than research sketches)
- `generator/generate_csv.py` — `generate_rows()`, `_valid_value()`/`_fake_string_value()` (where
  `customer_id`/`order_id` currently become Faker words, D-06/D-08's target), `write_csv()`,
  `output_path()`, `main()`/CLI arg parsing (D-22's target for the new combined mode).
- `configs/datasets/customers.json` / `orders.json` — `customer_id` schema (`type: string,
  nullable: false, required: true` in both) — format change (D-06) must stay compatible with this
  contract (still a `string` column, just structured content).
- `docker/oracle/init/02_customers.sql` / `03_orders.sql` — existing DDL (interval partitioning
  already present, D-17) that D-13/D-14/D-15's new PK/index/trigger get added to (or a new
  `04_add_correlation_constraints.sql`-style file, following the existing numbered-init-script
  convention alongside `04_widen_invalid_columns.sql`).
- `Makefile` — `generate` target (line 24, two independent subprocess calls — D-22's target),
  `reset` target (line 9-10, `docker compose down -v` — D-19's "start fresh" mechanism, already
  exists, no changes needed to `reset` itself).
- `scripts/verify_evidence.sql` — the JOIN query that starts returning real rows once D-01–D-05
  land; SQL text must stay mirrored verbatim in `scripts/regenerate_readme_summary.py` per Phase
  6's established discipline (never re-authored independently).
- `scripts/regenerate_readme_summary.py` — `_run_ingestion()` (currently generates each dataset's
  CSV independently per-dataset loop — D-21/D-23's target for adopting the shared correlated
  generator and staging/rename), `_DATASETS = ("customers", "orders")` iteration order already
  matches D-23's customers-then-orders requirement.
- `airflow/dags/csv_ingest.py` — existing `wait_for_file` deferrable sensor pattern (D-25's proof
  target) and the `/opt/airflow/data/{{ params.dataset }}/` path template D-24's rename target must
  match exactly.
- `docker-compose.yml` — `./data:/opt/airflow/data` bind mount (line 74) — confirms D-24's staging
  subdirectory stays on the same filesystem/volume as the watched path.
- `tests/e2e/test_csv_ingest_e2e.py` — existing customers-only e2e test (stays as-is per D-10); its
  `importlib.util.spec_from_file_location` sibling-module-loading convention (for `generate_csv`/
  `dag_polling`) should be mirrored by the new e2e test file.
- `tests/integration/test_load_oracle.py`, `tests/integration/test_engine_process_oracle.py` —
  confirmed during discussion: neither currently loads `orders_valid` with hardcoded `customer_id`
  values, so the new D-15 trigger does not break either existing suite.
- `docs/benchmark.md` — existing 182.85× figure (Phase 6) that D-20 requires re-measuring and
  updating.
- `docker/airflow/Dockerfile` — pins `apache/airflow:3.3.1-python3.12`; D-29's Airflow-3
  manual-trigger `logical_date`/`data_interval` behavior is version-specific to this pin.

### External docs consulted during discussion
- Apache Airflow docs (via Context7, `/apache/airflow`) — `airflow-core/docs/installation/
  upgrading_to_airflow3.rst`: "In Airflow 3, manual DAG runs do not guarantee that the
  'data_interval' is derived from or equal to the supplied 'logical_date'" — the direct basis for
  D-29. Re-verify against the pinned 3.3.1 docs specifically during implementation if behavior
  seems to differ.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `generate_csv.py`'s existing `rng = random.Random(seed)` instance — D-05's determinism guarantee
  reuses this exact object for pool-sampling/Zipf-weighting draws, no new RNG needed.
- `Makefile`'s numbered-init-script convention (`01_ingestion_metadata.sql` →
  `04_widen_invalid_columns.sql`) — D-13/D-14/D-15's new DDL follows this same pattern.
- `airflow/dags/csv_ingest.py`'s `wait_for_file` deferrable-sensor pattern — direct template for
  D-28's new cross-dataset-completion sensor (same "poll without blocking a worker slot" shape).
- `scripts/dag_polling.py`'s REST-API trigger/poll helpers (`trigger_dag`, `get_jwt_token`,
  `wait_for_task_state`) — reusable as-is for the new e2e test (D-09/D-10) and D-26's DAG, which
  both need to trigger/observe the existing `csv_ingest` DAG.

### Established Patterns
- Never re-author the same SQL/logic in two places (Phase 6 D-04, D-09) — directly drives D-21
  (centralized correlation function) and D-15's trigger reusing the same "customer_id must exist"
  concept the Python side already guarantees, as a safety net rather than a second independent
  implementation.
- Write-then-observe-then-act ordering for the file sensor (Pitfall 4, already established in
  `regenerate_readme_summary.py`) — D-23/D-24/D-25 extend this same discipline to the two-dataset,
  staged-file case.
- `packages/csv-processor/` has zero Airflow imports (Phases 2-5 discipline) — the new
  `generate_correlated_datasets()` (D-21) belongs in `generator/`, not `packages/csv-processor/`,
  consistent with `generate_csv.py`'s existing placement.

### Integration Points
- D-26's new report DAG and the existing `csv_ingest` DAG are independent — no DAG-to-DAG trigger
  dependency, since D-28's sensor polls Oracle state directly rather than Airflow's own run
  tracking (this was itself a locked decision, D-29, to avoid `logical_date`/`data_interval`
  fragility for manually-triggered runs).
- D-24's staging directory (`data/<dataset>/.staging/`) needs to be excluded from
  `configs/datasets/*.json`'s `file_pattern` matching (`customers_*.csv*` / `orders_*.csv*`) or
  live outside the dataset's watched directory entirely — otherwise the sensor could see
  in-progress staging files. Concrete placement is the planner's call within D-24's constraint
  (same filesystem, genuinely different path).

</code_context>

<specifics>
## Specific Ideas

This phase grew far beyond its ROADMAP-scoped starting point through discussion — every expansion
below was explicit and user-confirmed, several after Claude pushed back once with a concrete
tradeoff explanation:

- User's own words on Zipf weighting: chose "Weighted distribution" over the recommended uniform
  sampling — wanted realistic "power customer" order concentration.
- User caught a genuine correctness risk Claude hadn't raised: sequential ID reset would cause
  cross-run duplicate `customer_id` values and JOIN fan-out once more than one generation run's
  data exists in the accumulating Oracle tables (D-07) — directly shaped the seed-derived-uniqueness
  design (D-07's mitigation, later reinforced by the PK constraint in D-14).
- User pushed for Oracle table partitioning as in-scope even after Claude flagged it as an unrelated
  new capability — Claude then discovered during codebase scouting that partitioning already exists
  (Phase 1 DDL, `INTERVAL (NUMTODSINTERVAL(1, 'DAY'))`), which reframed the ask into D-12's
  multi-partition test-data requirement instead.
- User asked for "triggers, use Oracle PLSQL for processes, including the final report," then
  "move as much of data logic and data processing to the Oracle PLSQL from Python, whenever
  possible." Claude pushed back clearly (contradicts ROADMAP's literal success criterion 1 wording,
  reverses this project's shipped Python-engine architecture, "whenever possible" has no bounded
  scope) and the user agreed to scope it down to a single bounded piece: an INSERT-time validation
  trigger (D-15/D-16), with the broader PL/SQL migration recorded as a deferred future-direction
  idea rather than decided in passing.
- User introduced the new report-sensing DAG concept unprompted ("maybe we should have a DAG which
  checks if data arrived to tables orders, customers on specific data partition. If yes, then
  create the report") and then specifically asked Claude to consult Context7/Airflow docs on
  execution_date vs. logical_date semantics before locking the sensor's date-detection approach
  (D-29) — this is now grounded in Airflow 3's own documented behavior for manually-triggered DAGs,
  not assumption.
- User separately flagged a live, unrelated bug (Airflow UI logs not showing at :8080) — explicitly
  deferred to be handled after this phase via its own investigation (e.g. `/gsd-debug`), not folded
  into Phase 7.

</specifics>

<deferred>
## Deferred Ideas

- **Broad PL/SQL migration** — "move as much data logic and data processing to Oracle PL/SQL from
  Python, whenever possible." Explicitly deferred as its own future architectural direction
  (needs dedicated research/milestone discussion, not a Phase 7 implementation decision) — directly
  contradicts this project's current, shipped "thin Python engine" architecture (PROJECT.md), so
  deciding it in passing here would be irresponsible. Recorded for STATE.md's deferred-items
  tracking.
- **Airflow UI logs not showing at :8080** — unrelated live operational bug, explicitly out of this
  phase's scope. User agreed to handle it separately (e.g. via `/gsd-debug`) after Phase 7.

### Reviewed Todos (not folded)
None — `todo.match-phase` returned zero matches for Phase 7 (STATE.md's "Pending Todos: None yet"
already confirmed this).

</deferred>

---

*Phase: 7-Correlated Customer-Order Business Report*
*Context gathered: 2026-08-30*
