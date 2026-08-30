# Phase 6: End-to-End Verification, Benchmark, CI & Docs - Context

**Gathered:** 2026-08-29
**Status:** Ready for planning

<domain>
## Phase Boundary

The completion-quality gate for the whole project: an automated end-to-end test proves the full
HTTP-trigger → deferred-file-wait → CSV-engine → Oracle path works with real evidence (not just a
green DAG run), a benchmark proves the chunked/bulk design is measurably faster than a genuinely
naive row-by-row approach, GitHub Actions runs lint/type-check/unit-tests **and** the Oracle+e2e
suite on every PR as a required check, and docs (README + topic docs) let a new developer go from
`git clone` to a completed HTTP-triggered ingestion with evidence of it working. This phase
consumes Phase 5's already-live-verified DAG (`csv_ingest`) and Phase 4's `csv_processor.process()`
entrypoint; it does not change either.

</domain>

<decisions>
## Implementation Decisions

### Benchmark (TEST-04)

- **D-01:** The "row-by-row vs. chunked/bulk" comparison uses a **genuine, separate naive-loop Oracle
  insert baseline** (single `cursor.execute()` per row, in a loop) — **not** the existing
  `executemany()` code path run with `chunk_size=1`. User's own words: "choose the most
  representative approach to test" — a `chunk_size=1` run still goes through `executemany()` and
  wouldn't reproduce the actual per-round-trip cost pattern PITFALLS.md flags as the real-world
  mistake this benchmark exists to catch. — **Reversibility:** reversible — isolated to new
  benchmark-only code, doesn't touch `csv_processor`.
- **D-02:** Benchmark dataset is **`customers`** (simpler 6-column schema) at **~100K rows** (matches
  TEST-04's literal text and `research/SUMMARY.md`'s spec-aligned figure). `generator/generate_csv.py`
  already supports `--rows N` directly — no new "large profile" needed, just `--rows 100000`.
- **D-03:** The benchmark calls `csv_processor.engine.process()` / `csv_processor.load` **directly**,
  bypassing Airflow entirely — isolates CSV-parse + Oracle-load performance from Airflow scheduling/
  polling overhead, giving a cleaner rows/sec number that measures what the benchmark is actually
  about (chunked I/O + bulk insert design), not DAG orchestration cost.
- **D-04:** The naive-loop baseline is throwaway, clearly-separate code in a **new top-level
  `benchmark/` directory** (`benchmark/naive_loader.py` + `benchmark/run_benchmark.py`) — not inside
  `packages/csv-processor/` (never confuse it with the reusable engine) and not folded into
  `tests/benchmark/` (keeps it out of the normal pytest run path; it's a benchmark script, not a
  test that must pass/fail on every CI run).
- **D-05:** Results are committed to **`docs/benchmark.md`**, containing: (a) a side-by-side
  comparison table (naive vs. chunked/bulk, all three required metrics — rows/sec, peak memory,
  Oracle load time — as rows, both approaches as columns), (b) an explicit speedup ratio / %
  improvement line, (c) run metadata (row count, dataset, machine/environment specs, run date), and
  (d) a raw per-chunk timing breakdown for the chunked run (demonstrates flat/bounded-memory
  streaming behavior across chunks, not just an aggregate number).

### End-to-end test & CI scope (TEST-03, CI-01)

- **D-06 (scope expansion beyond CI-01's literal text — explicit, user-approved):** GitHub Actions
  spins up **Oracle + Airflow as real service containers** and runs the e2e test **in the PR
  pipeline itself**, not just locally. CI-01's literal text only says "lint, type check, and unit
  tests," and `research/SUMMARY.md` explicitly notes Oracle-in-CI is optional per spec — the user
  chose to go beyond that baseline deliberately. — **Reversibility:** costly — once CI depends on
  Oracle+e2e as a required check, reverting to lint/type/unit-only changes what "PR passing" means
  for every future contributor.
- **D-07:** The Oracle+e2e job is a **required status check** (blocks merge like lint/type/unit) and
  runs on **every PR**, not just main-branch pushes — matches CI-01's literal "on every PR" wording;
  the project is small enough that `-faststart` Oracle boot time keeps this reasonable.
- **D-08:** The e2e test must prove the **full trigger chain**, not just "the DAG completed": a file
  appearing on disk → the deferred `wait_for_file` task genuinely waking (not polling) → `process_csv`
  reading it via the real CSV engine → correct rows landing in `<DATASET>_VALID` and deliberately-bad
  rows landing in `<DATASET>_INVALID`. User's own words: "we have to be able to see that DAG worked
  from catching up that the file shown up, that event of showing up a file should start the DAG...
  either correct or incorrect into oracle tables." This is stronger than DAG-03's Phase 5 structural
  proof — it must be evidenced with real Oracle query results, not just Airflow task state.

### Evidence & Executive Summary (new, user-initiated — folds into TEST-03/DOC-01)

- **D-09 (major scope addition — user-initiated):** Evidence of a working pipeline must be captured
  by **querying the Oracle tables directly**, via a **committed SQL script + Makefile target**
  (e.g. `scripts/verify_evidence.sql` + `make verify-evidence`) — not ad hoc/undocumented manual
  `sqlplus` commands. Reproducible by anyone who runs the target, not a one-time manual capture.
- **D-10 (major scope addition — user-initiated):** A **customers⋈orders business report** is part
  of that evidence — join on `customer_id`, aggregate by **region and date**, with a count and
  numeric metrics ("something typical"). **Schema note (Claude's substitution, flagged explicitly):**
  neither table has a literal `region` column — `customers.country` is used as the region proxy
  (closest real geographic field); date grouping uses month-of-`orders.order_date`. Metrics: order
  count + total/average `amount` per (country, month). This does **not** reopen the "orders'
  `customer_id` FK not enforced" out-of-scope decision (Phase 3/PROJECT.md) — it's a read-only JOIN
  for reporting evidence, not a referential-integrity validator at ingestion time.
- **D-11 (major scope addition — user-initiated):** README.md gets a top **"Executive Summary"**
  section as proof-of-life evidence, containing: (a) total/valid/invalid row counts per dataset from
  the latest run, (b) a **deferred-wake proof line** (timestamp/evidence that `wait_for_file`
  genuinely reported Airflow state `deferred` before the file existed — proves the non-blocking
  file-wait specifically, not just that the DAG ran), and (c) the customers⋈orders business report,
  **top N rows only** (not a full dump — keeps the README readable).
- **D-12 (major scope addition — user-initiated, non-default choice):** The Executive Summary is
  **live/regenerated, not a static snapshot** — user explicitly chose this over the recommended
  "snapshot with a last-verified date" option. Regeneration is triggered by **CI auto-committing**
  the refreshed section back to `README.md` after every successful merge to `main` (not a
  locally-run, manually-committed maintainer command — user explicitly chose the CI-auto-commit
  option over the simpler local-only alternative). — **Reversibility:** costly — undoing this means
  removing a CI write-back step and re-establishing a different (manual or no) update path; also a
  one-way door on repo hygiene (bot commits in history).
- **D-13:** The CI auto-commit step uses the standard **`[skip ci]`** convention in the bot's commit
  message to prevent an infinite CI-triggers-itself loop. Needs `permissions: contents: write` (or
  equivalent PAT) on that job step — flagged for the phase-researcher to confirm the exact
  GitHub Actions syntax/permissions model for a workflow-triggered commit.

### Lint / type-check scope (CI-01)

- **D-14 (non-default choice):** `mypy` and `ruff` cover the **whole repo, including `airflow/dags/`**
  — user explicitly chose this over the recommended "engine-only, DAGs excluded" option, accepting
  that Airflow's decorator-heavy TaskFlow API and dynamic `conf` handling may need some
  `type: ignore` comments in DAG code. Versions already pinned in `research/STACK.md`: `ruff==0.16.5`,
  `mypy==2.3.1`, installed via `uv add --dev`.

### Docs (DOC-01)

- **D-15:** Full topic-doc split (not consolidated) — README.md + `docs/architecture.md` +
  `docs/configuration.md` + `docs/csv-engine.md` + `docs/oracle.md` + `docs/development.md`, in
  addition to Phase 5's already-existing `docs/airflow-dag.md` and Phase 1's `docs/environment.md`.
  Mirrors PROJECT.md's original doc-structure sketch.
- **D-16:** README.md is **"summary + links"**, not a fully self-contained walkthrough — a short
  overview plus links into the topic docs (`docs/environment.md`, `docs/airflow-dag.md`, etc.) for
  the actual command sequences. The Executive Summary (D-11/D-12) sits at the **top** of this
  same README, ahead of the summary/links content.
- **D-17:** `docs/development.md` covers **all three** of: (a) local dev workflow (running unit/
  integration/e2e tests, resetting the Oracle container, regenerating fixtures, running lint/
  type-check locally before pushing), (b) architecture/contribution notes (code layout, how to add a
  new dataset, coding conventions matching CLAUDE.md's own style), and (c) CI/troubleshooting (what
  CI actually runs, how to debug a failing PR check locally).

### Claude's Discretion

- Exact peak-memory measurement method for the benchmark (`tracemalloc`, `resource.getrusage`,
  `psutil`, or `/usr/bin/time -v`) — implementation detail, pick whichever gives the cleanest
  peak-RSS number for both the naive and chunked runs.
- Exact GitHub Actions workflow YAML structure (job names, matrix strategy, `uv` caching approach) —
  `astral-sh/setup-uv@v10.0.1` is the pinned action per `research/STACK.md`, but
  `research/SUMMARY.md` flags re-verifying the exact latest immutable tag at implementation time
  since action releases move faster than the research's cache TTL.
- Exact content/SQL of `scripts/verify_evidence.sql` and the Executive Summary's marker syntax in
  README.md (e.g. HTML comment markers the regeneration script writes between) — implementation
  detail once D-09/D-11's shape is locked.
- Whether the deferred-wake proof (D-11b) is captured by the same e2e test/evidence script or a
  separate check — implementation detail, as long as the proof ends up in the Executive Summary.
- `verify-phase6` Makefile target — follows the established `verify-phaseN` convention already in
  the Makefile (unit suite + phase-specific live checks); exact composition (lint + type-check +
  unit + Oracle/e2e + benchmark?) is Claude's call during planning.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (this phase's exact scope)
- `.planning/REQUIREMENTS.md` — TEST-03, TEST-04, CI-01, DOC-01 full text (lines 107-117).
- `.planning/ROADMAP.md` §Phase 6 — goal and the 4 literal success criteria; success criterion 3's
  "lint, type check, and unit tests" is explicitly widened by D-06 to include Oracle+e2e — the
  literal text is not overridden by mistake, it's a recorded, user-approved scope expansion.

### Research (tooling versions and CI conventions, already pinned — not open questions)
- `.planning/research/STACK.md` — `ruff==0.16.5`, `mypy==2.3.1`, `uv add --dev "pytest==9.1.1"
  "ruff==0.16.5" "mypy==2.3.1"`, `astral-sh/setup-uv@v10.0.1` (pin exact immutable tag, re-verify
  live per SUMMARY.md's own flag), `gvenzl/oracle-free:23.26.2-faststart` for CI `services:` block,
  `testcontainers` vs. docker-compose-service-in-CI tradeoff note (§89-90 area).
- `.planning/research/PITFALLS.md` — the row-by-row-vs-`executemany()` pitfall (§"Common Mistake"
  table, ~line 274) directly behind D-01; the "chunked design must be built as the real code path,
  not retrofitted, because the benchmark's whole point is proving it" pitfall (~line 253) behind
  D-01/D-03/D-05's per-chunk timing requirement.
- `.planning/research/SUMMARY.md` — Phase 8 (renumbered to this project's Phase 6) "CI & Docs"
  section (~line 118-121): "Oracle-in-CI is explicitly optional per spec" (the baseline D-06
  explicitly goes beyond) and the `astral-sh/setup-uv` tag re-verification flag.

### Prior-phase decisions this phase builds on
- `.planning/phases/05-airflow-dag-wiring-deferrable-file-wait/05-CONTEXT.md` — D-01 (single
  `csv_ingest` DAG, no dataset branches — what the e2e test triggers), D-03 (domain-failure statuses
  never fail the task — relevant to what "DAG worked" means for D-08's evidence), D-05 (file-path
  convention `/opt/airflow/data/<dataset>/`), D-07 (report_result logs only).
- `.planning/phases/05-airflow-dag-wiring-deferrable-file-wait/05-SUMMARY.md` (both 05-01 and 05-02)
  — the actual live-verification evidence already captured (customers success run, orders run,
  deferred-state observation via REST API `taskInstances`) — the e2e test in this phase should
  automate what these SUMMARYs already proved manually.
- `.planning/phases/04-oracle-bulk-load-idempotency-engine-entrypoint/04-CONTEXT.md` — `Status` enum
  and `ProcessingResult` shape (D-01/D-02 area) — what the benchmark's direct `process()` calls
  return and what the e2e test asserts against.
- `.planning/phases/02-config-contract-csv-generator/02-CONTEXT.md` — `generator/generate_csv.py`'s
  `--rows`/`--dataset` interface (D-06's csv output convention) used to build the ~100K-row benchmark
  fixture.
- `.planning/PROJECT.md` — "orders.customer_id → customers.customer_id FK not enforced" (Out of
  Scope) — D-10's business report is a read-only JOIN, does not reopen this decision.

### Actual code this phase integrates with (more authoritative than research sketches)
- `Makefile` — existing `verify-phaseN` convention (`verify-phase2` through `verify-phase5`) and its
  own comment: "Later phases (2-6) add targets here (make test, make lint, make benchmark)" —
  `verify-phase6`, `make benchmark`, `make lint`, and `make verify-evidence` should follow this
  established pattern, not invent a new one.
- `generator/generate_csv.py` — `--dataset`/`--rows` CLI interface, already supports arbitrary row
  counts directly (`--rows 100000`), no new generation profile needed.
- `packages/csv-processor/src/csv_processor/engine.py`'s `process()` and `csv_processor/load.py` —
  the exact functions the benchmark calls directly (D-03) and the e2e test's assertions target.
- `docker-compose.yml` — the Airflow/Oracle service definitions and env vars Phase 5 already fixed
  (Oracle DSN/credentials, `configs/` mount, `fs_default` connection, execution-API URL, JWT secret)
  — the CI `services:` block (D-06) needs the same env vars, not a fresh reinvention.
- `docs/airflow-dag.md`, `docs/environment.md` — existing docs this phase's README (D-16) links into
  rather than duplicating.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `Makefile`'s `verify-phaseN` pattern and its own explicit invitation ("Later phases (2-6) add
  targets here") — directly reusable convention for `verify-phase6`/`make benchmark`/`make lint`/
  `make verify-evidence`.
- `generator/generate_csv.py --dataset customers --rows 100000` — the exact command for the
  benchmark's input fixture, no new tooling needed.
- Phase 5's `scripts/trigger_dag.sh` — the REST-API trigger pattern (auth token flow, `conf` payload
  shape) the e2e test's HTTP-trigger step should follow/reuse.
- `docker-compose.yml`'s already-fixed Airflow/Oracle env vars (Phase 5 D-fixes) — reusable directly
  in the CI `services:` block rather than rediscovering the same gaps.

### Established Patterns
- Every prior phase's Makefile target follows unit-suite-first, then phase-specific live checks
  (`verify-phase4`/`verify-phase5` require `make up` first) — `verify-phase6` should follow the same
  shape.
- `packages/csv-processor/` has zero Airflow imports anywhere (Phases 2-5 discipline) — the benchmark
  calling `process()`/`load` directly (D-03) fits this: it's exercising the engine, not Airflow.

### Integration Points
- The e2e test (D-08) is the automated version of what Phase 5's `05-01-SUMMARY.md`/
  `05-02-SUMMARY.md` already proved manually via live REST-API triggers — same trigger mechanism,
  now scripted and asserted rather than eyeballed.
- The Executive Summary's CI auto-commit step (D-12/D-13) is a new GitHub Actions capability this
  project hasn't used before — needs `permissions: contents: write` and a bot git identity, wired
  into whichever job runs the evidence script (D-09) after the e2e job passes.

</code_context>

<specifics>
## Specific Ideas

User drove significant scope beyond the roadmap's literal text in this phase, all confirmed and
locked (see D-06 through D-13):

- The e2e/CI scope was explicitly pushed beyond CI-01's literal "lint, type check, unit tests" to
  include a real Oracle+Airflow CI job as a required, every-PR check (D-06/D-07).
- User introduced the whole "Executive Summary in README" concept unprompted, in their own words:
  *"we have to be able to see that DAG worked from catching up that the file shown up... report
  evidenced to README.md in a top Executive Summary as evidence of working lightweight airflow+oracle
  etl platform"* (D-08/D-09/D-11).
- User then added the customers⋈orders business-report requirement on top of that: *"we have to be
  able to join our two tables: customers and orders to get some sort of business report"* (D-10),
  specifying region+date aggregation with a count — "region" mapped to `customers.country` since no
  literal region column exists (flagged explicitly in D-10, not silently assumed).
- User chose the more demanding, less-recommended path at two separate decision points: live/
  regenerated Executive Summary over a static snapshot (D-12), and CI-auto-commit over a
  local-maintainer-run command (D-12) — both recorded with their reasoning.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope. The Executive Summary / business-report additions are
scope *expansions* within Phase 6 (they strengthen TEST-03's "prove it works" and DOC-01's
"documented" requirements), not new capabilities that belong in a different phase.

### Reviewed Todos (not folded)
None — `todo.match-phase` returned zero matches for Phase 6.

</deferred>

---

*Phase: 6-End-to-End Verification, Benchmark, CI & Docs*
*Context gathered: 2026-08-29*
