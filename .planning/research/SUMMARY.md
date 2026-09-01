# Project Research Summary

**Project:** Lightweight Airflow CSV→Oracle ETL Platform
**Milestone:** v1.1 — Hourly Ingestion Automation
**Domain:** Airflow-native scheduling/orchestration wiring — a new `@hourly` parent DAG that
generates fresh fixture CSVs in-process and chain-triggers two already-shipped, unmodified DAGs
(`csv_ingest` ×2, `report_ready`) via `TriggerDagRunOperator`, plus the environment/permission
fixes and one trigger-robustness fix needed to support it.
**Researched:** 2026-09-01
**Confidence:** HIGH — this is a delta milestone against a working v1.0 system; all four research
passes verified claims against this repo's own committed source, this project's exact pinned
dependency versions (live-fetched/live-imported, not assumed), and Airflow's own upstream source
at the exact pinned provider tag (`apache-airflow-providers-standard==1.17.0`). The one genuine
open question is flagged explicitly below, not silently resolved.

## Executive Summary

This milestone does not introduce a new architectural pattern — it is a small, dependency-ordered
wiring exercise on top of an already-working v1.0 system. The new `csv_generate_schedule` DAG
follows the same "thin TaskFlow DAG, no business logic" convention `csv_ingest.py` already
established: an in-process `@task` calls `generator.generate_csv`'s existing functions to produce
fresh customers+orders CSVs, then three sequential `TriggerDagRunOperator` tasks chain-trigger
`csv_ingest` (customers) → `csv_ingest` (orders) → `report_ready`, each blocking on the prior
step's actual completion. `csv_ingest.py` and `report_ready.py` need **zero code changes** — every
researcher independently confirmed that correct sequencing (customers before orders, no
overlapping hourly cycles) is achievable entirely from the new parent DAG's own parameters
(`max_active_runs=1`, sequential `wait_for_completion=True` triggers). Two supporting fixes round
out the milestone: making the Airflow image and `data/` directory support container-side CSV
generation (a new capability, not just a new schedule), and hardening
`OraclePartitionReadyTrigger.run()`'s exception handling so a transient Oracle outage no longer
permanently crashes the deferred `report_ready` sensor.

The research converged with unusually high consistency on several concrete, load-bearing details
that were previously open questions: `faker==40.37.0` (already pinned in root `pyproject.toml`)
can go straight into the Dockerfile's existing *constrained* pip-install line alongside `oracledb`/
`pydantic`/the two providers — a live fetch of Airflow's own `constraints-3.3.1` file confirms
`faker` has zero entry there, so there is nothing for `--constraint` to conflict with; `generator/`
must be bind-mounted at exactly `/opt/airflow/generator` (not `/opt/airflow/dags/generator` or any
other path) because `generate_csv.py`'s own `_REPO_ROOT = Path(__file__).resolve().parent.parent`
arithmetic only lines up with the already-mounted `configs/`/`data/` at that one path; and the
`data/` directory's read-only-friendly permission fix from v1.0 must be replaced with a proper
read-write fix — best done as a `chown -R`-in-`airflow-init` compose step (running once as uid 0,
gated by the existing `depends_on: service_completed_successfully` chain) rather than a repeat of
the old manual host-side `mkdir`/`chmod` workaround, since generation now happens *inside* the
container for the first time.

The single most dangerous risk this research surfaced — because it produces no visible error at
all — is `generate_csv.py`'s CLI defaulting `--seed` to the fixed literal `20260101`. Combined with
`engine.process()`'s checksum-keyed idempotency (a match against a prior run's checksum
short-circuits straight to the old recorded outcome, inserting nothing new), an hourly generate
task that doesn't explicitly vary its seed per run would produce byte-identical CSVs every hour
after the first, and every ingestion after hour one would become a silent, green-checkmarked
no-op. The seed must be derived from the run's own trigger time (e.g. the logical-date hour) and
this must be covered by an explicit test asserting two different hours produce two different
checksums. One genuine, unresolved design question — whether the three chain-trigger
`TriggerDagRunOperator` tasks should run `deferrable=False` (poke) or `deferrable=True` (defer) —
came back split 2-2 across the four research passes and is called out explicitly below rather than
averaged away; it needs a deliberate decision during requirements/roadmap definition, not an
assumption baked silently into a phase plan.

## Key Findings

### Recommended Stack

No new frameworks or provider packages. The only net-new library dependency is `faker==40.37.0`
(already pinned in root `pyproject.toml`/`uv.lock`), and the only new environment wiring is a
volume mount for `generator/` and a `PYTHONPATH` extension. `TriggerDagRunOperator` already ships
in the already-pinned `apache-airflow-providers-standard==1.17.0` and was confirmed importable in
this exact built image this session — no new package to add for chain-triggering.

**Core technologies:**
- `faker==40.37.0` — realistic fake string values inside `generator/generate_csv.py` — **must
  match, not just approximate**, the version already locked in root `pyproject.toml`/`uv.lock`, so
  host-run (`make generate`) and container-run generation stay byte-identical for the same seed;
  confirmed (via live fetch of Airflow's `constraints-3.3.1/constraints-3.12.txt`) to have zero
  competing constraint entry, so it is safe to add to the *same constrained* `pip install` call as
  `oracledb`/`pydantic`/the two providers — this directly resolves an open verification question
  ARCHITECTURE.md itself flagged as needing a build-time check, and supersedes PITFALLS.md's more
  cautious inference (Pitfall 5) that it should go in the second, unconstrained call — the
  live-fetched constraints file is the higher-confidence source here.
- `TriggerDagRunOperator` — chain-triggers `csv_ingest` (×2) and `report_ready` sequentially,
  blocking each on the prior run's actual completion — already available, natively supports
  `wait_for_completion`, `poke_interval`, `allowed_states`/`failed_states`, `deferrable`, and (on
  this project's pinned Airflow 3.3.1, which is ≥3.2.0) `fail_when_dag_is_paused` — no custom
  sensor/trigger needed.
- No new Oracle, database, or Airflow-executor decisions — LocalExecutor, `python-oracledb`
  thin-mode, and Pydantic-v2-for-config-only remain exactly as validated in v1.0.

### Expected Features

**Must have (table stakes) for this milestone:**
- `csv_generate_schedule` DAG: `schedule="@hourly"`, `catchup=False` (non-negotiable — with
  `catchup=True`, a first deploy would immediately backfill every missed hourly interval,
  overwriting the same day's file and re-triggering downstream for no benefit), `max_active_runs=1`
  (closes a real same-filename race: `csv_ingest`'s existing `FileSensor` has `timeout=3600`,
  exactly one hour, so a stuck generation step can occupy nearly the full hour right up against the
  next scheduled cycle without this).
- An in-process `generate_csv_task` calling `generator.generate_csv`'s functions directly
  (`generate_correlated_datasets()` + `write_staged()`, or `main(["--correlated"])`), mirroring
  `csv_ingest.py`'s own "thin DAG, no subprocess" pattern — not a `BashOperator`/`subprocess` shell-out.
- Three sequential `TriggerDagRunOperator` tasks (customers → orders → `report_ready`), each
  `wait_for_completion=True`, matching `csv_ingest`'s existing `conf={"dataset": ..., "config_path": ...}`
  `Param` contract exactly; `report_ready` takes no `conf`.
- `fail_when_dag_is_paused=True` on every trigger task (available on the pinned 3.3.1; fails loudly
  if `csv_ingest`/`report_ready` is ever manually paused, rather than hanging).
- `trigger_run_id` left unset in the default/happy path — auto-generated, timestamp-derived IDs
  never collide across hourly cycles, sidestepping the entire `reset_dag_run` question entirely.
- No changes to `csv_ingest.py`/`report_ready.py` — confirmed unnecessary by every researcher;
  correct ordering and overlap-prevention live entirely in the new parent DAG's own parameters.

**Should have (add after the core cascade is proven):**
- Cascade-level summary logging at the parent DAG (log-only, mirrors the existing
  `report_result_task`/`build_report_task` convention — no new Slack/email channel).
- Configurable row counts/invalid-ratio via parent-DAG `Param`s (the generator functions already
  accept these as parameters; just needs exposing).

**Defer / explicitly excluded:**
- `reset_dag_run=True` — combined with `deferrable=True` and an explicit `trigger_run_id`, this has
  a documented, filed Airflow bug (apache/airflow#57756) causing the deferred triggering task to
  stay permanently stuck; also unneeded here since `trigger_run_id` stays unset.
- A deterministic/derived `trigger_run_id` "for idempotency" — the checksum-keyed idempotency
  already living in `engine.process()`/`ingestion_metadata` (v1.0, Phase 4) already makes a
  duplicate trigger a safe no-op one layer down; run-ID-level idempotency solves an already-solved
  problem and reintroduces the `reset_dag_run` question needlessly.
- Fan-out/parallel triggering of customers and orders — Phase 7's DB-level `BEFORE INSERT` FK
  trigger on `orders_valid` requires `customers_valid` to be populated first; this must stay
  strictly sequential.
- Fixing `csv_ingest.py`'s `FileSensor` timeout being numerically equal to the new hourly schedule
  period — a real structural tightness worth flagging, but out of this milestone's scope since it
  would touch a file this milestone must leave unmodified.

### Architecture Approach

`csv_generate_schedule` is the only new DAG; `csv_ingest` and `report_ready` are consumed exactly
as they exist today, triggered via `TriggerDagRunOperator` — the same trigger mechanism their
existing `schedule=None` design already anticipated (a DAG-to-DAG trigger is no different in kind
from the REST API trigger v1.0 already proved end-to-end). Three integration points carry all of
this milestone's real design weight: (1) exposing `generator/generate_csv.py` inside the container
at exactly the right mount path so its own path arithmetic resolves against the already-mounted
`configs/`/`data/` directories; (2) `TriggerDagRunOperator` + `wait_for_completion` + LocalExecutor
worker-slot semantics (the source of the flagged open decision below); (3) fixing `data/`'s
permissions at the compose level (an `airflow-init` root-user chown step, mirroring Airflow's own
official quick-start compose file) so container-side writes work on a genuinely fresh clone, not
just container-side reads.

**Major components:**
1. `csv_generate_schedule` (new DAG file) — hourly cron entrypoint: generate fresh CSVs in-process,
   then chain-trigger the three downstream DAGs sequentially.
2. `generate_task` — calls `generator.generate_csv`'s existing functions in-process; requires
   `generator/` mounted at exactly `/opt/airflow/generator` and `/opt/airflow` added to
   `PYTHONPATH` (as a namespace package, alongside the existing `/opt/airflow/dags` entry needed by
   the triggerer's `_common` import).
3. `csv_ingest` / `report_ready` (unchanged) — consumed purely as trigger targets.
4. `docker-compose.yml` / `docker/airflow/Dockerfile` (modified) — new volume mount, extended
   `PYTHONPATH`, `faker` added to the image, `airflow-init` chown fix for `data/`.

Recommended build order (strict, not stylistic — the DAG cannot be verified until the environment
supports it): (1) `airflow-init` chown fix for `data/`, verified independently via `make destroy &&
make up`; (2) `generator/` mount + `PYTHONPATH` extension + `faker` in the Dockerfile, verified via
`make rebuild` and a one-off manual import/exec check *before* wiring into a DAG; (3) the new
`csv_generate_schedule.py` DAG itself, verified structurally (`BundleDagBag` import check) then
watched through a real live-triggered run; (4) the `OraclePartitionReadyTrigger` exception-handling
fix, independent of 1–3 and can land in parallel or in either order.

### Critical Pitfalls

1. **Fixed default `--seed` makes every hourly generation byte-identical, silently no-op'ing
   ingestion after hour one** — the generate task must pass an explicit, per-run-varying `--seed`
   derived from the DAG run's own trigger time (e.g. `{{ ts_nodash }}` cast to int), never the
   CLI's bare default; add a test asserting two different logical dates produce two different
   SHA-256 digests and two distinct `ingestion_metadata` rows. This is the single most dangerous
   pitfall in the milestone because it produces a green DAG run with no error at all.
2. **The v1.0 `data/` permission fix only ever solved reads, not the writes this milestone newly
   needs** — `./data` is `root:root` mode `755` today; that was sufficient when only the host wrote
   into it and containers only read. With generation moving inside the container (uid 50000:gid 0),
   `write_staged()`'s staging-dir creation will hit `PermissionError` even with the old fix applied.
   Fix at the compose level via an `airflow-init` root-user `chown -R` step that runs before every
   other service starts (gated by the existing `depends_on: service_completed_successfully` chain),
   not a repeat of the old manual host-side `mkdir -p`/read-only-sufficient fix.
3. **Editing the Dockerfile without rebuilding silently keeps the stale image** — `docker compose up
   -d` alone does not rebuild an image that already exists locally under the same tag. Any
   Dockerfile change (adding `faker`) must be followed by `docker compose build` (or `up -d
   --build`), verified explicitly (`python -c "import faker"` inside a freshly-recreated container)
   before considering the environment fix done — the failure otherwise surfaces confusingly, hours
   later, deep inside the generate task's first real execution.
4. **`TriggerDagRunOperator`'s auto-generated `run_id` is wall-clock-derived at execute time, not
   retry-idempotent** — a retried trigger task creates a second, distinct downstream DagRun rather
   than resuming the first (safe here only because of `engine.process()`'s own checksum idempotency
   one layer down, but it clutters DagRun history). If retry-idempotency matters for this milestone,
   pass an explicit `trigger_run_id` derived from the parent run's own identity (e.g. `"{{
   dag_run.run_id }}__customers"`) and decide explicitly how `DagRunAlreadyExists` should be handled
   on retry, rather than leaving the default silently create a duplicate run.
5. **A naive exception-handling fix for `OraclePartitionReadyTrigger.run()` can miss the actual
   crash point or swallow genuine bugs** — `connect_async()` currently sits outside the existing
   `try/finally`, so a fix that only wraps the cursor/execute/fetch lines never protects the most
   likely real failure point. Catch `oracledb.OperationalError` specifically (verified live against
   the pinned `oracledb==4.0.2` exception hierarchy) — never the broader `DatabaseError`/`Error` —
   so genuine bugs (a typo'd column, a dropped table) still surface as visible failures instead of
   retrying forever. Also guard `finally: connection.close()` independently, since a close call on
   an already-broken connection can itself raise and mask the original, more diagnostic exception.

## Open Decision — Not Resolved by Research: `TriggerDagRunOperator(deferrable=...)`

The four research passes split 2-2 on whether the three chain-trigger tasks should run
`deferrable=False` (poke/blocking) or `deferrable=True` (defer to the triggerer). This is a genuine,
unresolved disagreement on a load-bearing design decision for the DAG-implementation phase — it is
recorded here explicitly, with both positions and their cited rationale, rather than picked
silently or averaged away. **This needs an explicit choice during requirements/roadmap
definition.**

**Position A — `deferrable=False` (poke), argued by STACK.md and ARCHITECTURE.md:**
- `TriggerDagRunOperator(deferrable=True)` has multiple still-open upstream Airflow bugs as of this
  research pass: apache/airflow#60049 (defers even when `wait_for_completion=False`),
  apache/airflow#57756 (deferred mode stuck when combined with `reset_dag_run`),
  apache/airflow#38353 (deferred `wait_for_completion` "not working as expected"), and
  apache/airflow#52247 (deferred trigger tasks stuck in Airflow 3.0.2) — none independently
  confirmed fixed at this project's exact pinned `apache-airflow-providers-standard==1.17.0`.
- This project's LocalExecutor has 32 concurrent task slots by default, the hourly chain is the
  only pipeline in the project, and the full cascade (generate + 2x ingest + report) completes in
  low tens of seconds once an hour per the existing benchmark record — there is no real worker-slot
  scarcity problem for `deferrable=True` to solve here, unlike `FileSensor(deferrable=True)`'s
  genuine win in `csv_ingest` (which can legitimately wait up to an hour on an external file).
  Occupying one of 32 slots for tens of seconds, once an hour, is immaterial.
- This project's own established discipline is "verify by actually running it, not assumed" (per
  PROJECT.md's Key Decisions on the FileSensor research) — adopting `deferrable=True` here without
  a live-verification pass against the exact pinned version combination would violate that
  discipline for a benefit that doesn't matter at this scale.

**Position B — `deferrable=True`, argued by FEATURES.md and PITFALLS.md:**
- Consistency with this project's own already-established, explicit convention: "defer, never
  block a worker slot" — already applied to `FileSensor(deferrable=True)` in `csv_ingest` and the
  custom `OraclePartitionReadyTrigger`/`ReportReadySensor` in `report_ready` (whose own docstring
  states it "never occupies a worker slot while waiting"). Introducing a *new* DAG in this project
  that reverts to blocking behavior specifically for chain-triggering would be an inconsistency
  against a pattern the project has otherwise applied everywhere a wait exists.
- `deferrable` defaults to `False`, meaning a non-deferred `wait_for_completion=True` task
  genuinely does occupy a LocalExecutor worker slot for the entire triggered DAG's runtime (verified
  directly against the operator's own source) for each of the three chained triggers in sequence —
  the exact "held slot while waiting" anti-pattern this project has deliberately avoided elsewhere.
- PITFALLS.md additionally recommends pairing `deferrable=True` with an **explicit,
  parent-run-derived `trigger_run_id`** (e.g. `"{{ dag_run.run_id }}__customers"`) rather than
  leaving it auto-generated, specifically to make retries of the trigger task idempotent against
  the same child DagRun rather than creating orphaned duplicates — independent of which side of the
  `deferrable` question is chosen, but cited as most relevant if `deferrable=True` is adopted.

**Recommendation for roadmap/requirements:** treat this as an explicit, named decision to make
during Phase/requirements definition for the DAG-implementation work, not a detail to leave
implicit in a task description. Whichever side is chosen, PITFALLS.md's `reset_dag_run` guidance
still applies unconditionally: never combine `reset_dag_run=True` with `deferrable=True` and an
explicit `trigger_run_id` (documented bug, apache/airflow#57756), and the default (unset)
`trigger_run_id` path should be the fallback if no explicit idempotency need is identified.

## Implications for Roadmap

Based on combined research, the pitfalls' own "phase to address" groupings map cleanly onto three
sequential phases, consistent with ARCHITECTURE.md's strict build-order dependency (environment
must support the DAG before the DAG can be verified; the trigger-robustness fix is independent of
both).

### Phase 1: Environment & Docker Fixes
**Rationale:** Every downstream phase depends on the container actually being able to run
`generate_csv.py` and write into `data/` — this is infrastructure, not DAG logic, and must be
proven correct on its own (via `make destroy && make up` / `make rebuild`) before any DAG code is
written against it.
**Delivers:** `airflow-init` root-user `chown -R` fix for `data/` (compose-level, idempotent,
gated by the existing `depends_on` chain); `./generator:/opt/airflow/generator` mount (read-only);
extended `PYTHONPATH` (`/opt/airflow/dags:/opt/airflow`); `faker==40.37.0` added to the Dockerfile's
existing constrained `pip install` line.
**Addresses:** the "supporting environment fixes" line item in PROJECT.md's Current Milestone scope.
**Avoids:** Pitfall 2 (permission fix only ever covering reads), Pitfall 3 (bind-mount type/inode
caching gotcha — verify with a full `down && up`, not `restart`), Pitfall 4/5 (stale image after a
Dockerfile edit; `faker` in the wrong `pip install` stanza).

### Phase 2: `csv_generate_schedule` Orchestrator DAG
**Rationale:** Can only be meaningfully implemented and verified once Phase 1's environment fixes
are proven — the DAG's own generate task calls directly into the now-working container capability.
**Delivers:** the new `airflow/dags/csv_generate_schedule.py` DAG (`schedule="@hourly"`,
`catchup=False`, `max_active_runs=1`), an in-process `generate_task` with a per-run-varying `--seed`,
three sequential `TriggerDagRunOperator` tasks (customers -> orders -> `report_ready`) with the
`deferrable` question above resolved explicitly before implementation, `fail_when_dag_is_paused=True`,
`trigger_run_id` policy decided per the Open Decision above.
**Addresses:** the milestone's core stated goal — hourly, unattended automation with no manual
`make generate` step; zero changes to `csv_ingest.py`/`report_ready.py`.
**Avoids:** Pitfall 1 (fixed-seed checksum collision — the most dangerous, silent-failure pitfall in
this milestone), Pitfall 7 (`run_id`/worker-slot behavior — resolved by the Open Decision), Pitfall 8
(no `max_active_runs` cap allowing overlapping hourly chains).

### Phase 3: `OraclePartitionReadyTrigger` Robustness Fix
**Rationale:** Independent of Phases 1-2 (different file, different failure mode); can land in
parallel with either, included in this milestone because it is the explicit CR-01 follow-up already
scoped in PROJECT.md's Current Milestone.
**Delivers:** `connect_async()` moved inside its own `try` block; `except oracledb.OperationalError`
specifically (not the broader `DatabaseError`/`Error`) for the retry-worthy path; an independently
guarded `finally: connection.close()`; a bounded retry count/elapsed-time cap so an extended real
Oracle outage eventually surfaces rather than sitting `deferred` forever.
**Addresses:** the "robustness fix" line item in PROJECT.md's Current Milestone scope (Phase 7
code-review Critical finding).
**Avoids:** Pitfall 9 (naive exception handling missing the real crash point or swallowing genuine
bugs), Pitfall 10 (`finally`-block close failures masking the original exception).

### Phase Ordering Rationale

- Environment fixes come first because they are a hard, verifiable-in-isolation prerequisite — the
  DAG literally cannot run generation successfully until the container can write to `data/` and
  import/execute the generator, and ARCHITECTURE.md's own "Build Order" section treats this as
  strict, not stylistic.
- The orchestrator DAG comes second because it is the milestone's actual deliverable and the point
  where the Open Decision (deferrable) must be settled — sequencing it after the environment is
  proven isolates DAG-authoring bugs from environment bugs during verification.
- The trigger-robustness fix is independent and can be parallelized with either phase — it touches
  a different file (`_common/oracle_partition_trigger.py`) with no dependency on the new DAG or the
  environment changes.

### Research Flags

Phases likely needing an explicit decision during requirements/planning (not necessarily more
external research, but a deliberate choice this research could not make unilaterally):
- **Phase 2 (`csv_generate_schedule` DAG):** the `deferrable=True` vs. `deferrable=False` split
  above is the primary open item — resolve explicitly before writing the three
  `TriggerDagRunOperator` tasks. Secondarily, decide the `trigger_run_id` policy (auto-generated
  vs. explicit parent-derived) consistent with whichever `deferrable` choice is made.

Phases with standard, well-documented patterns (research-phase likely unnecessary):
- **Phase 1 (Environment & Docker Fixes):** the `airflow-init` chown pattern is lifted directly
  from Airflow's own official quick-start compose file; the `faker`/constraints-file interaction
  was verified via a live fetch, not inferred.
- **Phase 3 (Trigger Robustness Fix):** the exact `oracledb` exception hierarchy was verified live
  against the pinned `oracledb==4.0.2` in a throwaway venv — the fix shape is fully specified, not
  exploratory.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | No new frameworks; the one new dependency (`faker==40.37.0`) was cross-checked against this repo's own `pyproject.toml`/`uv.lock` and a live fetch of Airflow's pinned constraints file. Only the `deferrable` behavior of `TriggerDagRunOperator` is MEDIUM (WebSearch-sourced open GitHub issues, not independently reproduced at this exact pinned version). |
| Features | HIGH | Verified directly against the installed `apache-airflow-providers-standard==1.17.0` source for operator mechanics; MEDIUM only on open-bug applicability across provider versions (some issues filed against older versions, fix-landing version not independently confirmed). |
| Architecture | HIGH for compose/Dockerfile/permission mechanics (verified against this repo's real files plus Airflow's own official docker-compose.yaml) and for `TriggerDagRunOperator` worker-slot semantics (verified against Airflow's own source); MEDIUM only on the exact `faker` pip-install placement being untested at actual build time (though STACK.md's live constraints-file fetch substantially raises confidence here beyond ARCHITECTURE.md's own stated MEDIUM). |
| Pitfalls | HIGH — every pitfall verified against this repo's own committed source, its own recorded debug history (`.planning/debug/knowledge-base.md`, prior phase SUMMARY files), the actual pinned `trigger_dagrun.py` source, and the actual `oracledb==4.0.2` exception hierarchy imported live in a throwaway venv. |

**Overall confidence:** HIGH, with one explicitly unresolved design decision (deferrable mode) that
is a genuine judgment call, not a confidence gap in the research itself — both positions were
independently derived from verified, cited primary sources.

### Gaps to Address

- **`deferrable=True` vs. `deferrable=False` on the three `TriggerDagRunOperator` tasks:** the
  primary open item — see "Open Decision" above. Resolve explicitly during requirements/roadmap
  definition for Phase 2; do not let a phase-planning pass silently default to one side.
- **In-process import vs. subprocess invocation of `generator/generate_csv.py`:** three of the four
  research passes (STACK, ARCHITECTURE, FEATURES) converge on in-process import
  (`from generator.generate_csv import ...`), enabled by the agreed-upon `PYTHONPATH` extension to
  `/opt/airflow`. PITFALLS.md's Pitfall 6 independently reasoned toward a `subprocess.run([...])`
  invocation instead, on the premise that `generator/` lacks an `__init__.py` and isn't on
  `PYTHONPATH` — a premise the other three passes' own recommended fix (the `PYTHONPATH` extension,
  treating `generator/` as a namespace package) directly addresses. This is a much lower-stakes
  disagreement than the `deferrable` split (3-of-4 convergence, and the dissent's own premise is
  resolved by an already-agreed-upon fix), so it is noted here for completeness rather than
  elevated to an "Open Decision," but the DAG-implementation phase should confirm the namespace-
  package import actually resolves at build time before committing to it over subprocess.
- **`faker` pip-install stanza placement:** STACK.md's live-fetched-constraints-file finding
  (constrained line, zero conflict) supersedes PITFALLS.md's more cautious inference (Pitfall 5,
  unconstrained line) — treat STACK.md's live verification as authoritative, but still confirm with
  an actual `docker compose build` at implementation time before considering it settled, per
  ARCHITECTURE.md's own "verification step, not a certainty" framing.
- **Exact hourly runtime margin:** `csv_ingest`'s `FileSensor` timeout (3600s) is numerically equal
  to the new schedule period (hourly) — a real structural tightness flagged by FEATURES.md as
  worth awareness even though fixing it is out of this milestone's scope (would require modifying
  `csv_ingest.py`, which must stay unmodified). No action needed this milestone; revisit if a
  cycle is ever observed running close to the full hour.

## Sources

### Primary (HIGH confidence)
- This repository's own committed source, read directly across all four research passes:
  `docker-compose.yml`, `docker/airflow/Dockerfile`, `generator/generate_csv.py`,
  `airflow/dags/csv_ingest.py`, `airflow/dags/report_ready.py`,
  `airflow/dags/_common/oracle_partition_trigger.py`, `airflow/dags/_common/paths.py`,
  `packages/csv-processor/src/csv_processor/engine.py`, `pyproject.toml`, `uv.lock`,
  `docs/environment.md`, `Makefile`, `.planning/PROJECT.md`, `.planning/STATE.md`,
  `.planning/debug/knowledge-base.md`, prior milestone SUMMARY files
  (`05-02-SUMMARY.md`).
- `apache-airflow-providers-standard==1.17.0`'s actual `trigger_dagrun.py` source — downloaded and
  read directly (`pip download`), matching this project's exact pinned version.
- `oracledb==4.0.2`'s actual exception class hierarchy — imported directly in a throwaway venv,
  matching this project's exact pinned version.
- Live fetch: `curl https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt`
  — confirmed `faker` absent, `PyYAML==6.0.3` present.
- Apache Airflow's own official `airflow-core/docs/howto/docker-compose/docker-compose.yaml`
  (Context7/WebFetch) — the `airflow-init` `user: "0:0"` + `chown -R` pattern this research
  recommends adapting.
- Apache Airflow source via Context7: `providers/standard/.../trigger_dagrun.py` (wait/defer code
  path), `airflow-core/src/airflow/jobs/scheduler_job_runner.py` (`ConcurrencyMap.load`, confirming
  `DEFERRED` tasks don't count toward worker-slot accounting), `authoring-and-scheduling/deferring.rst`.

### Secondary (MEDIUM confidence)
- [apache/airflow#60049](https://github.com/apache/airflow/issues/60049),
  [#57756](https://github.com/apache/airflow/issues/57756),
  [#38353](https://github.com/apache/airflow/issues/38353),
  [#52247](https://github.com/apache/airflow/issues/52247) — open upstream GitHub issues on
  `TriggerDagRunOperator(deferrable=True)` behavior, central to the Open Decision above.
- WebSearch: `fail_when_dag_is_paused` default value and Airflow 3.2.0+ version gate.
- [python-oracledb Issue #234](https://github.com/oracle/python-oracledb/issues/234) — corroborates
  DPY-6005/DPY-4011-class errors surfacing as `OperationalError`, consistent with the live-verified
  exception hierarchy.

### Tertiary (LOW confidence)
- None flagged for this milestone — all four research passes anchored claims in this repo's own
  source or live-verified upstream source rather than inference/web-search-only synthesis, with the
  narrow exception of the `deferrable`-bug-applicability-across-provider-versions question already
  captured as MEDIUM above.

---
*Research completed: 2026-09-01*
*Ready for roadmap: yes — with one explicit open decision (TriggerDagRunOperator deferrable mode) requiring resolution during requirements/roadmap definition, not silent defaulting.*
