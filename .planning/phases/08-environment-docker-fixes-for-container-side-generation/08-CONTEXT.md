# Phase 8: Environment & Docker Fixes for Container-Side Generation - Context

**Gathered:** 2026-09-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Make the Airflow container able to (a) import and run `generator/generate_csv.py` in-process
(mount + `PYTHONPATH` + `faker` dependency) and (b) write generated CSVs into `data/<dataset>/` on
a genuinely fresh clone (via a compose-level fix, not a manual host-side step) — proven
independently, before Phase 9's `csv_generate_schedule` DAG depends on either capability. Also
covers a closely related, same-class Docker bind-mount gotcha this session already hit
(`docker/airflow/simple_auth_manager_passwords.json.generated` auto-created as a directory) and a
docs update. Does NOT include writing the `csv_generate_schedule` DAG itself (Phase 9) or the
`OraclePartitionReadyTrigger` exception-handling fix (Phase 10).

</domain>

<decisions>
## Implementation Decisions

### docs/environment.md rewrite

- **D-01:** Once the `airflow-init` chown fix lands, replace `docs/environment.md`'s "First-Clone
  Setup Gaps" step 2 (`mkdir -p data/customers data/orders`) and the "Known First-Boot Gotcha:
  Permission Error Creating `data/<dataset>/`" section — both become obsolete.
- **D-02:** Present **clean current-state only** — no historical note about the old manual
  workaround. User explicitly chose this over keeping a "this used to require X" breadcrumb.
- **D-03:** Briefly document the **new container capability** this phase adds — `generator/` is
  now mounted at `/opt/airflow/generator`, `faker` is installed, importable via extended
  `PYTHONPATH` — so a future developer understands why `generator/` is mounted and how Phase 9's
  DAG can generate files in-process without shelling out.

### Bundled fix: passwords-file bind-mount gotcha

- **D-04:** (scope addition beyond ENV-01/ENV-02's literal text — user-approved, non-default choice
  relative to REQUIREMENTS.md's exact wording) Phase 8 also fixes
  `docker/airflow/simple_auth_manager_passwords.json.generated` getting auto-created as a
  directory instead of a file (same root-cause class as ENV-02's `data/` problem: Docker
  auto-creating a missing bind-mount path wrong) — rather than leaving it as the documented manual
  `chmod 666` step. Same phase is already touching `airflow-init`/compose, so fixing both gotchas
  in one pass avoids a near-identical follow-up phase later.
- **D-05:** Implementation mechanism is the **same `airflow-init` compose-level pattern** as the
  `data/` chown fix (running once as uid 0, gated by the existing `depends_on:
  service_completed_successfully` chain) — ensures the passwords file exists as a real file with
  correct content/permissions before other services start. User explicitly chose this over a
  Makefile-level pre-flight step (which would only help `make up` callers, not raw `docker compose
  up`).

### Permanent automated verification

- **D-06:** Phase 8 adds a **permanent, committed check** (not a one-off manual verification
  during execution) proving the container can `import generator.generate_csv` and write into
  `data/customers/`/`data/orders/` — matches this project's established `make verify`/
  `verify-phaseN` discipline (every phase since Phase 2 has its own gate) and catches a future
  regression (e.g. someone removing the `generator/` mount) automatically.
- **D-07:** This check is added **into `scripts/verify_environment.py`** itself (the project's
  existing canonical "confirm the stack is genuinely alive" script), not a separate Makefile-only
  check. User explicitly accepted that this gives the script a new dependency shape it didn't have
  before — it currently only makes Oracle/HTTP network calls; this check needs a `docker compose
  exec` subprocess call to run code *inside* the container. Chosen to keep all environment
  verification in the one place a developer already knows to run (`make verify`), consistent with
  D-06's committed-regression-coverage rationale.
- **D-08:** The `data/` write-access check **actually writes then deletes a real probe file**
  inside `data/customers/`/`data/orders/` (with cleanup so it doesn't pollute the directory or
  confuse `csv_ingest`'s `FileSensor` glob pattern) — not just a permission-bits/ownership check.
  User explicitly chose the stronger proof, matching this project's established working preference
  ("don't trust exit codes as proof — confirm by actually doing/querying the real thing", already
  applied to Oracle DDL verification in Phase 1).

### Claude's Discretion

- Exact REQ-ID handling for D-04's passwords-file bundled fix (fold into ENV-02's scope during
  planning, or add a new `ENV-03`) — follow REQUIREMENTS.md's existing ID convention, planner's
  call, same pattern as Phase 7 D-30's "new REQ IDs during planning" precedent.
- Exact recursion/idempotency shape of the `airflow-init` chown step (chown the whole `data/` tree
  every `make up`, or only conditionally) — no user-facing behavioral difference either way, pick
  whichever is simplest and safely idempotent.
- Exact probe-file naming/location for D-08's write-then-delete check (e.g. a
  `.verify_write_probe` dotfile, chosen so it never matches `customers_*.csv*`/`orders_*.csv*`
  glob patterns) — implementation detail once the "actually write, don't just check bits" decision
  is locked.
- Exact wording/placement of D-03's new-capability doc note in `docs/environment.md` — implementation
  detail once the "document it briefly" decision is locked.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap (this phase's exact scope)
- `.planning/ROADMAP.md` §Phase 8 — goal, depends-on (Phase 7, v1.0 shipped), and the 3 literal
  success criteria.
- `.planning/REQUIREMENTS.md` — ENV-01, ENV-02 full text; D-04's bundled passwords-file fix extends
  this phase's scope beyond their literal wording (see Claude's Discretion on REQ-ID handling).

### Research (already resolved the deep technical questions — not open)
- `.planning/research/STACK.md` — `faker==40.37.0` exact pin, confirmed conflict-free with
  Airflow's `constraints-3.3.1` file, goes in the Dockerfile's existing constrained `pip install`
  line (not the separate unconstrained `clevercsv`/`chardet` line).
- `.planning/research/ARCHITECTURE.md` — exact mount path (`/opt/airflow/generator`, load-bearing
  because `generate_csv.py`'s own `_REPO_ROOT = Path(__file__).resolve().parent.parent` arithmetic
  must line up with the already-mounted `/opt/airflow/data`/`/opt/airflow/configs`); the
  `airflow-init` root-user `chown -R` pattern (mirrors Airflow's own official quick-start compose
  file) — this phase's D-05 extends the same pattern to the passwords file.
- `.planning/research/PITFALLS.md` — Pitfall 2 (the v1.0 permission fix only ever solved reads, not
  the writes this milestone newly needs), Pitfall 3 (the bind-mount-becomes-directory gotcha
  already happened once, documented in `05-02-SUMMARY.md`, needs a full `down && up` not
  `restart`), Pitfall 4/5 (stale image after a Dockerfile edit; verify the exact pip-install
  stanza).
- `.planning/research/SUMMARY.md` — "Phase 1: Environment & Docker Fixes" section, recommended
  build order (chown fix → verify via `make destroy && make up`; mount/PYTHONPATH/faker → verify
  via `make rebuild` + a manual import/exec check *before* wiring into a DAG).

### Existing docs this phase updates
- `docs/environment.md` — "First-Clone Setup Gaps" (both the `.env` and passwords-file manual-create
  steps — passwords manual step becomes automatic per D-04/D-05; the `.env` step is untouched,
  unrelated) and "Known First-Boot Gotcha: Permission Error on the Passwords File" /
  "...Creating `data/<dataset>/`" sections — both rewritten per D-01/D-02/D-03.

### Actual code/config this phase integrates with (more authoritative than research sketches)
- `docker-compose.yml` — `x-airflow-common` anchor: `user: "${AIRFLOW_UID:-50000}:0"`, existing
  volumes (`./docker/airflow/simple_auth_manager_passwords.json.generated:/opt/airflow/simple_auth_manager_passwords.json.generated`,
  `./data:/opt/airflow/data`, `./configs:/opt/airflow/configs:ro`), `airflow-init` service
  (`command: db migrate`, gates every other service via `depends_on:
  service_completed_successfully`) — this phase's target for the new `generator/` mount and the
  chown-based fixes (D-05).
- `docker/airflow/Dockerfile` — existing two-stage pip install (constrained line with
  `oracledb`/`pydantic`/the two providers; separate unconstrained line for
  `clevercsv`/`charset-normalizer`/`chardet`) — `faker==40.37.0` joins the constrained line per
  research.
- `generator/generate_csv.py` — `_REPO_ROOT`/`_CONFIGS_DIR`/`_DATA_DIR` path arithmetic (the exact
  mechanism the mount path must satisfy), `write_staged()` (the write path the container-side
  `data/` write-access check (D-08) must actually exercise/mirror).
- `scripts/verify_environment.py` — existing structure (`verify_tables()`/`verify_columns()`
  standalone functions, `verify_airflow_auth()`'s retry/backoff pattern for cold-start races) — the
  new import+write-access check (D-07) should follow this file's existing conventions (standalone,
  testable functions; clear `OK:`/`FAILED:` print output).
- `Makefile` — existing `verify`/`verify-phaseN` convention (`verify-phase2` through
  `verify-phase7`) — this phase's `verify-phase8` should follow the same shape (likely: unit suite,
  if any new unit-testable logic exists, plus the extended `scripts/verify_environment.py` run,
  requires `make up`/`make rebuild` first).
- `airflow/dags/_common/paths.py` — `DATA_ROOT = Path("/opt/airflow/data")` convention already
  established — any new path constants this phase introduces (e.g. for the generator mount) should
  follow the same explicit-`Path`-constant style.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/verify_environment.py`'s existing retry/backoff pattern (`AUTH_RETRY_ATTEMPTS`,
  `AUTH_RETRY_BASE_DELAY_SECONDS`) for a known cold-start race — the new container-exec check
  should reuse the same defensive discipline if it hits similar timing issues (container just
  started, not yet ready to exec into).
- `Makefile`'s `verify-phaseN` pattern and its own explicit invitation for later phases to add
  targets — direct template for `verify-phase8`.
- Airflow's own official quick-start `docker-compose.yaml`'s `airflow-init` `user: "0:0"` +
  `chown -R` pattern (found during v1.1 research) — the concrete template for both D-05's fixes.

### Established Patterns
- "Don't trust exit codes as proof — confirm by actually querying/doing the real thing" (PROJECT.md
  working preference, originally about Oracle DDL) — directly drives D-08's write-then-delete probe
  file choice.
- Every phase since Phase 2 has its own `verify-phaseN` Makefile gate — D-06 keeps this phase
  consistent with that established discipline rather than being the first phase to skip it.

### Integration Points
- The new `generator/` mount + extended `PYTHONPATH` is the single integration point Phase 9's
  `csv_generate_schedule` DAG will depend on entirely — Phase 9 should not need any further
  environment changes once this phase's success criteria hold.
- The `airflow-init` service already gates every other service's startup
  (`depends_on: service_completed_successfully`) — both D-05 fixes (data/ chown, passwords-file
  fix) hang off this existing gate, no new ordering wiring needed.

</code_context>

<specifics>
## Specific Ideas

- User's own framing when bundling the passwords-file fix: fix both Docker bind-mount gotchas in
  one pass "since the phase is already touching airflow-init/compose" rather than leaving one as a
  documented manual step and risking a near-identical follow-up phase later.
- User consistently chose the more rigorous option at each decision point in this discussion:
  a permanent automated check over a one-off manual verification (D-06), a real write-then-delete
  probe over a permission-bits check (D-08), and bundling the second gotcha fix rather than
  narrowly scoping to REQUIREMENTS.md's literal text (D-04) — all three echo this project's
  established "verify for real, don't assume" discipline already visible in PROJECT.md's Working
  Preferences.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within Phase 8's domain (environment/Docker fixes for container-side
generation). The passwords-file bundling (D-04) is a scope *expansion* within Phase 8 (same
root-cause class as ENV-02), not a new capability belonging to a different phase.

### Reviewed Todos (not folded)
None — `todo.match-phase 8` returned zero matches (`.planning/todos/pending/` doesn't exist yet).

</deferred>

---

*Phase: 8-Environment & Docker Fixes for Container-Side Generation*
*Context gathered: 2026-09-01*
