# Phase 1: Environment & Oracle Foundation - Context

**Gathered:** 2026-08-28
**Status:** Ready for planning

<domain>
## Phase Boundary

A developer can stand up the entire local stack from a fresh `git clone` — Airflow
(LocalExecutor), its metadata DB, and a schema-ready Oracle Database Free instance — with
documented resource requirements. Concretely: `docker-compose up` brings up all services healthy;
Oracle's `<DATASET>_VALID`/`<DATASET>_INVALID`/`ingestion_metadata` tables exist for both
`customers` and `orders`, verified by querying Oracle's own metadata/dictionary views (not just
DDL exit status); CPU/RAM/disk needs are documented; a single `admin`/`admin` credential pair
authenticates against both Oracle and the Airflow webserver.

Requirements: INFRA-01, INFRA-02, INFRA-03.

</domain>

<decisions>
## Implementation Decisions

### Oracle Target Schema

- **D-01:** `CUSTOMERS_VALID`/`CUSTOMERS_INVALID` mirror the reference repo's real
  `customers.yaml` column shape — `CUSTOMER_ID`, `NAME`, `COUNTRY`, `BIRTH_DATE`, `EVENT_TS`,
  `SIGNUP_COUNTRY` — with SCD-era metadata (`business_key`, `scd_type` annotations) stripped since
  this project has no SCD/CDC. `ORDERS_VALID`/`ORDERS_INVALID` mirror `orders.yaml`'s
  `ORDER_ID`, `CUSTOMER_ID`, `ORDER_DATE`, `AMOUNT`. `_INVALID` tables carry the same original
  columns plus `ERROR_CODE`, `ERROR_MESSAGE`, `SOURCE_FILE`, `ROW_NUMBER` per ENGINE-06.
  — **Reversibility:** costly — **rationale:** Phase 2's `config.json` column schema will be
  written to match these tables; changing columns later means touching config, DDL, and any rows
  already loaded.
- **D-02:** All identifiers (table and column names) are plain uppercase, unquoted
  (`CUSTOMERS_VALID`, `CUSTOMER_ID`) — Oracle's default folding behavior, not quoted lowercase.
  — **Reversibility:** costly — **rationale:** every SQL statement across every later phase
  depends on this; switching to quoted-lowercase later means touching every query.
- **D-03:** `CUSTOMERS_VALID`/`CUSTOMERS_INVALID`/`ORDERS_VALID`/`ORDERS_INVALID` use Oracle
  **INTERVAL partitioning** (daily) on an ingestion-date column — Oracle auto-creates each new
  day's partition on first insert, no manual partition-maintenance job needed.
  — **Reversibility:** one-way — **rationale:** converting a partitioned table's partitioning
  scheme after data exists requires a partition-exchange/rebuild migration, not a simple DDL edit.
- **D-04:** `INGESTION_METADATA` has a `UNIQUE(dataset, checksum)` constraint — a DB-level
  idempotency guard against inserting the same file twice, in addition to (not instead of) the
  date-partition truncate/reload mechanism on the data tables. Both coexist: the unique constraint
  prevents duplicate processing of the identical file; partition truncate/reload is a separate,
  explicit operation for intentionally reprocessing a whole day's data.
  — **Reversibility:** reversible — **rationale:** a constraint can be dropped/added without a
  data migration.
- **D-05:** Oracle-side schema DDL is committed to a verification script (e.g.
  `scripts/verify_environment.py`) using `python-oracledb`, asserting all 5 tables and their
  expected columns exist via `USER_TABLES`/`ALL_TAB_COLUMNS` — not just a one-time manual check.
  Reusable by Phase 4's Oracle integration tests (TEST-02).
  — **Reversibility:** reversible.
- **D-06:** Schema DDL is delivered as plain `.sql` files (`docker/oracle/init/*.sql`) mounted to
  Oracle Free's init-script directory (`/container-entrypoint-initdb.d/startup`), run once on
  first container boot — no migration tool (no Alembic). Matches the project's explicit exclusion
  of "complex schema registry" scope; this schema is not expected to evolve within v1.
  — **Reversibility:** reversible — **rationale:** a migration tool could be introduced later
  without touching the DDL content itself, only the delivery mechanism.

### Oracle Schema Ownership & Credentials

- **D-07:** ETL tables live under a dedicated Oracle application schema/user (via
  `gvenzl/oracle-free`'s `APP_USER`/`APP_USER_PASSWORD` env vars), not the default `SYSTEM`
  schema — both set to `admin`/`admin`, keeping the single-credential-pair requirement (INFRA-03)
  intact while giving the app its own schema.
  — **Reversibility:** costly — **rationale:** moving tables between schemas after DDL/data exist
  needs a re-create-and-copy migration.
- **D-08:** Oracle's port 1521 is exposed to the host (mapped in docker-compose) so external SQL
  clients (DBeaver, SQL Developer, `sqlplus` from WSL) can connect directly for debugging, separate
  from Airflow's own connection.
- **D-09:** `.env.example` is committed with placeholder `admin`/`admin` values; the real `.env`
  is gitignored. A new developer runs `cp .env.example .env` on first clone (documented in
  README). Applies even though the credential is a throwaway local-dev value — standard practice,
  keeps the door open if per-developer credentials are ever needed.
- **D-10:** Airflow's REST API/webserver auth uses Airflow 3's `simple_auth_manager` (token-based,
  file-backed) rather than the legacy FAB auth manager — no `airflow-init`/`airflow users create`
  step needed; `admin`/`admin` becomes the one local user Airflow generates.
  — **Reversibility:** costly — **rationale:** switching auth managers later changes how the HTTP
  trigger (DAG-02, Phase 5) authenticates — would need re-verifying the trigger flow.
- **D-11:** An Airflow Connection for Oracle is registered (via docker-compose's init step) in
  addition to the `.env` vars that `csv_processor` actually reads — for UI visibility
  (`airflow connections list`/`test`) only. `csv_processor` itself stays Airflow-agnostic
  (ENGINE-09) and never touches the Connection object; it only ever receives plain config/env
  values. User was explicit they don't mind the duplication for UI convenience.

### Airflow Image & Persistence

- **D-12:** Airflow's worker image is built from a small custom Dockerfile
  (`docker/airflow/Dockerfile`, `FROM apache/airflow:3.3.1-python3.12`) that pip-installs the
  pinned deps (`python-oracledb`, `pydantic`, etc.) and the local `csv_processor` package —
  mirrors the reference repo's `docker/airflow/Dockerfile` pattern minus its Kubernetes bits.
  Rejected: the quick-start's `_PIP_ADDITIONAL_REQUIREMENTS` env var (Airflow's own docs call it
  dev-only — reinstalls deps on every container start).
- **D-13:** Oracle and Airflow's metadata Postgres both use **persistent named Docker volumes** —
  survives `docker-compose down`/`up` cycles, avoiding re-paying Oracle's first-boot time on every
  restart, and preserves DAG-run history for debugging.

### Makefile & Lifecycle

- **D-14:** A Makefile is established now as the project-wide standard command entrypoint (not
  just Phase 1-scoped) — `make up`/`make down`/`make reset`/`make logs` today, with `make test`,
  `make lint`, `make verify`, `make benchmark` etc. added by the phases that introduce those
  capabilities (2-6). Later phases should add Makefile targets rather than inventing ad hoc script
  conventions.
- **D-15:** `make down` stops containers only — persistent volumes (D-13) stay intact. A separate
  `make reset` (or `make nuke`) target runs `docker-compose down -v` for an explicit full wipe when
  starting completely clean is actually wanted. `make down` never silently destroys data.

### Repository Layout

- **D-16:** Repo mirrors the reference repo's nested `packages/`/`src/` layout rather than a flat
  layout, even though this project has only one package:
  ```
  airflow/dags/
  packages/csv-processor/src/csv_processor/
  docker/airflow/Dockerfile
  docker/oracle/init/*.sql
  docker-compose.yml
  Makefile
  docs/environment.md
  .env.example
  ```
  — **Reversibility:** costly — **rationale:** every later phase's import paths, docker-compose
  volume mounts, and CI config reference these paths; moving files after Phase 2+ starts adding
  code is a repo-wide path rewrite.

### Documentation

- **D-17:** CPU/RAM/disk requirements (INFRA-02) and `.wslconfig`/WSL networking guidance
  (PITFALLS.md) go into a dedicated `docs/environment.md`, started in this phase — not just a
  README section. README links to it. Phase 6's doc-completion work extends this same file rather
  than creating it from scratch.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project-level requirements & decisions
- `.planning/PROJECT.md` — core value, two-tier reuse decision, pinned tech decisions
- `.planning/REQUIREMENTS.md` — INFRA-01/02/03 full text and traceability
- `.planning/ROADMAP.md` §Phase 1 — goal and success criteria for this phase

### Research (produced before this discussion, verified against live sources)
- `.planning/research/STACK.md` — pinned versions: `apache-airflow` 3.3.1,
  `apache-airflow-providers-standard` 1.18.0, Python 3.12, `oracledb` 4.0.2, Pydantic 2.13.4,
  `gvenzl/oracle-free:23.26.2-faststart`; docker-compose base = Airflow's official quick-start +
  `airflow-triggerer` service + one Oracle Free service block
- `.planning/research/PITFALLS.md` §"Docker Desktop + WSL2 lets the VM's memory grow unbounded" —
  `.wslconfig` sizing and IPv4/mirrored-networking guidance for `docs/environment.md`;
  §"Oracle Database Free container readiness" — healthcheck-gated startup ordering; §Security
  Mistakes — `.env`/gitignore credential handling
- `.planning/research/ARCHITECTURE.md` — `config.json` (not YAML) mirrors reference repo columns
  minus `quality:`/`scd:`/`retention:` blocks; confirms `csv_processor` has zero Airflow imports
- `.planning/research/SUMMARY.md` — phase-to-research mapping, confidence ratings
- `.planning/STATE.md` §Blockers/Concerns — `gvenzl/oracle-free:23.26.2-faststart` tag is
  LOW-confidence (web-search only); needs a one-time manual pull-and-boot check before this
  phase's docker-compose locks it in

### Reference repo (read-only — never imported, never a dependency)
- `/home/user/projects/airflow-platform/configs/datasets/customers.yaml` — real column shape for
  `CUSTOMERS_VALID`/`CUSTOMERS_INVALID` (D-01)
- `/home/user/projects/airflow-platform/configs/datasets/orders.yaml` — real column shape for
  `ORDERS_VALID`/`ORDERS_INVALID` (D-01)
- `/home/user/projects/airflow-platform/docker/airflow/Dockerfile` — pattern to adapt for this
  project's `docker/airflow/Dockerfile` (D-12), stripped of Kubernetes-specific layers
- `/home/user/projects/airflow-platform/Makefile` — pattern to adapt for this project's
  project-wide Makefile (D-14)

### Needs verification during research/planning (not yet confirmed)
- Oracle Database Free 23ai's support and exact syntax for INTERVAL partitioning (D-03) — confirm
  via Context7 `/oracle/python-oracledb` or Oracle's own docs before locking DDL syntax
- Airflow 3.3.1's `simple_auth_manager` exact configuration shape for docker-compose (D-10) —
  verify via Context7 `/apache/airflow`

</canonical_refs>

<code_context>
## Existing Code Insights

This is the first phase of a greenfield project — the repository currently contains only
`.planning/` and `.claude/`. No existing application code to reuse; all "reusable assets" for this
phase come from reading (not importing) the reference repo per the Tier A/B strategy in
`PROJECT.md`.

### Reusable Assets
- None in this repo yet — first phase.

### Established Patterns
- None yet — this phase establishes the repo layout (D-16) that all later phases build on.

### Integration Points
- Phase 2 (`config.json` contract) reads the column shapes locked in D-01 to define its schema.
- Phase 4 (Oracle bulk load) reads D-04 (unique constraint), D-03 (partitioning), and D-05
  (verification script pattern) when building `executemany()` loading + idempotency logic.
- Phase 5 (DAG wiring) reads D-10 (auth manager) and D-11 (Airflow Connection) when building the
  HTTP-triggerable DAG and its Oracle-facing tasks.
- Phase 6 (docs/CI) extends `docs/environment.md` (D-17) and the Makefile (D-14).

</code_context>

<specifics>
## Specific Ideas

- User explicitly wants the Makefile to become the standard command entrypoint for the whole
  project starting now, not just this phase (D-14) — later phases should add targets to it rather
  than inventing their own tooling conventions.
- User was explicit about wanting a clean "destroy and rebuild" story: `make down` must never
  silently wipe data given the persistent-volumes decision; a full wipe needs its own explicit
  target (D-15).
- User specifically asked for date-range partitioning with a truncate-and-reload reprocessing
  story on the data tables (D-03), on top of (not replacing) the checksum-based duplicate-file
  guard (D-04) — their own words: "The tasks in DAGs have to be idempotent."

</specifics>

<deferred>
## Deferred Ideas

- **Customers↔Orders reporting join DAG task** — a new Airflow DAG task joining
  `CUSTOMERS_VALID`/`ORDERS_VALID` (or transactions) using "best PL/SQL reporting practices," with
  partitioning and indexing applied appropriately. This is a new capability (reporting/analytics),
  not part of this project's current ingest-only scope, and cuts against PROJECT.md's locked
  decision that `orders.customer_id → customers.customer_id` referential integrity is explicitly
  **not enforced** here (spec §28). **User marked this as required for a future phase/milestone —
  not merely optional** — flag for roadmap backlog review, not silently dropped.

### Reviewed Todos (not folded)
None — no pending todos matched this phase (`todo.match-phase` returned 0 matches).

</deferred>

---

*Phase: 1-Environment & Oracle Foundation*
*Context gathered: 2026-08-28*
