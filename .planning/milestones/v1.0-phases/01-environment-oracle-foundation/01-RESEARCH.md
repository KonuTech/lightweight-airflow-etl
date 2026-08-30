# Phase 1: Environment & Oracle Foundation - Research

**Researched:** 2026-08-28
**Domain:** Local docker-compose environment (Airflow LocalExecutor + Airflow metadata Postgres + Oracle Database Free), Oracle DDL/partitioning, Airflow 3 `simple_auth_manager` provisioning
**Confidence:** HIGH — the two items CONTEXT.md flagged as unresolved (Oracle INTERVAL partitioning support, `simple_auth_manager` non-interactive admin/admin provisioning) were resolved by **empirically running both pieces of software this session** (`docker pull` + `docker run` against `gvenzl/oracle-free:23.26.2-faststart` and `apache/airflow:3.3.1-python3.12`), not by documentation alone.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** `CUSTOMERS_VALID`/`CUSTOMERS_INVALID` mirror the reference repo's real
  `customers.yaml` column shape — `CUSTOMER_ID`, `NAME`, `COUNTRY`, `BIRTH_DATE`, `EVENT_TS`,
  `SIGNUP_COUNTRY` — with SCD-era metadata (`business_key`, `scd_type` annotations) stripped since
  this project has no SCD/CDC. `ORDERS_VALID`/`ORDERS_INVALID` mirror `orders.yaml`'s
  `ORDER_ID`, `CUSTOMER_ID`, `ORDER_DATE`, `AMOUNT`. `_INVALID` tables carry the same original
  columns plus `ERROR_CODE`, `ERROR_MESSAGE`, `SOURCE_FILE`, `ROW_NUMBER` per ENGINE-06.
  — **Reversibility:** costly.
- **D-02:** All identifiers (table and column names) are plain uppercase, unquoted
  (`CUSTOMERS_VALID`, `CUSTOMER_ID`) — Oracle's default folding behavior, not quoted lowercase.
  — **Reversibility:** costly.
- **D-03:** `CUSTOMERS_VALID`/`CUSTOMERS_INVALID`/`ORDERS_VALID`/`ORDERS_INVALID` use Oracle
  **INTERVAL partitioning** (daily) on an ingestion-date column — Oracle auto-creates each new
  day's partition on first insert, no manual partition-maintenance job needed.
  — **Reversibility:** one-way.
- **D-04:** `INGESTION_METADATA` has a `UNIQUE(dataset, checksum)` constraint — a DB-level
  idempotency guard against inserting the same file twice, in addition to (not instead of) the
  date-partition truncate/reload mechanism on the data tables.
  — **Reversibility:** reversible.
- **D-05:** Oracle-side schema DDL is committed to a verification script (e.g.
  `scripts/verify_environment.py`) using `python-oracledb`, asserting all 5 tables and their
  expected columns exist via `USER_TABLES`/`ALL_TAB_COLUMNS`. Reusable by Phase 4's Oracle
  integration tests (TEST-02). — **Reversibility:** reversible.
- **D-06:** Schema DDL is delivered as plain `.sql` files (`docker/oracle/init/*.sql`) mounted to
  Oracle Free's init-script directory, run once on first container boot — no migration tool.
  — **Reversibility:** reversible.
  **CORRECTION (this research pass):** the exact path in D-06's own text
  (`/container-entrypoint-initdb.d/startup`) does not exist as written — see Pitfall 1 below for
  the two real, verified directories.
- **D-07:** ETL tables live under a dedicated Oracle application schema/user (via
  `gvenzl/oracle-free`'s `APP_USER`/`APP_USER_PASSWORD` env vars), not the default `SYSTEM`
  schema — both set to `admin`/`admin`, keeping the single-credential-pair requirement (INFRA-03)
  intact while giving the app its own schema. — **Reversibility:** costly.
- **D-08:** Oracle's port 1521 is exposed to the host so external SQL clients (DBeaver, SQL
  Developer, `sqlplus` from WSL) can connect directly for debugging, separate from Airflow's own
  connection.
- **D-09:** `.env.example` is committed with placeholder `admin`/`admin` values; the real `.env`
  is gitignored. A new developer runs `cp .env.example .env` on first clone.
- **D-10:** Airflow's REST API/webserver auth uses Airflow 3's `simple_auth_manager` (token-based,
  file-backed) rather than the legacy FAB auth manager — no `airflow-init`/`airflow users create`
  step needed; `admin`/`admin` becomes the one local user Airflow generates.
  — **Reversibility:** costly.
- **D-11:** An Airflow Connection for Oracle is registered (via docker-compose's init step) in
  addition to the `.env` vars that `csv_processor` actually reads — for UI visibility only.
  `csv_processor` itself stays Airflow-agnostic and never touches the Connection object.
- **D-12:** Airflow's worker image is built from a small custom Dockerfile
  (`docker/airflow/Dockerfile`, `FROM apache/airflow:3.3.1-python3.12`) that pip-installs the
  pinned deps (`python-oracledb`, `pydantic`, etc.) and the local `csv_processor` package.
  Rejected: `_PIP_ADDITIONAL_REQUIREMENTS` (Airflow's own docs call it dev-only).
- **D-13:** Oracle and Airflow's metadata Postgres both use **persistent named Docker volumes**.
- **D-14:** A Makefile is established now as the project-wide standard command entrypoint —
  `make up`/`make down`/`make reset`/`make logs` today, with more targets added by later phases.
- **D-15:** `make down` stops containers only — volumes stay intact. A separate `make reset` runs
  `docker-compose down -v` for an explicit full wipe.
- **D-16:** Repo mirrors the reference repo's nested `packages/`/`src/` layout:
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
  — **Reversibility:** costly.
- **D-17:** CPU/RAM/disk requirements (INFRA-02) and `.wslconfig`/WSL networking guidance go into
  `docs/environment.md`, started in this phase.

### Claude's Discretion

Not explicitly delegated as a separate section in CONTEXT.md beyond what's noted inline above;
treat any DDL/compose detail not covered by a locked decision (e.g. exact partition transition-
point date, exact healthcheck intervals, exact Makefile target bodies) as open to research-backed
recommendation.

### Deferred Ideas (OUT OF SCOPE)

- **Customers↔Orders reporting join DAG task** — a new Airflow DAG task joining
  `CUSTOMERS_VALID`/`ORDERS_VALID` using "best PL/SQL reporting practices." Not part of this
  project's current ingest-only scope; cuts against the locked decision that
  `orders.customer_id → customers.customer_id` referential integrity is explicitly **not
  enforced**. Flagged for roadmap backlog review, not silently dropped.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| INFRA-01 | `docker-compose` stands up Airflow (LocalExecutor), Airflow's metadata DB, and a pinned Oracle Database Free image tag, runnable from WSL against Docker Desktop | Empirically confirmed `gvenzl/oracle-free:23.26.2-faststart` boots to ready in ~16s; confirmed `apache/airflow:3.3.1-python3.12` tag exists and boots under `LocalExecutor`; official quick-start compose needs active stripping of Celery/Redis/FAB, not just additive edits — see Architecture Patterns and Pitfall 3 |
| INFRA-02 | CPU/RAM/disk resource allocation for the environment is documented | Airflow's own official compose's `airflow-init` preflight thresholds (4 GB RAM / 2 CPU / 10 GB disk) read verbatim from the source file, this session — see Common Pitfalls #5 and `docs/environment.md` guidance below; add Oracle Free's own footprint on top |
| INFRA-03 | Oracle and Airflow credentials for local dev are managed consistently through one documented credential pair (`admin`/`admin`) via `.env`/docker-compose environment variables | Empirically confirmed both halves: (1) `APP_USER=admin`/`APP_USER_PASSWORD=admin` creates an Oracle schema user `ADMIN` reachable at `admin/admin@host:1521/FREEPDB1`; (2) pre-seeding `simple_auth_manager`'s passwords file with `{"admin": "admin"}` plus `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin` authenticates `admin`/`admin` against `POST /auth/token` and the resulting JWT authorizes `GET /api/v2/dags` — see Code Examples |
</phase_requirements>

## Summary

Both items CONTEXT.md flagged as needing this research pass to confirm before DDL/compose syntax
locks in were resolved with the strongest possible evidence: running the actual software. Oracle
Database Free 23.26.2-faststart **does** support `INTERVAL` partitioning — this was verified by
actually creating a table with `PARTITION BY RANGE (ingest_date) INTERVAL (NUMTODSINTERVAL(1,
'DAY'))`, inserting a row, and querying `USER_PART_TABLES`/`USER_TAB_PARTITIONS` to confirm Oracle
auto-created a new partition (`SYS_P700`) for the inserted date. Airflow 3.3.1's
`simple_auth_manager` **can** be provisioned non-interactively with a fixed `admin`/`admin`
credential (no `airflow users create` step) by pre-seeding its passwords file via a bind mount and
setting `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin` — this was verified end-to-end:
boot the API server, `POST /auth/token` with `admin`/`admin`, get a JWT, and use it to successfully
call `GET /api/v2/dags`.

A third, previously-unflagged risk surfaced during this verification and is now the most important
correction in this document: CONTEXT.md's own D-06 names a mount path
(`/container-entrypoint-initdb.d/startup`) that does not exist in `gvenzl/oracle-free`. Reading the
image's actual `container-entrypoint.sh` (this session) shows two distinct top-level directories —
`/container-entrypoint-initdb.d` (first boot only) and `/container-entrypoint-startdb.d` (every
boot) — and confirms every script in either one runs via `sqlplus -s / as sysdba`, which lands in
the **root container (`CDB$ROOT`)**, not the `FREEPDB1` pluggable database where `APP_USER`'s
schema lives. This was independently confirmed by connecting `/ as sysdba` inside the running
container and observing `CON_NAME = CDB$ROOT`. Init DDL must therefore explicitly run `ALTER
SESSION SET CONTAINER = FREEPDB1;` then `ALTER SESSION SET CURRENT_SCHEMA = ADMIN;` before any
`CREATE TABLE` — skipping either step silently creates the tables in the wrong place (or fails)
rather than raising an obvious "wrong schema" error.

**Primary recommendation:** Mount DDL into `docker/oracle/init/` → `/container-entrypoint-initdb.d`
(not `.../startup`), open every init `.sql` file with `ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;`, use the verified `PARTITION BY RANGE (ingested_at)
INTERVAL (NUMTODSINTERVAL(1,'DAY'))` syntax for all four data tables, and provision Airflow's
`admin`/`admin` credential by bind-mounting a pre-seeded `simple_auth_manager_passwords.json`
alongside `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin` — no `airflow-init` user-creation
step required.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Docker service topology (Airflow components, Oracle, volumes, networking) | Infra/Container (docker-compose) | — | Defines the entire runtime; no application code owns this |
| Airflow webserver/API authentication (`admin`/`admin`) | API/Backend (Airflow API server process, `simple_auth_manager`) | Infra/Container (`.env` supplies the credential value) | Auth-manager logic and the JWT issuance/verification live inside Airflow's own API server process, not in docker-compose |
| Oracle schema DDL (`_VALID`/`_INVALID`/`ingestion_metadata`, partitioning) | Database/Storage (Oracle Database Free, `FREEPDB1`/`ADMIN` schema) | — | Table structure, partitioning strategy, and constraints are pure database-tier concerns |
| Airflow DAG/task-run metadata | Database/Storage (Airflow's own Postgres, `postgres:16`) | — | Airflow's internal state store, opaque to this project's own code |
| Credential distribution (single `admin`/`admin` pair) | Infra/Container (`.env` + docker-compose `environment:` blocks) | Database/Storage + API/Backend (both consume the same value) | `.env` is the single source of truth; two different tiers (Oracle, Airflow API) each read and validate it independently |
| Oracle-facing Airflow Connection (D-11, UI visibility only) | API/Backend (Airflow Connection object, registered non-interactively) | Database/Storage (target it points at) | Exists purely for `airflow connections list`/`test` UX; `csv_processor` never reads it |

## Standard Stack

### Core

| Library/Image | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `apache/airflow` (Docker image) | `3.3.1-python3.12` | Base image for the custom Airflow Dockerfile (D-12) | `[VERIFIED: docker manifest inspect]` — tag exists on Docker Hub for both amd64/arm64, confirmed this session |
| `gvenzl/oracle-free` (Docker image) | `23.26.2-faststart` | Oracle Database Free container | `[VERIFIED: docker pull + docker run, this session]` — tag exists, pulls cleanly, boots to `DATABASE IS READY TO USE!` in **16 seconds** measured this session (matches project-level STACK.md's ~10-20s estimate, now confirmed rather than web-search-only) |
| `postgres` (Docker image) | `16` | Airflow's own metadata database | `[CITED: airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml]` — exact image the official Airflow 3.3.1 quick-start compose pins for its `postgres` service |
| `oracledb` (python-oracledb) | 4.0.2 | Oracle connectivity for the `scripts/verify_environment.py` verification script (D-05) | Already pinned at project level (STACK.md); `[VERIFIED: pip index versions, this session]` — 39 published versions on PyPI, contradicting the package-legitimacy seam's "too-new" heuristic (see Package Legitimacy Audit) |
| Pydantic | 2.13.4 (2.13.5 now latest) | Not directly used by Phase 1 code, but installed into the Airflow image now (D-12) since later phases need it | `[VERIFIED: pip index versions, this session]` — 100+ published versions |

### Supporting

| Library/Image | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `apache-airflow-providers-standard` | 1.18.0 | Ships `FileSensor`, other core sensors used by later phases | Installed in the custom Airflow image now (D-12) so later phases don't need a rebuild; `[VERIFIED: pip index versions, this session]` |
| GNU Make | (system) | Project-wide command entrypoint (D-14) | `up`/`down`/`reset`/`logs` targets this phase, more added by later phases |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Airflow's official quick-start `docker-compose.yaml` used as-is | Hand-writing a minimal compose from scratch | The official file is `CeleryExecutor` + `redis` + `FabAuthManager` by default — using it as a *starting template* still requires deleting the `redis`/`airflow-worker`/`flower` services and rewriting `AIRFLOW__CORE__EXECUTOR`/`AIRFLOW__CORE__AUTH_MANAGER`, not just adding an Oracle service block. Recommend: start from the official file's `x-airflow-common` shape (env-var conventions, healthcheck commands, volume mounts) but treat the executor/auth-manager/redis/worker/flower parts as **replaced**, not extended. |
| `simple_auth_manager` with a pre-seeded passwords file | `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=true` (no login at all — everyone is auto-admin) | `ALL_ADMINS` mode is simpler (no password file, no login screen) but does not produce an `admin`/`admin` **credential pair** to hand to `curl`/DBeaver-style external tools or to satisfy INFRA-03's literal "authenticates against both Oracle and the Airflow webserver" wording — the pre-seeded-file approach is the one that actually produces a usable `admin`/`admin` login, so it's the recommended path, not `ALL_ADMINS`. |
| Plain `.sql` init scripts (D-06) | A migration tool (Alembic-style) | Already rejected in CONTEXT.md — schema isn't expected to evolve within v1. |

**Installation:**
```bash
# Airflow image build (docker/airflow/Dockerfile), pattern verified against the
# reference repo's own docker/airflow/Dockerfile (same convention: no USER root
# switch needed, base image already runs as non-root airflow:0)
FROM apache/airflow:3.3.1-python3.12
RUN pip install --no-cache-dir \
      "oracledb==4.0.2" \
      "pydantic==2.13.4" \
      "apache-airflow-providers-standard==1.18.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"
```

**Version verification:** `pip index versions oracledb` → `4.0.2` (39 releases); `pip index
versions pydantic` → `2.13.5` latest, `2.13.4` present (100+ releases); `pip index versions
apache-airflow` → `3.3.1` (100+ releases); `pip index versions apache-airflow-providers-standard`
→ `1.18.0` (35 releases). All four run live this session — `[VERIFIED: pip index versions, this
session]`.

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | Verdict | Disposition |
|---------|----------|-----|-----------|-------------|---------|-------------|
| `oracledb` | PyPI | 39 releases back to 1.0.0 `[VERIFIED: pip index versions]` | not queryable by the legitimacy seam (`unknown-downloads`) | `github.com/oracle/python-oracledb` | `SUS` (seam) → **Approved on override** | Seam flagged `unknown-downloads`; overridden with direct evidence — official Oracle-maintained driver, already pinned at project level |
| `pydantic` | PyPI | 100+ releases back to 0.0.1 `[VERIFIED: pip index versions]` | not queryable | `github.com/pydantic/pydantic` | `SUS` (seam, reasons: `too-new`, `unknown-downloads`) → **Approved on override** | "too-new" reads the *latest* release timestamp (2026-08-28), not the package's actual age — overridden with the full version-history evidence above |
| `apache-airflow` | PyPI | 100+ releases back to 1.8 `[VERIFIED: pip index versions]` | not queryable | `airflow.apache.org` | `SUS` (seam, reasons: `too-new`, `unknown-downloads`) → **Approved on override** | Same "too-new" false-positive pattern; this is the Apache Software Foundation's own flagship project and the entire subject of this codebase — already locked at project level |
| `apache-airflow-providers-standard` | PyPI | 35 releases back to 0.0.1 `[VERIFIED: pip index versions]` | not queryable | none listed by seam (`no-repository`) | `SUS` (seam, reasons: `too-new`, `unknown-downloads`, `no-repository`) → **Approved on override** | Official ASF sub-package of `apache-airflow` itself (same PyPI namespace/maintainer, published from the same monorepo); the seam's registry lookup doesn't surface a repo URL for provider sub-packages, not a legitimacy signal here |

**Packages removed due to `[SLOP]` verdict:** none.
**Packages flagged as suspicious `[SUS]`:** all four above were mechanically flagged `SUS` by the
package-legitimacy seam, but every flag traces to the seam's heuristic reading "most recent
release date" as "package age" plus an inability to query PyPI download counts — not to any actual
identity/trust signal. All four are foundational, multi-year, officially-maintained packages
already locked as pinned decisions in this project's `PROJECT.md`/`CLAUDE.md` before this phase
began. **Recommendation to planner:** no `checkpoint:human-verify` task is warranted for these
four specific packages given the direct version-history evidence above; if the planner prefers
maximum caution regardless, a single lightweight checkpoint before the `docker/airflow/Dockerfile`
`pip install` step is a reasonable belt-and-braces addition, but is not required by this research's
findings.

**Docker images** (not covered by the pip/PyPI legitimacy seam — evaluated manually):
- `gvenzl/oracle-free` — maintained by Gerald Venzl, an Oracle employee; built from Oracle's own
  published binaries (not reverse-engineered); the de facto community-standard Oracle Free image,
  already named directly in this project's own seed spec. `[VERIFIED: docker pull + docker run,
  this session]` — image pulls, boots, and its internal `/opt/oracle/healthcheck.sh` exists
  (confirmed via `docker run --entrypoint find`). No legitimacy concern.
- `apache/airflow` — official image published by the Apache Software Foundation itself.
  `[VERIFIED: docker manifest inspect, this session]`. No legitimacy concern.

## Architecture Patterns

### System Architecture Diagram

```
Developer machine (WSL2 + Docker Desktop)
        │  `make up` → docker compose up
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│ docker-compose network                                                 │
│                                                                          │
│  ┌────────────┐     ┌──────────────────────────────────────────────┐  │
│  │ postgres:16│◄────┤ Airflow (LocalExecutor, custom image)         │  │
│  │ (metadata  │     │  api-server ──POST /auth/token──► JWT         │  │
│  │  DB, named │     │  scheduler                                     │  │
│  │  volume)   │     │  dag-processor                                 │  │
│  └────────────┘     │  triggerer                                     │  │
│                       │  simple_auth_manager reads pre-seeded          │  │
│                       │  simple_auth_manager_passwords.json.generated  │  │
│                       │  (mounted, contains {"admin":"admin"})         │  │
│                       └───────────────┬─────────────────────────────┘  │
│                                        │ oracledb (thin mode)            │
│                                        │ admin/admin @ 1521/FREEPDB1     │
│                                        ▼                                 │
│                       ┌──────────────────────────────────────────────┐ │
│                       │ gvenzl/oracle-free:23.26.2-faststart          │ │
│                       │  CDB$ROOT (SYS)                               │ │
│                       │   └─ FREEPDB1 (pluggable DB)                  │ │
│                       │       └─ ADMIN schema (APP_USER)              │ │
│                       │            CUSTOMERS_VALID / _INVALID         │ │
│                       │            ORDERS_VALID / _INVALID            │ │
│                       │            INGESTION_METADATA                 │ │
│                       │  (named volume, port 1521 exposed to host)    │ │
│                       └──────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────┘
        ▲
        │  DBeaver / sqlplus / curl (host, debugging) — port 1521 / 8080 exposed
```

Init DDL flow (first boot only):
```
container start → /container-entrypoint-initdb.d/*.sql executed as SYS in CDB$ROOT
  → ALTER SESSION SET CONTAINER = FREEPDB1
  → ALTER SESSION SET CURRENT_SCHEMA = ADMIN
  → CREATE TABLE ... PARTITION BY RANGE (ingested_at) INTERVAL (...)
  → (repeated for all 5 tables)
```

### Recommended Project Structure

Matches CONTEXT.md D-16 verbatim (already locked); this research adds no changes to it.

### Pattern 1: Custom Airflow image, never `_PIP_ADDITIONAL_REQUIREMENTS`

**What:** `docker/airflow/Dockerfile` is a single-stage `FROM apache/airflow:3.3.1-python3.12`
image with `RUN pip install --no-cache-dir ... --constraint <pinned-constraints-url>`, mirroring
the reference repo's own `docker/airflow/Dockerfile` shape: no `USER root` switch (the base image
already runs as the non-root `airflow` user, uid 50000/gid 0 — confirmed by the reference repo's
own comment and Airflow's documented "Customizing the image" pattern), `--chown=airflow:0` on any
`COPY`.

**Why:** Airflow's own docs call `_PIP_ADDITIONAL_REQUIREMENTS` "very bad and dangerous... useful
only when iterating and debugging" — it re-resolves from PyPI on every container start with no
lockfile, which both this project's determinism goals and the reference repo's own Dockerfile
comment explicitly reject.

**Example (adapted from the reference repo, Kubernetes/otel/dataplat-specific parts removed):**
```dockerfile
# docker/airflow/Dockerfile
FROM apache/airflow:3.3.1-python3.12

RUN pip install --no-cache-dir \
      "oracledb==4.0.2" \
      "pydantic==2.13.4" \
      "apache-airflow-providers-standard==1.18.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"

COPY --chown=airflow:0 packages/csv-processor/pyproject.toml packages/csv-processor/pyproject.toml
COPY --chown=airflow:0 packages/csv-processor/src packages/csv-processor/src
RUN pip install --no-cache-dir --no-deps packages/csv-processor/
```

### Pattern 2: Oracle init DDL must switch container AND schema explicitly

**What:** Every `docker/oracle/init/*.sql` file mounted to
`/container-entrypoint-initdb.d/` must begin:
```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;
```
before any `CREATE TABLE`.

**Why:** `[VERIFIED: docker exec sqlplus, this session]` — a bare `/ as sysdba` connection (the
exact command `gvenzl/oracle-free`'s own `container-entrypoint.sh` uses to run every init/startup
script: `echo "exit" | sqlplus -s / as sysdba @"${f}"`) lands in `CDB$ROOT` as `SYS`, not
`FREEPDB1`. Without the `ALTER SESSION` lines, DDL either lands in the wrong container (invisible
to the app, which connects to `FREEPDB1`) or under the `SYS` schema instead of `ADMIN`. This was
reproduced directly: `SHOW USER` returned `SYS` and `CON_NAME` returned `CDB$ROOT` on a bare
connection; after the two `ALTER SESSION` statements, a table created without further qualification
was confirmed via `all_tables` to belong to `OWNER = ADMIN`.

**When to use:** Every init `.sql` file, every time, with no exceptions — this is not conditional
on which table is being created.

### Pattern 3: INTERVAL partitioning on a dedicated ingestion-date column

**What:** All four data tables partition on a column dedicated to partitioning (not a business
column like `order_date`/`birth_date`, which are nullable per D-01's config-driven schema and
partition keys should not be) — an `INGESTED_AT` column populated at load time.

**Example — `[VERIFIED: docker exec sqlplus, this session]`, exact syntax that ran successfully:**
```sql
CREATE TABLE customers_valid (
  customer_id      VARCHAR2(64)  NOT NULL,
  name             VARCHAR2(255) NOT NULL,
  country          VARCHAR2(64)  NOT NULL,
  birth_date       DATE,
  event_ts         TIMESTAMP WITH TIME ZONE NOT NULL,
  signup_country   VARCHAR2(64),
  ingested_at      DATE          DEFAULT SYSDATE NOT NULL
)
PARTITION BY RANGE (ingested_at)
INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
( PARTITION p_initial VALUES LESS THAN (DATE '2020-01-01') );
```

Confirmed via query, this session:
```sql
SELECT table_name, partitioning_type, interval
  FROM user_part_tables WHERE table_name = 'TEST_INTERVAL';
-- TEST_INTERVAL | RANGE | NUMTODSINTERVAL(1,'DAY')

SELECT partition_name, high_value
  FROM user_tab_partitions WHERE table_name = 'TEST_INTERVAL';
-- P_INITIAL | TO_DATE(' 2026-01-01 00:00:00', ...)
-- SYS_P700  | TO_DATE(' 2026-08-29 00:00:00', ...)   ← auto-created on insert, exactly as D-03 expects
```

**Why:** Confirms D-03 (INTERVAL partitioning, daily) is fully supported in Oracle Database Free
23ai/23.26.2 — this corrects the historical assumption (Oracle XE lacked partitioning) that
STATE.md itself carried forward as an open risk. `[VERIFIED]` — Oracle documentation
`[CITED: docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/CREATE-TABLE.html]` gives the
identical syntax shape independently; the empirical run confirms it against the exact pinned image.

**When to use:** All four `_VALID`/`_INVALID` tables per D-03. `INGESTION_METADATA` (the 5th table)
is not partitioned per CONTEXT.md's decisions (only D-03 names the four data tables).

**Pitfall inside this pattern:** `PARTITION BY RANGE (...) INTERVAL (...)` requires **at least one**
named initial partition (`PARTITION p_initial VALUES LESS THAN (...)`) — a bare interval clause
with zero partitions raises an error. Pick a transition-point date safely in the past (e.g. `DATE
'2020-01-01'`, well before any real ingestion) so every real inserted row falls into an
auto-created interval partition, never into `p_initial` itself.

### Pattern 4: `simple_auth_manager` non-interactive `admin`/`admin` provisioning

**What:** Enable the auth manager, declare the user, and **pre-seed its password file** rather than
letting Airflow auto-generate a random password.

**Example — `[VERIFIED: docker run apache/airflow:3.3.1-python3.12, this session]`, end-to-end:**
```yaml
# docker-compose.yml (airflow-common environment block)
environment:
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  AIRFLOW__CORE__AUTH_MANAGER: airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
  AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "admin:admin"   # username:role pairs
volumes:
  - ./docker/airflow/simple_auth_manager_passwords.json.generated:/opt/airflow/simple_auth_manager_passwords.json.generated
```
```json
// docker/airflow/simple_auth_manager_passwords.json.generated (committed alongside .env.example
// per D-09's spirit — this is the throwaway local-dev credential, not a secret)
{"admin": "admin"}
```

Confirmed this session: after `airflow db migrate && airflow api-server`, the bind-mounted file's
contents were **unchanged** post-boot (Airflow did not overwrite the existing `admin` entry with a
freshly generated password), and:
```bash
curl -s -X POST http://localhost:8080/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}'
# → {"access_token": "eyJ...", ...}

curl -s -X GET http://localhost:8080/api/v2/dags \
  -H "Authorization: Bearer <access_token>"
# → {"dags": [], "total_entries": 0}   (200 OK — token accepted)
```

**Why:** This is the exact non-interactive mechanism D-10 requires ("no `airflow-init`/`airflow
users create` step needed; `admin`/`admin` becomes the one local user Airflow generates") — the
official docs only describe the auto-generate-and-print-to-logs behavior
`[CITED: github.com/apache/airflow/blob/main/airflow-core/docs/core-concepts/auth-manager/simple/index.rst]`;
whether pre-seeding the file suppresses regeneration was **not stated in the docs** and had to be
tested directly.

**When to use:** In `docker/airflow/`, alongside the Dockerfile, gitignored the same way `.env` is
(D-09) even though it's a throwaway local value — treat it with the same discipline. Default file
location is `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`
(`$AIRFLOW_HOME` is `/opt/airflow` in the stock image) — the bind-mount target path must match this
exactly or the pre-seed silently has no effect and Airflow falls back to a random password,
breaking INFRA-03 without an obvious error (see Common Pitfall #4).

### Pattern 5: Oracle Connection registered via `AIRFLOW_CONN_*` env var (D-11)

**Example — `[CITED: github.com/apache/airflow/blob/main/providers/oracle/docs/connections/oracle.rst]`:**
```bash
# .env (D-09 pattern — placeholder in .env.example, real value gitignored)
AIRFLOW_CONN_ORACLE_DEFAULT='oracle://admin:admin@oracle:1521/?service_name=FREEPDB1&encoding=UTF-8&threaded=False&events=False'
```
Airflow reads any `AIRFLOW_CONN_{CONN_ID}` environment variable (uppercase conn id) in URI or JSON
form and registers it as a Connection automatically — no CLI call, no init container, no
`connections.yaml` needed. `[CITED: github.com/apache/airflow/blob/main/airflow-core/docs/howto/connection.rst]`.
This satisfies D-11 with zero additional moving parts: docker-compose's `env_file: - .env` already
propagates it to every Airflow service.

### Anti-Patterns to Avoid

- **Copying the official Airflow quick-start compose verbatim and only adding an Oracle block:**
  the official file's `x-airflow-common` block hardcodes `AIRFLOW__CORE__EXECUTOR: CeleryExecutor`
  and `AIRFLOW__CORE__AUTH_MANAGER: ...FabAuthManager`, plus a `redis` service, `airflow-worker`,
  and `flower` — all four must be actively removed/replaced, not left in place alongside the
  Oracle addition. `[VERIFIED: webfetch of docker-compose.yaml for 3.3.1, this session]`.
- **Mounting DDL to `/container-entrypoint-initdb.d/startup`:** this path doesn't exist as a
  distinct thing — see Pitfall 1.
- **Relying on `_AIRFLOW_WWW_USER_CREATE`/`_AIRFLOW_WWW_USER_USERNAME` env vars (from the official
  compose's `airflow-init` service):** these are FAB-auth-manager-specific and have no effect under
  `simple_auth_manager` — use Pattern 4 instead.
- **Setting `ORACLE_PASSWORD` (the SYS/SYSTEM password) to a different value than
  `APP_USER_PASSWORD`:** technically two separate accounts, but for INFRA-03's "not scattered
  inline or hardcoded differently" spirit and to keep DBeaver/`sqlplus` debugging (D-08) simple,
  set both to `admin` in `.env` — document explicitly that these are two different Oracle accounts
  sharing one literal value, not one account.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Daily partition maintenance (creating tomorrow's partition before it's needed) | A cron job / Airflow DAG task that runs `ALTER TABLE ... ADD PARTITION` | Oracle's native `INTERVAL` partitioning (Pattern 3) | Oracle auto-creates the partition transparently on first insert into a new date range — a maintenance job would be redundant machinery duplicating what the database already does natively, verified working this session |
| Airflow user/password management | A custom auth backend, or scripting `airflow users create` at container startup | `simple_auth_manager` + pre-seeded passwords file (Pattern 4) | This is Airflow 3's own built-in, purpose-built dev/test auth manager; scripting FAB's `users create` CLI is what D-10 explicitly rejects |
| Oracle readiness detection | A custom polling script (`sqlplus`-in-a-loop) for docker-compose healthchecks | `gvenzl/oracle-free`'s own bundled `/opt/oracle/healthcheck.sh` | `[VERIFIED: docker run --entrypoint find, this session]` — the script exists inside the image and is the image maintainer's own purpose-built readiness check; reference it directly in `healthcheck.test` rather than reimplementing |
| Schema migration tooling | Alembic or a hand-rolled versioned-migration runner | Plain `.sql` files in `docker/oracle/init/` (D-06, already locked) | Schema isn't expected to evolve within v1; a migration tool is unjustified machinery for a fixed schema |

**Key insight:** Every "don't hand-roll" item in this phase has an off-the-shelf mechanism already
built into either Oracle itself or the exact Docker images this project pins — the temptation in
an infrastructure-provisioning phase is to script around a perceived gap that, on closer (and in
this case, empirical) inspection, doesn't actually exist.

## Common Pitfalls

### Pitfall 1: CONTEXT.md's own D-06 names a non-existent mount path

**What goes wrong:** D-06 says DDL is "mounted to Oracle Free's init-script directory
(`/container-entrypoint-initdb.d/startup`)." This path does not exist in `gvenzl/oracle-free` as a
single directory.

**Why it happens:** The image actually exposes **two distinct top-level directories** —
`/container-entrypoint-initdb.d` (run once, first boot only) and `/container-entrypoint-startdb.d`
(run on **every** container start) — `[VERIFIED: read container-entrypoint.sh source, this
session]`:
```
run_custom_scripts /container-entrypoint-initdb.d     # first boot only
run_custom_scripts /docker-entrypoint-initdb.d         # (backwards-compat alias)
run_custom_scripts /container-entrypoint-startdb.d     # every boot
run_custom_scripts /docker-entrypoint-startdb.d        # (backwards-compat alias)
```
D-06's `/startup` suffix appears to conflate the two directory *names* (`initdb` vs `startdb`) into
one imagined path.

**How to avoid:** Mount `docker/oracle/init/*.sql` to `/container-entrypoint-initdb.d` (schema
DDL — must run exactly once, on the volume's first initialization). Never mount anything meant to
run once into `/container-entrypoint-startdb.d`, since `CREATE TABLE` would fail with "already
exists" on every subsequent container restart if it landed there by mistake.

**Warning signs:** `CREATE TABLE ... already exists` errors after a `docker-compose restart` (DDL
mounted to the wrong directory and re-running every boot); or tables silently missing after first
boot (DDL never ran at all because the mount path was misspelled and matched neither directory).

**Phase to address:** This phase — get the mount path right in the first commit of
`docker-compose.yml`, since D-05's verification script depends on the tables actually existing.

---

### Pitfall 2: Init scripts run as `SYS` in `CDB$ROOT`, not as `ADMIN` in `FREEPDB1`

**What goes wrong:** DDL written assuming it runs "as the app user, in the app database" silently
lands in the wrong container/schema instead.

**Why it happens:** `gvenzl/oracle-free`'s entrypoint runs every init script via `echo "exit" |
sqlplus -s / as sysdba @"${f}"` — a bequeath (`/`) connection using the container's `ORACLE_SID`
(confirmed `FREE` this session), which connects to the multitenant container database's root
(`CDB$ROOT`) as `SYS`, not to the `FREEPDB1` pluggable database. `[VERIFIED: docker exec sqlplus,
this session — `SHOW USER` returned `SYS`, `show con_name` returned `CDB$ROOT` on the exact same
connection command the entrypoint uses]`.

**How to avoid:** Every init `.sql` file's first two statements must be `ALTER SESSION SET
CONTAINER = FREEPDB1;` then `ALTER SESSION SET CURRENT_SCHEMA = ADMIN;` — see Pattern 2. Verified
this session: without these, a `CREATE TABLE` from the bare connection succeeds but creates the
table under `SYS` in `CDB$ROOT`, invisible to any application connecting to `FREEPDB1`.

**Warning signs:** D-05's verification script reports tables missing from `USER_TABLES` even though
the init script's `CREATE TABLE` "succeeded" with no error during container startup — this is
exactly the silent-wrong-schema failure mode, not a crash.

**Phase to address:** This phase.

---

### Pitfall 3: The official Airflow quick-start compose is CeleryExecutor + FAB by default

**What goes wrong:** Starting from `https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml`
(a reasonable base per STACK.md's own recommendation) and only adding an Oracle service block
leaves `AIRFLOW__CORE__EXECUTOR: CeleryExecutor`, a `redis` service, `airflow-worker`, and `flower`
in place, and `AIRFLOW__CORE__AUTH_MANAGER` pointed at FAB — directly contradicting INFRA-01
(LocalExecutor) and D-10 (`simple_auth_manager`).

**Why it happens:** `[VERIFIED: webfetch of the exact 3.3.1 compose file, this session]` — the
file's `x-airflow-common` anchor block hardcodes both values; a naive "add my service, keep
everything else" edit preserves them unnoticed.

**How to avoid:** Explicitly delete the `redis`, `airflow-worker`, and `flower` service blocks;
change `AIRFLOW__CORE__EXECUTOR` to `LocalExecutor`; replace `AIRFLOW__CORE__AUTH_MANAGER`'s FAB
class path with the `simple_auth_manager` one (Pattern 4); delete the Celery-specific env vars
(`AIRFLOW__CELERY__RESULT_BACKEND`, `AIRFLOW__CELERY__BROKER_URL`) and the `airflow-init` service's
FAB-specific `_AIRFLOW_WWW_USER_*` vars.

**Warning signs:** `docker-compose up` succeeds but a `redis` container is running that this
project has no use for; `airflow-worker` container present despite `LocalExecutor` being set
(LocalExecutor doesn't use a separate worker service at all).

**Phase to address:** This phase — review the first `docker-compose.yml` commit specifically for
leftover Celery/FAB artifacts before considering it done.

---

### Pitfall 4: Pre-seeded `simple_auth_manager` passwords file silently ignored if the mount path is wrong

**What goes wrong:** The bind-mount target must be exactly `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`
(`/opt/airflow/simple_auth_manager_passwords.json.generated` in the stock image, confirmed this
session). A typo, wrong `$AIRFLOW_HOME`, or wrong filename causes Airflow to fall back to its
default auto-generate-and-print-to-logs behavior — the container still boots fine, so there's no
obvious error, but the resulting password is a random string, not `admin`, breaking INFRA-03.

**Why it happens:** The default filename ends in `.generated`, which reads like an
output-only artifact, not something meant to be provided as input — easy to assume pre-seeding
"isn't really supported" and skip verifying it, when in fact (per Pattern 4) it works exactly as
provided.

**How to avoid:** After `docker-compose up`, always confirm `admin`/`admin` actually authenticates
(`curl -X POST .../auth/token -d '{"username":"admin","password":"admin"}'` should return a JWT,
not a 401) as part of this phase's own verification step — don't just check that the container
booted.

**Warning signs:** Airflow's startup logs contain a freshly generated password string for `admin`
(the auto-generate behavior logs it) — if that log line appears, the pre-seed didn't take effect.

**Phase to address:** This phase.

---

### Pitfall 5: Airflow's own documented minimum resources (from the quick-start's `airflow-init` preflight)

**What goes wrong:** INFRA-02 requires documenting CPU/RAM/disk, but a developer might document
"whatever felt fine" rather than a real threshold.

**Why it happens:** `[VERIFIED: read the exact 3.3.1 compose file's `airflow-init` service
inline script, this session]` — Airflow's own official compose already contains a built-in
preflight check with exact literal thresholds:
```bash
if (( mem_available < 4000 )) ; then   # < 4000 MB
if (( cpus_available < 2 )); then      # < 2 CPUs
if (( disk_available < one_meg * 10 )); then   # < 10 GB
```
These are Airflow's own stated minimums for its slice of the stack alone (Celery-based, i.e. a
superset of what this project's simpler LocalExecutor/no-Celery stack needs) — Oracle Database
Free adds its own separate footprint on top (Oracle's own docs generally recommend ≥2 GB RAM for
Oracle Free specifically; `gvenzl/oracle-free`'s README recommends similar).

**How to avoid:** `docs/environment.md` (D-17) should state a combined minimum — reusing Airflow's
own 4 GB/2 CPU/10 GB figures as the Airflow-side floor, and adding Oracle Free's own documented
footprint on top, rather than inventing a number from scratch. Actual practical minimum should be
verified by running the full stack (Airflow + Oracle) together and observing `docker stats`, not
just summing documented minimums, since INFRA-02 explicitly requires the number to "match what
running it in practice requires."

**Phase to address:** This phase — the `docs/environment.md` numbers should be checked against a
real `docker-compose up` of the full stack before being written down as final.

---

### Pitfall 6 (carried forward from project-level PITFALLS.md, still applies): WSL2/Docker Desktop memory and Oracle readiness

Already documented in `.planning/research/PITFALLS.md` (`.wslconfig` sizing, IPv4/mirrored-
networking caveat, Oracle readiness healthchecks). This research adds one concrete number to that
existing guidance: Oracle Free's own boot-to-ready time on `-faststart` is **16 seconds**
(measured this session, matching PITFALLS.md's estimate) — use this as the `start_period` floor for
the Oracle service's docker-compose healthcheck, with margin for a slower host.

## Code Examples

### Docker-compose healthcheck for Oracle (uses the image's own bundled script)

```yaml
# docker-compose.yml
services:
  oracle:
    image: gvenzl/oracle-free:23.26.2-faststart
    environment:
      ORACLE_PASSWORD: ${ORACLE_PASSWORD:-admin}
      APP_USER: ${ORACLE_APP_USER:-admin}
      APP_USER_PASSWORD: ${ORACLE_APP_USER_PASSWORD:-admin}
    ports:
      - "1521:1521"   # D-08: expose for external SQL clients
    volumes:
      - oracle-data:/opt/oracle/oradata
      - ./docker/oracle/init:/container-entrypoint-initdb.d
    healthcheck:
      test: ["CMD", "/opt/oracle/healthcheck.sh"]   # [VERIFIED: script exists in image, this session]
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 30s   # measured boot time was 16s; margin for slower hosts
```

### Verification script pattern (D-05), targeting `FREEPDB1`

```python
# scripts/verify_environment.py
import oracledb

conn = oracledb.connect(
    user="admin", password="admin",
    dsn="localhost:1521/FREEPDB1",   # note: service_name, not SID
)
cursor = conn.cursor()
cursor.execute("""
    SELECT table_name FROM user_tables
    WHERE table_name IN (
      'CUSTOMERS_VALID', 'CUSTOMERS_INVALID',
      'ORDERS_VALID', 'ORDERS_INVALID', 'INGESTION_METADATA'
    )
""")
found = {row[0] for row in cursor.fetchall()}
expected = {
    "CUSTOMERS_VALID", "CUSTOMERS_INVALID",
    "ORDERS_VALID", "ORDERS_INVALID", "INGESTION_METADATA",
}
assert found == expected, f"Missing tables: {expected - found}"
```

### Makefile skeleton (D-14/D-15 — simple docker-compose lifecycle, not the reference repo's Kubernetes-heavy targets)

```makefile
.PHONY: up down reset logs

up:              ## Start the full stack (Airflow + Oracle)
	docker compose up -d

down:             ## Stop containers only — volumes (D-13) stay intact
	docker compose down

reset:            ## Full wipe: stop containers AND remove volumes (D-15)
	docker compose down -v

logs:             ## Tail logs from every service
	docker compose logs -f
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| Oracle XE lacked table partitioning entirely (a real, historical limitation) | Oracle Database Free (23ai/23.26.2) supports `INTERVAL` partitioning fully — confirmed empirically this session | Oracle Database Free (formerly "XE") gained substantially more Enterprise-Edition-equivalent features over the 21c→23ai line | D-03's INTERVAL-partitioning decision is fully implementable on the pinned image — no fallback/workaround needed |
| Airflow 2.x: `FabAuthManager` was the only/default auth manager; REST API at `/api/v1/...` | Airflow 3.x: pluggable auth managers, `simple_auth_manager` is the new default for dev/test; REST API moved to `/api/v2/...` | Airflow 3.0 (2026) | D-10's choice of `simple_auth_manager` over FAB is the *new default*, not a niche opt-in — confirmed the exact API shape (`POST /auth/token` → JWT → `Authorization: Bearer`) this session |
| Airflow's stock quick-start compose defaults to `CeleryExecutor` + Redis | `LocalExecutor` needs no broker at all | N/A (quick-start intentionally demos the more complex setup) | Confirms STACK.md's recommendation to treat the quick-start as a template to prune, not a file to extend as-is |

**Deprecated/outdated:**
- FAB auth manager as the assumed default for new Airflow 3 projects — still supported, but
  `simple_auth_manager` is now Airflow's own documented recommendation for dev/test deployments.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `-faststart`'s fast boot behavior persists identically across `docker-compose down`/`up` cycles once a **persistent named volume** (D-13) already has datafiles from a prior run, not just on a genuinely fresh volume (this session's test used an ephemeral, unnamed volume) | Package Legitimacy Audit / Standard Stack (boot time) | Low — a restart from an already-initialized volume is normally *faster* than first boot (no datafile expansion needed at all), so this is very unlikely to be worse than the measured 16s; still worth a one-time check with the project's actual named volume before finalizing `start_period` |
| A2 | Setting `ORACLE_PASSWORD` (SYS/SYSTEM) to the same literal `admin` value as `APP_USER_PASSWORD` is the right call for INFRA-03's "single credential pair" intent, even though they are technically two different Oracle accounts | Anti-Patterns / Standard Stack | Low — this is a local-dev-only convenience recommendation, not a security-sensitive decision; if the planner disagrees, `.env.example` can document them as two separately-named (but same-valued) variables instead |
| A3 | 4 GB RAM / 2 CPU / 10 GB disk (Airflow's own preflight numbers) plus Oracle Free's separate documented footprint is a reasonable **starting point** for INFRA-02's documented minimum, before the phase's own real `docker stats` observation of the combined stack | Common Pitfall #5 | Medium — INFRA-02 explicitly requires the number to match real-world practice, not just summed vendor minimums; the planner should schedule an actual `docker stats` check against the finished compose file as a verification step, not treat this sum as final |

## Open Questions

1. **Exact combined resource footprint under WSL2/Docker Desktop with both Airflow and Oracle running simultaneously**
   - What we know: Airflow's own documented floor (4 GB/2 CPU/10 GB) and Oracle Free's general
     recommendation (~2 GB+) — see Assumption A3.
   - What's unclear: The actual combined peak, measured on this specific developer's WSL2/Docker
     Desktop setup, which INFRA-02 requires as the documented number.
   - Recommendation: The plan should include a verification task that runs `docker stats` against
     the finished stack and records the observed peak before finalizing `docs/environment.md`'s
     numbers — this is empirical work naturally scoped to the phase's own execution, not to this
     research pass.

2. **Whether `AIRFLOW_CONN_ORACLE_DEFAULT`'s exact DSN component names (`service_name=FREEPDB1` vs. a bare host/port) match what `python-oracledb`'s own connection code (used elsewhere by `csv_processor`, per D-11's note that `csv_processor` never touches the Connection object) expects**
   - What we know: The URI-format example from Airflow's own docs
     (`oracle://user:pass@host:port?params`) and that `FREEPDB1` is the correct service name
     (confirmed this session by successfully connecting to it).
   - What's unclear: Whether Airflow's Oracle provider parses `?service_name=FREEPDB1` correctly
     from the URI form, versus needing it in the `schema` position of the URI
     (`oracle://user:pass@host:port/FREEPDB1`) — the one official example found used a bare
     host:port with `sysdba` mode-related params, not a schema/service_name.
   - Recommendation: Register the Connection during this phase and verify with `airflow connections
     test oracle_default` before considering D-11 complete, rather than assuming the URI shape is
     correct from the docs snippet alone.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker Engine / Docker Desktop | Everything in this phase | ✓ | 29.7.2 (client) `[VERIFIED: docker --version, this session]` | — |
| `gvenzl/oracle-free:23.26.2-faststart` (image) | Oracle Database Free service | ✓ | pulled and booted successfully `[VERIFIED, this session]` | — |
| `apache/airflow:3.3.1-python3.12` (image) | Airflow custom image base | ✓ | pulled and booted successfully `[VERIFIED, this session]` | — |
| `pip` / PyPI reachability | Version verification (`pip index versions`) | ✓ | `[VERIFIED, this session]` | — |
| GNU Make | D-14's Makefile entrypoint | not directly probed this session (ubiquitous on Linux/WSL) | — | Document as a prerequisite in `docs/environment.md`; trivially installable if absent |

**Missing dependencies with no fallback:** none identified.
**Missing dependencies with fallback:** none identified — every dependency this phase needs was
directly confirmed available and working in this environment.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | `python-oracledb` + plain assertions inside `scripts/verify_environment.py` (per D-05) — no pytest framework exists yet in this greenfield repo |
| Config file | none yet — this phase is where the repo's first Python tooling is introduced |
| Quick run command | `python scripts/verify_environment.py` (once Oracle is up) |
| Full suite command | same — this phase has no larger test suite to run |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| INFRA-01 | `docker-compose up` brings all services to healthy | smoke | `docker compose up -d && docker compose ps --format json \| jq -e 'all(.Health == "healthy" or .State == "running")'` | ❌ Wave 0 |
| INFRA-02 | Documented resource numbers match real usage | manual-only (justification: requires human observation of `docker stats` on the actual dev machine over a real session, not scriptable as pass/fail) | — | n/a |
| INFRA-03 | `admin`/`admin` authenticates against both Oracle and Airflow | integration | `python scripts/verify_environment.py` (Oracle half) + `curl -X POST .../auth/token -d '{"username":"admin","password":"admin"}'` (Airflow half, should return 200 with a JWT) | ❌ Wave 0 |
| (Success Criterion 2) | 5 tables + expected columns exist, verified via `USER_TABLES`/`ALL_TAB_COLUMNS` | integration | `python scripts/verify_environment.py` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `docker compose up -d` + `python scripts/verify_environment.py` (fast, ~20s given the measured 16s Oracle boot time)
- **Per wave merge:** full manual walkthrough — `make reset && make up`, confirm every success criterion in ROADMAP.md §Phase 1 from a genuinely fresh state
- **Phase gate:** All 4 success criteria in ROADMAP.md's Phase 1 section must be independently demonstrated true before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `scripts/verify_environment.py` — covers D-05, INFRA-03 (Oracle half), Success Criterion 2
- [ ] No Python project scaffolding (`pyproject.toml`, `uv`) exists yet in this repo — needed before `scripts/verify_environment.py` can even import `oracledb`; this phase's plan should include creating minimal project scaffolding as a prerequisite task, not assume it already exists
- [ ] No pytest/test framework installed yet — acceptable for this phase (D-05 explicitly asks for a standalone verification script, not a pytest suite), but the planner should note that Phase 3+ will need to introduce pytest properly

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | yes | Airflow `simple_auth_manager` (JWT-based, Pattern 4); Oracle native user/password auth (`APP_USER`/`APP_USER_PASSWORD`) |
| V3 Session Management | yes | Airflow JWT `expires_in` (3600s, observed this session in the token response) — stateless, no server-side session store to manage |
| V4 Access Control | partial | `simple_auth_manager_users = "admin:admin"` grants the `admin` role — full RBAC is out of scope for a single-developer local-dev credential per INFRA-03's own intent |
| V5 Input Validation | n/a this phase | No user-facing input surface exists yet in Phase 1 (DDL and compose config only) — applies starting Phase 2 (`config.json`) |
| V6 Cryptography | n/a — local dev only | Credentials are plaintext local-dev throwaway values by explicit project decision (D-09); no TLS/encryption-at-rest requirement was scoped for this project (spec explicitly excludes Vault/production-grade secret management) |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Committing the real `.env` (with credentials) to git | Information Disclosure | `.env` gitignored, `.env.example` with placeholders committed instead (D-09, already locked) |
| `simple_auth_manager`'s own logged warning: "stores passwords in plaintext... prints generated passwords to stdout/logs" `[VERIFIED: observed in container logs, this session]` | Information Disclosure | Acceptable for this project's explicit local-dev-only scope; Airflow's own docs state `simple_auth_manager` "is intended for development and testing" — do not use this pattern if this project's scope ever expands beyond local dev |
| Oracle port 1521 exposed to host (D-08) widening the local attack surface beyond `localhost` if Docker Desktop's port binding defaults to `0.0.0.0` | Elevation of Privilege (network-adjacent) | Confirm docker-compose's port mapping binds to `127.0.0.1:1521:1521` rather than a bare `1521:1521` if the developer's LAN is untrusted — worth an explicit call-out in `docs/environment.md`, not assumed safe by default |

## Sources

### Primary (HIGH confidence — empirical, this session)
- `docker pull` + `docker run` against `gvenzl/oracle-free:23.26.2-faststart` — image existence, boot time (16s), `APP_USER`/`APP_USER_PASSWORD` schema behavior, `CDB$ROOT` vs `FREEPDB1` container context, `/opt/oracle/healthcheck.sh` existence
- `docker exec sqlplus` against the running Oracle container — `INTERVAL` partitioning DDL success, `USER_PART_TABLES`/`USER_TAB_PARTITIONS` confirmation, `SYS`/`CDB$ROOT` default connection context, `ADMIN`-schema table ownership after `ALTER SESSION`
- `docker pull` + `docker run` against `apache/airflow:3.3.1-python3.12` — image existence, `simple_auth_manager` end-to-end provisioning (pre-seeded passwords file, `POST /auth/token`, `GET /api/v2/dags` with the resulting JWT)
- `pip index versions` for `oracledb`, `pydantic`, `apache-airflow`, `apache-airflow-providers-standard` — version-history evidence overriding the package-legitimacy seam's `SUS` verdicts
- `docker manifest inspect` for both pinned image tags — confirms multi-arch manifest existence on the registry independent of any local pull

### Secondary (MEDIUM confidence — official docs, Context7/WebFetch)
- Context7 `/apache/airflow` — `simple_auth_manager` password-file behavior, `AIRFLOW_CONN_*` env var format, REST API `/auth/token` shape
- Context7 `/websites/oracle_en_database_oracle_oracle-database_26_sqlrf` — `PARTITION BY RANGE ... INTERVAL` syntax reference (independently confirms the empirically-tested syntax)
- WebFetch of `https://airflow.apache.org/docs/apache-airflow/3.3.1/docker-compose.yaml` — exact official quick-start service topology, executor/auth-manager defaults, `airflow-init` resource-preflight thresholds
- WebFetch of `github.com/gvenzl/oci-oracle-free/blob/main/container-entrypoint.sh` — exact init/startup directory paths and connection command

### Tertiary (LOW confidence)
- None used for load-bearing claims in this document — every claim that would otherwise be
  web-search-only was either empirically verified or cited to an official doc/source file this
  session.

## Metadata

**Confidence breakdown:**
- Oracle partitioning support/syntax: HIGH — empirically verified against the exact pinned image
- Airflow `simple_auth_manager` provisioning: HIGH — empirically verified end-to-end (boot → token → authorized API call)
- Init script schema/container context: HIGH — empirically verified, corrects a locked decision's own stated path
- Resource requirements (INFRA-02): MEDIUM — Airflow's own documented floor is HIGH-confidence, but the *combined* real-world number still needs an in-phase `docker stats` check (Open Question 1)
- Package legitimacy: MEDIUM — the automated seam's heuristic produced false positives, overridden with direct registry-history evidence; a human reviewing the override reasoning is a reasonable belt-and-braces step, not because the packages are actually suspect

**Research date:** 2026-08-28
**Valid until:** ~30 days for Airflow/Oracle image tag pins (re-verify if either project cuts a new patch release before this phase executes); the empirically-verified behavioral findings (partitioning syntax, auth-manager mechanism, init-script schema context) are stable Oracle/Airflow platform behavior, not expected to change on a 23.26.x/3.3.x patch bump.

---
*Phase 1 research completed: 2026-08-28*
