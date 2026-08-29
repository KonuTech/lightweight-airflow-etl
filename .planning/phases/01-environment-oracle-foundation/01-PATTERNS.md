# Phase 1: Environment & Oracle Foundation - Pattern Map

**Mapped:** 2026-08-28
**Files analyzed:** 9
**Analogs found:** 5 / 9 (reference-repo analogs); 4 files have no analog anywhere (greenfield, first-of-kind)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|--------------------|------|-----------|-----------------|----------------|
| `docker/airflow/Dockerfile` | config | file-I/O (image build) | `/home/user/projects/airflow-platform/docker/airflow/Dockerfile` | role-match (pattern only — content is a superset with K8s/otel/dataplat concerns to strip) |
| `Makefile` | config | request-response (CLI commands) | `/home/user/projects/airflow-platform/Makefile` | role-match (convention only — actual targets are Kubernetes/Helm-specific, not reusable) |
| `docker/oracle/init/01_customers.sql` | migration | batch (DDL, run-once) | `/home/user/projects/airflow-platform/configs/datasets/customers.yaml` | partial (column-shape source only — YAML config, not DDL; no SQL analog exists anywhere) |
| `docker/oracle/init/02_orders.sql` | migration | batch (DDL, run-once) | `/home/user/projects/airflow-platform/configs/datasets/orders.yaml` | partial (column-shape source only — YAML config, not DDL) |
| `docker/oracle/init/03_ingestion_metadata.sql` | migration | batch (DDL, run-once) | none | no analog — new table with no reference-repo equivalent (its `meta.*` tables are Postgres-shaped and CDC/SCD-coupled) |
| `docker-compose.yml` | config | event-driven (container orchestration) | none (reference repo uses Kubernetes/Helm/kind, not docker-compose) | no analog — built from RESEARCH.md's own verified Code Examples |
| `scripts/verify_environment.py` | test | request-response (DB query + assert) | none | no analog — reference repo's equivalent (`tests/e2e/cluster`) is pytest+Kubernetes-coupled, not a portable pattern |
| `docs/environment.md` | config (docs) | n/a | none | no analog — reference repo's `docs/` is Kubernetes/Helm-specific |
| `.env.example` | config | n/a | none | no analog — trivial key=value file, no pattern needed beyond RESEARCH.md's own Code Examples |

## Pattern Assignments

### `docker/airflow/Dockerfile` (config, file-I/O)

**Analog:** `/home/user/projects/airflow-platform/docker/airflow/Dockerfile` (126 lines, read in full)

**Base image + no-USER-root pattern** (lines 31-45):
```dockerfile
ARG GIT_SHA=unknown
FROM apache/airflow:3.3.0-python3.12
ARG GIT_SHA

# No `--user` flag, no `USER root` switch: the upstream `apache/airflow` image is built to
# allow exactly this customization as the already-active, already-non-root `airflow` user
```
Apply as: `FROM apache/airflow:3.3.1-python3.12` (this project's pinned tag per STACK.md), no `USER root` anywhere — the base image already runs as non-root `airflow` (uid 50000, gid 0).

**Pinned, constrained pip install pattern** (lines 46-47):
```dockerfile
RUN pip install --no-cache-dir "apache-airflow[otel]==3.3.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt"
```
Apply as (per this phase's RESEARCH.md Pattern 1, already adapted):
```dockerfile
RUN pip install --no-cache-dir \
      "oracledb==4.0.2" \
      "pydantic==2.13.4" \
      "apache-airflow-providers-standard==1.18.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"
```
Never use `_PIP_ADDITIONAL_REQUIREMENTS` — both this analog's own header comment and this phase's RESEARCH.md (Pattern 1) reject it identically ("very bad and dangerous... useful only when iterating and debugging").

**Local package COPY + install pattern** (lines 115-117):
```dockerfile
COPY --chown=airflow:0 packages/dataplat/pyproject.toml packages/dataplat/pyproject.toml
COPY --chown=airflow:0 packages/dataplat/src packages/dataplat/src
RUN pip install --no-cache-dir --no-deps packages/dataplat/ && rm -rf packages/
```
Apply directly to this project's own local package, per D-16's repo layout and RESEARCH.md's own worked example:
```dockerfile
COPY --chown=airflow:0 packages/csv-processor/pyproject.toml packages/csv-processor/pyproject.toml
COPY --chown=airflow:0 packages/csv-processor/src packages/csv-processor/src
RUN pip install --no-cache-dir --no-deps packages/csv-processor/
```
Key convention carried over: **always `--chown=airflow:0` on every `COPY`** (base image's non-root ownership) and **always `--no-deps`** on the local-package install (avoid uncontrolled transitive-dependency drift against what the pinned `RUN pip install` layer above already fixed).

**What NOT to port from this analog:** the `psycopg[binary]` layer (lines 49-56, DAG-parse-time Postgres import — not applicable, this project has no `meta.files` inline-write pattern), the `dataplat` package COPY (lines 115-117 — reference-only for the *shape*, this project has no `dataplat`), and the multi-paragraph inline commentary about ADR-0004/two-image-two-dependency-sets — that entire concern is Kubernetes/multi-image specific and does not exist in this project's single-image setup. The `LABEL org.opencontainers.image.*` block (lines 123-125) is optional polish, not required by any locked decision in this phase's CONTEXT.md.

---

### `Makefile` (config, request-response/CLI)

**Analog:** `/home/user/projects/airflow-platform/Makefile` (822 lines — only scanned for structural convention via `grep`, not read in full; content itself is Kubernetes/Helm-specific and explicitly non-portable per the phase brief)

**Convention extracted** (from `grep -n` scan of target definitions, lines 56-432):
```makefile
.PHONY: help uv-guard install lock-check lint format typecheck imports test policy \
        ...
help:                          ## Show targets
uv-guard:                      ## Fail if the installed uv is not the pinned version
install: uv-guard              ## Create the venv from the lockfile
```
Pattern: every target has a trailing `## description` comment (used by a `help:` target to self-document, standard `grep '##'` Makefile-help idiom), `.PHONY` is declared once up front listing every non-file target, and dependency chaining is expressed via `target: prerequisite-target` (e.g., `install: uv-guard`).

**This phase's actual target bodies** — do not copy the reference repo's Kubernetes/kind/Helm content. Instead, use RESEARCH.md's own already-verified skeleton (Code Examples section, D-14/D-15):
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
Adopt the reference repo's `## description` comment convention on every target (for consistency with a project style that will scale to `make test`/`make lint`/`make verify` in later phases per D-14), and align indentation to tabs (Make requires literal tab, not spaces, before each recipe line — verify this when writing the file).

---

### `docker/oracle/init/01_customers.sql` and `02_orders.sql` (migration, batch DDL)

**Analog:** `/home/user/projects/airflow-platform/configs/datasets/customers.yaml` and `orders.yaml` (both read in full — column-shape source only, per D-01; these are YAML dataset configs, not SQL, so there is no DDL syntax to copy, only the column list/types to mirror)

**Column shape to mirror** (customers.yaml lines 161-194, stripped of `scd_type`/`business_key` annotations per D-01):
```yaml
columns:
  - name: customer_id   # string, not null  -> CUSTOMER_ID VARCHAR2(...) NOT NULL
  - name: name           # string, not null  -> NAME VARCHAR2(...) NOT NULL
  - name: country         # string, not null  -> COUNTRY VARCHAR2(...) NOT NULL
  - name: birth_date       # date, nullable    -> BIRTH_DATE DATE
  - name: event_ts          # timestamp, not null -> EVENT_TS TIMESTAMP WITH TIME ZONE NOT NULL
  - name: signup_country     # string, nullable  -> SIGNUP_COUNTRY VARCHAR2(...)
```
(orders.yaml lines 98-121, same treatment):
```yaml
columns:
  - name: order_id      # string, not null -> ORDER_ID VARCHAR2(...) NOT NULL
  - name: customer_id    # string, not null -> CUSTOMER_ID VARCHAR2(...) NOT NULL
  - name: order_date      # date, nullable   -> ORDER_DATE DATE
  - name: amount            # decimal, nullable -> AMOUNT NUMBER(...)
```

**Actual DDL syntax and session-setup pattern** — no analog exists anywhere (reference repo has no Oracle DDL, only Postgres migrations under `migrations/`). Use RESEARCH.md's own empirically-verified Pattern 2 + Pattern 3 code example verbatim as the template (RESEARCH.md lines 310-352, 338-352):
```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;

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
Apply this exact `ALTER SESSION` preamble + `PARTITION BY RANGE ... INTERVAL` shape to all four `_VALID`/`_INVALID` tables (customers/orders × valid/invalid). Per D-01, `_INVALID` tables carry the same original columns plus `ERROR_CODE`, `ERROR_MESSAGE`, `SOURCE_FILE`, `ROW_NUMBER`.

---

### `docker/oracle/init/03_ingestion_metadata.sql` (migration, batch DDL)

**No analog** — this table (checksum-based idempotency guard, D-04) has no reference-repo equivalent; the reference repo's `meta.*` tables live in Postgres and are coupled to its own CDC/SCD job-tracking model, not portable even as a shape reference. Build from RESEARCH.md's own D-04 description: `UNIQUE(dataset, checksum)` constraint, not partitioned (RESEARCH.md Pattern 3 "When to use" explicitly excludes this table from partitioning). Same `ALTER SESSION SET CONTAINER = FREEPDB1; ALTER SESSION SET CURRENT_SCHEMA = ADMIN;` preamble as the other init scripts (Pattern 2 applies uniformly, no exceptions, per RESEARCH.md's own wording).

---

### `docker-compose.yml` (config, event-driven orchestration)

**No analog** — the reference repo has no docker-compose file (it uses Kubernetes/kind/Helm exclusively). Build entirely from RESEARCH.md's own verified Code Examples and Anti-Patterns sections:
- Oracle service block with healthcheck (RESEARCH.md "Code Examples" section, lines 627-649) — uses the image's own bundled `/opt/oracle/healthcheck.sh`, `start_period: 30s` (measured 16s boot + margin).
- Start from the official Airflow 3.3.1 quick-start compose as a template, then **actively strip** `redis`, `airflow-worker`, `flower` services and rewrite `AIRFLOW__CORE__EXECUTOR` to `LocalExecutor` and `AIRFLOW__CORE__AUTH_MANAGER` to the `simple_auth_manager` class path (RESEARCH.md Anti-Patterns + Pitfall 3, lines 443-459, 535-559) — do not use the official file unmodified.
- `simple_auth_manager` provisioning via bind-mounted `simple_auth_manager_passwords.json.generated` + `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin` (RESEARCH.md Pattern 4, lines 381-428).
- Oracle Connection registration via `AIRFLOW_CONN_ORACLE_DEFAULT` env var, D-11 (RESEARCH.md Pattern 5, lines 430-441).
- Bind Oracle's exposed port to `127.0.0.1:1521:1521` rather than a bare `1521:1521` per RESEARCH.md's Security Domain section (line 801) if the developer's LAN is untrusted.

---

### `scripts/verify_environment.py` (test, request-response)

**No analog** — reference repo's closest equivalent (`tests/e2e/cluster`) is a full pytest suite coupled to a live Kubernetes cluster; not a portable single-script pattern. Use RESEARCH.md's own Code Examples section verbatim as the base (RESEARCH.md lines 653-675):
```python
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
Extend per D-05: also assert expected columns exist via `ALL_TAB_COLUMNS` (not just table names, which this skeleton only covers). Reusable pattern noted by Phase 4 (RESEARCH.md Integration Points) — write this script with reuse in mind (e.g., a `verify_tables()` function importable later), even though this phase only needs a runnable script.

---

### `docs/environment.md` (docs)

**No analog** — reference repo's `docs/` directory is Kubernetes/Helm-specific end to end. Content requirements come entirely from CONTEXT.md D-17 and RESEARCH.md Common Pitfall #5 / #6 and Open Question 1: document Airflow's own preflight minimums (4 GB RAM / 2 CPU / 10 GB disk, RESEARCH.md lines 596-599) plus Oracle Free's separate footprint, `.wslconfig` sizing guidance (carried forward from project-level PITFALLS.md), and a note that the combined number should be checked against a real `docker stats` run before being finalized (RESEARCH.md Open Question 1).

---

### `.env.example` (config)

**No analog needed** — trivial flat key=value file. Use RESEARCH.md's own scattered examples for the variable set: `ORACLE_PASSWORD`, `ORACLE_APP_USER`, `ORACLE_APP_USER_PASSWORD` (all `admin` per D-09/D-07), `AIRFLOW_CONN_ORACLE_DEFAULT` (RESEARCH.md Pattern 5, line 435).

## Shared Patterns

### Non-root, `--chown`-qualified Docker image builds
**Source:** `/home/user/projects/airflow-platform/docker/airflow/Dockerfile` lines 40-45, 99-105
**Apply to:** `docker/airflow/Dockerfile` only (the sole Dockerfile this phase creates)
```dockerfile
# No USER root switch anywhere; every COPY uses --chown=airflow:0
COPY --chown=airflow:0 <src> <dest>
```

### `ALTER SESSION` preamble before any Oracle DDL
**Source:** RESEARCH.md Pattern 2 (this phase's own research, empirically verified — no reference-repo source exists)
**Apply to:** All three `docker/oracle/init/*.sql` files, with no exceptions
```sql
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = ADMIN;
```

### Pinned-constraint `pip install`, never `_PIP_ADDITIONAL_REQUIREMENTS`
**Source:** `/home/user/projects/airflow-platform/docker/airflow/Dockerfile` lines 19-25, 46-47 — corroborated independently by this phase's own RESEARCH.md Pattern 1
**Apply to:** `docker/airflow/Dockerfile`
```dockerfile
RUN pip install --no-cache-dir "<pkg>==<version>" ... \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"
```

### Makefile `.PHONY` + `## description` self-documentation
**Source:** `/home/user/projects/airflow-platform/Makefile` (structural convention only, lines 56-65 pattern)
**Apply to:** `Makefile` (all targets, this phase and every later phase per D-14)
```makefile
.PHONY: up down reset logs
up:              ## Start the full stack (Airflow + Oracle)
	docker compose up -d
```

## No Analog Found

Files with no close match anywhere (reference repo or otherwise) — planner should rely entirely on RESEARCH.md's own empirically-verified Code Examples for these:

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `docker/oracle/init/03_ingestion_metadata.sql` | migration | batch | No Oracle DDL exists in the reference repo (Postgres-only `migrations/`); no `meta.*`-equivalent table shape is portable |
| `docker-compose.yml` | config | event-driven | Reference repo uses Kubernetes/kind/Helm exclusively, never docker-compose |
| `scripts/verify_environment.py` | test | request-response | Reference repo's verification is a full pytest+Kubernetes e2e suite, not a portable single-script pattern |
| `docs/environment.md` | config (docs) | n/a | Reference repo's docs are Kubernetes/Helm-specific end to end |
| `.env.example` | config | n/a | Trivial flat file, no pattern needed |

## Metadata

**Analog search scope:** `/home/user/projects/airflow-platform` (read-only reference repo named explicitly in CONTEXT.md canonical_refs — `docker/airflow/`, `configs/datasets/`, `Makefile`, top-level directory listing); this repo (`lightweight-airflow-etl`) itself has zero application code to search (confirmed greenfield — only `.planning/` and `.claude/` exist)
**Files scanned:** 4 (Dockerfile, customers.yaml, orders.yaml — read in full; Makefile — targeted `grep` scan of target/`.PHONY` structure only, not read in full given its Kubernetes-specific content is explicitly non-portable per the phase brief)
**Pattern extraction date:** 2026-08-28
