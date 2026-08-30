# Phase 1: Environment & Oracle Foundation - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-28
**Phase:** 1-Environment & Oracle Foundation
**Areas discussed:** Target table columns, Oracle schema/user ownership, Airflow image build,
resource docs placement, Airflow auth manager, Docker volume persistence, .env handling, repo
layout, schema verification, schema DDL delivery, idempotency/partitioning, Oracle Connection
registration, Makefile down/reset semantics, partition management, table identifier casing,
Oracle host port exposure, Makefile scope.

---

## Target Table Columns

| Option | Description | Selected |
|--------|-------------|----------|
| Mirror reference columns, strip SCD metadata | Keep customer_id/name/country/birth_date/event_ts/signup_country and order_id/customer_id/order_date/amount; drop business_key/scd_type annotations | ✓ |
| Trim further — drop signup_country too | Same, but also drop signup_country | |
| Design a fresh minimal schema, don't mirror | Ignore reference repo columns entirely | |

**User's choice:** Mirror reference columns, strip SCD metadata.

---

## Oracle Schema/User Ownership

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated app schema, same admin/admin creds | APP_USER/APP_USER_PASSWORD env vars create a real app schema, still admin/admin | ✓ |
| Default SYSTEM schema | Tables live directly under SYSTEM using the SYS admin password | |

**User's choice:** Dedicated app schema, same admin/admin creds.

---

## Airflow Image Build

| Option | Description | Selected |
|--------|-------------|----------|
| Custom Dockerfile extending apache/airflow | FROM apache/airflow:3.3.1-python3.12, pip-installs pinned deps + csv_processor | ✓ |
| Quick-start's _PIP_ADDITIONAL_REQUIREMENTS | Zero extra files, but Airflow's own docs call it dev-only | |

**User's choice:** Custom Dockerfile extending apache/airflow.

---

## Resource Docs Placement

| Option | Description | Selected |
|--------|-------------|----------|
| Start docs/environment.md now | Dedicated doc file with resource + .wslconfig + boot-time notes | ✓ |
| README section for now | Simpler, single file, split out later if needed | |

**User's choice:** Start docs/environment.md now.

---

## Airflow Auth Manager

| Option | Description | Selected |
|--------|-------------|----------|
| simple_auth_manager (Airflow 3 default) | Token-based, file-backed, no FAB metadata tables | ✓ |
| FAB auth manager (Airflow 2-style) | Traditional flask-appbuilder auth with airflow-init user creation | |

**User's choice:** simple_auth_manager (Airflow 3 default).

---

## Docker Volume Persistence

| Option | Description | Selected |
|--------|-------------|----------|
| Persistent named volumes | Data survives down/up, avoids re-paying Oracle boot time | ✓ |
| Ephemeral (no named volumes) | Every up starts clean, useful for idempotency-story testing | |

**User's choice:** Persistent named volumes.

---

## .env Handling

| Option | Description | Selected |
|--------|-------------|----------|
| .env.example template, gitignore .env | Standard practice, copy-on-clone workflow | ✓ |
| Commit .env directly | Zero setup friction since values are meaningless outside local dev | |

**User's choice:** .env.example template, gitignore .env.

---

## Repo Layout

| Option | Description | Selected |
|--------|-------------|----------|
| Flat layout — csv_processor/ + airflow/ at repo root | No packages/ nesting, no src/ layout | |
| Mirror reference repo's src/ + packages/ nesting | packages/csv-processor/src/csv_processor/, airflow/dags/, docker/csv-processor/ | ✓ |

**User's choice:** Mirror reference repo's src/ + packages/ nesting.

---

## Schema Verification

| Option | Description | Selected |
|--------|-------------|----------|
| Committed verification script | scripts/verify_environment.py querying USER_TABLES/ALL_TAB_COLUMNS, reusable by Phase 4 | ✓ |
| One-time manual check, not a repo artifact | Query manually once, don't commit anything | |

**User's choice:** Committed verification script.

---

## Schema DDL Delivery

| Option | Description | Selected |
|--------|-------------|----------|
| Plain SQL DDL via Oracle's init-script mount | docker/oracle/init/*.sql, no migration history | ✓ |
| Alembic (or similar) migration tool | Mirrors reference repo's migrations/ setup | |

**User's choice:** Plain SQL DDL via Oracle's init-script mount.

---

## Idempotency / Partitioning

| Option | Description | Selected |
|--------|-------------|----------|
| UNIQUE(dataset, checksum) on ingestion_metadata | DB-level guard against duplicate file processing | ✓ (selected, then extended) |
| No DB constraint, application-level only | Phase 4's loader fully responsible | |

**User's choice:** Selected the UNIQUE constraint option, then added free-text: "Add partitioning
by date, possibly truncating a partition on reload. The tasks in DAGs have to be idempotent." This
triggered two follow-up questions:

**Follow-up — which table(s) partitioned:**

| Option | Description | Selected |
|--------|-------------|----------|
| _VALID/_INVALID tables, partitioned by ingestion date | Date-range partition on the data tables themselves | ✓ |
| ingestion_metadata only, partitioned by run date | Data tables stay unpartitioned | |

**Follow-up — coexistence with the UNIQUE constraint:**

| Option | Description | Selected |
|--------|-------------|----------|
| Both coexist | Constraint guards duplicate files; partition truncate/reload is a separate explicit operation | ✓ |
| Partition truncate/reload replaces the constraint | No separate checksum-based check needed | |

**Follow-up — partition management mechanism:**

| Option | Description | Selected |
|--------|-------------|----------|
| INTERVAL partitioning (auto-creates) | Oracle auto-creates each new day's partition | ✓ |
| Plain RANGE partitioning, pre-created | Needs a maintenance job | |

---

## Oracle Connection Registration

| Option | Description | Selected |
|--------|-------------|----------|
| Env vars only, no Airflow Connection | Single source of truth in .env | |
| Both — register an Airflow Connection too | UI visibility, csv_processor still never touches it | ✓ ("Decide for me" — user noted they don't mind a Connection in the UI) |

**User's choice:** Both. **Notes:** User also added two general reminders not tied to this
specific question: "Remember to structure it logically when destroying and rebuilding whole
lightweight airflow platform" and "Remember to use Makefile for essential CI/CD tasks" — these
were captured as new discussion areas below (Makefile down/reset semantics, Makefile scope).

---

## Makefile down/reset Semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Separate targets: down (stop) vs reset (wipe) | make down keeps volumes; make reset/nuke does down -v | ✓ |
| make down always wipes volumes | Simpler mental model but contradicts persistent-volumes decision | |

**User's choice:** Separate targets: down (stop) vs reset (wipe).

---

## Partition Management

| Option | Description | Selected |
|--------|-------------|----------|
| INTERVAL partitioning (auto-creates) | Zero ongoing maintenance | ✓ |
| Plain RANGE partitioning, pre-created | Needs upkeep, can fail on unplanned dates | |

**User's choice:** INTERVAL partitioning (auto-creates). *(Confirmed again as part of the
idempotency/partitioning follow-up chain above.)*

---

## Table Identifier Casing

| Option | Description | Selected |
|--------|-------------|----------|
| Uppercase, unquoted (Oracle default/idiomatic) | CUSTOMERS_VALID, CUSTOMER_ID — no quoting needed anywhere | ✓ |
| Lowercase, quoted (matches Python/Airflow style) | Requires double-quoting every SQL statement forever | |

**User's choice:** Uppercase, unquoted (Oracle default/idiomatic).

---

## Oracle Host Port Exposure

| Option | Description | Selected |
|--------|-------------|----------|
| Expose to host | 1521:1521 mapped, external SQL clients can connect directly | ✓ |
| Container-network only | Smaller surface, debugging requires exec-ing into a container | |

**User's choice:** Expose to host.

---

## Makefile Scope

| Option | Description | Selected |
|--------|-------------|----------|
| Project-wide convention, starting now | Makefile is the standard entrypoint from Phase 1 onward; later phases add targets | ✓ |
| Phase 1 lifecycle only for now | Later phases decide their own tooling independently | |

**User's choice:** Project-wide convention, starting now.

---

## Claude's Discretion

None — every gray area received an explicit user choice (including the "decide for me" on Oracle
Connection registration, where the user still stated a clear preference: fine either way, with UI
visibility as a plus).

## Deferred Ideas

- **Customers↔Orders reporting join DAG task** (PL/SQL best practices, partitioning/indexing) —
  raised by the user after declaring planning "done." Identified as a new capability outside this
  project's current ingest-only scope and in tension with PROJECT.md's locked
  no-referential-integrity-enforcement decision. User explicitly marked this deferred idea as
  required for a future phase/milestone, not merely optional — captured in CONTEXT.md's
  `<deferred>` section for roadmap backlog review.
