## Project

**Lightweight Airflow CSV→Oracle ETL Platform** (working name)

A small, local Airflow environment focused on one problem: detect, parse, validate and bulk-load
generated CSV files into Oracle Database Free, orchestrated by a thin Airflow TaskFlow DAG that
delegates to a reusable Python CSV processing engine.

Full requirements: `.planning/research/lightweight-spec.md` (60-point spec, seed input for
`/gsd-new-project`).

Explicitly out of scope: Kubernetes, MinIO, Vault, CDC, SCD, complex data-lake/lineage, multi-DB
warehouse architecture, production-grade observability stack. See §3 of the spec for the full list.

## Reference repo — read, do not depend on

There is a prior, production-shaped sibling project at:

`/mnt/c/Users/borow/VSC/projects/airflow-platform`

It solves a superset of this problem (full CDC/SCD, Kubernetes, MinIO, Vault, Postgres) and its
patterns are worth reading before implementing the equivalent piece here. **Do not add it as a
dependency, uv workspace member, or git submodule** — its `dataplat` package pulls in Vault, S3/
boto3 and Postgres-`COPY`-specific loading that has no place in this project. Port logic in by
reading and rewriting a smaller version, never by importing.

Worth reading there, by subtree:

- `packages/csv-processor/src/csv_processor/detect/{dialect,encoding,header,schema,filename}.py`,
  `compression.py`, `source.py` — DB-agnostic CSV dialect/encoding/header detection. Most directly
  portable piece; Oracle needs the same sniffing Postgres does.
- `packages/dataplat/src/dataplat/normalize/{dates,numeric,unicode,boolean_null}.py` — read for the
  type-conversion approach (CSV string → Python type → DB type); reimplement smaller here, don't
  import.
- `packages/dataplat/src/dataplat/validate/` — only the structural/type/nullability subset applies
  here. Ignore referential/uniqueness/volume-anomaly/completeness/circuit-breaker validators —
  explicitly out of scope for this project.
- `airflow/dags/csv_ingest_customers.py` + `airflow/dags/_common/` — the working reference for "thin
  TaskFlow DAG delegates to a processing engine." Skip the KubernetesPodOperator-specific parts
  (`_common/kpo.py`, `_common/tracing_kpo.py`) — this project has no Kubernetes; `process_csv` runs
  in-process under Airflow's LocalExecutor instead.
- `docker/csv-processor/Dockerfile` — optional reference only if this project containerizes its
  processor.

## Known gaps in the seed spec to resolve early (before/during `/gsd-new-project`)

The spec (`.planning/research/lightweight-spec.md`) doesn't pin these — resolve with a short
verification pass, same discipline as the reference repo's `STACK.md`, before building on them:

1. **Oracle driver**: use `python-oracledb` (Oracle's actively-maintained driver, successor to
   `cx_Oracle`; "thin" mode needs no separate Oracle Client install). Bulk insert = `cursor.
   executemany()` with array binding — Oracle has no `COPY` equivalent. Pin an exact version.
2. **Config validation**: Pydantic v2 for `config.json` (validated once, contract-shaped) — never
   Pydantic per CSV row (construction cost + it raises instead of collecting, which fights the
   collect-and-continue invalid-row model the spec wants).
3. **Airflow executor**: LocalExecutor (no Kubernetes here, so KubernetesExecutor is moot; no need
   for Celery/Redis at this scale).
4. **Oracle Database Free image tag**: pin an explicit tag, don't float on `latest`.

## Conventions

Conventions not yet established. Will populate as patterns emerge during development.

## Architecture

Architecture not yet mapped. Follow existing patterns found in the codebase.
