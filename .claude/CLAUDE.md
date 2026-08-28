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

Verified by reading actual imports (not assumed) — reuse is **two-tier**, not uniform:

**Tier A — vendor the file, then strip 1-2 lines of coupling:**
- `packages/csv-processor/src/csv_processor/detect/{dialect,encoding,header,filename,schema}.py` —
  each imports `from dataplat.errors import <SomeError>`. Replace with a local exception class of
  the same name; the actual detection logic (clevercsv dialect sniffing, chardet/charset-normalizer
  encoding detection, the header-scoring heuristic) is pure and has zero Postgres/S3/Vault/K8s
  coupling. Copy the file, fix the one import, done.
- `packages/csv-processor/src/csv_processor/compression.py` — same treatment, plus one extra swap:
  its file-opening call is `dataplat.storage.objectstore.open_text_stream` (S3-backed). Replace
  with a plain local `open()`/`pathlib.Path.open()`; the gzip/zip handling around it is untouched.

**Tier B — read the algorithm, do not extract the file:**
- `packages/csv-processor/src/csv_processor/source.py` — fully wired into dataplat's `Source`
  protocol, `SchemaRepository`, `RecordChunk` model. Not a portable file; read it only to see the
  *sequence* (detect compression → decode encoding → detect dialect → detect header → stream rows)
  and write your own smaller orchestrator following that sequence.
- `packages/dataplat/src/dataplat/normalize/{dates,numeric,unicode,boolean_null}.py` and all of
  `packages/dataplat/src/dataplat/validate/*` — **every one of these is implemented as a "stage"**
  plugged into dataplat's custom streaming pipeline engine (`dataplat.pipeline.protocol.
  StreamingStage`/`BarrierStage`, `dataplat.models.record.RejectedRecord`/`StageResult`,
  `dataplat.observability.metrics` calls baked into each). None of that stage scaffolding applies
  here. What's worth reading is the algorithm *inside* each stage — e.g. the strict-`strptime`
  date-rejection logic in `normalize/dates.py`, the regex checks in `validate/pattern.py` — then
  reimplement just that logic as a plain function against this project's own row model. Only the
  structural/type/nullability validators are even in scope per spec §28 — ignore referential/
  uniqueness/volume-anomaly/completeness/circuit-breaker entirely, they're explicitly excluded.
- `airflow/dags/csv_ingest_customers.py` + `airflow/dags/_common/` — the working reference for "thin
  TaskFlow DAG delegates to a processing engine." Skip the KubernetesPodOperator-specific parts
  (`_common/kpo.py`, `_common/tracing_kpo.py`) — this project has no Kubernetes; `process_csv` runs
  in-process under Airflow's LocalExecutor instead.
- `docker/csv-processor/Dockerfile` — optional reference only if this project containerizes its
  processor.

This tiering is a recorded decision, not just a note here — restate it explicitly when
`/gsd-new-project`'s Q&A asks about prior art / technical approach, so it lands in `PROJECT.md`
and survives into the phase that actually implements the CSV engine.

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
