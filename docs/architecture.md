# Architecture: HTTP Trigger → Airflow DAG → CSV Engine → Oracle

This document covers the system-level shape of the pipeline: the full request path, the
component boundary between Airflow and the reusable CSV engine, the two-tier reuse decision this
project made against its reference sibling repo, and the `docker-compose.yml` service topology.
See `docs/airflow-dag.md` for the DAG's own task graph and `docs/csv-engine.md`/`docs/oracle.md`
for the engine's and Oracle's internals — this document stays at the "how the pieces fit together"
level, not the per-module detail level.

## The Full Path

```
HTTP POST /api/v2/dags/csv_to_oracle_ingest/dagRuns   (Airflow's own REST API, admin/admin JWT)
        |
        v
csv_to_oracle_ingest DAG (airflow/dags/csv_to_oracle_ingest.py)
        |
        +-- load_config_task        -- validates runtime conf, loads configs/datasets/<name>.json
        +-- route_after_config      -- the ONLY branch, on config validity, never on dataset identity
        +-- wait_for_file           -- deferrable FileSensor, releases its worker slot while waiting
        +-- process_csv_task        -- the SOLE Oracle-writing integration point
        |       |
        |       v
        |   csv_processor.engine.process(file_path, config)   (Airflow-agnostic, ENGINE-09)
        |       |
        |       +-- detect (dialect/encoding/header/compression)
        |       +-- parse -> validate -> normalize -> chunk        (process_chunks(), docs/csv-engine.md)
        |       +-- load.insert_rows()  -- cursor.executemany() per chunk, into Oracle
        |       +-- record_ingestion()  -- ingestion_metadata row, checksum-keyed idempotency
        |
        +-- load_results_task       -- thin XCom pass-through, never touches csv_processor/oracledb
        +-- report_result_task      -- logs a concise summary; runs on BOTH the success and
                                        config-error early-exit paths
```

A single HTTP request carries `{"conf": {"dataset": "customers", "config_path":
"configs/datasets/customers.json"}}` (see `docs/airflow-dag.md`'s "Triggering the DAG" section for
the exact `scripts/trigger_dag.sh` flow). The DAG never branches on which dataset it received —
`route_after_config` only ever branches on whether the loaded config is valid — so the exact same,
unmodified `csv_to_oracle_ingest.py` handles both `customers` and `orders` purely by the runtime `conf` it's
given (DAG-05).

## The `customers_orders_report` DAG (business report)

A second, independent DAG — `airflow/dags/customers_orders_report.py` — senses when both `customers` and
`orders` have ingested data for the current day's partition, then materializes the same
customers⋈orders business report every other path in this project produces (see
`docs/oracle.md`'s "Business Report Evidence"):

```
customers_orders_report DAG (airflow/dags/customers_orders_report.py)
        |
        +-- wait_for_both_datasets  -- ReportReadySensor, a deferrable BaseSensorOperator
        |       |
        |       v
        |   OraclePartitionReadyTrigger (airflow/dags/_common/oracle_partition_trigger.py)
        |       -- polls ingestion_metadata via oracledb.connect_async() until COUNT(DISTINCT
        |          dataset) = 2 for today's TRUNC(SYSDATE) partition, then fires
        |
        +-- build_report_task       -- runs the business-report SQL, logs each returned row
```

`apache-airflow-providers-oracle` ships no sensor of its own, so `OraclePartitionReadyTrigger` is a
custom, non-blocking (`connect_async()`) `BaseTrigger` — the only way to get a deferrable
"both datasets ready" check without occupying a worker slot while waiting. This DAG runs alongside
`scripts/regenerate_readme_summary.py`'s CI-triggered README regeneration, not replacing it — both
paths independently materialize the identical, never-re-authored business-report SQL.

## Component Boundary: `airflow/dags/` vs. `packages/csv-processor/`

This is the single most load-bearing architectural line in the project:

- **`airflow/dags/`** (the DAG file + `_common/`) is thin orchestration only. It validates the two
  runtime `conf` fields (`dataset`, `config_path`), calls `csv_processor.config.loader.load_config`
  and `csv_processor.engine.process`, and translates the returned `ProcessingResult` into log lines
  and XCom values. It contains **zero** CSV-parsing, validation, or Oracle-writing logic of its
  own.
- **`packages/csv-processor/`** is Airflow-agnostic (ENGINE-09) — nothing under
  `packages/csv-processor/src/csv_processor/` imports `airflow.*` anywhere, checked mechanically
  across every phase of this project. It can be (and is, in `benchmark/run_benchmark.py`) called
  directly from a plain Python script with no Airflow process running at all.

The practical payoff: `process_csv_task` calls `csv_processor.engine.process(file_path, config)`
exactly once and never re-implements any part of what that function does. If the CSV engine ever
needed to run outside Airflow (a CLI tool, a different scheduler), it would work unmodified —
this is the same property `benchmark/run_benchmark.py` already exploits by calling
`process_chunks()` directly, bypassing Airflow entirely (see `docs/benchmark.md`).

## Two-Tier Reuse of the Reference Repo (prior art)

This project has a prior, production-shaped sibling project (`airflow-platform`, see `CLAUDE.md`)
solving a superset of this problem — full CDC/SCD, Kubernetes, MinIO, Vault, Postgres. Its
`dataplat` package was never imported, added as a dependency, or vendored as a submodule; its
`dataplat.storage`/`dataplat.pipeline`/Vault/S3 coupling has no place in this deliberately smaller
project. Instead, reuse followed two explicit tiers, verified by reading actual imports rather than
assumed:

- **Tier A — vendor the file, strip 1-2 lines of coupling.** The `csv_processor/detect/*.py`
  modules (dialect sniffing via `clevercsv`, encoding detection via `chardet`/
  `charset-normalizer`, the header-scoring heuristic, filename-mask matching) and
  `csv_processor/compression.py` had near-zero real coupling to `dataplat` — each imported one
  exception class (`from dataplat.errors import <SomeError>`), replaced with a local exception
  class of the same name, or one S3-backed file-open call (`compression.py`), replaced with a plain
  `pathlib.Path.open()`. The detection *logic itself* — the interesting, hard-won part — is pure
  and was copied essentially verbatim.
- **Tier B — read the algorithm, do not extract the file.** `source.py`'s orchestration sequence
  (detect compression → decode encoding → detect dialect → detect header → stream rows),
  `normalize/dates.py`'s strict-`strptime` date-rejection logic, and `validate/pattern.py`'s regex
  checks were all fully wired into the reference repo's own custom streaming-pipeline engine
  (`StreamingStage`/`BarrierStage`, baked-in observability calls) — not portable as files. This
  project reimplemented just the algorithm each one embodies, as plain functions against its own
  row model (`csv_processor.source`, `csv_processor.validate`), scoped down to only the
  structural/type/nullability validation this project's spec actually requires (referential/
  uniqueness/volume-anomaly/completeness/circuit-breaker validation is explicitly out of scope —
  see `PROJECT.md`'s Out of Scope section).

This tiering is a recorded project decision (`CLAUDE.md`), not an implementation afterthought —
every plan in Phases 2-3 that touched detection/parsing code cited it explicitly.

## `docker-compose.yml` Topology (6 Services)

```
                         +------------------+
                         |     postgres     |  Airflow's own metadata DB
                         +------------------+
                                  ^
                     service_healthy | depends_on
                                  |
   +------------------+   +------+-------+   +--------------------+
   |  airflow-init    |-->| airflow-     |   | airflow-scheduler   |
   |  (db migrate,    |   | apiserver    |   +--------------------+
   |  runs once)      |   | (REST API,   |   +--------------------+
   +------------------+   |  port 8080)  |   | airflow-dag-processor|
            |             +--------------+   +--------------------+
            |                                +--------------------+
            +------------------------------->| airflow-triggerer   |
                                              +--------------------+
                                                       ^
                                        service_healthy | depends_on
                                                       |
                                              +------------------+
                                              |      oracle      |  port 1521
                                              | (gvenzl/oracle-  |
                                              |  free, pinned    |
                                              |  tag)            |
                                              +------------------+
```

All five Airflow processes (`airflow-init`, `airflow-apiserver`, `airflow-scheduler`,
`airflow-dag-processor`, `airflow-triggerer`) share one built image
(`docker/airflow/Dockerfile`) and one environment block (`docker-compose.yml`'s
`x-airflow-common` anchor) — `csv-processor` is installed directly into that image so
`process_csv_task` calls it as a plain in-process Python function under `LocalExecutor`, with no
`DockerOperator`/`KubernetesPodOperator` indirection (see README.md's Notes & Q&A for the full
reasoning). The same shared env block also carries a single, identical `AIRFLOW__API_AUTH__JWT_SECRET`
(task-token verification) and `AIRFLOW__API__SECRET_KEY` (`[api] secret_key`, log-fetch request
signing) across every component, and every component mounts one shared named volume
(`airflow-logs:/opt/airflow/logs`) so a task's logs remain fetchable from any component regardless
of which container originally wrote them. `airflow-init` runs once (`db migrate`) and every other
Airflow service depends on it completing successfully, plus on `oracle` reporting `service_healthy`
via its own `healthcheck.sh`-based check. Both exposed ports (`1521` for Oracle, `8080` for
Airflow's API/UI) are bound to `127.0.0.1` only, never `0.0.0.0` — see `docs/environment.md`'s "Port
Bindings" section for the reasoning (T-01-02).

For CPU/RAM/disk sizing and first-boot troubleshooting, see `docs/environment.md`. For both DAGs'
own task graphs and live-verification evidence, see `docs/airflow-dag.md`.
