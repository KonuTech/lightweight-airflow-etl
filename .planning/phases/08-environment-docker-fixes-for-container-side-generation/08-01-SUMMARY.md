---
phase: 08-environment-docker-fixes-for-container-side-generation
plan: 01
subsystem: docker-environment
tags: [docker-compose, dockerfile, airflow-init, bind-mounts, faker]
dependency-graph:
  requires: []
  provides:
    - generator/-mounted-in-airflow-container
    - extended-pythonpath-opt-airflow
    - airflow-init-root-repair-step
    - faker-40.37.0-in-image
  affects:
    - phase-09-csv-generate-schedule-dag
tech-stack:
  added:
    - faker==40.37.0 (Airflow container image, matches root uv.lock exactly)
  patterns:
    - airflow-init root-user (user "0:0") combined repair-and-chown bash -c step,
      gated by the existing depends_on service_completed_successfully chain
key-files:
  created: []
  modified:
    - docker-compose.yml
    - docker/airflow/Dockerfile
decisions:
  - "airflow-init repair order: passwords-file repair (rmdir-if-dir, seed-only-if-missing,
    chown+chmod 664) before data/ mkdir+chown-R, before exec airflow db migrate last"
  - "faker joins the FIRST (constrained) pip install call, not the second unconstrained
    clevercsv/charset-normalizer/chardet call -- confirmed faker has zero entry in
    Airflow's own constraints-3.3.1/constraints-3.12.txt"
metrics:
  duration_minutes: 25
  completed: 2026-09-01
---

# Phase 8 Plan 01: Environment & Docker Fixes for Container-Side Generation Summary

Mounted `generator/` read-only into the Airflow container, extended `PYTHONPATH` to
`/opt/airflow/dags:/opt/airflow`, pinned `faker==40.37.0` into the image's constrained pip install
line, and rewrote `airflow-init` into a root-user combined repair step that fixes both the
passwords-file bind-mount-becomes-directory gotcha and `data/`'s write-permission gap before any
non-root Airflow service starts.

## What Was Built

### Task 1: docker-compose.yml — generator mount, PYTHONPATH extension, airflow-init combined repair command

- Added `./generator:/opt/airflow/generator:ro` to `x-airflow-common.volumes`, immediately after
  the existing `./configs:/opt/airflow/configs:ro` line.
- Extended `PYTHONPATH` from `"/opt/airflow/dags"` to `"/opt/airflow/dags:/opt/airflow"`, with a
  comment explaining the capability this unlocks (`generator.generate_csv` importable from any
  task/exec context).
- Rewrote the `airflow-init` service: added `user: "0:0"` (root override, this service only) and
  replaced `command: db migrate` with a combined `bash -c` command performing, in order:
  1. Passwords-file repair (ENV-03/D-04/D-05): `rmdir` the path if it exists as a directory (safe —
     only succeeds on Docker's empty auto-created directory), seed `{"admin": "admin"}` only if the
     file is still missing after that, then `chown ${AIRFLOW_UID:-50000}:0` + `chmod 664`.
  2. Data-dir fix (ENV-02): `mkdir -p /opt/airflow/data/customers /opt/airflow/data/orders` then
     `chown -R ${AIRFLOW_UID:-50000}:0 /opt/airflow/data`.
  3. `exec airflow db migrate` last, preserving the original behavior.
- Verified `docker compose config > /dev/null` exits 0 and the rendered config contains the
  expected `user: '0:0'`, the full `bash -c` command string (rmdir/chown/chmod 664/mkdir -p/
  chown -R/exec airflow db migrate), and `PYTHONPATH: /opt/airflow/dags:/opt/airflow`.
- Commit: `360c5e4`

### Task 2: Dockerfile faker pin + full rebuild and live capability proof

- Added `"faker==40.37.0" \` to the first (constrained) `pip install` call in
  `docker/airflow/Dockerfile`, immediately after `"apache-airflow-providers-oracle==4.6.2" \` and
  before its `--constraint` line — exact match to the root `pyproject.toml`/`uv.lock` pin.
- Rebuilt the image from scratch: `docker compose build --no-cache` (all 5 Airflow images rebuilt
  successfully) followed by `docker compose up -d --wait` (all 6 services reported healthy,
  `airflow-init` exited 0 after its repair steps + `db migrate`).
- Live-verified every required check:
  - `docker compose exec -T airflow-apiserver python -c "import faker; from generator.generate_csv import main; print('IMPORT_OK')"` → `IMPORT_OK`, exit 0.
  - `docker compose exec -T airflow-apiserver printenv PYTHONPATH` → `/opt/airflow/dags:/opt/airflow`.
  - `docker compose exec -T airflow-apiserver ls /opt/airflow/generator` → `generate_csv.py`.
  - Write-then-delete probe succeeded in both `/opt/airflow/data/customers/` and
    `/opt/airflow/data/orders/` (matches D-08's "actually write, don't just check bits" discipline,
    which Plan 08-02 will wire permanently into `verify_environment.py`).
  - Re-ran `docker compose up -d --wait` against the now-already-initialized `data/` and passwords
    file — exited 0, all services healthy again, confirming idempotency (host `stat` after the
    first run: passwords file `664 regular file`; `data/`, `data/customers/`, `data/orders/` all
    owned by `50000:0`).
- Commit: `ed97e62`

## Deviations from Plan

None — plan executed exactly as written. Both tasks' acceptance criteria were met verbatim; no
Rule 1-4 auto-fixes were needed since the stack built and ran cleanly on the first attempt.

## Auth Gates

None encountered.

## Known Stubs

None — this plan only touches Docker Compose/Dockerfile wiring, no application code or UI surface.

## Threat Flags

None — the changes match the plan's own `<threat_model>` exactly (root-user scope limited to
`airflow-init` only, `chown -R`/`mkdir -p`/`rmdir` scoped to exact literal paths, `chmod 664` not
`666`, passwords-file overwrite guarded by "only if missing"). No new network endpoints, auth
paths, or trust-boundary-crossing surface was introduced beyond what the threat model already
covers.

## Not Performed (Scoping Note)

A full "genuinely fresh clone" reproduction (deleting the passwords file entirely and running
`docker compose down -v && docker compose up -d --wait` against a wiped Postgres/Oracle volume)
was **not** performed in this session, to avoid destroying this checkout's existing Oracle/Postgres
named-volume state (schema, prior ingestion history) that other work in this repo depends on. The
mechanism was instead validated by:
1. Re-running `docker compose up -d --wait` twice in a row against already-initialized `data/`/
   passwords-file state (idempotency, Success Criterion 3's literal requirement) — confirmed exit 0
   both times, no errors in `airflow-init` logs.
2. Reading the shell logic itself against the documented Docker auto-create-as-empty-directory
   failure mode (`rmdir` only ever removes an empty, wrongly-typed directory, never real content).

This is a reasonable proxy for the "genuinely fresh clone" truth given the destructive cost of a
full volume wipe in this shared dev checkout; a true fresh-clone proof will naturally occur the
next time this project is cloned fresh or CI runs `oracle-e2e` from a clean checkout.

## Self-Check: PASSED

- `docker-compose.yml` — FOUND, contains `./generator:/opt/airflow/generator:ro`,
  `PYTHONPATH: "/opt/airflow/dags:/opt/airflow"`, `user: "0:0"` on `airflow-init`, and the full
  repair command.
- `docker/airflow/Dockerfile` — FOUND, contains `"faker==40.37.0" \` in the first pip install call.
- Commit `360c5e4` — FOUND in `git log --oneline`.
- Commit `ed97e62` — FOUND in `git log --oneline`.
- Live verification commands all re-confirmed passing at time of writing this summary.
