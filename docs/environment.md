# Environment: Resource Requirements & Networking Notes

This document covers what it takes to run the full local stack (Airflow LocalExecutor + Airflow's
own metadata Postgres + Oracle Database Free) via `make up` / `docker compose up -d --wait`.
Phase 6 extends this same file with CI/docs polish rather than replacing it (D-17).

## CPU / RAM / Disk Requirements

There are two different numbers here, and this document deliberately keeps them separate rather
than presenting one blended guess as "the" requirement (INFRA-02):

### 1. Documented floor (Airflow's own preflight check)

Airflow's official `docker-compose.yaml` quick-start ships an `airflow-init` preflight script with
these literal thresholds, read verbatim from the source this phase's research pass:

```bash
if (( mem_available < 4000 )) ; then   # < 4000 MB RAM
if (( cpus_available < 2 )); then      # < 2 CPUs
if (( disk_available < one_meg * 10 )); then   # < 10 GB disk
```

This is Airflow's own stated minimum for its slice of the stack alone (and is actually a
Celery-oriented default — this project's simpler LocalExecutor/no-Celery/no-Redis topology needs
less than this floor implies). Oracle Database Free adds its own separately-documented footprint on
top — Oracle's own docs and `gvenzl/oracle-free`'s README generally recommend **≥ 2 GB RAM** for
Oracle Free specifically. Summed as vendor minimums: **~6 GB RAM / 2 CPUs / 10 GB disk** — but see
below, this sum is not what was actually observed running the finished stack.

### 2. Measured against this project's own docker-compose.yml on 2026-08-28

With the full 6-service stack (`postgres`, `oracle`, `airflow-apiserver`, `airflow-scheduler`,
`airflow-dag-processor`, `airflow-triggerer`) up and idle (no DAG runs executing), `docker stats
--no-stream` reported:

| Service | CPU % | Memory |
|---------|-------|--------|
| airflow-apiserver | 0.07% | 226.9 MiB |
| airflow-scheduler | 1.16% | 414.4 MiB |
| airflow-dag-processor | 0.50% | 158.8 MiB |
| airflow-triggerer | 0.83% | 262.9 MiB |
| postgres (Airflow metadata) | 1.01% | 62.2 MiB |
| oracle | 0.99% | 2.04 GiB |
| **Total (observed, idle)** | **~4.6%** | **~3.16 GiB** |

This is an **idle steady-state** measurement, not a load-tested peak — CPU usage under an active
DAG run processing a CSV file (Phase 3+) will be higher, though RAM is expected to be the binding
constraint for this stack (Oracle's SGA/PGA dominates), not CPU.

Disk footprint, `docker system df -v` against this project's own images/volumes on the same date:

| Component | Size | Notes |
|-----------|------|-------|
| 5 custom Airflow images (`docker/airflow/Dockerfile`) | ~3.24 GB total | All 5 share the same built layer (`SHARED SIZE` 3.235 GB) — not 5x that size on disk |
| `gvenzl/oracle-free:23.26.2-faststart` base image | 7.74 GB | Unique, no sharing with other images |
| `postgres:16` base image | 642 MB | Airflow's metadata DB image |
| `lightweight-airflow-etl_oracle-data` volume | 3.15 GB | Persistent Oracle datafiles (D-13), grows as data is loaded in later phases |
| `lightweight-airflow-etl_postgres-db-volume` volume | 68.2 MB | Airflow's own DAG-run/metadata history (D-13) |
| **Total (images + volumes, this project only)** | **~14.8 GB** | One-time image-pull/build cost (~11.6 GB) plus persistent data (~3.2 GB) |

**Documented requirement for this project (combining both numbers above):**

- **RAM:** 4 GB minimum (matches observed ~3.16 GiB idle usage plus headroom for an active DAG
  run); 6 GB+ recommended for comfortable headroom under load.
- **CPU:** 2 cores minimum (Airflow's own floor; observed idle usage is well under this, but
  reserve the full 2 cores for when a DAG run is actively parsing/loading a CSV file).
- **Disk:** 20 GB minimum free — 10 GB matches Airflow's own preflight floor, but the actual image
  set for this project (~11.6 GB) plus Oracle's persistent datafile volume (starts at ~3.2 GB, grows
  with real ingested data) already exceeds Airflow's floor before any application data is added.
  20 GB gives realistic headroom rather than being exactly at the edge on day one.

The vendor-summed floor (~6 GB RAM / 2 CPU / 10 GB disk) is not treated as the final answer here —
the measured figures above supersede it as this project's actual documented requirement.

## `.wslconfig` Sizing (WSL2 + Docker Desktop)

Carried forward from `.planning/research/PITFALLS.md` Pitfall 6: WSL2's VM (`vmmem`) dynamically
grows its memory allocation and does **not** release it back to Windows until an explicit `wsl
--shutdown` — Oracle's own memory footprint compounds this. Unmanaged growth has been observed
consuming most/all host RAM and spilling into the Windows pagefile, degrading the entire host (not
just the containers).

Pin explicit limits in `%UserProfile%\.wslconfig`, sized to the RAM/CPU numbers documented above:

```ini
[wsl2]
memory=6GB
processors=2
```

Periodically run `wsl --shutdown` during long dev sessions if host memory creeps upward — this is a
platform-level behavior, not something this project's code controls directly.

## IPv4 / Mirrored-Networking Caveat

Windows 11 24H2's WSL "mirrored" networking mode (on by default on updated hosts) enables IPv6
preference inside WSL/containers, which has caused intermittent connection timeouts to Oracle's
listener when the client stack still assumes IPv4 — this looks like "Oracle is randomly
unreachable" rather than an obvious config issue, because it's intermittent (works, then times out,
then works again) rather than consistently broken.

**If you see intermittent (not consistent) Oracle connection failures:**
1. Verify the DB connection string/DSN uses/accepts IPv4 explicitly.
2. Or pin `networkingMode=NAT` in `%UserProfile%\.wslconfig`:
   ```ini
   [wsl2]
   networkingMode=NAT
   ```
3. Restart WSL (`wsl --shutdown`, then reopen your WSL terminal / restart Docker Desktop).

Consistent (not intermittent) connection failures point to a real config/credential issue, not this
networking gotcha — don't reach for the `.wslconfig` fix first in that case.

## Port Bindings (Oracle 1521, Airflow 8080)

`docker-compose.yml` binds both exposed ports to the loopback interface only:

```yaml
ports:
  - "127.0.0.1:1521:1521"   # oracle
  - "127.0.0.1:8080:8080"   # airflow-apiserver
```

This is deliberate (T-01-02): binding a bare `1521:1521`/`8080:8080` (no host IP prefix) would
default to `0.0.0.0`, exposing Oracle's SQL listener and Airflow's REST API/webserver to any device
reachable on the same network as the Docker host — a real widened attack surface for what should be
a single-developer local environment, especially since both services currently authenticate with
the single `admin`/`admin` credential pair (INFRA-03).

**If you ever need to reach these ports from Windows-side tools (DBeaver, a browser) rather than
from inside WSL directly:** `127.0.0.1` inside WSL2 is generally reachable from Windows the same way
(WSL2's `localhost` forwarding makes container ports bound to `127.0.0.1` inside WSL also reachable
via `localhost` on the Windows host, in the default NAT networking mode). If that forwarding isn't
working for your setup, find the WSL2 interface IP from Windows with `wsl hostname -I` (run from a
Windows terminal) and connect to that IP + port directly, rather than widening the docker-compose
bind to `0.0.0.0`.

## First-Clone Setup Gaps

A genuinely fresh `git clone` needs two files created manually before `make up` — neither is
generated automatically yet:

1. **`.env`** — `cp .env.example .env` (D-09). Placeholder `admin`/`admin` values are already
   correct for local dev; no editing required unless you want different credentials.
2. **`docker/airflow/simple_auth_manager_passwords.json.generated`** — this file is gitignored (same
   discipline as `.env`, even though it's a throwaway local-dev value) and has **no `.example`
   template**. Create it manually before first boot:
   ```bash
   mkdir -p docker/airflow
   echo '{"admin": "admin"}' > docker/airflow/simple_auth_manager_passwords.json.generated
   ```
   Without this file, `simple_auth_manager` falls back to auto-generating a random password and
   printing it to the `airflow-apiserver` container logs — the container still boots fine, so
   there's no obvious error, but `admin`/`admin` won't authenticate against `POST /auth/token` and
   INFRA-03's single-credential-pair requirement silently breaks.

## Known First-Boot Gotcha: Permission Error on the Passwords File

On first boot, the `airflow-apiserver` service may crash with a `PermissionError` when trying to
read/write `simple_auth_manager_passwords.json.generated`. This happens when the bind-mounted
file's host-side permissions don't allow the container's `airflow` user (uid 50000) to read it —
typically because the file was just created by the host user with restrictive default permissions,
and WSL2's bind-mount UID mapping doesn't line up.

**Fix:** `chmod 666 docker/airflow/simple_auth_manager_passwords.json.generated`, then re-run `make
up` (or `docker compose up -d --wait`). This is safe for a throwaway local-dev credential file that
is already gitignored and never leaves your machine.

## Verifying the Stack

After `make up`, confirm everything is actually healthy rather than trusting `docker compose up`'s
own exit code alone:

```bash
docker compose ps --format json   # every service should report healthy or running
uv run python scripts/verify_environment.py   # confirms all 5 Oracle tables + admin/admin Oracle auth
curl -s -X POST http://localhost:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}'  # should return an access_token (admin/admin Airflow auth)
```
