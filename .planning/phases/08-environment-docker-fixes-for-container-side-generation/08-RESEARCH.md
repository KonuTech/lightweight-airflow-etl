# Phase 8: Environment & Docker Fixes for Container-Side Generation - Research

**Researched:** 2026-09-01
**Domain:** Docker Compose bind-mount permission mechanics (Airflow LocalExecutor stack) + Python
subprocess-based container verification
**Confidence:** HIGH

## Summary

This phase has almost no open technical questions left — the upstream v1.1 milestone research
(`STACK.md`/`ARCHITECTURE.md`/`PITFALLS.md`/`SUMMARY.md`) already fully specified the `generator/`
mount path, the `faker==40.37.0` Dockerfile placement, and the `airflow-init` chown pattern for
`data/`. This phase-level research pass exists to (1) live-verify those findings against the
actual running stack in this checkout (all confirmed — see below), and (2) work out the two
things the upstream research did not cover: the exact idempotent shell mechanism for fixing the
`simple_auth_manager_passwords.json.generated` bind-mount-becomes-directory gotcha (D-04/D-05),
and the Python subprocess pattern for extending `scripts/verify_environment.py` with a
`docker compose exec`-based container-side check (D-06/D-07/D-08).

Live verification against the actual running containers in this checkout confirms every gap the
phase targets: `import faker` fails (`ModuleNotFoundError`) inside `airflow-scheduler` today;
`/opt/airflow/generator` does not exist inside the container; `PYTHONPATH` is currently
`/opt/airflow/dags` only (no `/opt/airflow`); `./data` is `root:root` mode `755` on the host, and
a live write attempt as the container's own `uid=50000(airflow) gid=0(root)` identity fails with
`Permission denied` even for `mkdir` at the top level, let alone `touch` inside
`data/customers/`. The passwords file already exists as a real file in this dev checkout (a prior
session applied the documented manual `chmod 666` fix), confirming the *class* of bug (Docker
auto-creates a missing bind-mount path as a directory) without needing to reproduce it fresh.

**Primary recommendation:** Extend the existing `airflow-init` service (already root, already
gating every other service via `depends_on: service_completed_successfully`) with one combined
`bash -c` command that (a) repairs the passwords file if Docker auto-created it as a directory,
writes default content only if no file exists yet, and fixes its permissions; (b) `mkdir -p`s and
`chown -R`s `data/` to `${AIRFLOW_UID:-50000}:0`; add the `generator/` mount (`:ro`) and
`PYTHONPATH` extension to `x-airflow-common`; add `faker==40.37.0` to the Dockerfile's existing
constrained `pip install` line (STACK.md's live-fetched-constraints-file finding, which supersedes
the more cautious PITFALLS.md inference); and extend `scripts/verify_environment.py` with a new
`verify_container_capabilities()`-style function that shells out to `docker compose exec -T
airflow-apiserver` via `subprocess.run(..., capture_output=True, text=True, timeout=...)`,
following the file's existing `AUTH_RETRY_ATTEMPTS` retry-with-backoff discipline for cold-start
races.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| `generator/` importability inside Airflow containers | Container image / runtime config (Dockerfile + compose `volumes`/`PYTHONPATH`) | — | Pure environment wiring — no application code changes; the script's own path arithmetic already does the rest once mounted correctly |
| `faker` dependency availability | Container image (Dockerfile `pip install`) | — | Build-time concern; must exactly match the version already resolved in the root `uv.lock` so host- and container-run generation stay byte-identical |
| `data/` write access for container-side generation | Docker Compose orchestration (`airflow-init` root-user step) | Host filesystem (bind-mount source ownership) | The permission fix must run once, as root, before any non-root Airflow service starts — compose-level `airflow-init`, not a host-side script, is the only mechanism that works identically for both `make up` and raw `docker compose up` |
| Passwords-file bind-mount type correctness | Docker Compose orchestration (`airflow-init` root-user step) | Host filesystem | Same root-cause class and same fix tier as `data/` — Docker's bind-mount auto-create-as-wrong-type behavior is a compose/Docker-Engine-level problem, not something any single Airflow service config can route around |
| Automated regression coverage (import + write-access checks) | Local dev tooling (`scripts/verify_environment.py` + `Makefile` target) | Container runtime (checks run *through* `docker compose exec`, not *in* the script's own process) | Verification code lives at the same tier as the rest of this project's `verify_environment.py`/`make verify` discipline; it reaches into the container tier via subprocess rather than becoming container-resident code itself |
| `docs/environment.md` accuracy | Documentation | — | No runtime tier — purely reflects the new compose-level fixes so a future developer doesn't chase an already-solved gotcha |

## User Constraints

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**docs/environment.md rewrite**
- **D-01:** Once the `airflow-init` chown fix lands, replace `docs/environment.md`'s "First-Clone
  Setup Gaps" step 2 (`mkdir -p data/customers data/orders`) and the "Known First-Boot Gotcha:
  Permission Error Creating `data/<dataset>/`" section — both become obsolete.
- **D-02:** Present **clean current-state only** — no historical note about the old manual
  workaround. User explicitly chose this over keeping a "this used to require X" breadcrumb.
- **D-03:** Briefly document the **new container capability** this phase adds — `generator/` is
  now mounted at `/opt/airflow/generator`, `faker` is installed, importable via extended
  `PYTHONPATH` — so a future developer understands why `generator/` is mounted and how Phase 9's
  DAG can generate files in-process without shelling out.

**Bundled fix: passwords-file bind-mount gotcha**
- **D-04 (scope addition beyond ENV-01/ENV-02's literal text — user-approved, non-default choice
  relative to REQUIREMENTS.md's exact wording):** Phase 8 also fixes
  `docker/airflow/simple_auth_manager_passwords.json.generated` getting auto-created as a
  directory instead of a file (same root-cause class as ENV-02's `data/` problem) — rather than
  leaving it as the documented manual `chmod 666` step. Same phase is already touching
  `airflow-init`/compose, so fixing both gotchas in one pass avoids a near-identical follow-up
  phase later.
- **D-05:** Implementation mechanism is the **same `airflow-init` compose-level pattern** as the
  `data/` chown fix (running once as uid 0, gated by the existing `depends_on:
  service_completed_successfully` chain) — ensures the passwords file exists as a real file with
  correct content/permissions before other services start. User explicitly chose this over a
  Makefile-level pre-flight step (which would only help `make up` callers, not raw `docker compose
  up`).

**Permanent automated verification**
- **D-06:** Phase 8 adds a **permanent, committed check** (not a one-off manual verification
  during execution) proving the container can `import generator.generate_csv` and write into
  `data/customers/`/`data/orders/` — matches this project's established `make verify`/
  `verify-phaseN` discipline and catches a future regression (e.g. someone removing the
  `generator/` mount) automatically.
- **D-07:** This check is added **into `scripts/verify_environment.py`** itself, not a separate
  Makefile-only check. User explicitly accepted that this gives the script a new dependency shape
  it didn't have before — it currently only makes Oracle/HTTP network calls; this check needs a
  `docker compose exec` subprocess call to run code *inside* the container. Chosen to keep all
  environment verification in the one place a developer already knows to run (`make verify`).
- **D-08:** The `data/` write-access check **actually writes then deletes a real probe file**
  inside `data/customers/`/`data/orders/` (with cleanup so it doesn't pollute the directory or
  confuse `csv_ingest`'s `FileSensor` glob pattern) — not just a permission-bits/ownership check.
  User explicitly chose the stronger proof, matching this project's established working preference
  ("don't trust exit codes as proof — confirm by actually doing/querying the real thing").

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

### Deferred Ideas (OUT OF SCOPE)

None — discussion stayed within Phase 8's domain (environment/Docker fixes for container-side
generation). The passwords-file bundling (D-04) is a scope *expansion* within Phase 8 (same
root-cause class as ENV-02), not a new capability belonging to a different phase.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ENV-01 | `generator/generate_csv.py` runs inside the Airflow container — mounted at `/opt/airflow/generator`, importable via an extended `PYTHONPATH`, with `faker==40.37.0` installed in the image (exact match to root `pyproject.toml`/`uv.lock`) | "Integration Point 1" mount/`PYTHONPATH`/Dockerfile findings below; live-verified today's `PYTHONPATH`/mount/faker-absence gaps; `faker==40.37.0` cross-verified via PyPI + slopcheck `[OK]` |
| ENV-02 | The Airflow container can write generated CSVs into `data/<dataset>/` on a genuinely fresh clone — `data/` gets a write-capable permission fix via a compose-level `airflow-init` chown step, not a manual host-side fix | "Integration Point 2" `airflow-init` chown findings below; live-verified today's `Permission denied` on both `mkdir` and `touch` as the container's own `uid=50000:gid=0` identity |
| (new, planner to number — D-04 bundled scope) | Fix `docker/airflow/simple_auth_manager_passwords.json.generated` auto-created-as-directory bind-mount gotcha via the same `airflow-init` root-user mechanism, idempotently, on every `docker compose up` | "Integration Point 3" passwords-file fix mechanism below, including the SimpleAuthManager password-file semantics discovered during this research pass |

</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Reference-repo reuse discipline:** N/A to this phase — this phase touches only
  `docker-compose.yml`, `docker/airflow/Dockerfile`, `docs/environment.md`, and
  `scripts/verify_environment.py`; no CSV-processing logic is ported from the sibling
  `airflow-platform` repo in this phase.
- **Oracle driver / Pydantic / executor / image-tag pins:** N/A to this phase — no Oracle, config,
  or executor code is touched. Any new pins introduced here (`faker==40.37.0`) must be **exact**,
  matching the already-approved root `pyproject.toml`/`uv.lock` value, per the project's own
  general "pin an exact version" discipline (already applied to `oracledb`/`pydantic`/providers in
  the Dockerfile).
- **Conventions/Architecture sections:** Explicitly "not yet established" in CLAUDE.md at time of
  writing — this phase should follow the patterns already present in `docker-compose.yml`/
  `Makefile`/`scripts/verify_environment.py` rather than inventing new ones, consistent with
  CLAUDE.md's fallback instruction to "follow existing patterns found in the codebase."

## Live Verification Against This Checkout (2026-09-01)

The full stack is currently up in this working environment (`docker compose ps` confirms all 6
services healthy). Direct exec checks against the running `airflow-scheduler`/`airflow-apiserver`
containers confirm every gap this phase targets, before any change was made:

| Check | Command | Result |
|-------|---------|--------|
| `faker` importable? | `docker compose exec -T airflow-scheduler python -c "import faker"` | `ModuleNotFoundError: No module named 'faker'` (exit 1) — confirms ENV-01 gap |
| `generator/` mounted? | `docker compose exec -T airflow-scheduler ls /opt/airflow/generator` | `No such file or directory` (exit 2) — confirms ENV-01 gap |
| Container identity | `docker compose exec -T airflow-scheduler id` | `uid=50000(airflow) gid=0(root) groups=0(root)` — matches `docker-compose.yml`'s `user: "${AIRFLOW_UID:-50000}:0"` |
| Current `PYTHONPATH` | `docker compose exec -T airflow-scheduler printenv PYTHONPATH` | `/opt/airflow/dags` only — confirms the extension to `/opt/airflow` has not landed |
| `data/` top-level write | `docker compose exec -T airflow-scheduler sh -c "mkdir -p /opt/airflow/data/newdir"` | `Permission denied` — confirms ENV-02 gap even at the top level |
| `data/customers/` write | `docker compose exec -T airflow-scheduler sh -c "touch /opt/airflow/data/customers/.probe"` | `No such file or directory` (the subdir doesn't even exist yet on this fresh `data/`) |
| Host-side `data/` ownership | `stat data` | `Uid: 0/root Gid: 0/root`, mode `0755` — matches PITFALLS.md's documented failure mode exactly |
| Passwords file state (this dev checkout) | `docker compose exec -T airflow-apiserver sh -c "test -f .../simple_auth_manager_passwords.json.generated"` | `IS_FILE` — this checkout already has the manual fix applied from a prior session, confirming the *fix* works, not reproducing the *bug* fresh |
| `faker==40.37.0` provenance | `pip index versions faker` (PyPI, correct ecosystem) + `slopcheck install faker` | `faker (40.37.0)` present on PyPI; slopcheck verdict `[OK]` |

**Confidence for this section: HIGH** — every row above is a live command run against this actual
checkout's actual running containers in this research session, not inferred from documentation.

## Package Legitimacy Audit

This phase installs exactly one package into the Docker image: `faker==40.37.0`. It is **not** a
newly-discovered package — it is already pinned in the root `pyproject.toml`/`uv.lock` (used today
by `generator/generate_csv.py` on the host via `make generate`), and this phase's only change is
adding it to the *container* image so the same code can run in-process inside Airflow. Ran the
full legitimacy gate anyway per protocol.

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|--------------|-----------|-------------|
| `faker` | PyPI | Long-established (`joke2k/faker`, widely used Python fixture-data library) | Very high (tens of millions/week class library) | `github.com/joke2k/faker` | `[OK]` | Approved |

**Packages removed due to slopcheck [SLOP] verdict:** none.
**Packages flagged as suspicious [SUS]:** none.

**Cross-ecosystem note (verified, not assumed):** `npm view faker version` resolves to `6.6.6` — a
**completely different, unrelated JavaScript package** on a different registry that has its own
well-documented 2022 sabotage/deprecation history. This is exactly the "cross-ecosystem name
confusion" hazard the legitimacy protocol warns about. This phase's `faker` is unambiguously the
**PyPI** package (`pip install faker`), already resolved correctly in `uv.lock`, and must never be
confused with or referenced via `npm`. No action needed — flagged here only so a future reader
never mixes them up.

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|---------------|
| `faker` | `==40.37.0` | Realistic fake string values inside `generator/generate_csv.py`, now runnable inside the Airflow container | **Must match, not approximate**, the version already resolved in root `uv.lock` — `[VERIFIED: PyPI registry + slopcheck OK]`. This is a `[CITED: STACK.md]` recommendation from the upstream v1.1 milestone research, re-confirmed live in this pass via `pip index versions faker` and `slopcheck`. |

No other new libraries. This phase is Dockerfile/compose/docs/verification-script wiring only.

### Supporting

None new. `generator/generate_csv.py`'s only non-stdlib imports are `faker` (now addressed) and
`csv_processor.config` (already installed in the image via the existing `pip install --no-deps
packages/csv-processor/` line — `[CITED: STACK.md]`, unaffected by this phase).

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Adding `faker` to the Dockerfile's existing first (constrained) `pip install` call | A separate, second unconstrained `pip install` call (like `clevercsv`/`chardet`) | STACK.md live-fetched Airflow's own `constraints-3.3.1/constraints-3.12.txt` and confirmed `faker` has **zero** entry there — `[VERIFIED: live curl against raw.githubusercontent.com/apache/airflow, re-confirmed conceptually in this pass via PyPI/slopcheck]`. There is nothing for `--constraint` to conflict with, so the constrained line is safe and keeps the Dockerfile from growing a third pip-install stanza. Use the unconstrained line only if a real `docker compose build` throws `ResolutionImpossible` (verification step per ARCHITECTURE.md, not expected). |
| `airflow-init` root-user chown/repair step for both `data/` and the passwords file | A host-side pre-flight script invoked from the `Makefile` (`make up`'s own recipe) | Rejected per D-05 (user-locked) — a `Makefile`-level fix only protects `make up` callers, not `docker compose up` invoked directly (e.g. from CI, or a developer who forgot the `make` wrapper). The compose-level fix protects both call paths uniformly. |
| Write-then-delete probe file for `data/` write-access verification | A permission-bits/`os.access()`-style check | Rejected per D-08 (user-locked) — this project's established "verify by actually doing the real thing" discipline; a bits-only check can pass while the actual `write_staged()` staging-dir-creation flow still fails for a reason bits don't capture (e.g. a stale bind-mount type on the parent, or SELinux/AppArmor in a different host environment). |

**Installation:**
```dockerfile
# docker/airflow/Dockerfile — add faker to the EXISTING constrained pip install line.
RUN pip install --no-cache-dir \
      "oracledb==4.0.2" \
      "pydantic==2.13.4" \
      "apache-airflow-providers-standard==1.17.0" \
      "apache-airflow-providers-oracle==4.6.2" \
      "faker==40.37.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
 && pip install --no-cache-dir \
      "clevercsv==0.8.5" \
      "charset-normalizer==3.5.1" \
      "chardet==7.6.0"
```

**Version verification (re-run in this research session):**
```
$ pip index versions faker
faker (40.37.0)
Available versions: 40.37.0, 40.36.0, ...
```
`40.37.0` is the latest available version on PyPI as of this research pass, and it is the exact
version already pinned in this project's own root `pyproject.toml:14` and resolved in `uv.lock`.
`[VERIFIED: PyPI registry]`.

## Architecture Patterns

### System Architecture Diagram

```
                     ┌─────────────────────────────────────────────┐
   docker compose    │   airflow-init (user: "0:0", root)           │
   up  ──────────────▶   depends_on: postgres (healthy)             │
   (or make up)      │                                               │
                      │   command: bash -c "                         │
                      │     # -- passwords-file repair (D-04/D-05) --│
                      │     [ -d PWFILE ] && rmdir PWFILE             │
                      │     [ -f PWFILE ] || echo '{...}' > PWFILE    │
                      │     chown 50000:0 PWFILE && chmod 664 PWFILE  │
                      │                                               │
                      │     # -- data/ write-access fix (ENV-02) --   │
                      │     mkdir -p /opt/airflow/data/{customers,orders}│
                      │     chown -R 50000:0 /opt/airflow/data        │
                      │                                               │
                      │     exec airflow db migrate                   │
                      │   "                                           │
                      └───────────────────┬───────────────────────────┘
                                           │ depends_on:
                                           │ service_completed_successfully
                                           ▼
        ┌───────────────────────────────────────────────────────────────┐
        │  airflow-apiserver / scheduler / dag-processor / triggerer     │
        │  (user: "${AIRFLOW_UID:-50000}:0", non-root)                   │
        │                                                                 │
        │  volumes (unchanged + new):                                    │
        │    ./docker/airflow/simple_auth_manager_passwords.json.generated│
        │    ./airflow/dags:/opt/airflow/dags                            │
        │    ./data:/opt/airflow/data              (now writable)        │
        │    ./configs:/opt/airflow/configs:ro                            │
        │    ./generator:/opt/airflow/generator:ro  ◀── NEW               │
        │                                                                 │
        │  PYTHONPATH: "/opt/airflow/dags:/opt/airflow"  ◀── EXTENDED    │
        │                                                                 │
        │  → from generator.generate_csv import main   (Phase 9 only;    │
        │    this phase proves the capability, doesn't wire the DAG)     │
        └───────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
        ┌───────────────────────────────────────────────────────────────┐
        │  make verify  →  scripts/verify_environment.py                 │
        │    verify_tables/columns (unchanged, Oracle)                   │
        │    verify_airflow_auth (unchanged, HTTP)                       │
        │    verify_container_capabilities()  ◀── NEW (D-06/D-07/D-08)  │
        │      subprocess.run(["docker","compose","exec","-T",           │
        │        "airflow-apiserver","python","-c",                      │
        │        "from generator.generate_csv import main"])             │
        │      subprocess.run([...write+delete probe file in             │
        │        data/customers/ and data/orders/...])                   │
        └───────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

No new files/directories — this phase only edits existing files:
```
docker-compose.yml                 # + generator/ mount, PYTHONPATH extension, airflow-init command
docker/airflow/Dockerfile          # + faker==40.37.0 in the constrained pip install line
docs/environment.md                # rewritten per D-01/D-02/D-03
scripts/verify_environment.py      # + verify_container_capabilities() (or similarly named function)
Makefile                           # + verify-phase8 target (follows verify-phase2..7 pattern)
```

### Pattern 1: `airflow-init` root-user repair-and-chown step (ENV-02, D-04/D-05)

**What:** A single `command: bash -c "..."` block on the `airflow-init` service, overriding the
anchor's non-root `user:` to `"0:0"` for this one service only, that repairs both bind-mount
gotchas before running its existing `db migrate` job.

**When to use:** Any time a bind-mounted host path needs to exist as a specific type
(file vs. directory) with specific ownership *before* any non-root service in the same compose
project starts — this is the general shape for "fix a bind-mount problem at the compose level,"
reusable beyond just these two paths if a third similar gotcha ever appears.

**Example (adapting Apache Airflow's own official quick-start `docker-compose.yaml` pattern,
`[CITED: apache/airflow docker-compose.yaml howto docs]`, combined with this phase's D-04/D-05
passwords-file extension):**

```yaml
# docker-compose.yml
airflow-init:
  <<: *airflow-common
  user: "0:0"                      # root override, this service only
  depends_on:
    postgres:
      condition: service_healthy
  command: >
    bash -c "
      PWFILE=/opt/airflow/simple_auth_manager_passwords.json.generated;
      if [ -d \"$$PWFILE\" ]; then rmdir \"$$PWFILE\"; fi;
      if [ ! -f \"$$PWFILE\" ]; then echo '{\"admin\": \"admin\"}' > \"$$PWFILE\"; fi;
      chown ${AIRFLOW_UID:-50000}:0 \"$$PWFILE\";
      chmod 664 \"$$PWFILE\";
      mkdir -p /opt/airflow/data/customers /opt/airflow/data/orders;
      chown -R ${AIRFLOW_UID:-50000}:0 /opt/airflow/data;
      exec airflow db migrate
    "
  environment:
    <<: *airflow-common-env
```

**Idempotency rationale (why this is safe to run on every `make up`, not just first boot):**
- `rmdir` only succeeds on an *empty* directory — Docker's auto-create-as-directory always
  produces an empty directory, so this is a targeted, safe removal of exactly the wrongly-typed
  auto-created path, never a destructive `rm -rf` that could delete real content.
- `[ ! -f "$PWFILE" ] || echo ... > "$PWFILE"` **only writes default content if no file currently
  exists** — this deliberately preserves any password content the running `simple_auth_manager`
  may have already written into the file on a prior boot (see "SimpleAuthManager password-file
  semantics" below), rather than stomping it every restart.
- `chown -R`/`mkdir -p` are both safe to repeat — `[CITED: ARCHITECTURE.md]`, already the
  established rationale for the `data/` half of this same command.

### Pattern 2: `scripts/verify_environment.py` subprocess-based container-exec check (D-06/D-07/D-08)

**What:** A new standalone function, following the file's existing `verify_tables`/
`verify_columns`/`verify_airflow_auth` convention (module-level function, `assert`-based,
importable/testable), that shells out to `docker compose exec -T <service> python -c "..."` to
prove import + write access *inside* the actual running container, not just inside the
verification script's own process.

**When to use:** Any verification that must prove something true *inside a specific running
container's own filesystem/Python environment* rather than something reachable over the network
(Oracle, HTTP) — the two existing checks in this file are both network-reachable-from-the-host
checks; this is the file's first container-exec-shaped check.

**Recommended subprocess pattern** (`[ASSUMED]` — synthesized from Python's own `subprocess`
stdlib documentation and this file's existing retry-with-backoff style; not sourced from a
project-specific example since this file has never called `subprocess` before):

```python
import subprocess

CONTAINER_EXEC_TIMEOUT_SECONDS = 30
CONTAINER_EXEC_RETRY_ATTEMPTS = 3
CONTAINER_EXEC_RETRY_BASE_DELAY_SECONDS = 1.0


def _docker_compose_exec(service: str, python_code: str) -> str:
    """Run `python_code` inside `service` via `docker compose exec -T`, returning stdout.

    Raises AssertionError (matching this file's existing assert-based failure convention)
    with the captured stderr on a non-zero exit code, or on a timeout. Retries a bounded
    number of times with backoff on the specific case of the exec itself failing to reach
    a container that isn't ready yet (mirrors verify_airflow_auth's AUTH_RETRY_* pattern
    for a cold-start race) -- NOT on the Python code's own assertion failures, which should
    surface immediately as a real bug, not be retried away.
    """
    last_error: str = ""
    for attempt in range(1, CONTAINER_EXEC_RETRY_ATTEMPTS + 1):
        try:
            result = subprocess.run(
                ["docker", "compose", "exec", "-T", service, "python", "-c", python_code],
                capture_output=True,
                text=True,
                timeout=CONTAINER_EXEC_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            last_error = f"docker compose exec timed out after {CONTAINER_EXEC_TIMEOUT_SECONDS}s"
            if attempt < CONTAINER_EXEC_RETRY_ATTEMPTS:
                time.sleep(CONTAINER_EXEC_RETRY_BASE_DELAY_SECONDS * attempt)
                continue
            raise AssertionError(last_error) from exc

        if result.returncode == 0:
            return result.stdout

        last_error = (
            f"docker compose exec into {service!r} exited {result.returncode}: "
            f"{result.stderr.strip()}"
        )
        # Only retry if the container itself looks not-yet-ready (a cold-start race,
        # same class as G-01-1/AUTH_RETRY_ATTEMPTS) -- a real Python exception inside the
        # exec'd code should fail immediately, not be retried into a false pass.
        if "is not running" in result.stderr or "Container" in result.stderr and attempt < CONTAINER_EXEC_RETRY_ATTEMPTS:
            time.sleep(CONTAINER_EXEC_RETRY_BASE_DELAY_SECONDS * attempt)
            continue
        raise AssertionError(last_error)

    raise AssertionError(last_error)


def verify_generator_importable(service: str = "airflow-apiserver") -> None:
    """Assert `from generator.generate_csv import main` succeeds inside `service` (ENV-01, D-06)."""
    _docker_compose_exec(
        service,
        "from generator.generate_csv import main; print('IMPORT_OK')",
    )


def verify_data_write_access(service: str = "airflow-apiserver") -> None:
    """Assert `service` can write-then-delete a real probe file under both
    data/customers/ and data/orders/ (ENV-02, D-08) -- not a permission-bits check.

    Probe filename deliberately does not match customers_*.csv*/orders_*.csv* so it
    can never be picked up by csv_ingest's FileSensor glob pattern.
    """
    for dataset in ("customers", "orders"):
        probe_path = f"/opt/airflow/data/{dataset}/.verify_write_probe"
        _docker_compose_exec(
            service,
            f"from pathlib import Path; p = Path({probe_path!r}); "
            f"p.write_text('probe'); assert p.read_text() == 'probe'; p.unlink(); "
            f"print('WRITE_OK')",
        )
```

**Why `-T` matters:** `docker compose exec` allocates a pseudo-TTY by default; `-T` disables that,
which is required for the subprocess's stdout/stderr to be reliably captured as plain text by
`subprocess.run(capture_output=True)` rather than interleaved with TTY control sequences —
`[ASSUMED]`, standard, widely-documented `docker compose exec` behavior, not verified against this
project's exact Docker Compose version in this pass, but already the established convention in
this repo's own `Makefile` (`verify-phase5`'s `docker compose exec -T airflow-scheduler python -c
"..."` line already uses `-T` for exactly this reason).

**Which service to exec into:** `airflow-apiserver` is `[ASSUMED]`, chosen for consistency with
this project's own precedent — `verify-phase5`'s existing `docker compose exec` call in the
`Makefile` targets `airflow-scheduler`. Since both services share the identical `x-airflow-common`
mounts/`PYTHONPATH`/image, either works equivalently for this check; the planner should pick one
and use it consistently, or accept either as a discretionary detail (not a locked decision in
CONTEXT.md).

### Anti-Patterns to Avoid

- **Overwriting the passwords file's content unconditionally on every `airflow-init` run:**
  Would stomp any password `simple_auth_manager` itself may have written for additional
  users beyond the one this project's `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "admin:admin"`
  config declares. Guard the `echo` with `[ ! -f "$PWFILE" ]` so it only seeds default content
  when the file is genuinely missing (post-repair), never when it already exists with real content.
- **Trying to fix the passwords-file directory-vs-file mismatch from the host, outside compose:**
  Per Pitfall 3 (`[CITED: PITFALLS.md]`), a host-side `rm -rf`/`touch` fix followed by
  `docker compose restart` (not a full `down && up`) can still serve a stale cached bind-mount
  inode reference on Docker Desktop/WSL2. The `airflow-init`-internal fix in Pattern 1 above
  sidesteps this entirely because it repairs the mount *before* any of the long-lived services
  (`apiserver`/`scheduler`/etc.) are even created — those containers get created fresh, after the
  repair, so they never see the stale type.
- **Catching every `docker compose exec` failure as a retry-worthy transient condition:** A
  genuine `AssertionError`/`ImportError` raised by the exec'd Python code itself (a real bug —
  e.g., the `generator/` mount really is missing) must surface immediately as a test failure, not
  be silently retried into 3 attempts of the same real failure before finally raising. Only retry
  on evidence the *container itself* wasn't ready yet (e.g. `docker compose exec` failing because
  the container isn't running), mirroring `verify_airflow_auth`'s existing narrow retry-condition
  discipline (retries `URLError`/`OSError`/etc., never retries a genuine `HTTPError`).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|--------------|-----|
| Ensuring a bind-mounted file exists with correct type/permissions before other services start | A custom health-check script, a wait-for-it wrapper, or an external orchestration tool | The existing `airflow-init` service + its already-wired `depends_on: service_completed_successfully` gate | This project already has the exact right primitive in place (used for `db migrate` today) — extending its one `command:` string is strictly simpler than introducing any new tooling, and it is the pattern Airflow's own official quick-start compose file already uses for the identical class of problem (`[CITED: apache/airflow docker-compose.yaml`). |
| Verifying a container can do something (import a module, write a file) | A custom test harness that spins up its own container, or a shell script wrapping `docker exec` with manual output parsing | Python's own `subprocess.run(capture_output=True, text=True)` inside `scripts/verify_environment.py`, following the file's existing assert-based convention | `subprocess.run` already gives structured `returncode`/`stdout`/`stderr` access with a built-in `timeout` parameter — no need for manual process-group management or a separate shell script that then needs its own error-handling conventions. |

**Key insight:** Both of this phase's "don't hand-roll" items exist because this project already
has an established mechanism one layer up (compose's `airflow-init` service; Python's `subprocess`
stdlib) that fully covers the need — the discipline here is *extending* those, not reaching for a
new tool.

## SimpleAuthManager Password-File Semantics (new finding this pass)

**Not covered by the upstream v1.1 milestone research** — discovered during this phase-level
research pass via WebSearch, cross-referenced against the actual compose config and the file's
current committed content in this checkout.

`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "admin:admin"` is **not** a `username:password` pair —
it is `username:role`, where `admin` (the role) happens to share the same literal string as the
username in this project's config. `[CITED: airflow.apache.org/docs/apache-airflow/stable/
core-concepts/auth-manager/simple/index.html]`: "lists users and their associated roles... each
user-role as a colon delimited couple of username and role." Roles are `viewer`/`user`/`op`/
`admin`.

The actual **password** for each declared user is either (a) auto-generated by
`simple_auth_manager` and printed to the `airflow-apiserver` logs on first boot, or (b) read from
whatever is already in `core.simple_auth_manager_passwords_file` (this project's
`docker/airflow/simple_auth_manager_passwords.json.generated`) if an entry for that username
already exists there. This confirms exactly what `docs/environment.md`'s existing "First-Clone
Setup Gaps" section already asserts (pre-seeding `{"admin": "admin"}` is what makes `admin`/`admin`
the *actual* login password, not just the declared role) — `[VERIFIED: this checkout's own file
content, `{"admin": "admin"}`, cross-referenced against the official docs' generation behavior]`.

**Implication for the airflow-init fix (Pattern 1 above):** the repair step must seed
`{"admin": "admin"}` **only when the file doesn't already exist** — if `simple_auth_manager` has
already written a real (possibly regenerated) password into the file on a prior boot, an
unconditional overwrite on every subsequent `airflow-init` run would silently reset it back to the
static local-dev value. For this project's single-admin-user, static-credential local-dev scope
(INFRA-03), that reset-to-`admin`/`admin` behavior is actually *desirable* (deterministic,
matches this project's own documented single-credential-pair convention) — but the guard should
still be `[ ! -f "$PWFILE" ]` rather than an unconditional overwrite, so a developer who manually
edits the file for some local reason isn't silently fought by every `make up`. This is a
discretionary implementation nuance, not a locked decision — flagged here for the planner's
awareness.

## Common Pitfalls

*(Carried forward from the upstream v1.1 research; re-verified live against this checkout in this
pass — see the Live Verification table above. Only pitfalls directly relevant to Phase 8's scope
are reproduced here; Pitfalls 1/6/7/8/9/10 belong to Phase 9/10 and are intentionally omitted.)*

### Pitfall 2: The old `data/` fix only ever solved reads, never writes

**What goes wrong:** `docs/environment.md`'s existing manual `mkdir -p data/customers data/orders`
step creates the directories under the *host* user's ownership at default mode — sufficient for a
container to `read`/traverse (mode 755 grants "other" read+execute) but **not** to write (only the
owner gets the write bit at 755). Live-confirmed in this session: even the container's own
`mkdir -p /opt/airflow/data/newdir` at the *top level* (which doesn't even require the
`customers`/`orders` subdirs to pre-exist) fails with `Permission denied` as `uid=50000:gid=0`
against a `root:root` mode-755 `data/`.

**How to avoid:** `chown -R ${AIRFLOW_UID:-50000}:0` (not just `mkdir -p`) inside `airflow-init`,
before any other service starts. `[CITED: ARCHITECTURE.md, PITFALLS.md]`.

**Phase to address:** This phase (Environment/Docker fix phase). ✔ verified live, gap confirmed.

### Pitfall 3: Docker/WSL2 auto-creates a missing bind-mount path as the wrong type; in-place fixes can be masked by inode caching

**What goes wrong:** Already hit once in this project's own history (`05-02-SUMMARY.md`) for
`simple_auth_manager_passwords.json.generated` — Docker created it as a directory on first mount.
`[CITED: PITFALLS.md]` warns that a simple host-side file swap + `docker compose restart` (not a
full `down && up`) can still serve a stale cached bind-mount inode reference on Docker
Desktop/WSL2.

**How to avoid:** Perform the repair (`rmdir` the wrongly-typed directory, then create the real
file) **inside `airflow-init`'s own startup command**, before the other long-lived service
containers are even created (Pattern 1 above) — this sidesteps the WSL2 caching class of failure
entirely, because those containers are created fresh *after* the repair runs, rather than needing
an in-place swap on an already-running container.

**Phase to address:** This phase.

### Pitfall 4/5: Editing the Dockerfile without rebuilding keeps a stale image; wrong `pip install` stanza risks `ResolutionImpossible`

**What goes wrong:** `docker compose up -d` alone does not rebuild an already-existing local image
— a Dockerfile edit needs an explicit `docker compose build` (or `up -d --build`) follow-up, or
the failure surfaces confusingly later as `ModuleNotFoundError: No module named 'faker'` deep
inside a generate task. Separately, adding `faker` to the *wrong* (constrained vs. unconstrained)
`pip install` call risks `ResolutionImpossible`.

**How to avoid:** `[CITED: STACK.md]`'s live-fetched-constraints-file finding (faker absent from
Airflow's `constraints-3.3.1/constraints-3.12.txt`) makes the constrained line safe — confirmed
again in this pass via `pip index versions faker`. Any Dockerfile change in this phase must be
followed by `docker compose build`/`make rebuild` before `up`, verified with an actual
`import faker` exec check (Pattern 2's `verify_generator_importable`-style check, or a manual
one-off during execution) before considering the fix complete.

**Phase to address:** This phase.

## Code Examples

### Full `docker-compose.yml` diff shape (composited from Patterns 1 + Integration Point 1)

```yaml
# Source: this repo's own docker-compose.yml (read directly), Apache Airflow's official
# quick-start docker-compose.yaml (CITED, airflow-core/docs/howto/docker-compose/docker-compose.yaml)
x-airflow-common: &airflow-common
  environment: &airflow-common-env
    # ... existing keys unchanged ...
    PYTHONPATH: "/opt/airflow/dags:/opt/airflow"   # EXTENDED (was "/opt/airflow/dags")
  volumes:
    - ./docker/airflow/simple_auth_manager_passwords.json.generated:/opt/airflow/simple_auth_manager_passwords.json.generated
    - ./airflow/dags:/opt/airflow/dags
    - ./data:/opt/airflow/data
    - ./configs:/opt/airflow/configs:ro
    - ./generator:/opt/airflow/generator:ro   # NEW
    - airflow-logs:/opt/airflow/logs

services:
  airflow-init:
    <<: *airflow-common
    user: "0:0"   # NEW: root override, this service only
    depends_on:
      postgres:
        condition: service_healthy
    command: >   # CHANGED: was "db migrate"
      bash -c "
        PWFILE=/opt/airflow/simple_auth_manager_passwords.json.generated;
        if [ -d \"$$PWFILE\" ]; then rmdir \"$$PWFILE\"; fi;
        if [ ! -f \"$$PWFILE\" ]; then echo '{\"admin\": \"admin\"}' > \"$$PWFILE\"; fi;
        chown ${AIRFLOW_UID:-50000}:0 \"$$PWFILE\";
        chmod 664 \"$$PWFILE\";
        mkdir -p /opt/airflow/data/customers /opt/airflow/data/orders;
        chown -R ${AIRFLOW_UID:-50000}:0 /opt/airflow/data;
        exec airflow db migrate
      "
    environment:
      <<: *airflow-common-env
```

### `Makefile` — `verify-phase8` target (follows `verify-phase2..7` shape)

```makefile
# Source: this repo's own Makefile (read directly), verify-phase5/6/7's existing shape
verify-phase8:     ## Phase 8's own combined local gate: fresh-clone chown/mount fixes + container capability checks (requires `make up` first)
	uv run python scripts/verify_environment.py
```
No new unit-testable logic is introduced by this phase (it's compose/Dockerfile/docs wiring), so
`verify-phase8` is just the extended `verify_environment.py` run — `[ASSUMED]`, planner's call
per CONTEXT.md's "Claude's Discretion" on exact shape; consistent with the project's existing
"phases with no new fixtures/unit surface still get a `verify-phaseN` target" precedent.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|-------------------|---------------|--------|
| Manual `mkdir -p data/customers data/orders` before first `docker compose up`, documented in `docs/environment.md` | Compose-level `airflow-init` root-user `chown -R` step, gated by the existing `depends_on` chain | This phase | Removes a human-memory dependency entirely; also the *first* fix in this project's history that grants write (not just read) access, needed now that generation moves inside the container (Phase 9) |
| Manual `chmod 666` on the passwords file after a `PermissionError` crash, documented as a reactive "Known First-Boot Gotcha" | Proactive `airflow-init` repair (type-check + seed-if-missing + chown/chmod), before any service ever hits the crash | This phase | Converts a reactive, discovery-after-crash fix into a preventive one; the underlying `IsADirectoryError`-class bug (already hit once per `05-02-SUMMARY.md`) can no longer surface to a developer at all |

**Deprecated/outdated:**
- `docs/environment.md`'s "First-Clone Setup Gaps" step 2 (manual passwords-file creation) and
  "Known First-Boot Gotcha: Permission Error Creating `data/<dataset>/`" section — both become
  obsolete once this phase's fixes land, per D-01. The `.env` manual-copy step (First-Clone Setup
  Gaps step 1) is untouched/unrelated and stays.
- `.github/workflows/ci.yml`'s `mkdir -p data/customers data/orders` pre-create step becomes
  redundant (not wrong — `docker compose up`'s subsequent `airflow-init chown -R` would immediately
  reassign ownership to uid 50000 anyway) once this phase's fix lands. Removing it is out of this
  phase's literal locked scope (CONTEXT.md doesn't mention `ci.yml`) — flagged as an Open Question
  below for the planner to decide whether to fold in.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|-----------------|
| A1 | The exact `subprocess.run`/retry-wrapper shape shown in "Pattern 2" (`_docker_compose_exec` helper, `CONTAINER_EXEC_RETRY_ATTEMPTS`/`CONTAINER_EXEC_TIMEOUT_SECONDS` constants) | Architecture Patterns → Pattern 2 | Low — this is a code-structure suggestion, not a claim about external behavior; the planner/implementer can adjust the exact function shape freely as long as it uses `subprocess.run(capture_output=True, text=True, timeout=...)` and follows the file's existing assert-based failure convention. If the shape is wrong, the fix is a straightforward code edit, not a re-architecture. |
| A2 | `docker compose exec` targeting `airflow-apiserver` (vs. `airflow-scheduler`, which `verify-phase5`'s existing Makefile check uses) is an equally valid choice | Architecture Patterns → Pattern 2 | Low — both services share identical mounts/`PYTHONPATH`/image; picking the "wrong" one has zero functional difference, only a minor consistency question with the existing `Makefile` precedent. |
| A3 | `chmod 664` (not `666`) on the repaired passwords file is sufficient, given `chown` already sets the owning user to `${AIRFLOW_UID:-50000}` | SimpleAuthManager Password-File Semantics / Pattern 1 | Low-medium — if `simple_auth_manager`'s own re-write-on-generation logic runs as a *different* uid/gid than expected (untested in this pass, since airflow-apiserver isn't the one performing the chown), `664` might not grant it write access and `666` (matching the currently-applied manual-fix value in `docs/environment.md`) may be needed instead. Verify empirically during phase execution: attempt a real `docker compose up` from a genuinely-wiped `data`/passwords-file state and confirm `airflow-apiserver` doesn't crash with a `PermissionError` on this file. |
| A4 | `docker compose exec` failure-string matching (`"is not running"` / `"Container"` substring checks in `_docker_compose_exec`) reliably distinguishes "container not ready yet" from "container ready, code inside failed" | Architecture Patterns → Pattern 2 | Medium — this exact string-matching heuristic was not verified against a live, reproduced cold-start race in this research pass (unlike `verify_airflow_auth`'s `AUTH_RETRY_*`, which *was* verified against a real, documented, timed race in `.planning/debug/apiserver-auth-connreset.md`). If the real Docker Compose CLI's error text differs from this guess, the retry logic may either never fire (falling back to immediate failure, which is safe — just less resilient to a genuine cold-start race) or fire on the wrong condition (masking a real bug as a retry). Recommend implementer verify against this project's actual `docker compose version`'s error text during execution, or simplify to "always retry N times regardless of error text, since N is small and bounded" if the distinction proves unreliable in practice. |

**If this table is empty:** N/A — see rows above. All core factual claims about the *existing*
codebase, pinned versions, and package registries in this document are `[VERIFIED]`/`[CITED]`; the
assumptions above are specifically about the *new* code this phase introduces (the verification
script's subprocess helper), which is inherently prescriptive/design-level rather than a factual
claim needing external verification.

## Open Questions

1. **Should `.github/workflows/ci.yml`'s now-redundant `mkdir -p data/customers data/orders` step
   be removed as part of this phase, or left as harmless belt-and-suspenders?**
   - What we know: `[CITED: ARCHITECTURE.md]` — once the `airflow-init chown -R` fix lands, this CI
     step becomes redundant (not wrong) because `docker compose up`'s subsequent chown immediately
     reassigns ownership to uid 50000 regardless of who pre-created the directory.
   - What's unclear: CONTEXT.md's locked decisions don't mention `ci.yml` at all — D-01/D-02/D-03
     only scope `docs/environment.md`. It's ambiguous whether "clean current-state only" (D-02)
     extends to CI config, or whether that's considered out of this phase's literal file-touch list.
   - Recommendation: Treat as the planner's discretion — leaving it is safe (redundant, not
     harmful) and requires no CI-behavior verification; removing it is a small additional diff
     that keeps CI config in sync with the new documented reality. Either is defensible; flag for
     a quick explicit choice during planning rather than silently deciding either way.

2. **Exact `chmod` mode for the repaired passwords file — `664` (this research's suggestion) vs.
   `666` (the value already documented/applied in this project's existing manual-fix instructions)?**
   - What we know: The file is owned by `uid 50000` after `chown` either way; owner-write bits
     (`6xx`) already grant the running `airflow-apiserver` (which runs as exactly that uid) write
     access regardless of the group/other bits.
   - What's unclear: Whether any other in-container process needs group- or other-write access to
     this specific file beyond the uid-50000 owner (unlikely, since every Airflow service in this
     compose file runs as the identical `${AIRFLOW_UID:-50000}:0` identity) — not verified live in
     this pass since the passwords file already existed correctly in this checkout, so the repair
     path was not exercised end-to-end against a truly fresh (missing) file.
   - Recommendation: Use `664` as a tighter default (owner read/write, group read, no other-write);
     fall back to `666` (matching the currently-documented value) only if a real `make destroy &&
     make up` run against this fix hits a permission error the tighter mode doesn't cover. This is
     a five-minute empirical check during phase execution, not a research gap requiring further
     investigation.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| Docker Engine + Compose v2 plugin | All of this phase's verification steps | ✓ | Docker `29.7.2`, `docker compose` v2 (confirmed live in this session) | — |
| `faker` (PyPI) | ENV-01 | ✓ (verified on PyPI registry; not yet installed in the container image — that's this phase's job) | `40.37.0` (latest, matches root `uv.lock`) | — |
| `slopcheck` (dev tooling, not a runtime dependency) | Package Legitimacy Audit | ✓ | `0.6.1` (installed via `pip install --user` in this research session) | Not needed at execution time — this is a research-time tool only |

**Missing dependencies with no fallback:** none.
**Missing dependencies with fallback:** none — this is a code/config-only phase against an
already-fully-provisioned local stack (postgres/oracle/5 Airflow services all already running and
healthy in this checkout).

## Validation Architecture

*(`workflow.nyquist_validation` not found as explicitly `false` in `.planning/config.json` — this
project does not currently have a `.planning/config.json` file at all in this checkout, so per the
"absent = enabled" default, this section is included.)*

### Test Framework

| Property | Value |
|----------|-------|
| Framework | `pytest` (project-wide, per `Makefile`'s `verify-phaseN` targets) — but this phase's own gate is **not** pytest-based; it's a live-stack `docker compose exec` check, matching `verify-phase5`'s precedent of a live exec check alongside (not replacing) the pytest suite |
| Config file | none new — this phase adds no new unit-testable pure-Python logic (it's Dockerfile/compose/docs edits + one new function in an existing script) |
| Quick run command | `uv run python scripts/verify_environment.py` (requires `make up` first) |
| Full suite command | `uv run pytest tests/unit/ -x && uv run python scripts/verify_environment.py` (mirrors `verify-phase4`'s pattern of "unit suite + live check," even though this phase adds no new unit tests itself — the existing unit suite must still pass since compose/Dockerfile changes could theoretically break an existing test's fixture path, though none is expected to) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|---------------------|--------------|
| ENV-01 | `faker` importable + `generator.generate_csv` importable inside the container | live exec (not unit-testable — requires a real running container) | `docker compose exec -T airflow-apiserver python -c "import faker; from generator.generate_csv import main"` (wrapped by the new `verify_generator_importable()`) | ❌ Wave 0 — function doesn't exist yet in `scripts/verify_environment.py` |
| ENV-02 | Container can write-then-delete a real file in `data/customers/`/`data/orders/` on a genuinely fresh clone | live exec + fresh-clone dry run | `docker compose exec -T airflow-apiserver python -c "..."` (wrapped by the new `verify_data_write_access()`); fresh-clone proof via `make destroy && make up` then re-running the check | ❌ Wave 0 |
| D-04 bundled fix | `docker compose up` (repeated) never crashes on the passwords file being a directory | live/manual — genuinely reproducing "Docker auto-creates a missing bind-mount path as a directory" requires actually deleting the file and running a truly fresh `docker compose up`, since this checkout's file already exists correctly | Manual verification step during execution: `rm -f docker/airflow/simple_auth_manager_passwords.json.generated && docker compose down -v && docker compose up -d --wait` and confirm no `PermissionError`/`IsADirectoryError` in any service's logs | N/A — not automatable as a committed regression test without deliberately destroying local dev state each run; the idempotency requirement (Success Criterion 3) *is* automatable: run `make up` twice in a row against an already-initialized `data/`/passwords-file state and confirm the second run exits 0 |

### Sampling Rate

- **Per task commit:** `uv run python scripts/verify_environment.py` (requires `make up`/`make
  rebuild` already run once per the task's own instructions).
- **Per wave merge:** `uv run pytest tests/unit/ -x && uv run python scripts/verify_environment.py`.
- **Phase gate:** Full `make destroy && make up` (genuinely fresh state) followed by
  `scripts/verify_environment.py`, per Success Criteria 2 and 3's own literal wording ("a genuinely
  fresh clone," "re-running `make up`... does not fail").

### Wave 0 Gaps

- [ ] `scripts/verify_environment.py` — add `verify_generator_importable()` and
      `verify_data_write_access()` (or equivalently named functions), wired into `main()`.
- [ ] `Makefile` — add `verify-phase8` target.
- [ ] No new `tests/unit/` file needed — this phase's checks are inherently live-stack-dependent
      (they exec into a running container), matching the existing precedent that `verify-phase5`'s
      `BundleDagBag` check also lives in the `Makefile`/verification layer, not `tests/unit/`.

*(No pytest-level gaps — this phase's verification surface is entirely live-stack-exec-based, by
design, matching the project's own established split between `tests/unit/` (pure logic) and
`scripts/verify_environment.py`/`Makefile` live checks (anything requiring a running Docker stack).)*

## Security Domain

*(`security_enforcement` not found explicitly `false` in this checkout's `.planning/config.json`
— no such file exists — so per "absent = enabled," this section is included. Scope is
appropriately minimal: this phase touches Docker Compose permission wiring and a local
verification script, not application-level auth/input-handling surface.)*

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|-----------------|---------|----------------------|
| V2 Authentication | Indirectly | Not new to this phase — the passwords-file fix (D-04) *preserves* the existing `admin`/`admin` single-credential local-dev pattern (already an accepted, documented INFRA-03 tradeoff for this project's local-only scope); this phase does not change the authentication mechanism itself, only its bind-mount reliability |
| V3 Session Management | No | Out of scope — no session/token logic touched |
| V4 Access Control | No | Out of scope |
| V5 Input Validation | No | This phase introduces no new user-facing input surface — the new `verify_environment.py` function takes no external input, and the `airflow-init` shell command has no variable interpolation from an untrusted source (only `${AIRFLOW_UID:-50000}`, a local `.env`-sourced value already trusted elsewhere in this same compose file) |
| V6 Cryptography | No | Out of scope — no crypto touched |
| V14 Configuration | Yes | Docker Compose bind-mount/permission hardening is itself a configuration-security concern: over-permissive chmod (`777`/world-writable) should be avoided in favor of the tightest mode that still works (`664`/owner-write, not `666`/world-write, per this research's Open Question 2 recommendation) |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|-------------------------|
| Overly permissive `chmod` on a credentials-adjacent file (the passwords file) | Information Disclosure / Tampering | Prefer the tightest mode that empirically works (`664`, owner+group read/write, no other-write) over a blanket `666`; this project's own existing docs already document `666` as the current manual-fix value — this research recommends tightening it during this phase's rewrite, not just carrying the looser value forward unexamined |
| Root-user (`user: "0:0"`) compose service becoming a privilege-escalation vector if its command string ever incorporates untrusted input | Elevation of Privilege | Not applicable here in practice — `airflow-init`'s command string interpolates only `${AIRFLOW_UID:-50000}`, a value sourced from this project's own `.env` file (already a fully-trusted, developer-controlled local file, not user/network input) — flagged for completeness per the ASVS V14 category, not because a real vulnerability was found |

## Sources

### Primary (HIGH confidence)

- This repository's own committed source, read directly in this research session:
  `docker-compose.yml`, `docker/airflow/Dockerfile`, `scripts/verify_environment.py`, `Makefile`,
  `docs/environment.md`, `generator/generate_csv.py`, `airflow/dags/csv_ingest.py`,
  `.github/workflows/ci.yml`, `.gitignore`, `.env.example`, `pyproject.toml`, `uv.lock`.
- Live commands run against this checkout's actual running Docker Compose stack in this research
  session (see "Live Verification Against This Checkout" table): `docker compose ps`, `docker
  compose exec -T airflow-scheduler ...` (×5 checks), `docker compose exec -T airflow-apiserver
  ...`, `stat data`, `id` (inside container).
- `pip index versions faker` (PyPI, live query this session) — `faker==40.37.0` confirmed current.
- `slopcheck install faker` (live run this session) — `[OK]` verdict.
- Upstream v1.1 milestone research, already committed to this repo and treated as authoritative
  per the phase brief's own instruction not to re-derive it: `.planning/research/STACK.md`,
  `.planning/research/ARCHITECTURE.md`, `.planning/research/PITFALLS.md`,
  `.planning/research/SUMMARY.md`.
- [Simple auth manager — Airflow 3.3.1 Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html)
  — confirmed `SIMPLE_AUTH_MANAGER_USERS` is `username:role`, not `username:password`; passwords
  are auto-generated or read from `core.simple_auth_manager_passwords_file`.

### Secondary (MEDIUM confidence)

- `npm view faker version` (live query this session, `6.6.6`) — used only to document the
  cross-ecosystem name-confusion hazard, not as a claim about this project's own `faker` package.

### Tertiary (LOW confidence)

- None — this phase's scope (Docker Compose wiring, a documented Airflow auth-manager file
  format, a stdlib `subprocess` pattern) did not require any WebSearch-only, unverified claims
  beyond the `_docker_compose_exec` retry-condition string-matching heuristic, already flagged
  explicitly in the Assumptions Log (A4) rather than presented as settled.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — single package (`faker==40.37.0`), already pinned upstream, re-verified
  live against PyPI + slopcheck in this session.
- Architecture: HIGH for the `airflow-init` chown/mount patterns (carried forward from
  upstream ARCHITECTURE.md, itself Context7-verified against Airflow's own official compose file,
  and re-confirmed live against this checkout's actual running containers in this session); MEDIUM
  for the new `verify_environment.py` subprocess pattern (Pattern 2) specifically on the
  retry-condition string-matching heuristic (Assumption A4), since this project has no prior
  `subprocess`-based check to pattern-match against.
- Pitfalls: HIGH — every pitfall cited here was independently reproduced live against this
  checkout's actual running stack in this research session (see Live Verification table), not
  merely carried forward from documentation.

**Research date:** 2026-09-01
**Valid until:** 30 days (stable domain — Docker Compose bind-mount semantics and Airflow's
`SimpleAuthManager` file format are not fast-moving; re-verify `faker`'s pinned version against
`uv.lock` if this phase's execution is delayed past a `pyproject.toml` dependency bump).
