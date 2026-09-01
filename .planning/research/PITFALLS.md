# Pitfalls Research

**Domain:** Adding a scheduled fixture-generator DAG + `TriggerDagRunOperator` chain-orchestration
+ deferrable-trigger exception handling, to an existing Airflow 3.3.1 (LocalExecutor) + Oracle
Database Free stack (`lightweight-airflow-etl`, v1.1 "Hourly Ingestion Automation" milestone)
**Researched:** 2026-09-01
**Confidence:** HIGH (all findings verified against this repo's own committed source, the
project's own recorded debug history in `.planning/`, the actual `apache-airflow-providers-standard==1.17.0`
`trigger_dagrun.py` source, and the actual `oracledb==4.0.2` exception hierarchy imported live in a
throwaway venv) unless individually marked otherwise.

## Critical Pitfalls

### Pitfall 1: Fixed default `--seed` makes every hourly generation byte-identical, silently defeating the whole milestone via checksum-keyed idempotency

**What goes wrong:**
`generator/generate_csv.py`'s `--seed` argument defaults to the literal `20260101`
(`build_parser()`, line ~404) and nothing in `generate_rows()`/`generate_correlated_datasets()`
varies output by wall-clock time — only `output_path()` does, via the calendar date in the
filename. If the new `csv_generate_schedule` DAG's generate task invokes the script the same way
`make generate` does today (`generator/generate_csv.py --correlated`, no `--seed` override), every
hourly run produces **byte-for-byte identical** customers/orders CSV content. `engine.process()`
computes `sha256_file(file_path)` and checks `find_existing_ingestion(cursor, dataset, checksum)`
**before calling `process_chunks()` at all** (`engine.py` lines ~238-244) — a match short-circuits
straight to returning the *original* recorded outcome, inserting nothing. Since every hour's
checksum matches hour 1's, every ingestion after the first hour becomes a silent no-op: Airflow
shows a green `SUCCESS` DAG run, `report_ready` still fires (its poll query only checks
`TRUNC(processed_at) = TRUNC(SYSDATE)`, already satisfied by hour 1), and nothing looks wrong —
but zero new rows land in Oracle for the rest of the day. This is the single most dangerous pitfall
here because it produces *no error at all*.

**Why it happens:**
The CLI's `--seed` default exists for the pre-existing, human-invoked `make generate` workflow
(one deliberate run at a time, where determinism-for-testing was the whole point). Nobody has yet
had to make the seed *vary per invocation* because no automation has called this script on a
recurring schedule before.

**How to avoid:**
The generate task must pass an explicit, per-run-varying `--seed`, derived from the DAG run's own
trigger time — e.g. `int(logical_date.strftime("%Y%m%d%H"))` or `{{ ts_nodash }}` cast to an int —
never the CLI's bare default. Add an integration test that runs the generate step twice
back-to-back (or twice with different logical dates) and asserts the two output files' SHA-256
digests differ. Also verify `ingestion_metadata` gains a *new* row with a *new* checksum on the
second hourly run, not just that the DAG shows green.

**Warning signs:**
`ingestion_metadata`'s `total_rows`/`valid_rows` for a dataset stop changing hour over hour;
`process_csv_task`'s logged `ProcessingResult` shows the exact same row counts as the previous
hour; Oracle's `customers_valid`/`orders_valid` row counts stop growing after the first hourly run
of the day.

**Phase to address:**
DAG-implementation phase (the phase that writes `csv_generate_schedule`'s generate task) — this is
a design property of the task's own invocation, not an environment or trigger-robustness concern.

---

### Pitfall 2: The already-documented `data/` permission fix only ever solved *reads* — it does not grant the container write access this milestone newly needs

**What goes wrong:**
`docs/environment.md`'s existing "Known First-Boot Gotcha: Permission Error Creating `data/<dataset>/`"
section documents `mkdir -p data/customers data/orders` **before** first `docker compose up`, so the
directories exist under the *host user's* ownership rather than being auto-created `root:root` by
Docker on first bind-mount use. That fix was written for a world where `make generate` always ran
on the **host** (`uv run python generator/generate_csv.py --correlated`) and the Airflow containers
only ever *read* `/opt/airflow/data` (FileSensor glob + `process_csv_task`). Confirmed live right
now in this checkout: `./data` is `root:root`, mode `755` (`drwxr-xr-x`) — group/other have no
write bit, and the container runs as `${AIRFLOW_UID:-50000}:0` (uid 50000, gid 0 — see
`docker-compose.yml`'s `x-airflow-common.user`). Once generation moves *inside* the container for
the hourly DAG, `write_staged()`'s `staging_target.parent.mkdir(parents=True, exist_ok=True)` and
its subsequent write will hit `PermissionError` even with the *old* fix applied, because that fix
only ever addressed host-side directory creation, never container-side write access. Someone
re-running the documented `mkdir -p data/customers data/orders` step and seeing it "not work" is
likely to burn time re-diagnosing a bug that's actually just an incomplete carry-over fix.

**Why it happens:**
The permission model that was "good enough" changes the moment a *new* actor (the container's
uid 50000:gid 0 process) needs a *new* capability (write, not just read) on a path whose ownership
was only ever tuned for the old actor/capability pair.

**How to avoid:**
Grant the container's `50000:0` identity write access explicitly — e.g.
`sudo chown -R $(id -u):0 data && chmod -R 775 data` (mirrors the Phase 5 fix already used for this
exact `root:root`-on-first-bind-mount problem: `docker run --rm -v "$(pwd)/data:/data" alpine:3.20
chown -R 1000:1000 /data`, adapted to `0` for the group so gid-0 processes get the write bit) —
**775, not 755**, is the actual fix; a host-owned-but-still-755 directory (the old fix alone) is not
sufficient. Update `docs/environment.md`'s existing gotcha section in place rather than adding a
disconnected new one, since it's the same underlying path being re-scoped for a new writer.

**Warning signs:**
`csv_generate_schedule`'s generate task fails with `PermissionError: [Errno 13] Permission denied:
'/opt/airflow/data/customers/.staging'` (or similar) even though `data/customers`/`data/orders`
already exist and `docs/environment.md`'s documented fix was already applied.

**Phase to address:**
Environment/Docker fix phase (this is exactly the "fix `data/` directory permissions so the
container (`uid 50000:gid 0`) can write generated CSVs" line already scoped in `PROJECT.md`'s
Current Milestone — treat the *precise* fix (775 + gid-0 ownership, not a repeat of the read-only
fix) as the acceptance criterion).

---

### Pitfall 3: Docker Desktop/WSL2 auto-creates a missing bind-mount path as the wrong type, and caches stale inode references across recreates

**What goes wrong:**
This project has already hit this exact class of bug once
(`05-02-SUMMARY.md`, "Task 2's own gotcha"): `docker/airflow/simple_auth_manager_passwords.json.generated`
didn't exist in a fresh worktree, and Docker auto-created it **as a directory** on first mount
(`IsADirectoryError`) rather than as a file, because the bind-mount source path didn't exist on the
host when the container started. The same failure mode applies to *any* new file-shaped path this
milestone introduces (e.g., if a future config or lock file gets bind-mounted rather than baked
into the image). Separately — and this is the more insidious half — the recorded fix required a
**full `docker compose down && up`** cycle, not just a container restart or `docker compose up -d`
in place, because "Docker Desktop/WSL2 caches bind-mount inode references." A simple in-place file
swap (delete the wrongly-created directory, put a real file there, `docker compose restart`) will
often still show the stale directory/`IsADirectoryError` inside the running containers until a full
teardown-and-recreate happens.

**Why it happens:**
Docker's bind-mount implementation on Linux (and, more sharply, through Docker Desktop's WSL2
integration layer) resolves the mount source at container-*create* time; if the path doesn't exist,
it gets created with a type Docker infers as a directory unless the Compose file's own volume
syntax disambiguates it. WSL2's virtiofs/9p translation layer then caches that inode reference
across subsequent `up`/`restart` cycles that don't actually destroy and recreate the container.

**How to avoid:**
`generator/generate_csv.py` already exists as a **directory containing a file** in this repo (not
gitignored, already tracked) — the intended new mount (`./generator:/opt/airflow/generator:ro`)
is a directory-to-directory mount of something that already exists, so this specific milestone's
`generator/` mount is *not* at risk of the IsADirectoryError class by itself. The risk is
procedural: if the DAG-implementation phase introduces any *new* file-shaped bind-mount path (a
lock file, a `.env`-style override, a per-run marker file) that doesn't yet exist on the host, it
must be pre-created (`touch`) before the first `docker compose up` that references it — exactly the
same discipline already documented for `simple_auth_manager_passwords.json.generated`. If a stale
mount is ever suspected, use `docker compose down && docker compose up -d` (full recreate), never
just `docker compose restart` or `up -d` alone, to force Docker Desktop/WSL2 to re-resolve the
mount from scratch.

**Warning signs:**
`IsADirectoryError` or `NotADirectoryError` inside a container's startup logs immediately after
adding a new bind-mount line to `docker-compose.yml`; a fix that "should have worked" (file
recreated correctly on the host) but the container still behaves as if the old, wrongly-typed path
is there.

**Phase to address:**
Environment/Docker fix phase — verify explicitly, as part of that phase's acceptance criteria, that
every *new* mount path this milestone adds already exists as the correct type on a clean checkout
before `docker compose up` ever runs against it (add to `docs/environment.md` if a new
file-shaped — not directory-shaped — path is introduced).

---

### Pitfall 4: Editing the Dockerfile to add `faker` but running `docker compose up -d` without rebuilding silently keeps the stale image

**What goes wrong:**
`docker/airflow/Dockerfile` currently has no `faker` dependency at all (only `oracledb`, `pydantic`,
the two `apache-airflow-providers-*` packages, and the unconstrained `clevercsv`/
`charset-normalizer`/`chardet` trio). Adding `faker==40.37.0` to the Dockerfile changes the image
definition, but `docker compose up -d` alone does **not** rebuild an image that already exists
locally under the same tag — Compose only rebuilds automatically when using `docker compose up
--build` (or after an explicit `docker compose build`). If whoever implements this milestone edits
the Dockerfile and then just runs `docker compose up -d` (matching the muscle-memory habit from
every other change in this project, most of which are `docker-compose.yml` env-var edits that
*do* take effect on plain `up -d`), the running containers keep using the old image with no
`faker` installed at all. The failure then surfaces much later and much more confusingly: not at
build time, not at container-start time, but deep inside the generate task's first execution
(`ModuleNotFoundError: No module named 'faker'`), potentially hours after the "fix" was believed
complete.

**Why it happens:**
Every other environment-fix in this project's history (`AIRFLOW__API__SECRET_KEY`,
`AIRFLOW_CONN_FS_DEFAULT`, `PYTHONPATH`, etc.) has been a `docker-compose.yml` *environment
variable* change, which genuinely does take effect on a plain `up -d`/recreate with no rebuild
needed. A Dockerfile change is a different class of edit with a different required follow-up
command, and it's easy to reach for the same "just `up -d` it" habit.

**How to avoid:**
Any Dockerfile change in this milestone must be followed by `docker compose build` (or
`docker compose up -d --build`) before `up -d`/`up -d --wait`, and this should be called out
explicitly wherever the milestone's setup steps are documented (README/`docs/environment.md`),
the same way the passwords-file pre-seed step already is.

**Warning signs:**
`ModuleNotFoundError: No module named 'faker'` in a task log, despite the Dockerfile clearly
listing `faker` in its `pip install` call; `docker compose images` or `docker inspect` showing an
image `Created` timestamp older than the Dockerfile's own last-modified time.

**Phase to address:**
Environment/Docker fix phase — make the rebuild step an explicit, checked acceptance criterion
(e.g., confirm `python -c "import faker"` succeeds inside a freshly-recreated
`airflow-scheduler` container before moving on to the DAG-implementation phase).

---

### Pitfall 5: Adding `faker` to the wrong `pip install` stanza risks `ResolutionImpossible` against Airflow's constraints file

**What goes wrong:**
The Dockerfile already splits its `pip install` calls in two, specifically because Airflow's own
`constraints-3.3.1` branch pins versions of shared transitive dependencies (the comment cites
`charset-normalizer`/`chardet` having drifted incompatible with this project's own approved pins).
The first call installs `oracledb`, `pydantic`, and both `apache-airflow-providers-*` packages
**with** `--constraint ".../constraints-3.12.txt"`; the second, unconstrained call installs
`clevercsv`/`charset-normalizer`/`chardet` — packages that are `csv_processor`'s own dependencies,
never covered by Airflow's compatibility matrix. `faker` belongs in the same category as that
second group (a `generator/`-only dependency, irrelevant to Airflow's own dependency graph). Adding
it to the *first*, constrained call risks pip's resolver failing with `ResolutionImpossible` if
Airflow's constraints file happens to pin an incompatible version of one of `faker`'s own
transitive dependencies (e.g. `python-dateutil`, `typing-extensions`) — a class of failure this
project has already hit once for a different pair of packages.

**How to avoid:**
Add `"faker==40.37.0"` to the **second**, unconstrained `pip install --no-cache-dir` call
alongside `clevercsv`/`charset-normalizer`/`chardet`, not the first constrained one.

**Warning signs:**
`docker compose build` fails with `ResolutionImpossible` / `ERROR: Cannot install ... because these
package versions have conflicting dependencies` immediately after adding the `faker` line.

**Phase to address:**
Environment/Docker fix phase.

---

### Pitfall 6: Invoking `generator/generate_csv.py` from inside the DAG as an import instead of the same subprocess/CLI shape `make generate` already uses

**What goes wrong:**
`generator/` has no `__init__.py` (confirmed: `ls generator/` shows only `generate_csv.py`, no
package marker) and is not on any `PYTHONPATH` the Airflow containers currently set —
`docker-compose.yml`'s `x-airflow-common-env.PYTHONPATH` is `/opt/airflow/dags` only (added in
Phase 7 specifically so the triggerer's `importlib` reconstruction of `_common.oracle_partition_trigger`
resolves). A DAG task written as `from generator.generate_csv import main` (or similar) will fail
with `ModuleNotFoundError: No module named 'generator'` unless `generator/` is turned into an
actual importable package and `/opt/airflow` (its parent once mounted) is added to `PYTHONPATH`
too — extra surface area for zero benefit, since the script is deliberately a standalone CLI
(`argparse`-driven, `if __name__ == "__main__"` entrypoint) with a working, already-proven
invocation shape (`make generate` runs it as `uv run python generator/generate_csv.py
--correlated`).

**How to avoid:**
Mount `generator/` read-only at `/opt/airflow/generator` (matching the existing `./configs:/opt/airflow/configs:ro`
pattern) and invoke it from the DAG task via `subprocess.run([sys.executable, "/opt/airflow/generator/generate_csv.py",
"--correlated", "--seed", str(seed)], check=True, capture_output=True, text=True)` — the same shape
`make generate` already uses, just parameterized. This also keeps `_REPO_ROOT = Path(__file__).resolve().parent.parent`
resolving correctly: at `/opt/airflow/generator/generate_csv.py`, `.parent.parent` is `/opt/airflow`,
which already has `configs/` and `data/` mounted at exactly those sibling paths — no code change to
`generate_csv.py` needed, only the mount and the subprocess call. Raise on non-zero return code
rather than trusting exit status silently; surface `stderr` in the Airflow task log on failure.

**Warning signs:**
`ModuleNotFoundError: No module named 'generator'`; or, if someone instead tries `COPY generator/
...` into the image at build time (baking it in rather than mounting it), the DAG then silently
runs a stale, rebuild-only copy of the generator that never reflects host-side edits — a subtler
version of Pitfall 4's stale-image class specific to this one directory.

**Phase to address:**
DAG-implementation phase.

---

### Pitfall 7: `TriggerDagRunOperator`'s auto-generated `run_id` is wall-clock-derived at execute time, not idempotent across task retries, and non-deferred `wait_for_completion=True` occupies a LocalExecutor worker slot for the entire triggered DAG's runtime

**What goes wrong:**
Read directly from the installed `apache-airflow-providers-standard==1.17.0` source
(`trigger_dagrun.py`): when `trigger_run_id` isn't explicitly supplied, `execute()` computes
`run_id = DagRun.generate_run_id(run_type=MANUAL, logical_date=parsed_logical_date, run_after=parsed_run_after
or timezone.utcnow())` — i.e., a value derived from **wall-clock time at the moment this specific
task attempt runs**, not from the parent DAG run's own logical date/run_id. Two consequences:

1. **Not retry-idempotent.** If the "trigger `csv_ingest` for customers" task is retried (Airflow
   task retry, or a manual clear), the retry computes a *new* `run_id` from a *new* `utcnow()` and
   creates a **second, distinct** `csv_ingest` DagRun rather than resuming or deduplicating against
   the first attempt's run. (This happens to be *safe* at the data layer here, since `engine.process()`'s
   own checksum-keyed idempotency check means a second run against the same file content is a
   harmless no-op — but it is not safe as a general assumption, and it means Airflow's own DagRun
   history for `csv_ingest` will show orphaned/duplicate runs from any retried trigger task.)
2. **Worker-slot occupation.** The milestone's own plan is `wait_for_completion=True`, sequential,
   for three chained triggers (customers → orders → `report_ready`), with no mention of
   `deferrable=True`. Per the source (`_trigger_dag_af_3`), the `deferrable` flag is forwarded
   through to the runtime, but the *default* is `False` — a non-deferred `wait_for_completion=True`
   task occupies one of LocalExecutor's limited worker slots via blocking polling for the *entire*
   duration of the triggered DAG's run, for each of the three chained triggers in sequence. This is
   the exact "occupies a worker slot while waiting" anti-pattern this project has already
   deliberately avoided elsewhere (`ReportReadySensor`'s own docstring: "never occupies a worker
   slot while waiting" — achieved via `defer()`).

**How to avoid:**
Pass an explicit, deterministic `trigger_run_id` derived from the *parent* DAG run's own identity —
e.g. `trigger_run_id="{{ dag_run.run_id }}__customers"` (Jinja-templated, since `trigger_run_id` is
a `template_fields` entry) — so a retried parent task always targets the *same* child `run_id`,
and a genuine re-trigger of an already-existing run_id surfaces via Airflow's own
`DagRunAlreadyExists` handling (`reset_dag_run=False` default raises; explicitly decide whether
`reset_dag_run=True` or `skip_when_already_exists=True` is the right behavior for a retried trigger
task, rather than leaving the default silently create a second run). Set `deferrable=True` alongside
`wait_for_completion=True` on all three `TriggerDagRunOperator` instances, consistent with this
project's own already-established "defer, never block a worker slot" discipline used for
`FileSensor(deferrable=True)` and `ReportReadySensor`.

**Warning signs:**
Duplicate/orphaned `csv_ingest` or `report_ready` DagRuns with near-identical timestamps in the
Airflow UI after any task retry; LocalExecutor's available worker slots visibly exhausted
(`airflow tasks states-for-dag-run` or the UI's Gantt view showing the trigger tasks themselves
occupying slots for minutes at a time) once the pipeline is under any real load.

**Phase to address:**
DAG-implementation phase.

---

### Pitfall 8: No `max_active_runs` cap anywhere in the new chain lets a slow hour overlap the next, racing writes to the same date-based file path

**What goes wrong:**
Neither `csv_ingest.py` nor `report_ready.py` sets `max_active_runs` today (both default to
Airflow's global `[core] max_active_runs_per_dag`, typically 16) — reasonable when both DAGs are
only ever manually/API-triggered one at a time. Once `csv_generate_schedule` runs `@hourly` and
chain-triggers `csv_ingest` for the *same* dataset every hour, nothing stops a slow hour's chain
(Oracle briefly under load, a triggerer hiccup, `wait_for_completion` polling taking longer than
expected) from still being mid-flight when the *next* hourly schedule fires and starts its own
generate → trigger chain. Two concurrent `csv_ingest` DagRuns for the same dataset are then both
alive at once, and — combined with `output_path()`'s one-file-per-calendar-day naming — the second
hour's generator task can `write_staged()`-rename a fresh file into the exact path the first hour's
still-running `csv_ingest` may not have finished reading yet. The atomic rename itself prevents
*corruption* (a reader that already opened the file keeps reading the old inode's bytes to
completion, per POSIX rename semantics), but it does not prevent *attribution* confusion — a
still-running older `csv_ingest` run could end up processing content that was actually generated
for a later hour, or `report_ready`'s daily-partition poll could be satisfied by a race between two
overlapping chains in a way that's hard to reason about after the fact.

**How to avoid:**
Set `max_active_runs=1` on `csv_generate_schedule` itself (the new DAG being added) so Airflow's
own scheduler refuses to start hour N+1's chain while hour N's is still active — the simplest,
most direct fix, requiring no change to the existing, unmodified `csv_ingest`/`report_ready` DAGs.
Combine with a `dagrun_timeout` on `csv_generate_schedule` so a genuinely stuck chain doesn't block
forever, and confirm hourly runtime (generate + 2×ingest + report_ready, sequential,
`wait_for_completion=True`) comfortably fits inside the hourly cadence with margin.

**Warning signs:**
Two `csv_ingest` DagRuns for the same `dataset` param both in `running` state simultaneously in
the Airflow UI; `ingestion_metadata` rows whose `processed_at` timestamp doesn't line up with which
hourly `csv_generate_schedule` run's generated content they actually correspond to.

**Phase to address:**
DAG-implementation phase.

---

### Pitfall 9: A naive exception-handling fix for `OraclePartitionReadyTrigger.run()` still misses the actual crash point, or swallows genuine bugs

**What goes wrong:**
The current code (`airflow/dags/_common/oracle_partition_trigger.py`, lines 111-125):

```python
async def run(self) -> AsyncIterator[Any]:
    while True:
        connection = await oracledb.connect_async(
            user=oracle_user(), password=oracle_password(), dsn=oracle_dsn()
        )
        try:
            cursor = connection.cursor()
            await cursor.execute(_POLL_QUERY)
            (count,) = await cursor.fetchone()
        finally:
            await connection.close()
        ...
```

The `oracledb.connect_async(...)` call — the single most likely failure point for a *transient*
Oracle outage (exactly CR-01's flagged crash) — sits **outside** the `try` block entirely. A fix
that only wraps the existing `try/finally` (the cursor/execute/fetch lines) in a broader
`except oracledb.Error:` still leaves a connection failure completely unhandled, because it never
enters that `try` block in the first place. Separately, the opposite mistake is just as likely:
wrapping the *whole* `while True` loop body in a blanket `except oracledb.Error:` (or worse,
`except Exception:`) `continue` catches not just transient connectivity failures but also genuine
bugs in `_POLL_QUERY` itself (a typo, a renamed column, a dropped table) — `oracledb.DatabaseError`
covers both. A blanket catch retries a permanently-broken query forever, every `poke_interval`
(30s), with the deferred task sitting in Airflow's UI showing `deferred` indefinitely and no
failure ever surfacing — arguably worse than today's immediate crash, since at least a crash is
visible.

**Why it happens:**
`oracledb.Error`'s exception hierarchy is deeper than it looks at a glance, and "catch the base
class to be safe" is a natural first instinct for a fix explicitly motivated by "don't crash on any
DB error."

**How to avoid:**
Verified live against the exact pinned version (`oracledb==4.0.2`, imported directly in a
throwaway venv):

```
Error
 └─ DatabaseError
     ├─ DataError
     ├─ IntegrityError
     ├─ InternalError
     ├─ NotSupportedError
     ├─ OperationalError   ← connection/network-level failures (DPY-6005 "cannot connect",
     │                        DPY-4011 "database or network closed the connection",
     │                        ORA-12541/ORA-03113/ORA-01033-class TNS/listener/instance issues)
     └─ ProgrammingError   ← genuine SQL/usage bugs (bad column/table name, ORA-00904/ORA-00942)
 └─ InterfaceError          ← client-side driver misuse, separate branch entirely
```

Move `oracledb.connect_async(...)` **inside** a `try` block of its own (or wrap the whole
loop-body, connect included, in one try), and catch specifically `oracledb.OperationalError` —
never the broader `oracledb.DatabaseError` or bare `oracledb.Error` — for the retry-with-backoff
path. Anything else (`ProgrammingError`, `IntegrityError`, etc.) should propagate out of `run()`
uncaught: Airflow's triggerer already has its own top-level handling for an uncaught exception
escaping a trigger's `run()` generator — it fails the deferred task instance and surfaces the
traceback in the trigger's own log stream, exactly the "genuine bugs must not be silently
swallowed" behavior being asked for, with zero custom code needed for that half. Add a bounded
retry count or elapsed-time cap even for the `OperationalError` retry path (e.g., give up and
raise/yield a failure `TriggerEvent` after N consecutive transient failures or M total minutes) —
an unbounded retry-forever loop is not meaningfully better than today's immediate crash if Oracle
is down for a genuinely extended period; at least surface *something* eventually rather than
`deferred` forever with no visible signal.

**Warning signs:**
The deferred `report_ready` sensor sits in `deferred` state indefinitely in the Airflow UI with no
progress and no failure, while `ingestion_metadata` genuinely has both datasets' rows for today —
a sign the poll query itself is broken (e.g., a typo introduced while adding the try/except) and
is being silently retried instead of surfacing.

**Phase to address:**
Trigger-robustness fix phase (the explicit CR-01 follow-up already scoped in `PROJECT.md`'s
Current Milestone / Active requirements).

---

### Pitfall 10: `finally: await connection.close()` can itself raise on an already-broken connection, masking the original, more diagnostic exception

**What goes wrong:**
Even with Pitfall 9's fix applied, the existing `finally: await connection.close()` runs
unconditionally after any exception inside the `try` block — including exactly the case where the
connection *itself* is the thing that broke (e.g., `DPY-4011`, "the database or network closed the
connection"). Calling `.close()` on an already-severed connection can itself raise
(`oracledb.Error`/`oracledb.InterfaceError`), and an exception raised inside a `finally` block
**replaces** the original exception being propagated — the more informative original error (which
Oracle error code actually caused the failure) is lost, and only the less-useful "failed to close a
connection that was already dead" error is what ends up in the trigger's logged traceback.

**How to avoid:**
Guard the close call independently:

```python
finally:
    try:
        await connection.close()
    except oracledb.Error:
        _LOGGER.debug("connection.close() failed on an already-broken connection", exc_info=True)
```

so a close-time failure is logged at low severity but never masks whatever exception is actually
propagating out of the `try` block.

**Warning signs:**
The trigger's failure log shows a close/disconnect-shaped error message with no clear indication of
what the *original* Oracle problem was (e.g. no ORA-/DPY- code visible at all, just a generic
"not connected" message on close).

**Phase to address:**
Trigger-robustness fix phase (same phase as Pitfall 9 — implement both together, they're the same
code block).

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|-----------------|------------------|
| Overwriting one file per calendar day instead of a per-run-uniquely-named file (`<dataset>_<YYYYMMDD>_<HH>.csv` or similar) | No `output_path()`/`FileSensor` glob-pattern changes needed | No historical on-disk CSV trail survives past the next hourly overwrite; harder to manually re-inspect "what did hour 6 actually generate" after the fact | Acceptable for this milestone's stated scope (data safety lives in Oracle via checksum-keyed `ingestion_metadata`, not on-disk file retention) — but should be an explicit, recorded decision, not an accidental side effect |
| Sequential `wait_for_completion=True` `TriggerDagRunOperator` chain instead of Airflow Datasets/Assets-based triggering | Simplest possible wiring, mirrors the milestone's own literal spec | No parallelism between customers/orders ingestion (they could run concurrently, not sequentially); a slow/failed step blocks the whole hourly chain | Acceptable at this project's deliberately small scale (two datasets, hourly cadence, LocalExecutor) — would not scale to many more datasets or a tighter schedule |
| Catching only `oracledb.OperationalError` (not all connectivity-adjacent errors) for retry | Simple, precise, matches the DB-API 2.0 exception taxonomy exactly | A genuinely transient error that Oracle/`python-oracledb` happens to raise as a different `DatabaseError` subclass in some edge case would not be retried and would instead fail the sensor | Acceptable — narrow-and-correct is the right tradeoff for "don't silently swallow bugs"; widen only if a *specific*, evidenced transient error is observed escaping this net in practice |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|-----------------|--------------------|
| Docker Desktop + WSL2 bind mounts | Assuming a file/directory swap on the host takes effect immediately inside a running container | Full `docker compose down && docker compose up -d` when a bind-mount source's *type* (file vs. directory) or ownership changes — in-place `restart`/`up -d` can serve a stale cached inode reference |
| Airflow LocalExecutor + `TriggerDagRunOperator` | Leaving `wait_for_completion=True` with `deferrable` at its `False` default | Set `deferrable=True` explicitly whenever `wait_for_completion=True` is used, so the triggering task defers to the triggerer instead of blocking a LocalExecutor worker slot |
| `python-oracledb` async (`connect_async`) inside a custom `BaseTrigger` | Catching `oracledb.DatabaseError`/`oracledb.Error` broadly "to be safe" | Catch `oracledb.OperationalError` specifically for retry-worthy transient/connectivity failures; let `ProgrammingError`/other `DatabaseError` subclasses propagate as genuine bugs |
| Airflow triggerer's single shared event loop | Adding any blocking call (`time.sleep`, synchronous file I/O, a blocking `oracledb.connect()`) inside a trigger's exception-handling/backoff path | Always `await asyncio.sleep(...)` for backoff; never introduce a synchronous blocking call anywhere in `run()`, including inside newly-added except blocks — it stalls every other DAG's deferred tasks project-wide, not just this trigger's |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|-----------------|
| Non-deferred `wait_for_completion=True` chained triggers occupying LocalExecutor worker slots | Airflow UI Gantt view shows trigger tasks themselves "running" for minutes at a time; other DAG tasks queue up waiting for a free slot | `deferrable=True` on all three `TriggerDagRunOperator` instances (Pitfall 7) | Becomes visible the moment any single hourly chain's total runtime approaches a meaningful fraction of LocalExecutor's configured `parallelism`/worker slot count, or once any other DAG needs to run concurrently |
| Unbounded retry-forever backoff in `OraclePartitionReadyTrigger` after Pitfall 9's fix | `report_ready` sits `deferred` indefinitely during an extended real Oracle outage, invisible in the UI as anything other than "still waiting" | Bounded retry count / elapsed-time cap that eventually raises or yields a failure event (Pitfall 9) | Breaks down (silently, from an operator's perspective) the moment Oracle is down longer than anyone happens to notice manually |

## "Looks Done But Isn't" Checklist

- [ ] **`csv_generate_schedule` "runs successfully" hourly:** Often missing genuinely new data —
      verify `ingestion_metadata` gains a row with a **new, distinct checksum** each hour, not just
      a green Airflow UI checkmark (Pitfall 1).
- [ ] **`generator/` "mounted into the container":** Often missing actual write access — verify the
      generate task can complete a full `write_staged()` cycle (staging dir create + write + atomic
      rename) inside the real container, not just that `data/customers`/`data/orders` exist on the
      host (Pitfall 2).
- [ ] **`faker` "added as a dependency":** Often missing a rebuild — verify
      `docker compose images` shows a freshly-built image timestamp, and `python -c "import
      faker"` succeeds inside a running `airflow-scheduler` container, not just that the Dockerfile
      text contains the line (Pitfall 4/5).
- [ ] **`OraclePartitionReadyTrigger` "has exception handling now":** Often missing the actual
      crash point — verify a real Oracle-down scenario (e.g., `docker compose stop oracle`
      mid-poll) is retried/recovered rather than crashing, **and** a deliberately-broken
      `_POLL_QUERY` (typo a column name) still surfaces as a visible task failure rather than
      retrying forever silently (Pitfall 9/10).
- [ ] **`TriggerDagRunOperator` chain "wired end to end":** Often missing failure-propagation
      verification — deliberately fail one triggered `csv_ingest` run (e.g., a temporarily-broken
      `config.json`) and confirm the parent chain's downstream tasks (orders trigger,
      `report_ready` trigger) behave as intended (fail/skip, not silently continue) (Pitfall 7).

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|-----------------|-------------------|
| Checksum-collision silent no-op (Pitfall 1) | LOW | Fix the seed derivation, re-trigger `csv_generate_schedule` manually once; no data was corrupted, only nothing new was written — no cleanup needed beyond the code fix |
| `data/` permission mismatch (Pitfall 2) | LOW | `chown -R $(id -u):0 data && chmod -R 775 data` on the host, no container recreate needed (bind-mount picks up host-side permission changes immediately, unlike the file-type/inode-caching issue in Pitfall 3) |
| Stale bind-mount inode reference (Pitfall 3) | LOW | `docker compose down && docker compose up -d` — full recreate, not just restart |
| Stale image after Dockerfile edit (Pitfall 4/5) | LOW | `docker compose build && docker compose up -d` (or `up -d --build`) |
| Duplicate/orphaned triggered DagRuns (Pitfall 7) | MEDIUM | Manually mark the duplicate DagRun as failed/skipped in the Airflow UI; no data corruption expected given `engine.process()`'s own checksum idempotency, but the DagRun history stays cluttered until manually cleaned or the retention job prunes it |
| Overlapping concurrent chains racing the same file path (Pitfall 8) | MEDIUM | Identify and manually resolve which `csv_ingest` run actually processed which hour's content via `ingestion_metadata`'s `checksum`/`processed_at` columns (the source of truth, not the on-disk filename); add `max_active_runs=1` going forward |
| Triggerer stuck in unbounded retry (Pitfall 9) | LOW | Fix the underlying query/connectivity bug, then manually clear the stuck `report_ready` task instance to force a fresh trigger reconstruction |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|--------------------|-----------------|
| 1. Fixed-seed checksum collision silently no-ops hourly ingestion | DAG-implementation phase | Run the generate task twice with different logical dates; assert differing SHA-256 digests and two distinct `ingestion_metadata` rows |
| 2. `data/` fix only ever covered reads, not the new write need | Environment/Docker fix phase | From inside a running `airflow-scheduler` container, touch-write a file under `/opt/airflow/data/customers/` as uid 50000 |
| 3. Bind-mount type/inode-caching gotcha (already hit once) | Environment/Docker fix phase | Fresh-clone dry run: confirm every new file-shaped mount path is pre-created before first `docker compose up`; confirm `down && up` (not just `restart`) is the documented recovery step |
| 4. Stale image after adding `faker` without rebuild | Environment/Docker fix phase | `docker compose images` timestamp check + `import faker` smoke test inside the container |
| 5. `faker` added to the wrong constrained `pip install` stanza | Environment/Docker fix phase | `docker compose build` succeeds with no `ResolutionImpossible` |
| 6. Import-vs-subprocess invocation of `generator/generate_csv.py` | DAG-implementation phase | Generate task's implementation reviewed for `subprocess.run(...)` (not `import generator...`) before merge |
| 7. `TriggerDagRunOperator` run_id/worker-slot behavior | DAG-implementation phase | Retry the trigger task manually; confirm it targets the same child `run_id` rather than creating a new DagRun; confirm `deferred` (not `running`) state appears in the UI while waiting |
| 8. No `max_active_runs` cap allows overlapping hourly chains | DAG-implementation phase | Manually delay one hourly run (e.g. pause Oracle briefly) and confirm the next scheduled run does not also start |
| 9. Naive/incomplete exception handling around `connect_async()` | Trigger-robustness fix phase | `docker compose stop oracle` mid-poll → confirm retry/recovery, not crash; introduce a deliberate query typo → confirm visible task failure, not silent infinite retry |
| 10. `finally: connection.close()` masking the original exception | Trigger-robustness fix phase | Same test as #9's Oracle-down scenario; inspect the logged traceback for the *original* Oracle error code, not a close-time secondary error |

## Sources

- This repository's own committed source, read directly for this research: `.planning/PROJECT.md`,
  `airflow/dags/_common/oracle_partition_trigger.py`, `generator/generate_csv.py`,
  `airflow/dags/csv_ingest.py`, `airflow/dags/report_ready.py`, `docker-compose.yml`,
  `docker/airflow/Dockerfile`, `docs/environment.md`, `.planning/STATE.md`,
  `.planning/milestones/v1.0-phases/05-airflow-dag-wiring-deferrable-file-wait/05-02-SUMMARY.md`,
  `packages/csv-processor/src/csv_processor/engine.py`, `.planning/debug/knowledge-base.md`.
- `apache-airflow-providers-standard==1.17.0`'s actual `trigger_dagrun.py` source, downloaded and
  read directly (`pip download apache-airflow-providers-standard==1.17.0`) — HIGH confidence,
  matches this project's own pinned version exactly.
- `oracledb==4.0.2`'s actual exception class hierarchy, imported directly in a throwaway venv
  (`Error > DatabaseError > {DataError, IntegrityError, InternalError, NotSupportedError,
  OperationalError, ProgrammingError}`, plus a sibling `InterfaceError`) — HIGH confidence, matches
  this project's own pinned version exactly.
- [python-oracledb DPY-4011/DPY-6005 discussion — GitHub Issue #234, "OperationalError: DPY-6005"](https://github.com/oracle/python-oracledb/issues/234) — MEDIUM confidence corroboration that DPY-6005/DPY-4011-class errors surface as `OperationalError` in practice, consistent with the verified class hierarchy above.
- Airflow's own documented triggerer behavior (an uncaught exception escaping a trigger's `run()`
  async generator fails the deferred task instance and surfaces the traceback in the trigger's log
  stream) — MEDIUM confidence, stable/well-established core framework behavior across Airflow
  2.x/3.x, not independently re-verified against this exact pinned Airflow 3.3.1 release's source
  for this research pass; consistent with this project's own already-observed behavior (CR-01's
  "a transient DB error currently crashes the deferred sensor permanently" is itself evidence this
  exact mechanism is already firing today).

---
*Pitfalls research for: hourly CSV-generation-and-ingestion automation DAG, lightweight-airflow-etl v1.1*
*Researched: 2026-09-01*
