---
status: resolved
trigger: "Airflow UI logs not visible at :8080 — user-reported during Phase 7 discuss-phase (2026-08-30), unrelated to Phase 7's correlation fix"
created: 2026-08-30T11:05:00Z
updated: 2026-08-30T11:52:00Z
---

## Current Focus

status: resolved
hypothesis: |
  SECRET_KEY FIX CONFIRMED (see below, unchanged) but user's checkpoint response surfaced a
  SECOND, DIFFERENT error on a pre-fix task run: NameResolutionError on a Docker container-ID
  hostname (32f3a8d0eda7) when fetching build_report_task's logs. New working hypothesis (b-branch
  confirmed by direct inspection, see Evidence): docker-compose.yml's x-airflow-common has NO
  shared/persistent volume for Airflow's base_log_folder (/opt/airflow/logs) across the 5
  components -- each container writes task logs to its own local, ephemeral filesystem. Airflow's
  UI log view therefore depends entirely on cross-container HTTP fetch (FileTaskHandler contacting
  TaskInstance.hostname:8793), which only works while the ORIGINAL container that ran the task is
  still alive. This is not merely an artifact of this session's secret_key-fix container recreate
  (though that recreate IS what orphaned this specific build_report_task log) -- it is a standing
  structural gap: ANY future container recycle (image rebuild, `docker compose down`/`up`, host
  reboot, another `--force-recreate`) will permanently orphan every log written before that
  recycle. Both (a) and (b) from the investigation directive are true simultaneously and are not
  mutually exclusive: (a) explains this SPECIFIC reported instance; (b) is the underlying gap that
  will keep reproducing new instances of the same error class going forward.
next_action: "DONE. User's checkpoint response confirmed the fix end-to-end in their own
  browser session against run manual__post-fix-verify-1788090433 (logs display cleanly, no
  secret_key error, no NameResolutionError). Session archived to
  .planning/debug/resolved/airflow-ui-logs-not-visible.md."

reasoning_checkpoint:
  hypothesis: "AIRFLOW__API__SECRET_KEY is unset across all 5 Airflow services in
    docker-compose.yml, so each container (apiserver, scheduler, dag-processor, triggerer,
    and any executor subprocess) mints its own random [api] secret_key at startup; the
    log-viewer's internal API request is signed with one component's key and verified with
    another's, causing Airflow's own secret_key-mismatch error to render instead of logs."
  confirming_evidence:
    - "grep across docker-compose.yml (rendered via `docker compose config`) finds
      AIRFLOW__API_AUTH__JWT_SECRET exactly once, shared in x-airflow-common, but zero
      matches for AIRFLOW__API__SECRET_KEY or the legacy AIRFLOW__WEBSERVER__SECRET_KEY
      anywhere in any of the 5 rendered services."
    - "The verbatim UI error text names the exact section this project is missing --
      \"'secret_key' configured in '[api]' section\" -- not a generic auth failure."
    - "Official Airflow 3.3.1 Configuration Reference (airflow.apache.org) confirms
      AIRFLOW__API__SECRET_KEY is the live env-var name (secret_key moved from [webserver]
      to [api] in Airflow 3.0+), and that this same key authorizes log-retrieval requests
      between components -- an exact mechanism match, not just a plausible-sounding config
      knob guessed from training data."
    - "Structurally identical, already-confirmed prior fix in this same file for
      AIRFLOW__API_AUTH__JWT_SECRET (Phase 5): unset shared secret across multi-container
      compose -> per-component random key -> cross-component signature verification failure.
      Same shape, different config knob, same root file."
  falsification_test: "If AIRFLOW__API__SECRET_KEY were already set consistently (or if the
    error persisted after setting it to an identical value across every Airflow service and
    restarting all containers), this hypothesis would be refuted -- it would mean the
    log-fetch failure has a different cause (e.g. clock skew across containers, which the
    same error message also warns about, or a JWT_SECRET-adjacent issue)."
  fix_rationale: "Adding AIRFLOW__API__SECRET_KEY to the shared x-airflow-common env block
    (rather than per-service) ensures every Airflow component -- apiserver, scheduler,
    dag-processor, triggerer, and airflow-init -- inherits the identical fixed value via the
    same YAML anchor mechanism already used for AIRFLOW__API_AUTH__JWT_SECRET, addressing the
    root cause (key divergence) rather than a symptom (e.g. suppressing the error display)."
  blind_spots: "UPDATE: Docker WAS available in this environment with the real stack already
    running, so this was live-verified end-to-end (containers recreated, fresh DAG run
    triggered, logs fetched via the exact API endpoint the UI calls -- see Evidence). Remaining
    blind spot: verification happened via direct REST API calls, not by clicking through the
    actual browser UI, so a UI-layer-only issue (e.g. frontend rendering, browser caching of the
    old error page) would not be caught by this self-verification alone -- hence still routing
    through the human-verify checkpoint. Also have not independently ruled out clock skew
    between containers as a compounding factor, though the fix alone fully resolved log fetching
    with zero clock changes, which is strong (not just theoretical) evidence against skew being
    a contributing cause."
  candidate_causes:
    - "config: AIRFLOW__API__SECRET_KEY absent from docker-compose.yml's shared env block"
    - "environment: clock skew between the 5 separate Airflow containers (same error
      message's second warning) -- considered and treated as a low-probability alternative,
      not folded into the primary fix, since Docker containers on a single host normally
      share the host clock and there is no evidence of container-specific time drift"
  and_gate: "no -- this is a single-cause config gap. The missing env var alone fully
    explains the observed error (every component independently minting a random key is
    sufficient to break every cross-component signature check, matching 'happens for every
    DAG/task, not just specific ones'); no second simultaneous condition is required to
    produce the symptom, unlike a true AND-gate bug where two independent failures must
    co-occur."

reasoning_checkpoint_2:
  hypothesis: "docker-compose.yml's x-airflow-common has no shared/persistent volume for
    /opt/airflow/logs, so each Airflow container's task logs live only on that container's own
    ephemeral filesystem; the UI's log view depends entirely on a fragile cross-container HTTP
    fetch keyed on TaskInstance.hostname (a container-ID-derived value that changes on every
    recreate), so ANY container recycle permanently orphans previously-written logs."
  confirming_evidence:
    - "docker exec into scheduler vs apiserver: scheduler's /opt/airflow/logs has real task log
      dirs, apiserver's is empty -- direct proof they don't share a filesystem."
    - "scheduler's own hostname (2a059b001a3b) is a live container-ID-style value, same shape as
      the now-unresolvable 32f3a8d0eda7 from the pre-fix run -- proves TaskInstance.hostname is
      tied to a specific container instance, not a stable service name."
    - "Official Airflow docs confirm FileTaskHandler checks local disk before falling back to the
      hostname:port HTTP fetch -- meaning a shared local log path removes the fragile path
      entirely rather than just working around it."
  falsification_test: "If, after adding a shared log volume across all 5 services and
    force-recreating the scheduler+apiserver containers (simulating a future recycle), logs
    written BEFORE that recreate were still unreachable (NameResolutionError or empty), this
    hypothesis would be refuted -- it would mean logs are being written to a path outside the
    shared mount, or the mount isn't actually shared across those two specific services."
  fix_rationale: "Adding a Docker named volume mounted at /opt/airflow/logs in the shared
    x-airflow-common.volumes list (inherited by all 5 services, same anchor mechanism already used
    for AIRFLOW__API_AUTH__JWT_SECRET/AIRFLOW__API__SECRET_KEY) makes every component read/write
    the SAME persistent log storage. This addresses the root cause (no shared, persistent log
    storage) rather than a symptom (e.g. only fixing this one orphaned run, or suppressing the
    error text) -- it also survives future container recreates, which a hostname-matching
    workaround would not."
  blind_spots: "Have not tested behavior under `docker compose down -v` (which would remove named
    volumes entirely, unlike `down` alone or `--force-recreate`) -- that remains a real, but
    expected and documented, way to lose logs (equivalent to deleting the log directory), not a
    regression this fix claims to prevent. Also have not tested multi-day log rotation/retention
    behavior since this is a short-lived local-dev verification, not a long-running deployment."
  candidate_causes:
    - "config: docker-compose.yml's x-airflow-common.volumes omits any mount for
      /opt/airflow/logs (base_log_folder default)"
    - "environment: Docker Compose's default container hostname (short container ID) is inherently
      unstable across recreates, which is what makes the missing shared volume observable as a
      user-facing bug rather than a latent gap"
  and_gate: "no -- the missing shared log volume alone fully explains both the original
    build_report_task instance (predates a recreate) and the general recurrence risk (any future
    recreate reproduces it for whatever ran most recently). The unstable per-container hostname is
    a property of the platform, not a second independent fault that must co-occur; it's the
    mechanism BY WHICH the missing-volume gap becomes visible, not a separate contributing cause."

## Symptoms

expected: |
  Clicking into a task instance's logs in the Airflow UI (localhost:8080) shows the task's real
  log output.
actual: |
  The UI shows: "▶Log message source details!!!! Please make sure that all your Airflow
  components (e.g. schedulers, api-servers, dag-processors, workers and triggerer) have the same
  'secret_key' configured in '[api]' section and time is synchronized on all your machines (for
  example with ntpd) See more at
  https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#secret-key"
errors: |
  Airflow's own secret_key-mismatch log-fetch error (verbatim above), not a stack trace or HTTP
  status the user could paste directly.
timeline: Always been this way (not a regression from a recent change) — never worked, first
  noticed during Phase 7 discuss-phase.
reproduction: Open the Airflow UI at localhost:8080, navigate to any task instance's log view.
  Happens for every DAG/task, not just specific ones.

## Evidence

- timestamp: 2026-08-30T11:05:00Z
  finding: |
    grep -n "secret_key\|SECRET_KEY\|JWT_SECRET" docker-compose.yml returns exactly one match:
    `AIRFLOW__API_AUTH__JWT_SECRET: "csv-ingest-local-dev-shared-jwt-secret-not-for-prod"` on line
    44, inside the shared x-airflow-common env block. No `AIRFLOW__API__SECRET_KEY` (or legacy
    `AIRFLOW__WEBSERVER__SECRET_KEY`) is set anywhere in the file.
  source: docker-compose.yml grep

- timestamp: 2026-08-30T11:20:00Z
  finding: |
    `docker compose config` (fully rendered/merged compose output, all YAML anchors resolved)
    confirms AIRFLOW__API_AUTH__JWT_SECRET appears in all 5 Airflow services' environment blocks
    (airflow-init, airflow-apiserver, airflow-scheduler, airflow-dag-processor,
    airflow-triggerer) but AIRFLOW__API__SECRET_KEY appears in none of them. Rules out any
    per-service override masking a value set only in the shared anchor.
  source: docker compose config | grep -A2 -B2 SECRET

- timestamp: 2026-08-30T11:22:00Z
  finding: |
    Web search of official Airflow docs (airflow.apache.org/docs/apache-airflow/stable/
    configurations-ref.html, Airflow 3.3.1) confirms: (1) secret_key was moved from
    [webserver] to [api] in Airflow 3.0+, matching this project's pinned 3.3.1; (2) the
    correct env var is AIRFLOW__API__SECRET_KEY (AIRFLOW__{SECTION}__{KEY} format); (3) the
    docs explicitly state this same secret_key is "used to authorize requests ... when logs
    are retrieved" and that mismatched keys across components cause exactly this failure
    class. Direct mechanism confirmation, not an inferred guess.
  source: WebSearch — airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html

- timestamp: 2026-08-30T11:28:00Z
  finding: |
    Live end-to-end verification (Docker was reachable in this environment; the real stack
    was already running). Recreated the 4 Airflow component containers
    (apiserver/scheduler/dag-processor/triggerer) with `docker compose up -d --force-recreate`
    to pick up the new AIRFLOW__API__SECRET_KEY. Confirmed via `docker exec ... printenv` that
    all 4 now hold the identical value. Fetching logs for a task instance from a DAG run that
    predated the fix (executed under the old, now-destroyed scheduler container) returned a
    DIFFERENT error (`NameResolutionError` on the old container's hostname) with zero
    secret_key mentions -- expected artifact of force-recreating containers mid-run, not a new
    bug. Triggered a fresh csv_ingest DAG run (dataset=customers) entirely under the fixed
    containers; it reached state=success. Fetched logs via
    `GET /api/v2/dags/csv_ingest/dagRuns/{run_id}/taskInstances/{task_id}/logs/1` (the exact
    endpoint the Airflow UI's log view calls) for all 6 tasks
    (load_config_task, route_after_config, wait_for_file, process_csv_task,
    load_results_task, report_result_task): every one returned HTTP 200 with real log content
    (worker startup, task stdout including the process_csv_task return value, XCom push
    confirmation) and zero secret_key or connection errors. Rest of stack confirmed healthy
    post-recreate (docker compose ps: all 6 services healthy; scheduler logs show no new
    errors).
  source: Live docker compose stack + Airflow REST API (localhost:8080)

- timestamp: 2026-08-30T11:32:00Z
  finding: |
    User's checkpoint response reported a NEW error on report_ready/build_report_task's logs
    (run_id=manual__2026-08-30T10:51:47.918061+00:00): "Could not read served logs:
    HTTPConnectionPool(host='32f3a8d0eda7', port=8793) ... NameResolutionError ... Failed to
    resolve '32f3a8d0eda7'". Zero secret_key mentions -- a structurally different failure class
    (DNS resolution of a Docker container-ID hostname) than the already-fixed secret_key mismatch.
  source: User checkpoint response (verbatim, DATA_START/DATA_END block)

- timestamp: 2026-08-30T11:33:00Z
  finding: |
    docker-compose.yml's x-airflow-common.volumes list (used by ALL 5 Airflow services via the
    `<<: *airflow-common` anchor) mounts only: simple_auth_manager_passwords.json.generated
    (file), ./airflow/dags, ./data, ./configs:ro. There is NO mount for /opt/airflow/logs (the
    default base_log_folder for AIRFLOW_HOME=/opt/airflow) anywhere in the file, and no
    AIRFLOW__LOGGING__REMOTE_LOGGING or AIRFLOW__LOGGING__BASE_LOG_FOLDER override either
    (confirmed absent via grep). Each container's /opt/airflow/logs is therefore purely local,
    ephemeral container filesystem.
  source: docker-compose.yml read + grep for logs/volumes/LOGGING

- timestamp: 2026-08-30T11:34:00Z
  finding: |
    Direct live inspection of the two containers involved in log serving: `docker exec
    airflow-scheduler-1 hostname` = 2a059b001a3b (a container-ID-derived hostname, matches the
    format of the orphaned 32f3a8d0eda7 from the pre-fix run -- confirms TaskInstance.hostname is
    populated from the container's own ID-based hostname, which changes on every recreate).
    `docker exec airflow-scheduler-1 ls /opt/airflow/logs` shows a real, populated
    dag_id=csv_ingest directory (tasks ran here, LocalExecutor runs in-process in the scheduler
    container). `docker exec airflow-apiserver-1 ls /opt/airflow/logs` is EMPTY (only a stale
    directory from Aug 12, before this session). This proves apiserver and scheduler do NOT share
    a log filesystem -- apiserver has no local copy and must always proxy over HTTP to whichever
    hostname the metadata DB recorded for that task instance.
  source: docker exec printenv/hostname/ls against live containers

- timestamp: 2026-08-30T11:35:00Z
  finding: |
    Web research (airflow.apache.org logging-tasks.html + logging-architecture.html, Airflow
    3.3.1) confirms FileTaskHandler's read order: it first checks whether the log file exists on
    LOCAL disk (the server process's own base_log_folder); only if absent does it construct the
    http://{TaskInstance.hostname}:{worker_log_server_port}/log/... URL and fetch remotely. This
    means a shared base_log_folder across apiserver and the executing component would let the
    apiserver satisfy every log read locally, never touching the fragile cross-container hostname
    fetch at all -- addressing both this specific error AND the general structural gap.
  source: WebSearch — airflow.apache.org logging-tasks.html / logging-architecture.html (Airflow
    3.3.1 docs)

- timestamp: 2026-08-30T11:36:00Z
  finding: |
    Live-verified branch (a) independently: triggered a fresh csv_ingest DAG run
    (manual__debug-recycle-check-1788089505) via REST API entirely under the CURRENTLY running
    (post-secret_key-fix) containers; it reached state=success. Fetched logs for all 6 task
    instances via the same `GET .../taskInstances/{task_id}/logs/1` endpoint the UI uses: all 6
    returned HTTP 200 with real log content (e.g. process_csv_task's actual return value and XCom
    push), zero NameResolutionError, zero secret_key mentions. Confirms the build_report_task
    error the user saw was specifically because that run predated the mid-session container
    recreate -- not a currently-reproducible failure for logs generated after the recreate,
    UNLESS/UNTIL another recycle happens (see next finding, which is why (b) still needed a fix).
  source: Live docker compose stack + Airflow REST API (localhost:8080)

- timestamp: 2026-08-30T11:40:00Z
  finding: |
    Fix applied: added a shared named Docker volume `airflow-logs:/opt/airflow/logs` to
    x-airflow-common.volumes (inherited by all 5 services). `docker compose config` confirmed 5
    occurrences (airflow-init, apiserver, scheduler, dag-processor, triggerer). Recreated all 4
    long-running Airflow containers; all reported healthy. Confirmed via `docker exec ... stat`
    that Docker auto-populated the new volume with the image's own baked-in ownership
    (50000:0, mode 775) matching the container's runtime user exactly -- no host-permission
    mismatch, no manual chmod/AIRFLOW_UID setup needed. Confirmed via a write-test file created
    in the scheduler container that it was immediately visible from the apiserver container at
    the same path -- proves both containers now share one real filesystem, not two independent
    ones.
  source: docker compose config / docker exec (live stack)

- timestamp: 2026-08-30T11:41:00Z
  finding: |
    CRITICAL RECURRENCE TEST (proves fix survives future container recycles, the actual (b)
    concern): triggered a fresh DAG run under the fixed containers (scheduler hostname
    cfb52eb058a8), confirmed success, then force-recreated all 4 Airflow containers again
    (scheduler hostname changed to b1f913e98439 -- a genuine recycle, same shape as what orphaned
    the original build_report_task log). Fetched that PRE-recycle run's logs afterward: HTTP 200,
    real content, and critically the "Log message source details" line now shows a LOCAL FILE
    PATH (/opt/airflow/logs/dag_id=csv_ingest/.../attempt=1.log) instead of an
    http://{hostname}:8793/... URL -- direct confirmation that FileTaskHandler found the log on
    local disk and never attempted the fragile cross-container HTTP fetch at all, exactly as the
    official docs' read-order predicted.
  source: Live docker compose stack + Airflow REST API (localhost:8080)

- timestamp: 2026-08-30T11:42:00Z
  finding: |
    REVERT-AND-RECONFIRM cycle (matches the rigor already used for the secret_key fix):
    1. Temporarily removed only the new airflow-logs volume lines (kept the confirmed
       secret_key fix untouched) via targeted edit, force-recreated containers (scheduler
       hostname 85c4305e5ae7), triggered a fresh run, confirmed success.
    2. Force-recreated again (scheduler hostname changed to c2c998d51f31 -- simulating a second
       recycle) and fetched the now-orphaned run's logs: reproduced the EXACT original bug --
       "Could not read served logs: HTTPConnectionPool(host='85c4305e5ae7', port=8793) ...
       NameResolutionError ... Failed to resolve '85c4305e5ae7'" -- same shape as the user's
       original build_report_task error, different container ID. Confirms the shared-volume
       gap alone is sufficient to reproduce this bug class on demand.
    3. Re-added the airflow-logs volume lines, force-recreated containers again, triggered a
       fresh run, fetched its logs: HTTP 200, local-file-path source line, real content. Fix
       confirmed to resolve the reproduced bug on reapply.
    This proves the shared-volume change specifically (not some other concurrent factor, and not
    merely "which container happens to be running right now") is what prevents the bug.
  source: Live docker compose stack + Airflow REST API (localhost:8080), targeted revert/reapply
    of only the logs-volume lines

- timestamp: 2026-08-30T11:47:00Z
  finding: |
    THIRD-ROUND checkpoint response investigated: user reported NameResolutionError on
    run_id=manual__revert-check-1788089875, task_id=report_result_task, host='85c4305e5ae7'.
    Fetched that DAG run's own metadata directly: triggered_by="rest_api",
    triggering_user_name="admin" (not a browser-UI-initiated run), logical_date
    2026-08-30T11:37:55.725103Z (exactly matches the run_id's embedded unix timestamp
    1788089875), end_date 2026-08-30T11:38:04Z. All 6 of its task instances ran under hostname
    85c4305e5ae7. Cross-referenced against this file's own revert-and-reconfirm Evidence entry
    (2026-08-30T11:42:00Z): step 1 of that cycle temporarily removed only the logs-volume lines,
    force-recreated containers to scheduler hostname 85c4305e5ae7, and triggered a fresh run to
    confirm success -- this IS that exact run. `docker inspect` on the CURRENTLY running scheduler
    container shows Created=2026-08-30T11:38:58Z, hostname=e205b13a3d10 -- created 54 seconds
    AFTER this run's end_date, i.e. by the cycle's step 3 (re-add volume lines, final
    force-recreate). Conclusively confirms: manual__revert-check-1788089875 ran entirely during
    the deliberate, temporary mid-test revert (before the fix's FINAL applied state existed) and
    was destroyed by the very next recreate -- it is a stale test artifact of this investigation's
    own rigor process, not a sign the shipped fix is broken.
  source: GET /api/v2/dags/csv_ingest/dagRuns/manual__revert-check-1788089875 (+ taskInstances) +
    docker inspect on live scheduler container

- timestamp: 2026-08-30T11:47:30Z
  finding: |
    Confirmed docker-compose.yml's current working-tree state (git diff against HEAD) contains
    BOTH fixes in their final form with no stray revert left over from testing: AIRFLOW__API__
    SECRET_KEY present once in x-airflow-common env (line 56), airflow-logs:/opt/airflow/logs
    present once in x-airflow-common.volumes (line 120) plus its top-level volume declaration
    (line 216). `docker compose config` (fully rendered) confirms AIRFLOW__API__SECRET_KEY
    appears 6 times and airflow-logs:/opt/airflow/logs appears 6 times (once per service
    inheriting the anchor, matching the 5 Airflow services + the anchor's own render). `docker exec
    ... printenv` on the CURRENTLY running scheduler and apiserver containers (created
    2026-08-30T11:38:58Z, i.e. after the revert-and-reconfirm cycle's final re-apply) confirms both
    hold the correct AIRFLOW__API__SECRET_KEY value; `docker exec ... ls /opt/airflow/logs` on both
    shows the same populated dag_id=csv_ingest directory (shared filesystem, not two independent
    ones).
  source: git diff docker-compose.yml, docker compose config, docker exec (live stack)

- timestamp: 2026-08-30T11:48:00Z
  finding: |
    Triggered a BRAND-NEW DAG run (manual__post-fix-verify-1788090433, dataset=customers) via the
    same REST API, entirely under the current final containers; reached state=success within one
    poll. All 6 task instances (load_config_task, route_after_config, wait_for_file,
    process_csv_task, load_results_task, report_result_task) ran under hostname e205b13a3d10 --
    the CURRENT scheduler container, confirmed via docker inspect to be the one created at the
    revert-and-reconfirm cycle's final re-apply. Fetched all 6 logs via the exact
    `GET /api/v2/dags/csv_ingest/dagRuns/{run_id}/taskInstances/{task_id}/logs/1` endpoint the UI
    calls: every one returned HTTP 200, with the "Log message source details" line showing a LOCAL
    FILE PATH (/opt/airflow/logs/dag_id=csv_ingest/run_id=manual__post-fix-verify-1788090433/
    task_id=.../attempt=1.log), zero secret_key mentions, zero NameResolutionError mentions.
    Cross-checked the stale manual__revert-check-1788089875/report_result_task log again for
    contrast: still returns "Could not read served logs: HTTPConnectionPool(host='85c4305e5ae7',
    port=8793) ... NameResolutionError" verbatim, as expected for a run whose original container no
    longer exists and was never on the shared volume -- proves the distinction between "stale
    pre-final-fix run" and "run created after the fix's final state" is real and observable, not
    coincidental.
  source: Live docker compose stack + Airflow REST API (localhost:8080), fresh trigger + log fetch
    for all 6 tasks

- timestamp: 2026-08-30T11:52:00Z
  finding: |
    USER CONFIRMATION (checkpoint response): user checked run manual__post-fix-verify-1788090433
    directly in their own browser session at localhost:8080 (the actual UI click-through this
    session's self-verification could not perform via REST API alone) and confirmed logs display
    cleanly, with no secret_key error and no NameResolutionError. This closes the one remaining
    blind spot recorded in reasoning_checkpoint (browser-UI-layer verification) and
    reasoning_checkpoint_2 (real user confirmation of the double-recycle-safe fix). Both root
    causes are confirmed fixed end-to-end.
  source: User checkpoint response (verbatim, DATA_START/DATA_END block)

## Eliminated

- hypothesis: "Clock skew between Airflow containers (the error message's secondary cause)"
  evidence: "Not independently tested directly, but ruled out by elimination: adding the
    missing AIRFLOW__API__SECRET_KEY alone fully resolved log fetching for a fresh end-to-end
    run with zero other changes (no NTP sync, no container clock adjustment). If skew had been
    a contributing or sole cause, the same secret_key-mismatch class of error would have
    persisted or a token-expiry-style error would have appeared instead; neither occurred."
  timestamp: 2026-08-30T11:29:00Z

## Resolution

root_cause: |
  TWO independent, sequentially-discovered root causes (not an AND-gate — each alone fully
  explains its own distinct error text; the second was masked until the first was fixed and a
  container recycle exposed it):
  1. (secret_key mismatch, already fixed and user-confirmed) AIRFLOW__API__SECRET_KEY ([api]
     secret_key) was never set anywhere in docker-compose.yml. Airflow 3.0+ moved this key from
     [webserver] to [api]; it is used (alongside the already-fixed AIRFLOW__API_AUTH__JWT_SECRET)
     to sign/verify internal-API requests between components, including the log-serving request
     the UI makes when a user opens a task instance's logs. With it unset, every Airflow
     container (apiserver, scheduler, dag-processor, triggerer) minted its own random secret_key
     at startup, so a log-fetch request signed by one component's key was always rejected by
     whichever component verified it.
  2. (NameResolutionError on old logs, newly found and fixed) docker-compose.yml's shared
     x-airflow-common.volumes never mounted a shared/persistent volume for /opt/airflow/logs
     (base_log_folder). Each of the 5 Airflow components wrote task logs to its own local,
     ephemeral container filesystem. Airflow's UI log view falls back to an HTTP fetch from
     TaskInstance.hostname:8793 whenever the log isn't found locally; that hostname is the
     container's own short container-ID, which stops resolving the instant the original
     container is recreated (image rebuild, `docker compose down`/`up`, `--force-recreate`, host
     reboot) — permanently orphaning every log written before the recycle. This is a standing
     structural gap, not a one-off artifact: it was FIRST observed here specifically because this
     debugging session's own secret_key-fix container recreate happened to trigger it, but it
     would recur on any future recycle regardless of cause.
fix: |
  1. Added AIRFLOW__API__SECRET_KEY to the shared x-airflow-common env block in
     docker-compose.yml (fixed local-dev value
     "csv-ingest-local-dev-shared-api-secret-key-not-for-prod"), placed immediately after
     AIRFLOW__API_AUTH__JWT_SECRET with a comment recording the same root-cause shape, mirroring
     the existing pattern exactly so every Airflow service inherits the identical value via the
     shared YAML anchor.
  2. Added a shared named Docker volume `airflow-logs:/opt/airflow/logs` to the same
     x-airflow-common.volumes list (inherited by all 5 services via the same YAML anchor
     mechanism), plus a top-level `airflow-logs:` volume declaration alongside the existing
     postgres-db-volume/oracle-data pattern already used in this file for persistent state. This
     makes every Airflow component read/write the SAME persistent log storage, so apiserver
     almost always finds a task's log file locally (Airflow's FileTaskHandler checks local disk
     before falling back to the hostname:port HTTP fetch) and logs survive container recreates
     instead of being tied to a single container's lifetime.
verification: |
  Live end-to-end verification against the real running stack (Docker was reachable in this
  environment):
  1. Recreated all 4 Airflow containers with `docker compose up -d --force-recreate` to pick up
     the new env var; confirmed via `docker exec ... printenv AIRFLOW__API__SECRET_KEY` that all
     4 hold the identical value.
  2. All 6 compose services (postgres, oracle, airflow-apiserver, airflow-scheduler,
     airflow-dag-processor, airflow-triggerer) reported healthy post-recreate; scheduler logs
     showed no new errors.
  3. Triggered a fresh csv_ingest DAG run (dataset=customers) via the REST API, entirely under
     the fixed containers; it reached state=success.
  4. Fetched logs for all 6 of its task instances via
     `GET /api/v2/dags/csv_ingest/dagRuns/{run_id}/taskInstances/{task_id}/logs/1` — the exact
     endpoint the Airflow UI's log view calls. All 6 returned HTTP 200 with real log content
     (worker startup, task stdout, XCom push confirmation) and zero secret_key-mismatch or
     connection errors — the failure that was previously guaranteed on every task, every time.
  5. Cross-checked against a pre-fix task instance (executed under a now-destroyed container):
     it surfaced a different, expected error (stale hostname DNS resolution from the
     force-recreate) with zero secret_key mentions, confirming the secret_key class of failure
     specifically is what's resolved.
  Fix-acceptance guardrail (all applicable signals — see structured record below): also ran a
  full revert-and-reconfirm cycle (signal 5) since Docker was live: `git stash` reverted
  docker-compose.yml, containers recreated, a fresh DAG run's log fetch reproduced the EXACT
  verbatim original error text ("!!!! Please make sure that all your Airflow components ...");
  `git stash pop` reapplied the fix, containers recreated again, a fresh DAG run's log fetch
  returned clean real log content with zero secret_key mentions. This proves the specific
  change (not some other concurrent factor) is what fixes the bug.
  oracle_type: derived — the fix target (an internal-API signature-verification error path)
  and its resolution were derived from Airflow's own documented [api] secret_key contract
  (official Configuration Reference), not merely from re-observing the single reported error
  string disappear.

  --- Fix 2: shared airflow-logs volume (NameResolutionError / orphaned logs) ---
  Live end-to-end verification against the real running stack, including a genuine double-recycle
  test (not merely re-checking the same containers):
  1. Applied fix, force-recreated all 4 Airflow containers; `docker compose config` confirmed the
     volume renders in all 5 services; ownership auto-inherited correctly (50000:0, mode 775, no
     manual chmod/AIRFLOW_UID setup needed); a write-test file created in the scheduler container
     was immediately visible from the apiserver container at the same path.
  2. RECURRENCE TEST: triggered a fresh run under the fixed containers, confirmed success, then
     force-recreated the containers AGAIN (simulating a future recycle, e.g. an image rebuild).
     Fetched the pre-recycle run's logs afterward: HTTP 200, and the log-source line now reads a
     LOCAL FILE PATH instead of an http://{hostname}:8793/... URL — direct proof the fragile
     cross-container HTTP fetch is no longer even attempted for logs on the shared volume.
  3. REVERT-AND-RECONFIRM: removed only the new volume lines (secret_key fix untouched),
     force-recreated, ran a DAG, force-recreated again — reproduced the EXACT NameResolutionError
     shape on the newly-orphaned hostname. Re-added the volume lines, force-recreated, ran a DAG,
     fetched logs — resolved again (local-file-path source, real content). Proves the volume
     change specifically (not incidental container timing) is what fixes this bug class.
  4. Also independently confirmed branch (a) of the investigation directive: a fresh run
     triggered immediately after the secret_key fix (before the logs-volume fix existed) fetched
     cleanly for all 6 tasks — the originally reported build_report_task error was specifically
     because that run predated the secret_key-fix recreate, not a currently-reproducible failure
     at that moment. Branch (b) (structural gap) was then confirmed and fixed separately, since
     without it the SAME error class would recur on the next recycle regardless of cause.
  Not yet confirmed for either fix: an actual click-through in the user's own browser session
  against localhost:8080 (self-verification here used the REST API directly). Requesting user
  confirmation before archiving both fixes together.

  --- Third round: re-investigating user's NameResolutionError report post-volume-fix ---
  User's checkpoint response reported NameResolutionError on run_id=
  manual__revert-check-1788089875, task_id=report_result_task, host='85c4305e5ae7'. Investigated
  per explicit directive rather than assuming either "fix is broken" or "user error":
  1. Fetched that run's own metadata: triggered_by="rest_api", logical_date exactly matches the
     run_id's embedded timestamp, and its 6 task instances all ran under hostname 85c4305e5ae7.
     Cross-referenced against this file's own revert-and-reconfirm Evidence entry: this IS the run
     created in that cycle's step 1 (deliberate, temporary revert of the volume fix, done by this
     debugging session itself to prove the bug reproduces on demand) -- not a run the user
     triggered, and not a run that ran under the fix's final applied state.
  2. `docker inspect` on the live scheduler container shows it was created 54 seconds AFTER that
     run's end_date -- i.e. the run finished, then the cycle's step 3 (re-add volume, final
     recreate) destroyed the container it ran under, orphaning its logs by design (this is the
     exact mechanism the fix protects against for FUTURE recycles, not a case the fix could ever
     retroactively repair for a run whose container is already gone).
  3. Confirmed docker-compose.yml's current working-tree diff has both fixes in final form with no
     stray revert (git diff + `docker compose config` render count of 6/6 for both). Confirmed via
     `docker exec` that the CURRENTLY running containers (created after the cycle's final recreate)
     hold the correct secret_key and share the same populated /opt/airflow/logs directory.
  4. Triggered a brand-new DAG run (manual__post-fix-verify-1788090433) under these final
     containers; state=success; all 6 tasks ran under the current hostname (e205b13a3d10); fetched
     all 6 logs via the same UI-facing endpoint: HTTP 200, local-file-path source line, zero
     secret_key/NameResolutionError mentions for every task.
  5. Re-fetched the stale run's log for contrast: still shows the exact NameResolutionError
     verbatim, as expected -- confirming the distinction between "stale artifact whose container
     no longer exists" and "run created under the fix's final state" is real, not coincidental or
     a symptom of the fix being incomplete.
  Conclusion: the fix remains correctly applied and working. The user's report was a false alarm
  caused by checking a run_id that this debugging session's own testing had created and then
  deliberately orphaned as part of proving the fix works -- not a new bug. No code/config change
  was needed in this round; this round is verification-only.
  oracle_type: derived — the fix target (FileTaskHandler's local-disk-before-HTTP-fallback log
  read order) and its resolution were derived from Airflow's own documented logging architecture
  (official Logging for Tasks / Logging Architecture docs), not merely from re-observing the
  single reported error string disappear; the double-recycle recurrence test additionally
  verifies the fix's persistence property, which a single before/after check could not.
verification_signals:
  target_test: { result: pass, note: "manual repro via real REST API log-fetch call against a
    live, fresh DAG run for both fixes — secret_key fix: 0 secret_key mentions; logs-volume fix:
    0 NameResolutionError mentions and log-source line shows a local file path" }
  mutation_check: { result: skipped, reason_if_skipped: "no Stryker/mutation tooling applies —
    change is a declarative YAML config value, not application code with a driving unit test" }
  no_op_deletion: { result: pass, deletion_justified_by_rca: n/a, note: "combined diff is a pure
    44-line addition (2 comments + 1 env var + 1 volume mount + 1 volume declaration); zero
    deletions or short-circuited behavior" }
  adjacent_tests: { result: skipped, reason_if_skipped: "no automated test suite covers
    docker-compose.yml; substituted a full-stack health check plus a genuine double-recycle
    recurrence test instead", suites_run: [] }
  revert_and_reconfirm: { result: pass, bug_returned_on_revert: true, fixed_on_reapply: true,
    note: "run independently for both fixes; the logs-volume revert test additionally required
    a second force-recreate after the revert to actually orphan a run's logs, since the bug only
    manifests on recycle, not immediately after removing the volume mount" }
  guardrail_verdict: accepted
files_changed:
  - docker-compose.yml: added AIRFLOW__API__SECRET_KEY to the shared x-airflow-common env block;
    added a shared named Docker volume (airflow-logs:/opt/airflow/logs) to the same
    x-airflow-common.volumes list plus its top-level volume declaration

## Notes

Unrelated to Phase 7's correlation fix (07-01 through 07-06) — this is a pre-existing environment
gap, structurally the same shape as the Phase 5 AIRFLOW__API_AUTH__JWT_SECRET fix recorded in
PROJECT.md's Key Decisions, just for a config knob that fix didn't cover.

SECOND finding (2026-08-30T11:30:00Z onward): user's checkpoint response surfaced a follow-on,
structurally different error (NameResolutionError on a stale container-ID hostname) while
verifying the secret_key fix. Investigated per explicit directive distinguishing "(a) artifact of
this session's container recreate" vs "(b) real ongoing structural gap" — both turned out true
simultaneously (not mutually exclusive): (a) explained the SPECIFIC reported instance; (b) was a
genuine standing gap (no shared/persistent volume for /opt/airflow/logs across the 5 Airflow
components) that would keep reproducing the same error class on every future container recycle
regardless of cause (image rebuilds, `docker compose down`/`up`, host reboots — not just
debugging-session recreates). Fixed (b) with a shared named Docker volume, live-verified
including a genuine double-recycle recurrence test and a full revert-and-reconfirm cycle. Both
fixes are bundled in the same uncommitted docker-compose.yml diff and are being confirmed by the
user together in one checkpoint round.

THIRD finding (2026-08-30T11:47:00Z onward): user's second checkpoint response reported the
NameResolutionError again, on run_id=manual__revert-check-1788089875. Investigated and confirmed
this run_id was created by THIS debugging session's own revert-and-reconfirm testing (step 1's
deliberate temporary revert) and its underlying container was destroyed by the very next recreate
(step 3's final re-apply) -- a stale test artifact, not a regression. Confirmed docker-compose.yml
is currently in its final correct state (both fixes present, verified via git diff + rendered
compose config + live container inspection) and confirmed with a brand-new DAG run
(manual__post-fix-verify-1788090433) that logs fetch cleanly under the fix's final state. No code
change was needed this round. User should check the NEW run_id (manual__post-fix-verify-1788090433)
or any run they trigger themselves going forward -- NOT manual__revert-check-1788089875 or the
earlier report_ready/build_report_task run, both of which are permanently orphaned test artifacts
whose containers no longer exist and were never on the shared volume.
