---
status: diagnosed
trigger: "UAT G-01-1: `make smoke-test` (reset && up && verify) -- docker compose up -d --wait reports airflow-apiserver Healthy, but immediately-following verify_airflow_auth() POST to /auth/token gets ConnectionResetError: [Errno 104] Connection reset by peer with a raw uncaught traceback. Bare retry of `make verify` (no restart) passes cleanly. Also: WR-03's just-shipped URLError catch (01-REVIEW-FIX.md, commit d7d0882) did not prevent the raw traceback."
created: 2026-08-28T17:00:00Z
updated: 2026-08-28T17:04:00Z
---

## Current Focus

status: ROOT CAUSE CONFIRMED for both parts (see Resolution). Investigation complete.
next_action: none -- return ROOT CAUSE FOUND to caller (goal: find_root_cause_only, no fix applied). Stack restored to healthy state and re-verified clean.

## Symptoms

expected: |
  A fresh `make reset && make up` (or `make smoke-test`) brings the full docker-compose stack
  (Oracle, Postgres, 5 Airflow services) to healthy, then `scripts/verify_environment.py`
  confirms all 5 Oracle tables exist and admin/admin authenticates against both Oracle and
  Airflow's /auth/token REST endpoint -- cleanly, no errors.
actual: |
  `docker compose up -d --wait` reported airflow-apiserver as Healthy before returning.
  Immediately after, verify_airflow_auth()'s POST http://localhost:8080/auth/token got
  ConnectionResetError: [Errno 104] Connection reset by peer, with a raw uncaught Python
  traceback (not a clean "FAILED: ..." message). A bare retry of `make verify` alone (no
  restart, no changes) immediately afterward passed cleanly.
errors: |
  ConnectionResetError: [Errno 104] Connection reset by peer
  raised inside urllib.request.urlopen() -> http.client.HTTPConnection.getresponse() ->
  response.begin() -> _read_status() -> socket readinto()
reproduction: |
  `make reset && make up && make verify` (or `make smoke-test`) from repo root.
  Test 1 in .planning/phases/01-environment-oracle-foundation/01-UAT.md.
  Reproduced on first attempt in this session; may be timing-sensitive (not guaranteed every run).
started: |
  Discovered during Phase 1 UAT, immediately after the 01-REVIEW-FIX.md code-review-fix pass
  (commit d7d0882, WR-03) which added a urllib.error.URLError except clause to
  verify_airflow_auth() specifically to handle "apiserver not reachable yet" -- that fix did not
  prevent this traceback.

## Eliminated

(none -- first hypothesis for each part confirmed directly, no alternatives needed elimination)

## Evidence

- timestamp: 2026-08-28T17:00:00Z
  checked: "docker-compose.yml full contents (already in required reading) + `docker inspect --format '{{json .Config.Healthcheck}}'` and `--format '{{json .State.Health}}'` on the running airflow-apiserver container"
  found: "docker-compose.yml defines an explicit `healthcheck:` block ONLY for `postgres` and `oracle`. The `airflow-apiserver` service (and scheduler/dag-processor/triggerer) has no `healthcheck:` key at all, and the base image `apache/airflow:3.3.1-python3.12` does not bake in a HEALTHCHECK either -- `docker inspect` returned `Config.Healthcheck: null` and `State.Health: null` for the apiserver container (contrast with oracle/postgres which return populated Health objects with Status/Log)."
  implication: "airflow-apiserver has zero real health monitoring. Any 'Healthy' status compose reports for it is not based on an actual readiness probe."

- timestamp: 2026-08-28T17:00:36Z
  checked: "Fresh `docker compose down -v` then `docker compose up -d --wait`, capturing wall-clock timestamps around the CLI call and correlating against `docker inspect .State.StartedAt` and `docker compose logs airflow-apiserver --timestamps`"
  found: "Container StartedAt=16:59:59.253Z. `docker compose up -d --wait` printed 'Container ...airflow-apiserver-1 Healthy' and returned control at 17:00:00.114Z -- under 1 second after container start. But the apiserver's own log shows the entrypoint's DB-connection-wait step + Python/Airflow bootstrap didn't finish printing the startup banner until 17:00:09.545Z, and uvicorn didn't log 'Uvicorn running on http://0.0.0.0:8080' (the last line of its ASGI startup sequence) until 17:00:12.350Z -- over 12 seconds AFTER compose already reported the service Healthy and returned."
  implication: "Confirms Docker Compose's `--wait` treats a service with no healthcheck as immediately 'Healthy' the instant the container process starts running, giving zero actual guarantee that the HTTP server inside is ready. The ~12s gap between compose's false-positive 'Healthy' and the real Uvicorn-ready log line is the exact window in which a client (verify_environment.py, run right after `make up`) can lose the race."

- timestamp: 2026-08-28T17:00:12Z
  checked: "A curl POST to http://localhost:8080/auth/token issued at 17:00:11.851Z (i.e., during the false-Healthy gap, ~0.5s before uvicorn's own 'Uvicorn running' log line)"
  found: "curl exited with code 56 (CURLE_RECV_ERROR -- 'Failure with receiving network data'), not code 7 (CURLE_COULDNT_CONNECT / connection refused). A retry after the gap closed (17:00:34Z) got a clean `HTTP/1.1 201 Created` with a JWT access_token."
  implication: "The failure mode during the gap is 'TCP accepted then reset', not 'connection refused outright' -- consistent with the reported ConnectionResetError, and distinguishes this from a simpler 'port not open yet' explanation."

- timestamp: 2026-08-28T17:02:00Z
  checked: "Python script using the exact same urllib.request.urlopen() call as scripts/verify_environment.py::verify_airflow_auth(), polling every 50ms starting immediately after `docker compose up -d --wait` returned 'Healthy'"
  found: "Every single attempt (30 attempts spanning 0.00s-1.50s post-'Healthy') raised `ConnectionResetError: [Errno 104] Connection reset by peer` uncaught by either `except urllib.error.HTTPError` or `except urllib.error.URLError`. Printed MRO: `['ConnectionResetError', 'ConnectionError', 'OSError', 'Exception', 'BaseException', 'object']` -- URLError is not in this chain."
  implication: "Direct, repeatable confirmation that ConnectionResetError is never wrapped as URLError in this failure path -- WR-03's fix (01-REVIEW-FIX.md, commit d7d0882) structurally cannot catch this exception type no matter how it's ordered."

- timestamp: 2026-08-28T17:02:30Z
  checked: "Full-window poll: `docker compose up -d` (no --wait) then immediate urllib polling every 20ms from the moment the 'Started' compose event fired, through to first success"
  found: "First poll attempt (t=0.00s, fired before compose even printed any 'Healthy' line) already raised ConnectionResetError. All 604 attempts over 12.66 seconds raised ConnectionResetError -- never ConnectionRefusedError, never a timeout. Attempt 605 (t=12.66s) succeeded with HTTP 201 + access_token."
  implication: "The ConnectionResetError behavior holds for the ENTIRE cold-start gap, not just near the tail end -- ruling out an alternate hypothesis where only the last instant before readiness produces resets and earlier instants would produce plain refused-connection errors. This matters for part 2: there is no code path here where a plain (URLError-wrappable) ConnectionRefusedError would ever occur first -- the WR-03 fix was never going to help for this specific gap, on this specific Docker setup."

- timestamp: 2026-08-28T17:03:00Z
  checked: "`ss -tlnp` on the host/WSL2 namespace while stack is up (showed 127.0.0.1:8080 already listening) + `docker info` (Operating System: Docker Desktop, Server Version 29.7.2, on Linux 6.18.33.2-microsoft-standard-WSL2)"
  found: "This environment runs Docker Desktop (not a bare native dockerd), which uses its own port-forwarding layer for published container ports on WSL2/Windows. Corroborates that the host-published port 127.0.0.1:8080 can be accepting TCP connections at the Docker Desktop networking layer independent of whether the in-container uvicorn process has bound/is ready to serve -- explaining the accept-then-reset pattern (vs. a bare-Linux-dockerd iptables DNAT, which would typically produce an immediate ECONNREFUSED when nothing is listening inside the container)."
  implication: "This is likely Docker-Desktop/WSL2-specific behavior -- the exact manifestation (reset vs refused) may differ on native Linux Docker, but the underlying race (healthcheck-less service reported Healthy before app readiness) is universal regardless of platform."

- timestamp: 2026-08-28T17:03:30Z
  checked: "CPython 3.12 source of `urllib.request.AbstractHTTPHandler.do_open()` via `inspect.getsource()`"
  found: |
    ```python
    try:
        try:
            h.request(req.get_method(), req.selector, req.data, headers, ...)
        except OSError as err:  # timeout error
            raise URLError(err)
        r = h.getresponse()
    except:
        h.close()
        raise
    ```
    The `except OSError as err: raise URLError(err)` wrap applies ONLY around `h.request(...)` (the connect+send phase). `r = h.getresponse()` -- where the response is read via `response.begin()` -> `_read_status()` -> socket `readinto()` -- sits OUTSIDE that inner except, covered only by the outer bare `except: h.close(); raise`, which re-raises the original exception unchanged.
  implication: "Definitive mechanism for part 2: urllib's URLError-wrapping is structurally scoped to the connect/send phase, not the response-read phase. In this bug, the TCP connect+send succeeds (Docker Desktop's port-forward layer accepts the connection and passes the request through), so `h.request()` never raises -- the reset only happens later while reading the response inside `h.getresponse()`, a code path urllib never wraps as URLError regardless of Python version or how the except clauses in verify_airflow_auth() are written/ordered."

- timestamp: 2026-08-28T17:04:00Z
  checked: "Re-ran `uv run python scripts/verify_environment.py` against the now-fully-warm stack after investigation concluded"
  found: "All three checks passed cleanly (5 tables, expected columns, admin/admin /auth/token 201 + access_token). `docker compose ps` shows all 6 services Up/healthy."
  implication: "Confirms the stack was restored to a running/healthy state after investigation, per instructions -- this is a transient cold-start race, not a persistent defect in the running services themselves."

## Resolution

root_cause:
  - "PART 1 (docker-compose false-Healthy): `airflow-apiserver` has no `healthcheck:` defined in docker-compose.yml (config category) -- confirmed via `docker inspect` (Config.Healthcheck: null). Because Docker Compose's `--wait` treats a service with no healthcheck as immediately 'Healthy' once the container process starts (not once the app inside is actually ready), `make up`/`make smoke-test` returns control ~12 seconds before uvicorn finishes its ASGI startup sequence (entrypoint DB-wait checks + Airflow bootstrap + Simple Auth Manager init + uvicorn bind/startup, empirically 12.66s in this session). Combined with this specific environment's Docker Desktop/WSL2 port-forwarding layer (environment category) already accepting TCP connections on the published host port before the in-container listener is ready, any client connecting during that ~12s gap gets its TCP handshake accepted and request sent, then the connection reset while reading the response -- producing ConnectionResetError rather than a plain connection-refused. Both conditions (no healthcheck AND this Docker networking behavior) are needed together to produce the exact symptom observed (Healthy-before-ready + reset-not-refused); on a bare native-Linux dockerd the missing-healthcheck race would likely still exist but might manifest as ConnectionRefusedError instead, which WR-03's URLError catch WOULD have caught."
  - "PART 2 (verify script doesn't catch it): `scripts/verify_environment.py::verify_airflow_auth()` catches `urllib.error.HTTPError` and `urllib.error.URLError` (code category, WR-03/commit d7d0882), but CPython's `urllib.request.AbstractHTTPHandler.do_open()` only wraps `OSError` into `URLError` around the connect+send phase (`h.request(...)`) -- confirmed by reading its source directly. The response-read phase (`h.getresponse()`) is outside that wrapping, covered only by a bare `except: raise` that re-raises unchanged. In this bug, the TCP connect+send succeeds (per Docker Desktop's port-forward behavior above), so the reset only surfaces later inside `h.getresponse()` as a raw, unwrapped `ConnectionResetError` (MRO: ConnectionResetError -> ConnectionError -> OSError -> Exception, no URLError anywhere in it). WR-03's fix was scoped to the wrong failure category for this specific race -- it addresses 'apiserver unreachable/refused' (which IS wrapped as URLError) but not 'apiserver reachable-but-reset-mid-response' (which structurally never is, in any Python version, given urllib's do_open() implementation)."
fix: ""
verification: ""
files_changed: []
