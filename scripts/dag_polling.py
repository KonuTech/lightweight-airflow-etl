"""Reusable Airflow REST-API trigger/poll helpers for the automated e2e suite
(TEST-03, D-08) and any later evidence-regeneration script (06-01-PLAN.md
flags Plan 04 as a future reuser of this exact module).

Mirrors ``scripts/verify_environment.py``'s stdlib-only style (``urllib.request``/
``json``, no new HTTP client dependency) and reuses ``scripts/trigger_dag.sh``'s
already-proven ``/auth/token`` -> ``Bearer`` -> ``POST .../dagRuns`` flow verbatim
via ``subprocess.run`` -- never re-derives the auth/trigger mechanism in Python
(06-RESEARCH.md "Don't Hand-Roll": re-deriving this risks re-discovering the same
``logical_date: null``/``interval``-has-no-default gotchas Phase 5 already found
and documented in ``docs/airflow-dag.md``).
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.request
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_TRIGGER_SCRIPT = _REPO_ROOT / "scripts" / "trigger_dag.sh"

AIRFLOW_BASE_URL = "http://localhost:8080"
AIRFLOW_USER = "admin"
AIRFLOW_PASSWORD = "admin"


def get_jwt_token(base_url: str = AIRFLOW_BASE_URL) -> str:
    """POST ``/auth/token`` with admin/admin, return the JWT ``access_token``.

    Same endpoint/credential/payload shape as ``scripts/trigger_dag.sh`` and
    ``scripts/verify_environment.py::verify_airflow_auth`` -- no new auth
    mechanism invented for this module.
    """
    payload = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}/auth/token",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read().decode("utf-8"))
    token = body.get("access_token")
    if not token:
        msg = f"Airflow auth response missing access_token: {body}"
        raise RuntimeError(msg)
    return str(token)


def trigger_dag(dataset: str, config_path: str) -> str:
    """Trigger ``csv_ingest`` for ``dataset`` via ``scripts/trigger_dag.sh``
    (subprocess reuse of the already-proven auth+trigger flow, D-08's
    Pattern 1) -- never re-derives the flow in Python.

    Returns the triggered ``dag_run_id``.
    """
    result = subprocess.run(
        [str(_TRIGGER_SCRIPT), dataset, config_path],
        capture_output=True,
        text=True,
        check=True,
        cwd=_REPO_ROOT,
    )
    return result.stdout.strip()


def trigger_dag_generic(
    dag_id: str, conf: dict[str, object] | None = None, base_url: str = AIRFLOW_BASE_URL
) -> str:
    """Trigger any DAG (not just ``csv_ingest``) via a plain ``urllib`` POST.

    Generalizes ``trigger_dag()``'s auth-then-POST flow to an arbitrary
    ``dag_id``/``conf`` payload -- ``trigger_dag()``/``scripts/trigger_dag.sh``
    are hard-coded to ``csv_ingest``'s URL and ``{dataset, config_path}`` conf
    shape, so they cannot trigger a dataset-agnostic DAG like ``report_ready``,
    which takes no runtime conf at all. Mirrors ``scripts/trigger_dag.sh``'s
    exact ``{"conf": ..., "logical_date": null}`` payload shape (Airflow
    3.3.1's ``TriggerDAGRunPostBody`` marks ``logical_date`` as required-but-
    nullable, per ``docs/airflow-dag.md``'s own API note), generalized to any
    ``dag_id``.

    Returns the triggered ``dag_run_id``.
    """
    jwt_token = get_jwt_token(base_url)
    payload = json.dumps({"conf": conf or {}, "logical_date": None}).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}/api/v2/dags/{dag_id}/dagRuns",
        data=payload,
        headers={"Authorization": f"Bearer {jwt_token}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read().decode("utf-8"))
    dag_run_id = body.get("dag_run_id")
    if not dag_run_id:
        msg = f"Airflow trigger response missing dag_run_id: {body}"
        raise RuntimeError(msg)
    return str(dag_run_id)


def poll_task_instance_state(
    base_url: str, run_id: str, task_id: str, jwt_token: str, *, dag_id: str = "csv_ingest"
) -> str:
    """GET ``.../dagRuns/{run_id}/taskInstances/{task_id}``, return its
    ``state`` field as a string (``"None"`` when the field is JSON ``null``,
    e.g. before the task has been scheduled at all).

    ``dag_id`` defaults to ``csv_ingest`` (every pre-existing caller's own
    implicit assumption) but is overridable for any other DAG, e.g.
    ``report_ready``.
    """
    request = urllib.request.Request(
        f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}",
        headers={"Authorization": f"Bearer {jwt_token}"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read().decode("utf-8"))
    return str(body["state"])


def wait_for_task_state(
    base_url: str,
    run_id: str,
    task_id: str,
    jwt_token: str,
    target_state: str,
    *,
    timeout: float = 60.0,
    interval: float = 2.0,
    dag_id: str = "csv_ingest",
) -> None:
    """Bounded poll loop: return once ``task_id`` (within ``run_id``) reports
    ``target_state``.

    Pitfall 4's explicit poll-then-assert step -- never a sleep-then-hope.
    Raises ``TimeoutError`` (naming the last-observed state) if
    ``target_state`` is never reached before ``timeout`` seconds elapse.
    """
    deadline = time.monotonic() + timeout
    last_state: str | None = None
    while True:
        last_state = poll_task_instance_state(base_url, run_id, task_id, jwt_token, dag_id=dag_id)
        if last_state == target_state:
            return
        if time.monotonic() >= deadline:
            break
        time.sleep(interval)
    msg = (
        f"Timed out after {timeout}s waiting for task {task_id!r} (run {run_id!r}) "
        f"to reach state {target_state!r}; last observed state: {last_state!r}"
    )
    raise TimeoutError(msg)


def wait_for_dag_run_result(
    base_url: str,
    run_id: str,
    jwt_token: str,
    *,
    result_task_id: str = "load_results_task",
    timeout: float = 120.0,
    interval: float = 1.0,
    dag_id: str = "csv_ingest",
) -> dict[str, object]:
    """GET ``.../dagRuns/{run_id}/wait?result={result_task_id}&interval={interval}``,
    blocking server-side (Airflow's own ``wait`` endpoint) until the DAG run
    completes.

    ``interval`` is a required query parameter with no server-side default
    (docs/airflow-dag.md's "API note") -- always passed explicitly. The
    response body is newline-delimited JSON (``Accept: application/x-ndjson``)
    with intermediate heartbeat lines (e.g. ``{"state": "running"}``) followed
    by a final line carrying ``results``; only the final line is parsed.

    ``dag_id`` defaults to ``csv_ingest`` (every pre-existing caller's own
    implicit assumption) but is overridable for any other DAG.

    Returns the parsed ``results[result_task_id]`` dict.
    """
    url = (
        f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}/wait"
        f"?result={result_task_id}&interval={interval}"
    )
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {jwt_token}", "Accept": "application/x-ndjson"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    lines = [line for line in raw.splitlines() if line.strip()]
    if not lines:
        msg = f"Empty response body from {url}"
        raise RuntimeError(msg)
    final = json.loads(lines[-1])
    results = final.get("results", {})
    if result_task_id not in results:
        msg = f"wait endpoint response missing results[{result_task_id!r}]: {final}"
        raise RuntimeError(msg)
    return dict(results[result_task_id])
