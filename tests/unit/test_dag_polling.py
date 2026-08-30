"""Fast, no-network unit coverage for ``scripts/dag_polling.py``'s polling
helpers (06-01-PLAN.md Task 2).

Mocks ``urllib.request.urlopen`` entirely -- no real HTTP, no live
``docker compose`` stack -- so this file stays in the fast
``uv run pytest tests/unit/ -x`` loop for every future task commit in this
phase (Plan 04 later imports ``scripts/dag_polling.py`` directly and
benefits from this same coverage).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from scripts.dag_polling import wait_for_dag_run_result, wait_for_task_state


def _fake_response(body: bytes) -> MagicMock:
    """Build a mock matching ``urllib.request.urlopen``'s context-manager
    protocol, whose ``.read()`` returns ``body``."""
    response = MagicMock()
    response.read.return_value = body
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    return response


def _state_response(state: str) -> MagicMock:
    return _fake_response(json.dumps({"state": state}).encode("utf-8"))


def test_wait_for_task_state_polls_until_target_state_reached() -> None:
    """Proves the function genuinely polls (queued -> queued -> deferred),
    not just checks once on the first call."""
    responses = [
        _state_response("queued"),
        _state_response("queued"),
        _state_response("deferred"),
    ]
    with patch("scripts.dag_polling.urllib.request.urlopen", side_effect=responses) as mock_urlopen:
        wait_for_task_state(
            "http://localhost:8080",
            "run-1",
            "wait_for_file",
            "fake-jwt",
            "deferred",
            timeout=5.0,
            interval=0.01,
        )

    assert mock_urlopen.call_count == 3


def test_wait_for_task_state_raises_timeout_error_with_last_observed_state() -> None:
    """The target state is never observed within a short, sub-second bounded
    timeout -- Pitfall 4's explicit poll-then-assert behavior, never a
    sleep-then-hope, verified in isolation."""
    with patch(
        "scripts.dag_polling.urllib.request.urlopen",
        return_value=_state_response("queued"),
    ):
        with pytest.raises(TimeoutError) as exc_info:
            wait_for_task_state(
                "http://localhost:8080",
                "run-1",
                "wait_for_file",
                "fake-jwt",
                "deferred",
                timeout=0.1,
                interval=0.05,
            )

    assert "queued" in str(exc_info.value)


def test_wait_for_dag_run_result_extracts_results_for_task_id() -> None:
    """Extracts ``results[result_task_id]`` from a mocked JSON response
    shaped like docs/airflow-dag.md's documented ``wait`` endpoint
    response (final ndjson line carries the full result)."""
    final_line = json.dumps(
        {
            "state": "success",
            "results": {
                "load_results_task": {
                    "status": "SUCCESS_WITH_INVALID_ROWS",
                    "dataset": "orders",
                    "checksum": "cfa476de",
                    "file_name": "orders_20260829.csv",
                    "total_rows": 100,
                    "valid_rows": 90,
                    "invalid_rows": 10,
                    "duration_seconds": 0.259,
                }
            },
        }
    )
    heartbeat_line = json.dumps({"state": "running"})
    body = f"{heartbeat_line}\n{final_line}\n".encode()

    with patch(
        "scripts.dag_polling.urllib.request.urlopen",
        return_value=_fake_response(body),
    ):
        result = wait_for_dag_run_result(
            "http://localhost:8080",
            "run-1",
            "fake-jwt",
            result_task_id="load_results_task",
        )

    assert result["status"] == "SUCCESS_WITH_INVALID_ROWS"
    assert result["dataset"] == "orders"
    assert result["valid_rows"] == 90
    assert result["invalid_rows"] == 10
