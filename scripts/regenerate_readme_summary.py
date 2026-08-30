"""Regenerate README.md's live Executive Summary section (D-11/D-12/D-13).

Runs a real ingestion for both datasets (customers, orders) via the same
HTTP-trigger -> deferred-wait -> Oracle-load flow already proven in
``scripts/dag_polling.py``/``tests/e2e/test_csv_ingest_e2e.py`` (Plan 01),
then opens one real Oracle connection and runs the exact two queries
mirrored from ``scripts/verify_evidence.sql`` (Task 1's own output -- the
business-report query text is intentionally NEVER re-authored independently
here, so the two can never silently diverge) to capture row-count and
customers-JOIN-orders evidence. The rendered Markdown is spliced into
``README.md`` between ``<!-- EXEC-SUMMARY:START -->``/``<!-- EXEC-SUMMARY:
END -->`` HTML-comment markers -- created at the very top of the file (ahead
of the existing ``# Lightweight Airflow...`` heading) on the first run, and
idempotently replaced (only the marked section, nothing else in README.md)
on every later run.

Every dataset generates a small, run-unique-seeded fresh fixture (D-12 --
"live", never a stale/content-identical re-run that LOAD-04's checksum
idempotency would silently short-circuit) via ``generator/generate_csv.py``,
written to disk only AFTER ``wait_for_file`` genuinely reports Airflow task
state ``deferred`` (Pitfall 4's exact ordering -- writing the file any
earlier risks the sensor short-circuiting without ever deferring). For the
``customers`` dataset specifically, the observed deferred-state moment and
``dag_run_id`` are captured as this run's own "deferred-wake proof line"
(D-11b) -- never spliced together from a different job's run.

If ANY step fails (trigger, poll timeout, Oracle query, file write), this
script exits non-zero and does NOT touch README.md -- the whole Executive
Summary body is built in memory first; README.md is written exactly once,
only after every step above has already succeeded. This is this plan's own
prohibition against a silent stale/misleading "proof of a working platform".

``scripts/dag_polling.py`` and ``generator/generate_csv.py`` have no
``__init__.py`` and are not installed packages, so both are loaded via
``importlib.util.spec_from_file_location`` -- the same convention already
established by ``tests/e2e/test_csv_ingest_e2e.py``, never a plain
``import``.

Usage:
    uv run python scripts/regenerate_readme_summary.py
"""

from __future__ import annotations

import importlib.util
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import oracledb
from csv_processor import load
from csv_processor.config.loader import load_config

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFIGS_DIR = _REPO_ROOT / "configs"
_DATA_DIR = _REPO_ROOT / "data"
_README_PATH = _REPO_ROOT / "README.md"

_DATASETS = ("customers", "orders")

_START_MARKER = "<!-- EXEC-SUMMARY:START -->"
_END_MARKER = "<!-- EXEC-SUMMARY:END -->"

# The dataset whose deferred-wake observation becomes the README's D-11b
# proof line -- Claude's Discretion per 06-CONTEXT.md/06-RESEARCH.md Open
# Question 2: captured from this job's own run, never spliced from a
# different run.
_DEFERRED_PROOF_DATASET = "customers"

# Mirrored verbatim from scripts/verify_evidence.sql's two SELECT statements
# (Task 1) -- never re-authored independently, per this plan's own key_link
# requirement, so the SQL script and this regeneration script can never
# silently diverge. FETCH FIRST 10 ROWS ONLY is the one addition here (D-11c
# top-N for the README, not present in the full evidence-script output).
_ROW_COUNT_SQL = """
SELECT
    im.dataset,
    im.file_name,
    im.checksum,
    im.total_rows,
    im.valid_rows,
    im.invalid_rows,
    im.status,
    im.processed_at
FROM ingestion_metadata im
WHERE im.processed_at = (
    SELECT MAX(im2.processed_at)
    FROM ingestion_metadata im2
    WHERE im2.dataset = im.dataset
)
ORDER BY im.dataset
"""

_BUSINESS_REPORT_SQL = """
SELECT
    c.country AS region,
    TRUNC(o.order_date, 'MM') AS order_month,
    COUNT(*) AS order_count,
    SUM(o.amount) AS total_amount,
    ROUND(AVG(o.amount), 2) AS avg_amount
FROM customers_valid c
JOIN orders_valid o ON o.customer_id = c.customer_id
GROUP BY c.country, TRUNC(o.order_date, 'MM')
ORDER BY region, order_month
FETCH FIRST 10 ROWS ONLY
"""


def _load_sibling_module(name: str, path: Path) -> ModuleType:
    """Load a non-package sibling script via ``importlib`` (see module
    docstring) -- mirrors ``tests/e2e/test_csv_ingest_e2e.py``'s exact
    convention, never a plain ``import``."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        msg = f"could not load module {name!r} from {path}"
        raise RuntimeError(msg)
    module = importlib.util.module_from_spec(spec)
    # Registering in sys.modules BEFORE exec_module matters for generate_csv:
    # its frozen dataclass (GeneratedCsv) uses postponed annotations, whose
    # forward-ref resolution looks the module up via
    # sys.modules[cls.__module__] -- mirrors
    # tests/e2e/test_csv_ingest_e2e.py's own documented workaround.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


dag_polling = _load_sibling_module("dag_polling", _REPO_ROOT / "scripts" / "dag_polling.py")
generate_csv = _load_sibling_module("generate_csv", _REPO_ROOT / "generator" / "generate_csv.py")


class RegenerationError(RuntimeError):
    """Raised whenever any step of the regeneration fails -- the only path
    that must ever leave README.md untouched (this plan's own prohibition)."""


def _clear_stale_fixtures(dataset: str, file_pattern: str) -> None:
    """Delete every pre-existing file matching ``file_pattern`` in the
    dataset's data dir BEFORE triggering -- a stale fixture left over from a
    prior run would make ``wait_for_file`` match immediately and never
    defer, silently invalidating the deferred-wake proof (Pitfall 4)."""
    data_dir = _DATA_DIR / dataset
    data_dir.mkdir(parents=True, exist_ok=True)
    for stale in data_dir.glob(file_pattern):
        stale.unlink()


def _run_ingestion(dataset: str) -> dict[str, Any]:
    """Trigger, wait-for-deferred, drop a fresh fixture, then wait for
    completion -- Pitfall 4's exact ordering (poll-then-assert-then-act).

    Returns a dict with ``dag_run_id``, ``deferred_observed_at`` (UTC,
    captured immediately after the deferred state was confirmed), and
    ``result`` (the DAG run's final ``load_results_task`` payload).

    Raises ``RegenerationError`` if the trigger, the deferred poll, or the
    completion poll fails -- never lets a partial ingestion silently look
    like success.
    """
    config_path = f"configs/datasets/{dataset}.json"
    config = load_config(
        _CONFIGS_DIR / "datasets" / f"{dataset}.json",
        defaults_path=_CONFIGS_DIR / "defaults.json",
    )
    _clear_stale_fixtures(dataset, config.file_pattern)

    try:
        run_id = dag_polling.trigger_dag(dataset, config_path)
        jwt_token = dag_polling.get_jwt_token(dag_polling.AIRFLOW_BASE_URL)

        # Poll until wait_for_file genuinely reaches "deferred" BEFORE any
        # fixture file is written -- never write the file before this
        # returns.
        dag_polling.wait_for_task_state(
            dag_polling.AIRFLOW_BASE_URL, run_id, "wait_for_file", jwt_token, "deferred"
        )
        deferred_observed_at = datetime.now(UTC)

        # D-12: a run-unique seed/filename (timestamp component) so LOAD-04's
        # checksum idempotency never silently returns a STALE prior result on
        # a content-identical re-run -- that would defeat "live".
        unique_suffix = time.time_ns()
        generated = generate_csv.generate_rows(
            config, rows=25, invalid_ratio=0.2, seed=unique_suffix % (2**31)
        )
        fixture_path = _DATA_DIR / dataset / f"{dataset}_{unique_suffix}.csv"
        generate_csv.write_csv(generated, config, fixture_path)

        result = dag_polling.wait_for_dag_run_result(
            dag_polling.AIRFLOW_BASE_URL, run_id, jwt_token
        )
    except Exception as exc:  # noqa: BLE001 -- deliberately broad: any failure here
        # must abort the whole regeneration (never a partial/stale README).
        msg = f"ingestion for dataset {dataset!r} failed: {exc}"
        raise RegenerationError(msg) from exc

    if result.get("status") not in {"SUCCESS", "SUCCESS_WITH_INVALID_ROWS"}:
        msg = f"ingestion for dataset {dataset!r} did not succeed: {result}"
        raise RegenerationError(msg)

    return {
        "dag_run_id": run_id,
        "deferred_observed_at": deferred_observed_at,
        "result": result,
    }


def _fetch_evidence() -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    """Open one real Oracle connection (``load.get_connection()``, never
    re-derived credentials -- 06-PATTERNS.md "Oracle connection lifecycle")
    and run the two queries mirrored from ``scripts/verify_evidence.sql``.

    Returns ``(row_count_rows, business_report_rows)``. Raises
    ``RegenerationError`` on any Oracle failure.
    """
    try:
        connection = load.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute(_ROW_COUNT_SQL)
            row_count_rows = cursor.fetchall()
            cursor.execute(_BUSINESS_REPORT_SQL)
            business_report_rows = cursor.fetchall()
        finally:
            connection.close()
    except oracledb.Error as exc:
        msg = f"evidence query against Oracle failed: {exc}"
        raise RegenerationError(msg) from exc
    return row_count_rows, business_report_rows


def _render_row_count_table(rows: list[tuple[Any, ...]]) -> str:
    header = (
        "| Dataset | File Name | Total Rows | Valid Rows | Invalid Rows "
        "| Status | Processed At (UTC) |\n"
        "|---|---|---|---|---|---|---|"
    )
    lines = [header]
    for (
        dataset,
        file_name,
        _checksum,
        total_rows,
        valid_rows,
        invalid_rows,
        status,
        processed_at,
    ) in rows:
        processed_str = (
            processed_at.isoformat() if hasattr(processed_at, "isoformat") else str(processed_at)
        )
        lines.append(
            f"| {dataset} | {file_name} | {total_rows} | {valid_rows} | {invalid_rows} "
            f"| {status} | {processed_str} |"
        )
    return "\n".join(lines)


def _render_business_report_table(rows: list[tuple[Any, ...]]) -> str:
    header = (
        "| Region | Order Month | Order Count | Total Amount | Avg Amount |\n|---|---|---|---|---|"
    )
    lines = [header]
    for region, order_month, order_count, total_amount, avg_amount in rows:
        month_str = (
            order_month.strftime("%Y-%m") if hasattr(order_month, "strftime") else str(order_month)
        )
        lines.append(f"| {region} | {month_str} | {order_count} | {total_amount} | {avg_amount} |")
    if len(lines) == 1:
        lines.append("| _(no rows yet -- run an ingestion for both datasets first)_ | | | | |")
    return "\n".join(lines)


def _render_deferred_proof_line(dataset: str, dag_run_id: str, observed_at: datetime) -> str:
    return (
        f"`wait_for_file` reported Airflow task state `deferred` for the "
        f"`{dataset}` dataset (`dag_run_id={dag_run_id}`) at "
        f"`{observed_at.isoformat()}` -- confirmed BEFORE the fixture file "
        f"existed on disk, proving the non-blocking file-wait genuinely "
        f"deferred rather than short-circuited against an already-present file."
    )


def render_executive_summary(
    ingestion_results: dict[str, dict[str, Any]],
    row_count_rows: list[tuple[Any, ...]],
    business_report_rows: list[tuple[Any, ...]],
) -> str:
    """Render the full Executive Summary Markdown body (without the marker
    lines themselves -- ``splice_readme`` adds those)."""
    regenerated_at = datetime.now(UTC).isoformat()
    proof = ingestion_results[_DEFERRED_PROOF_DATASET]
    deferred_line = _render_deferred_proof_line(
        _DEFERRED_PROOF_DATASET, proof["dag_run_id"], proof["deferred_observed_at"]
    )

    return f"""# Executive Summary

Live evidence of a working HTTP-trigger -> Airflow DAG -> Oracle ETL pipeline
(TEST-03/DOC-01), regenerated automatically after every merge to `main`
(D-11/D-12) by `scripts/regenerate_readme_summary.py` via
`.github/workflows/readme-summary.yml`, using the default `GITHUB_TOKEN`
(never a PAT -- D-13). Last regenerated: `{regenerated_at}`.

### Latest ingestion per dataset

{_render_row_count_table(row_count_rows)}

### Deferred-wake proof

{deferred_line}

### Customers x Orders business report (top 10)

Region is `customers.country` (no literal `region` column exists in this
schema -- explicit substitution, not silently assumed, D-10). Grouped by
region and month-of-`orders.order_date`; see `scripts/verify_evidence.sql`
for the full, un-truncated report and `make verify-evidence` to reproduce it.

{_render_business_report_table(business_report_rows)}
"""


def splice_readme(readme_text: str, exec_summary_body: str) -> str:
    """Idempotently replace only the content between
    ``<!-- EXEC-SUMMARY:START -->``/``<!-- EXEC-SUMMARY:END -->`` -- creating
    the markers at the very top of the file (ahead of any existing content)
    on the first run, never a naive full-file overwrite."""
    block = f"{_START_MARKER}\n\n{exec_summary_body}\n{_END_MARKER}"
    if _START_MARKER in readme_text and _END_MARKER in readme_text:
        start_idx = readme_text.index(_START_MARKER)
        end_idx = readme_text.index(_END_MARKER) + len(_END_MARKER)
        return readme_text[:start_idx] + block + readme_text[end_idx:]
    return f"{block}\n\n{readme_text}"


def main() -> int:
    try:
        ingestion_results: dict[str, dict[str, Any]] = {}
        for dataset in _DATASETS:
            ingestion_results[dataset] = _run_ingestion(dataset)

        row_count_rows, business_report_rows = _fetch_evidence()

        exec_summary_body = render_executive_summary(
            ingestion_results, row_count_rows, business_report_rows
        )

        current_readme = _README_PATH.read_text(encoding="utf-8")
        new_readme = splice_readme(current_readme, exec_summary_body)
    except RegenerationError as exc:
        # Every step above only ever mutates in-memory data or writes a new
        # CSV fixture -- README.md itself is never touched until this point,
        # which we never reach on failure.
        print(
            f"ERROR: Executive Summary regeneration failed, README.md left untouched: {exc}",
            file=sys.stderr,
        )
        return 1

    _README_PATH.write_text(new_readme, encoding="utf-8")
    print("README.md's Executive Summary regenerated successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
