"""Path/dataset-safety helpers for ``csv_ingest.py`` (D-05, DAG-02, T-05-01).

Plain, unit-testable functions with zero Airflow import (05-RESEARCH.md
"Validation Architecture" Wave 0 Gaps) -- mirrors
``csv_processor.config.loader``'s docstring/error-wrapping convention, but
this module is DAG-side, not part of ``csv_processor`` itself (this codebase's
established "Airflow imports only ever live under airflow/dags/" boundary,
ENGINE-09's spirit extended informally to the whole repo).
"""

from __future__ import annotations

from pathlib import Path

# D-05: file input path is not a config.json field -- it's this already-locked
# container-side convention (Phase 2 D-06's host ./data/<dataset>/ mount).
DATA_ROOT = Path("/opt/airflow/data")
CONFIGS_ROOT = Path("/opt/airflow/configs")
DEFAULTS_PATH = CONFIGS_ROOT / "defaults.json"
DATASETS_CONFIG_ROOT = CONFIGS_ROOT / "datasets"

# T-05-02: defense-in-depth re-check alongside the DAG's own Param(enum=[...]),
# matching this codebase's existing two-layer is_safe_identifier convention.
ALLOWED_DATASETS = frozenset({"customers", "orders"})


def resolve_matched_file(base_dir: Path, file_pattern: str) -> Path | None:
    """Glob ``base_dir`` for ``file_pattern`` and return the sorted-first match.

    ``FileSensor.poke()`` only ever returns a bare ``bool`` (05-RESEARCH.md
    Pitfall 1) -- ``process_csv_task`` must independently re-glob to get an
    actual file path once the sensor confirms existence; this is that re-glob.

    Args:
        base_dir: Directory to search (e.g. ``/opt/airflow/data/customers``).
        file_pattern: A shell-style glob, e.g. ``"customers_*.csv*"``.

    Returns:
        The sorted-first matching path, or ``None`` if nothing matches.
    """
    candidates = sorted(base_dir.glob(file_pattern))
    return candidates[0] if candidates else None


def validate_dataset(dataset: str) -> None:
    """Raise ``ValueError`` unless ``dataset`` is one of the known datasets.

    Defense-in-depth re-check alongside the DAG's own ``Param(enum=[...])``
    (T-05-02) -- Airflow's own JSON-Schema validation already rejects an
    unlisted value at trigger time, but this task-body check never trusts
    that alone.

    Args:
        dataset: The runtime-conf-supplied dataset name.

    Raises:
        ValueError: ``dataset`` is not in ``ALLOWED_DATASETS``.
    """
    if dataset not in ALLOWED_DATASETS:
        msg = f"unknown dataset {dataset!r}; must be one of {sorted(ALLOWED_DATASETS)}"
        raise ValueError(msg)


def resolve_safe_config_path(config_path: str) -> Path:
    """Resolve ``config_path`` and confirm it stays under ``configs/datasets/``.

    Rejects an absolute ``config_path`` FIRST, before any join, because
    ``Path.__truediv__``/``os.path.join`` silently discard the left operand
    when the right operand is absolute -- an unguarded join would let an
    absolute ``config_path`` (e.g. ``"/etc/passwd"``) bypass the allowlist
    entirely (T-05-01).

    Args:
        config_path: The runtime-conf-supplied config path, e.g.
            ``"configs/datasets/customers.json"``.

    Returns:
        The resolved, allowlisted path.

    Raises:
        ValueError: ``config_path`` is absolute, or its resolved path does
            not stay under ``DATASETS_CONFIG_ROOT``.
    """
    if Path(config_path).is_absolute():
        msg = f"config_path must be relative, got absolute path {config_path!r}"
        raise ValueError(msg)

    resolved = (Path("/opt/airflow") / config_path).resolve()
    allowed_root = DATASETS_CONFIG_ROOT.resolve()
    if not resolved.is_relative_to(allowed_root):
        msg = f"config_path {config_path!r} resolves outside {allowed_root} -- rejected"
        raise ValueError(msg)
    return resolved
