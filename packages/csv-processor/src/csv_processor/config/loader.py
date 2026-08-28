"""``load_config`` -- merge a dataset JSON config over shared defaults, then
validate.

Ported pattern from ``dataplat/config/loader.py:39-72`` (read directly during
this phase's research): read two files, shallow-merge ``path``'s document over
``defaults_path``'s (dataset keys win on any collision, top-level keys only --
never a recursive/deep merge, since a shallow merge is what this project's flat
schema actually needs and a deep-merge would hide the exact footgun the
reference repo's own ``defaults.yaml`` comment calls out), then validate. A raw
``pydantic.ValidationError``, ``OSError``, or ``json.JSONDecodeError`` is never
allowed to escape this module -- every failure path re-raises as
``ConfigurationError`` so every caller catches exactly one exception type for
"this config is bad" (CONFIG-02's "before any CSV processing begins"
requirement).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from csv_processor.config.errors import ConfigurationError
from csv_processor.config.models import DatasetConfig


def _parse_json_text(text: str) -> dict[str, Any]:
    """Empty/whitespace-only text parses as ``{}`` rather than raising a JSON
    decode error -- an empty defaults file means "no shared defaults"; an
    empty dataset file is caught by ``DatasetConfig`` validation instead
    (missing required fields), still surfacing as ``ConfigurationError``.
    """
    stripped = text.strip()
    return json.loads(stripped) if stripped else {}


def load_config(path: Path, *, defaults_path: Path) -> DatasetConfig:
    """Load, merge and validate one dataset config.

    Args:
        path: The dataset-specific JSON file, e.g.
            ``configs/datasets/customers.json``.
        defaults_path: The shared-defaults JSON file merged under ``path``,
            e.g. ``configs/defaults.json``. A missing defaults file is treated
            as ``{}`` -- it simply contributes no shared keys, since a dataset
            config may be fully self-contained.

    Returns:
        The validated, frozen ``DatasetConfig``.

    Raises:
        ConfigurationError: ``path`` is missing, empty, malformed JSON, or the
            merged document fails ``DatasetConfig`` validation. The error's
            ``context`` always carries ``path`` (as ``str``) and ``errors`` (a
            list of structured error dicts -- for a validation failure, this
            is exactly ``pydantic.ValidationError.errors()``'s own complete,
            deterministic list, never truncated to the first error).
    """
    try:
        defaults_text = defaults_path.read_text(encoding="utf-8") if defaults_path.exists() else ""
        defaults = _parse_json_text(defaults_text)
        dataset = _parse_json_text(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigurationError(
            f"could not read dataset config at {path}: {exc}",
            context={
                "path": str(path),
                "errors": [{"loc": (), "msg": str(exc), "type": "config_read_error"}],
            },
        ) from exc

    merged = {**defaults, **dataset}
    try:
        return DatasetConfig.model_validate(merged)
    except ValidationError as exc:
        raise ConfigurationError(
            f"invalid dataset config at {path}: {exc}",
            context={"path": str(path), "errors": exc.errors()},
        ) from exc
