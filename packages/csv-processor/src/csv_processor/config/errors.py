"""Local configuration exception -- never imported from ``dataplat`` (CLAUDE.md's
two-tier reuse rule). Mirrors ``dataplat.errors.ConfigurationError``'s call
signature (``ConfigurationError(message, context={...})``) closely enough that the
loader.py porting pattern in 02-RESEARCH.md applies unmodified, but this is a plain,
standalone ``Exception`` subclass with zero coupling to the reference repo.
"""

from __future__ import annotations


class ConfigurationError(Exception):
    """Raised whenever a dataset config fails to load or validate.

    Wraps every malformed-config path (missing file, empty file, JSON decode
    error, Pydantic ``ValidationError``) behind one exception type so callers
    only ever need to catch ``ConfigurationError`` for "this config is bad"
    (CONFIG-02).

    Attributes:
        context: Structured details about the failure -- always carries
            ``path`` (the config file path, as ``str``) and ``errors`` (a
            list of error dicts; for a Pydantic validation failure, exactly
            ``pydantic.ValidationError.errors()``'s own structured list).
    """

    def __init__(self, message: str, *, context: dict) -> None:
        super().__init__(message)
        self.context = context
