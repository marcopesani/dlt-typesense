"""Destination-specific exceptions and the SDK error → dlt taxonomy mapping."""

from __future__ import annotations

from functools import wraps
from typing import Any

import httpx
from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)
from dlt.common.typing import TFun
from typesense.exceptions import TypesenseClientError

# The typesense SDK re-raises raw httpx errors after its retry loop, so both
# families must be caught wherever the SDK is called.
TYPESENSE_ERRORS = (TypesenseClientError, httpx.HTTPError)

_TERMINAL_STATUSES = frozenset({400, 401, 403, 404, 409, 413, 422})
ERROR_DETAIL_MAX_LEN = 500


class TypesenseImportError(DestinationTerminalException):
    """Non-retryable Typesense import failure (includes per-line JSONL failures)."""


class TypesenseTransientError(DestinationTransientException):
    """Retryable Typesense failure (timeouts, 5xx, network)."""


class TypesensePartialImportError(TypesenseImportError):
    """Some documents in an import batch failed permanently."""

    def __init__(self, message: str, *, failed_count: int, total_count: int) -> None:
        super().__init__(message)
        self.failed_count = failed_count
        self.total_count = total_count


def map_typesense_error(
    exc: Exception, context: str
) -> TypesenseImportError | TypesenseTransientError:
    """Classify an SDK/httpx error as terminal or transient for dlt's retry engine.

    Typed SDK errors carry ``args == (status_code, message)``; so do base-class
    errors for statuses the SDK leaves unmapped (413, 429, 502, ...). Internal
    SDK errors and raw httpx errors carry no status and default to transient.
    """
    status: int | None = None
    detail = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, TypesenseClientError):
        if exc.args and isinstance(exc.args[0], int):
            status = exc.args[0]
        if len(exc.args) >= 2:
            detail = str(exc.args[1])
    suffix = f" (HTTP {status})" if status is not None else ""
    message = f"Typesense {context} failed{suffix}: {detail[:ERROR_DETAIL_MAX_LEN]}"
    if status in _TERMINAL_STATUSES:
        return TypesenseImportError(message)
    return TypesenseTransientError(message)


def wrap_typesense_error(f: TFun) -> TFun:
    """Map SDK/httpx errors to dlt's terminal/transient taxonomy (Weaviate pattern)."""

    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        except TYPESENSE_ERRORS as exc:
            raise map_typesense_error(exc, f.__name__) from exc

    return _wrap  # type: ignore[return-value]
