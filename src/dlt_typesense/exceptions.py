"""Destination-specific exceptions for Typesense loads."""

from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)


class TypesenseImportError(DestinationTerminalException):
    """Raised when Typesense document import fails in a non-retryable way.

    Typesense may return HTTP 200 with per-line failures in the JSONL response.
    Callers must parse each line; type/schema errors that will not succeed on
    retry should raise this (or a subclass) so dlt marks the job failed.
    """


class TypesenseTransientError(DestinationTransientException):
    """Raised for retryable Typesense failures (timeouts, 5xx, network)."""


class TypesensePartialImportError(TypesenseImportError):
    """Raised when some documents in an import batch failed permanently."""

    def __init__(self, message: str, *, failed_count: int, total_count: int) -> None:
        super().__init__(message)
        self.failed_count = failed_count
        self.total_count = total_count
