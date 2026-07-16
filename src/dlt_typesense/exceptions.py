"""Destination-specific exceptions for Typesense loads."""

from dlt.common.destination.exceptions import (
    DestinationTerminalException,
    DestinationTransientException,
)


class TypesenseImportError(DestinationTerminalException):
    """Non-retryable Typesense import failure (includes per-line JSONL failures)."""


class TypesenseTransientError(DestinationTransientException):
    """Retryable Typesense failure (timeouts, 5xx, network)."""


class TypesenseSchemaError(DestinationTerminalException):
    """Invalid or unsatisfiable Typesense schema hints (retry cannot fix them)."""


class TypesensePartialImportError(TypesenseImportError):
    """Some documents in an import batch failed permanently."""

    def __init__(self, message: str, *, failed_count: int, total_count: int) -> None:
        super().__init__(message)
        self.failed_count = failed_count
        self.total_count = total_count
