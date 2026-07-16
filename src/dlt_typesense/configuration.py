"""Credentials and client configuration for the Typesense destination."""

from __future__ import annotations

import dataclasses
from typing import Annotated, Final

from dlt.common.configuration import NotResolved, configspec
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhConfiguration,
)


@configspec
class TypesenseCredentials(CredentialsConfiguration):
    """Connection credentials for a Typesense cluster or Cloud instance."""

    host: str = "localhost"
    port: int = 8108
    protocol: str = "http"
    api_key: str = None  # type: ignore[assignment]
    """Admin API key used for collection and document writes."""

    connection_timeout_seconds: float = 5.0
    """Client connection timeout in seconds."""


@configspec
class TypesenseClientConfiguration(DestinationClientDwhConfiguration):
    """Destination configuration for Typesense collections and import jobs."""

    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="typesense", init=False, repr=False, compare=False
    )
    credentials: TypesenseCredentials = None  # type: ignore[assignment]

    dataset_separator: str = "_"
    """Separator between dataset name and table name in collection names."""

    # Optional empty dataset allowed (same pattern as qdrant); base type is str.
    dataset_name: Annotated[str | None, NotResolved()] = dataclasses.field(  # type: ignore[assignment]
        default=None, init=False, repr=False, compare=False
    )

    # Import / scale knobs (used by load jobs once implemented)
    client_batch_size: int = 1000
    """Number of documents per HTTP import request (client-side chunking)."""

    server_batch_size: int = 40
    """Typesense `batch_size` query param for import↔search interleave."""

    import_action: str = "upsert"
    """Default import action. Prefer upsert/emplace over create for retry safety."""

    max_parallel_load_jobs: int | None = None
    """Optional override for loader parallelism against a single node."""

    read_timeout_seconds: float = 180.0
    """Read timeout for long-running import requests."""

    def fingerprint(self) -> str:
        """Return a stable fingerprint of the connection location."""
        from dlt.common.utils import digest128

        creds = self.credentials
        if creds is None:
            return ""
        return digest128(f"{creds.protocol}://{creds.host}:{creds.port}")

    def physical_location(self) -> str:
        """Return a displayable Typesense URL."""
        creds = self.credentials
        if creds is None:
            return "typesense://"
        return f"{creds.protocol}://{creds.host}:{creds.port}"

    def can_write_from(self, other: DestinationClientConfiguration) -> bool:
        """Typesense cannot execute SQL models."""
        return False

    def can_read_from(self, other: DestinationClientConfiguration) -> bool:
        """Typesense does not support dlt SQL joins."""
        return False

    def __str__(self) -> str:
        return self.physical_location()
