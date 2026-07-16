"""Credentials and client configuration for the Typesense destination."""

from __future__ import annotations

import dataclasses
from typing import Annotated, Final

import httpx
import typesense
from dlt.common.configuration import NotResolved, configspec
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhConfiguration,
)
from dlt.common.destination.exceptions import DestinationCapabilitiesException
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128
from typesense.exceptions import ConfigError

from dlt_typesense.exceptions import TypesenseImportError


@configspec
class TypesenseCredentials(CredentialsConfiguration):
    """Connection credentials for a Typesense cluster or Cloud instance."""

    host: str = "localhost"
    port: int = 8108
    protocol: str = "http"
    # TSecretStrValue keeps the key out of reprs/logs; missing key stays unresolved.
    api_key: TSecretStrValue = None  # type: ignore[assignment]
    """Admin API key used for collection and document writes."""

    connection_timeout_seconds: float = 5.0
    """Client connection timeout in seconds."""

    def get_client(self, *, read_timeout_seconds: float = 180.0) -> typesense.Client:
        """Build an official Typesense client for these credentials."""
        try:
            return typesense.Client(
                {
                    "nodes": [{"host": self.host, "port": self.port, "protocol": self.protocol}],
                    "api_key": self.api_key,
                    # The SDK passes this value verbatim to httpx.Client(timeout=...),
                    # so an httpx.Timeout preserves the connect/read split: fast
                    # failure on dead hosts, long reads for large imports.
                    "connection_timeout_seconds": httpx.Timeout(  # type: ignore[typeddict-item]
                        connect=self.connection_timeout_seconds,
                        read=read_timeout_seconds,
                        write=read_timeout_seconds,
                        pool=self.connection_timeout_seconds,
                    ),
                    # Retries stay with dlt's load engine (whole-job retry, restricted
                    # to idempotent upsert/emplace actions); the SDK must fail fast so
                    # transient errors bubble immediately.
                    "num_retries": 0,
                }
            )
        except ConfigError as exc:
            raise TypesenseImportError(f"Invalid Typesense configuration: {exc}") from exc


@configspec
class TypesenseClientConfiguration(DestinationClientDwhConfiguration):
    """Destination configuration for Typesense collections and import jobs."""

    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="typesense", init=False, repr=False, compare=False
    )
    credentials: TypesenseCredentials = None  # type: ignore[assignment]

    dataset_separator: str = "_"
    """Separator between dataset name and table name in collection names."""

    # Optional empty dataset (qdrant pattern); base type is str.
    dataset_name: Annotated[str | None, NotResolved()] = dataclasses.field(  # type: ignore[assignment]
        default=None, init=False, repr=False, compare=False
    )

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

    def on_resolved(self) -> None:
        supported_replace = ["truncate-and-insert"]
        if self.replace_strategy is not None and self.replace_strategy not in supported_replace:
            raise DestinationCapabilitiesException(
                f"replace_strategy='{self.replace_strategy}' is not supported by the Typesense "
                f"destination (no staging dataset). Supported: {supported_replace}."
            )
        supported_actions = ["upsert", "emplace"]
        if self.import_action not in supported_actions:
            raise DestinationCapabilitiesException(
                f"import_action='{self.import_action}' is not supported; use one of "
                f"{supported_actions}. 'create' breaks dlt's whole-file retry idempotency."
            )

    def fingerprint(self) -> str:
        """Return a stable fingerprint of the connection location."""
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
