"""SDK error → terminal/transient taxonomy mapping (no server required)."""

from __future__ import annotations

import httpx
import pytest
from typesense.exceptions import (
    ObjectAlreadyExists,
    ObjectNotFound,
    ObjectUnprocessable,
    RequestForbidden,
    RequestMalformed,
    RequestUnauthorized,
    ServerError,
    ServiceUnavailable,
    TypesenseClientError,
)

from dlt_typesense.exceptions import (
    TypesenseImportError,
    TypesenseTransientError,
    map_typesense_error,
)


@pytest.mark.parametrize(
    "exc",
    [
        RequestMalformed(400, "bad request"),
        RequestUnauthorized(401, "bad key"),
        RequestForbidden(403, "forbidden"),
        ObjectNotFound(404, "missing"),
        ObjectAlreadyExists(409, "exists"),
        # Statuses the SDK leaves unmapped arrive as the base class.
        TypesenseClientError(413, "payload too large"),
        ObjectUnprocessable(422, "bad type"),
    ],
)
def test_terminal_statuses_map_terminal(exc: Exception) -> None:
    assert isinstance(map_typesense_error(exc, "test"), TypesenseImportError)


@pytest.mark.parametrize(
    "exc",
    [
        TypesenseClientError(429, "slow down"),
        ServerError(500, "boom"),
        TypesenseClientError(502, "bad gateway"),
        ServiceUnavailable(503, "later"),
        TypesenseClientError(504, "gateway timeout"),
        # Network errors escape the SDK as raw httpx exceptions.
        httpx.ConnectError("connection refused"),
        httpx.ReadTimeout("read timed out"),
        # Internal SDK errors carry a message but no status.
        TypesenseClientError("Cannot import an empty list of documents."),
    ],
)
def test_transient_and_statusless_errors_map_transient(exc: Exception) -> None:
    assert isinstance(map_typesense_error(exc, "test"), TypesenseTransientError)


def test_message_carries_context_status_and_detail() -> None:
    mapped = map_typesense_error(ObjectUnprocessable(422, "Field `v` type mismatch"), "import")
    message = str(mapped)
    assert "import" in message
    assert "422" in message
    assert "type mismatch" in message


def test_message_never_contains_api_key() -> None:
    # The mapper only ever sees exception args; the SDK sends the key as a
    # header and never embeds it in URLs or error messages.
    mapped = map_typesense_error(
        RequestUnauthorized(401, "Forbidden - a valid X-TYPESENSE-API-KEY was not sent"), "search"
    )
    assert "SECRET" not in str(mapped)
    assert "401" in str(mapped)
