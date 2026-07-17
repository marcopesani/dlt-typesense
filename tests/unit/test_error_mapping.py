"""SDK error → terminal/transient taxonomy mapping (no server required)."""

from __future__ import annotations

import httpx
import pytest
from typesense.exceptions import (
    ObjectUnprocessable,
    ServerError,
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
        TypesenseClientError(429, "slow down"),
        ServerError(500, "boom"),
        TypesenseClientError(502, "bad gateway"),
        httpx.ConnectError("connection refused"),
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
    assert isinstance(mapped, TypesenseImportError)
