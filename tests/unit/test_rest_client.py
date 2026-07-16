"""REST client transport behavior via a mocked httpx transport (no server)."""

from __future__ import annotations

import json

import httpx
import pytest

from dlt_typesense.configuration import TypesenseCredentials
from dlt_typesense.exceptions import TypesenseImportError, TypesenseTransientError
from dlt_typesense.rest_client import TypesenseRestClient


def _credentials() -> TypesenseCredentials:
    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol, creds.api_key = "localhost", 8108, "http", "k"
    return creds


def _client(handler) -> TypesenseRestClient:
    client = TypesenseRestClient(_credentials(), transport=httpx.MockTransport(handler))
    client.open()
    return client


def test_import_parses_per_line_failures() -> None:

    def handler(request: httpx.Request) -> httpx.Response:
        body = "\n".join(
            [
                json.dumps({"success": True}),
                json.dumps({"success": False, "error": "bad type", "document": '{"x":1}'}),
                json.dumps({"success": True}),
            ]
        )
        return httpx.Response(200, text=body)

    summary = _client(handler).import_documents("c", [{"id": "1"}, {"id": "2"}, {"id": "3"}])
    assert summary.total_count == 3
    assert summary.failed_count == 1
    assert summary.first_errors and "bad type" in summary.first_errors[0]


def test_import_streams_in_client_batches_and_passes_server_batch_size() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        lines = request.content.decode().splitlines()
        return httpx.Response(200, text="\n".join(json.dumps({"success": True}) for _ in lines))

    docs = [{"id": str(i)} for i in range(5)]
    summary = _client(handler).import_documents(
        "c", docs, client_batch_size=2, server_batch_size=40
    )
    assert summary.total_count == 5
    assert summary.failed_count == 0
    assert len(requests) == 3
    assert requests[0].url.params.get("batch_size") == "40"
    assert requests[0].url.params.get("action") == "upsert"


def test_import_streams_lazily_not_buffered_whole() -> None:
    pulled = {"n": 0}

    def documents():
        for i in range(25):
            pulled["n"] += 1
            yield {"id": str(i)}

    calls = {"n": 0}
    pulled_at_first_request = {}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            pulled_at_first_request["n"] = pulled["n"]
        lines = request.content.decode().splitlines()
        return httpx.Response(200, text="\n".join(json.dumps({"success": True}) for _ in lines))

    summary = _client(handler).import_documents("c", documents(), client_batch_size=10)
    assert summary.total_count == 25
    assert pulled_at_first_request["n"] == 10


@pytest.mark.parametrize("status", [400, 401, 403, 404, 413, 422])
def test_terminal_statuses_raise_terminal(status: int) -> None:

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"message": "nope"})

    with pytest.raises(TypesenseImportError):
        _client(handler).import_documents("c", [{"id": "1"}])


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_statuses_raise_transient(status: int) -> None:

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"message": "later"})

    with pytest.raises(TypesenseTransientError):
        _client(handler).import_documents("c", [{"id": "1"}])


def test_network_error_is_transient() -> None:

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    with pytest.raises(TypesenseTransientError):
        _client(handler).import_documents("c", [{"id": "1"}])


def test_error_message_never_contains_api_key() -> None:

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": "Forbidden - a valid X-TYPESENSE-API-KEY"})

    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol, creds.api_key = "localhost", 8108, "http", "SECRET_KEY"
    client = TypesenseRestClient(creds, transport=httpx.MockTransport(handler))
    client.open()
    with pytest.raises(TypesenseImportError) as excinfo:
        client.import_documents("c", [{"id": "1"}])
    assert "SECRET_KEY" not in str(excinfo.value)


def test_collection_exists_maps_status() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/present"):
            return httpx.Response(200, json={"name": "present"})
        return httpx.Response(404, json={"message": "Not Found"})

    client = _client(handler)
    assert client.collection_exists("present") is True
    assert client.collection_exists("absent") is False
