"""Thin Typesense HTTP layer built on httpx.

Import is streamed in client-sized chunks (never buffering whole files) and the
per-line import response is parsed line-by-line: Typesense returns HTTP 200 even
when individual documents fail, so success is decided per document, not by the
status code. Transport and HTTP errors are mapped onto the terminal/transient
taxonomy in :mod:`dlt_typesense.exceptions` so dlt retries only what can succeed
on retry.

The API key is sent as a header and is never placed in URLs, query strings, log
lines, or error messages.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx

from dlt_typesense.configuration import TypesenseCredentials
from dlt_typesense.exceptions import (
    TypesenseImportError,
    TypesenseTransientError,
)

API_KEY_HEADER = "X-TYPESENSE-API-KEY"

# HTTP statuses that will not succeed if the exact same request is retried.
# 413 (payload too large) is terminal: retrying the same oversized chunk cannot help.
_TERMINAL_STATUSES = frozenset({400, 401, 403, 404, 409, 413, 422})

# Cap on the number of failed-line samples kept for diagnostics (AC-NF-02).
_MAX_ERROR_SAMPLES = 5
_ERROR_SAMPLE_MAX_LEN = 500


@dataclass
class ImportSummary:
    """Outcome of a streamed bulk import.

    Only counts and a bounded set of failure samples are retained so that memory
    stays flat regardless of file size.
    """

    total_count: int = 0
    failed_count: int = 0
    first_errors: list[str] = field(default_factory=list)

    def add_failure(self, line: dict[str, Any]) -> None:
        self.failed_count += 1
        if len(self.first_errors) < _MAX_ERROR_SAMPLES:
            error = line.get("error", "unknown error")
            document = line.get("document")
            sample = f"{error}"
            if document is not None:
                sample = f"{error} | document: {str(document)[:_ERROR_SAMPLE_MAX_LEN]}"
            self.first_errors.append(sample[:_ERROR_SAMPLE_MAX_LEN])


def _chunked(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    """Yield lists of at most ``size`` items, consuming ``items`` lazily."""
    chunk: list[Any] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class TypesenseRestClient:
    """HTTP client for Typesense collections and streamed document import."""

    def __init__(
        self,
        credentials: TypesenseCredentials,
        *,
        read_timeout_seconds: float = 180.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._credentials = credentials
        self._read_timeout_seconds = read_timeout_seconds
        # Test seam: inject an httpx transport (e.g. MockTransport) to exercise
        # response handling without a live server.
        self._transport = transport
        self._client: httpx.Client | None = None

    # --- connection lifecycle -------------------------------------------------

    def open(self) -> None:
        if self._client is not None:
            return
        creds = self._credentials
        timeout = httpx.Timeout(
            connect=creds.connection_timeout_seconds,
            read=self._read_timeout_seconds,
            write=self._read_timeout_seconds,
            pool=creds.connection_timeout_seconds,
        )
        self._client = httpx.Client(
            base_url=self.base_url(),
            headers={API_KEY_HEADER: creds.api_key},
            timeout=timeout,
            transport=self._transport,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> TypesenseRestClient:
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def base_url(self) -> str:
        creds = self._credentials
        return f"{creds.protocol}://{creds.host}:{creds.port}"

    # --- low-level request helpers -------------------------------------------

    @property
    def _http(self) -> httpx.Client:
        if self._client is None:
            raise TypesenseTransientError("Typesense REST client is not open.")
        return self._client

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        try:
            return self._http.request(method, url, **kwargs)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            # Network-level failures are always worth retrying.
            raise TypesenseTransientError(
                f"Typesense request to {url} failed: {type(exc).__name__}: {exc}"
            ) from exc

    def _check_status(self, resp: httpx.Response, context: str) -> None:
        if resp.status_code < 400:
            return
        detail = _extract_message(resp)
        message = f"Typesense {context} failed (HTTP {resp.status_code}): {detail}"
        if resp.status_code in _TERMINAL_STATUSES:
            raise TypesenseImportError(message)
        # 5xx, 408, 429 and anything else: retry.
        raise TypesenseTransientError(message)

    # --- collection CRUD ------------------------------------------------------

    def create_collection(self, schema: dict[str, Any]) -> dict[str, Any]:
        resp = self._request("POST", "/collections", json=schema)
        # A concurrent creator winning the race (409) is fine: the collection exists.
        if resp.status_code == 409:
            return self.retrieve_collection(schema["name"])
        self._check_status(resp, "create collection")
        return resp.json()

    def delete_collection(self, name: str) -> None:
        resp = self._request("DELETE", f"/collections/{name}")
        if resp.status_code == 404:
            return
        self._check_status(resp, "delete collection")

    def collection_exists(self, name: str) -> bool:
        resp = self._request("GET", f"/collections/{name}")
        if resp.status_code == 404:
            return False
        self._check_status(resp, "retrieve collection")
        return True

    def retrieve_collection(self, name: str) -> dict[str, Any]:
        resp = self._request("GET", f"/collections/{name}")
        self._check_status(resp, "retrieve collection")
        return resp.json()

    def list_collections(self) -> list[dict[str, Any]]:
        resp = self._request("GET", "/collections")
        self._check_status(resp, "list collections")
        return resp.json()

    # --- document operations --------------------------------------------------

    def import_documents(
        self,
        collection_name: str,
        documents: Iterable[dict[str, Any]],
        *,
        action: str = "upsert",
        client_batch_size: int = 1000,
        server_batch_size: int = 40,
    ) -> ImportSummary:
        """Stream ``documents`` into a collection in client-sized chunks.

        Returns an :class:`ImportSummary`; per-line failures do not raise here so
        the caller can attach load context to the error. Transport and HTTP-level
        failures raise terminal/transient errors immediately.
        """
        summary = ImportSummary()
        params = {"action": action, "batch_size": server_batch_size}
        for chunk in _chunked(documents, client_batch_size):
            body = "\n".join(json.dumps(doc, ensure_ascii=False) for doc in chunk)
            resp = self._request(
                "POST",
                f"/collections/{collection_name}/documents/import",
                params=params,
                content=body.encode("utf-8"),
                headers={"Content-Type": "text/plain"},
            )
            self._check_status(resp, "document import")
            for line in resp.text.splitlines():
                if not line:
                    continue
                summary.total_count += 1
                result = json.loads(line)
                if not result.get("success", False):
                    summary.add_failure(result)
        return summary

    def upsert_document(
        self,
        collection_name: str,
        document: dict[str, Any],
        *,
        action: str = "upsert",
    ) -> dict[str, Any]:
        resp = self._request(
            "POST",
            f"/collections/{collection_name}/documents",
            params={"action": action},
            content=json.dumps(document, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "text/plain"},
        )
        self._check_status(resp, "document upsert")
        return resp.json()

    def get_document(self, collection_name: str, document_id: str) -> dict[str, Any] | None:
        resp = self._request("GET", f"/collections/{collection_name}/documents/{document_id}")
        if resp.status_code == 404:
            return None
        self._check_status(resp, "get document")
        return resp.json()

    def delete_document(self, collection_name: str, document_id: str) -> None:
        resp = self._request("DELETE", f"/collections/{collection_name}/documents/{document_id}")
        if resp.status_code == 404:
            return
        self._check_status(resp, "delete document")

    def delete_by_filter(self, collection_name: str, filter_by: str) -> int:
        resp = self._request(
            "DELETE",
            f"/collections/{collection_name}/documents",
            params={"filter_by": filter_by},
        )
        if resp.status_code == 404:
            return 0
        self._check_status(resp, "delete by filter")
        return int(resp.json().get("num_deleted", 0))

    def search(self, collection_name: str, params: dict[str, Any]) -> dict[str, Any]:
        resp = self._request(
            "GET", f"/collections/{collection_name}/documents/search", params=params
        )
        self._check_status(resp, "search")
        return resp.json()

    def search_documents(
        self,
        collection_name: str,
        *,
        filter_by: str | None = None,
        sort_by: str | None = None,
        per_page: int = 250,
        page: int = 1,
    ) -> list[dict[str, Any]]:
        """Return the document bodies of a wildcard search (hits only)."""
        params: dict[str, Any] = {"q": "*", "per_page": per_page, "page": page}
        if filter_by:
            params["filter_by"] = filter_by
        if sort_by:
            params["sort_by"] = sort_by
        response = self.search(collection_name, params)
        return [hit["document"] for hit in response.get("hits", [])]

    def count_documents(self, collection_name: str, *, filter_by: str | None = None) -> int:
        params: dict[str, Any] = {"q": "*", "per_page": 0}
        if filter_by:
            params["filter_by"] = filter_by
        response = self.search(collection_name, params)
        return int(response.get("found", 0))


def _extract_message(resp: httpx.Response) -> str:
    """Best-effort human-readable detail from a Typesense error response.

    Never includes credentials: Typesense error bodies carry only a ``message``.
    """
    try:
        payload = resp.json()
        if isinstance(payload, dict) and "message" in payload:
            return str(payload["message"])[:_ERROR_SAMPLE_MAX_LEN]
    except (ValueError, json.JSONDecodeError):
        pass
    return resp.text[:_ERROR_SAMPLE_MAX_LEN]
