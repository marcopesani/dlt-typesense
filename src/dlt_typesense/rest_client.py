"""Thin Typesense HTTP layer for streaming import and collection CRUD.

Prefer streaming JSONL over buffering whole files when loading millions of rows.
The official `typesense` client may be used for simple CRUD; import should stream.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from dlt_typesense.configuration import TypesenseCredentials


class TypesenseRestClient:
    """HTTP client seam for Typesense collections and document import."""

    def __init__(self, credentials: TypesenseCredentials) -> None:
        self._credentials = credentials
        # Wired in implementation: httpx.Client / typesense.Client
        self._client: Any = None

    def __enter__(self) -> TypesenseRestClient:
        raise NotImplementedError("TypesenseRestClient connection is not implemented yet.")

    def __exit__(self, *exc: object) -> None:
        raise NotImplementedError

    def create_collection(self, schema: dict[str, Any]) -> dict[str, Any]:
        """Create a Typesense collection from a schema document."""
        raise NotImplementedError

    def delete_collection(self, name: str) -> None:
        """Drop a Typesense collection."""
        raise NotImplementedError

    def collection_exists(self, name: str) -> bool:
        """Return True if the collection exists."""
        raise NotImplementedError

    def retrieve_collection(self, name: str) -> dict[str, Any]:
        """Retrieve collection metadata."""
        raise NotImplementedError

    def import_documents(
        self,
        collection_name: str,
        documents: Iterable[dict[str, Any]] | Iterator[str],
        *,
        action: str = "upsert",
        batch_size: int = 40,
    ) -> list[dict[str, Any]]:
        """Bulk-import documents via `/collections/{name}/documents/import`.

        Must parse the JSONL response line-by-line: HTTP 200 does not mean every
        document succeeded. Raise terminal vs transient errors accordingly.
        """
        raise NotImplementedError

    def search(
        self,
        collection_name: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Search a collection (used for state/schema lookups)."""
        raise NotImplementedError

    def upsert_document(
        self,
        collection_name: str,
        document: dict[str, Any],
    ) -> dict[str, Any]:
        """Upsert a single document (state/schema housekeeping)."""
        raise NotImplementedError

    def base_url(self) -> str:
        creds = self._credentials
        return f"{creds.protocol}://{creds.host}:{creds.port}"
