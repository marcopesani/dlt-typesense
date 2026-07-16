"""Chunked import behavior against a faked official client (no server required)."""

from __future__ import annotations

from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any, cast

import pytest
from typesense.exceptions import ObjectNotFound

from dlt_typesense.exceptions import TypesenseImportError
from dlt_typesense.load_jobs import ImportSummary
from dlt_typesense.load_jobs import import_documents as _import_documents


def import_documents(
    ts: FakeTs, collection: str, documents: Iterable[dict[str, Any]], **kwargs: Any
) -> ImportSummary:
    return _import_documents(cast("Any", ts), collection, documents, **kwargs)


class FakeTs:
    """Stands in for typesense.Client: supports ts.collections[name].documents.import_()."""

    def __init__(self, respond=None) -> None:
        self.calls: list[tuple[str, list[dict[str, Any]], dict[str, Any]]] = []
        self._respond = respond or (lambda chunk: [{"success": True} for _ in chunk])
        self.collections = self  # ts.collections[name] resolves via __getitem__ below

    def __getitem__(self, name: str):
        return SimpleNamespace(documents=SimpleNamespace(import_=self._make_import(name)))

    def _make_import(self, name: str):
        def _import(documents, params):
            chunk = list(documents)
            self.calls.append((name, chunk, dict(params)))
            return self._respond(chunk)

        return _import


def test_import_chunks_and_forwards_server_batch_size() -> None:
    ts = FakeTs()
    docs = [{"id": str(i)} for i in range(5)]
    summary = import_documents(ts, "c", docs, client_batch_size=2, server_batch_size=40)
    assert summary.total_count == 5
    assert summary.failed_count == 0
    assert [len(chunk) for _, chunk, _ in ts.calls] == [2, 2, 1]
    assert all(params == {"action": "upsert", "batch_size": 40} for _, _, params in ts.calls)


def test_import_streams_lazily_not_buffered_whole() -> None:
    pulled = {"n": 0}

    def documents():
        for i in range(25):
            pulled["n"] += 1
            yield {"id": str(i)}

    pulled_at_first_call = {}

    def respond(chunk):
        pulled_at_first_call.setdefault("n", pulled["n"])
        return [{"success": True} for _ in chunk]

    summary = import_documents(FakeTs(respond), "c", documents(), client_batch_size=10)
    assert summary.total_count == 25
    assert pulled_at_first_call["n"] == 10


def test_import_collects_per_line_failures() -> None:
    def respond(chunk):
        return [
            {"success": True},
            {"success": False, "error": "bad type", "document": '{"x":1}'},
            {"success": True},
        ]

    summary = import_documents(FakeTs(respond), "c", [{"id": str(i)} for i in range(3)])
    assert summary.total_count == 3
    assert summary.failed_count == 1
    assert summary.first_errors and "bad type" in summary.first_errors[0]


def test_import_empty_iterable_makes_no_requests() -> None:
    ts = FakeTs()
    summary = import_documents(ts, "c", [])
    assert summary.total_count == 0
    assert ts.calls == []


def test_import_maps_sdk_errors() -> None:
    def respond(chunk):
        raise ObjectNotFound(404, "collection missing")

    with pytest.raises(TypesenseImportError):
        import_documents(FakeTs(respond), "c", [{"id": "1"}])
