"""Document-id generation and json serialization (no server required)."""

from __future__ import annotations

import json

import pytest
from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext

from dlt_typesense.exceptions import TypesenseImportError
from dlt_typesense.load_jobs import TypesenseLoadJob


def _job(collection_name: str = "catalog_products") -> TypesenseLoadJob:
    return TypesenseLoadJob("/tmp/products.abc.0.jsonl", collection_name)


def _our_caps() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps.supported_merge_strategies = ["upsert", "insert-only"]
    return caps


def test_merge_id_is_deterministic_across_runs() -> None:
    row = {"sku": "A1", "title": "Widget", "_dlt_id": "row-1"}
    id_first = _job()._document_id(row, ["sku"])
    id_second = _job()._document_id(row, ["sku"])
    assert id_first == id_second
    assert _job()._document_id({"sku": "A2"}, ["sku"]) != id_first


def test_merge_id_depends_on_collection() -> None:
    row = {"sku": "A1"}
    assert _job("cat_a")._document_id(row, ["sku"]) != _job("cat_b")._document_id(row, ["sku"])


def test_compound_primary_key() -> None:
    job = _job()
    a = job._document_id({"tenant": "t1", "sku": "A1"}, ["tenant", "sku"])
    b = job._document_id({"tenant": "t2", "sku": "A1"}, ["tenant", "sku"])
    c = job._document_id({"tenant": "t1", "sku": "A1"}, ["tenant", "sku"])
    assert a == c  # same tuple -> same id
    assert a != b  # differ in one component -> distinct


def test_id_from_dlt_id_when_no_key() -> None:
    assert _job()._document_id({"_dlt_id": "xyz", "a": 1}, None) == "xyz"


def test_iter_documents_sets_id_from_dlt_id() -> None:
    docs = list(
        _job()._iter_documents(
            iter([json.dumps({"name": "x", "_dlt_id": "row-9"})]),
            id_fields=None,
            json_fields=[],
        )
    )
    assert docs == [{"name": "x", "_dlt_id": "row-9", "id": "row-9"}]


def test_json_field_is_serialized_to_string() -> None:
    docs = list(
        _job()._iter_documents(
            iter([json.dumps({"payload": {"a": 1, "b": [2, 3]}, "_dlt_id": "r"})]),
            id_fields=None,
            json_fields=["payload"],
        )
    )
    assert isinstance(docs[0]["payload"], str)
    assert json.loads(docs[0]["payload"]) == {"a": 1, "b": [2, 3]}


def test_json_fields_detected_from_schema() -> None:
    table = {
        "name": "t",
        "columns": {
            "payload": {"name": "payload", "data_type": "json"},
            "title": {"name": "title", "data_type": "text"},
        },
    }
    assert TypesenseLoadJob._json_fields(table) == ["payload"]


def test_id_fields_append_returns_none() -> None:
    table = {"name": "events", "write_disposition": "append", "columns": {}}
    assert _job()._id_fields(table) is None


def test_id_fields_merge_uses_primary_key() -> None:
    table = {
        "name": "products",
        "write_disposition": "merge",
        "columns": {"sku": {"name": "sku", "primary_key": True, "data_type": "text"}},
    }
    assert _job()._id_fields(table) == ["sku"]


def test_id_fields_merge_unique_fallback() -> None:
    table = {
        "name": "products",
        "write_disposition": "merge",
        "columns": {"code": {"name": "code", "unique": True, "data_type": "text"}},
    }
    assert _job()._id_fields(table) == ["code"]


def test_id_fields_merge_no_key_raises() -> None:
    table = {"name": "products", "write_disposition": "merge", "columns": {}}
    with pytest.raises(TypesenseImportError):
        _job()._id_fields(table)


def test_id_fields_ignores_dlt_id_unique_hint() -> None:
    table = {
        "name": "products",
        "write_disposition": "merge",
        "columns": {"_dlt_id": {"name": "_dlt_id", "unique": True, "data_type": "text"}},
    }
    with pytest.raises(TypesenseImportError):
        _job()._id_fields(table)


def test_id_fields_unique_fallback_excludes_dlt_id() -> None:
    table = {
        "name": "products",
        "write_disposition": "merge",
        "columns": {
            "code": {"name": "code", "unique": True, "data_type": "text"},
            "_dlt_id": {"name": "_dlt_id", "unique": True, "data_type": "text"},
        },
    }
    assert _job()._id_fields(table) == ["code"]


def test_document_id_rejects_null_merge_key() -> None:
    job = _job()
    with pytest.raises(TypesenseImportError):
        job._document_id({"sku": None, "_dlt_id": "r"}, ["sku"])
    with pytest.raises(TypesenseImportError):
        job._document_id({"_dlt_id": "r"}, ["sku"])  # missing key column


def test_id_fields_insert_only_uses_dlt_id() -> None:
    table = {
        "name": "products",
        "write_disposition": "merge",
        "x-merge-strategy": "insert-only",
        "columns": {"sku": {"name": "sku", "primary_key": True, "data_type": "text"}},
    }
    with Container().injectable_context(_our_caps()):
        assert _job()._id_fields(table) is None
