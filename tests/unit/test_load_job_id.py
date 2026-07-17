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
    # Golden pin: namespace/encoding must never change or every upsert duplicates.
    row = {"sku": "A1", "title": "Widget", "_dlt_id": "row-1"}
    assert _job()._document_id(row, ["sku"]) == "d93899e3-6e93-57c0-94a1-d6905be62c70"
    assert _job()._document_id({"sku": "A2"}, ["sku"]) != "d93899e3-6e93-57c0-94a1-d6905be62c70"


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


def test_compound_key_json_encoding_prevents_ambiguity() -> None:
    """["a_b","c"] must not collide with ["a","b_c"] (docstring contract)."""
    from dlt_typesense.load_jobs import merge_document_id

    assert merge_document_id("c", ["a_b", "c"]) != merge_document_id("c", ["a", "b_c"])
    assert merge_document_id("c", ["a_b", "c"]) == "7d62c874-ddc4-5b1a-b523-ec3f2bef90fb"


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


def test_json_fields_skip_non_string_type_overrides() -> None:
    # A json column pinned to a Typesense type expecting a native JSON value
    # (float[] vector, string[], object) must keep that value; only plain and
    # string-typed json columns are stringified.
    table = {
        "name": "t",
        "columns": {
            "payload": {"name": "payload", "data_type": "json"},
            "note": {
                "name": "note",
                "data_type": "json",
                "x-typesense-field": {"type": "string"},
            },
            "embedding": {
                "name": "embedding",
                "data_type": "json",
                "x-typesense-field": {"type": "float[]", "num_dim": 3},
            },
            "tags": {
                "name": "tags",
                "data_type": "json",
                "x-typesense-field": {"type": "string[]"},
            },
            "attrs": {
                "name": "attrs",
                "data_type": "json",
                "x-typesense-field": {"type": "object"},
            },
        },
    }
    assert TypesenseLoadJob._json_fields(table) == ["payload", "note"]


def test_json_fields_hint_without_type_still_stringified() -> None:
    table = {
        "name": "t",
        "columns": {
            "payload": {
                "name": "payload",
                "data_type": "json",
                "x-typesense-field": {"facet": True},
            },
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
    with pytest.raises(TypesenseImportError, match="sku") as excinfo:
        job._document_id({"sku": None, "_dlt_id": "r"}, ["sku"])
    assert "catalog_products" in str(excinfo.value)
    with pytest.raises(TypesenseImportError, match="sku"):
        job._document_id({"_dlt_id": "r"}, ["sku"])  # missing key column


def test_empty_string_dlt_id_and_merge_key_are_accepted() -> None:
    # `is not None` check: empty string is a legal (if unwise) id component.
    assert _job()._document_id({"_dlt_id": ""}, None) == ""
    assert isinstance(_job()._document_id({"sku": ""}, ["sku"]), str)


def test_id_fields_nested_table_uses_dlt_id() -> None:
    table = {
        "name": "products__items",
        "write_disposition": "merge",
        "parent": "products",
        "columns": {"sku": {"name": "sku", "primary_key": True, "data_type": "text"}},
    }
    assert _job()._id_fields(table) is None


def test_id_fields_insert_only_uses_dlt_id() -> None:
    table = {
        "name": "products",
        "write_disposition": "merge",
        "x-merge-strategy": "insert-only",
        "columns": {"sku": {"name": "sku", "primary_key": True, "data_type": "text"}},
    }
    with Container().injectable_context(_our_caps()):
        assert _job()._id_fields(table) is None
