"""Tests for collection schema helpers."""

from __future__ import annotations

import pytest

from dlt_typesense.type_mapper import collection_schema_auto, map_dlt_type


def test_collection_schema_auto() -> None:
    schema = collection_schema_auto("catalog_products")
    assert schema["name"] == "catalog_products"
    assert schema["enable_nested_fields"] is True
    assert schema["fields"] == [{"name": ".*", "type": "auto"}]


def test_map_dlt_type_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        map_dlt_type("text")
