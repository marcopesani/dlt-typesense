"""Tests for collection schema building and dlt→Typesense type mapping."""

from __future__ import annotations

from typing import Any

import pytest

from dlt_typesense.exceptions import TypesenseSchemaError
from dlt_typesense.naming import NamingConvention
from dlt_typesense.type_mapper import (
    collection_schema_auto,
    collection_schema_from_table,
)

NORMALIZE = NamingConvention().normalize_identifier

AUTO_FIELD = {"name": ".*", "type": "auto"}


def make_table(
    columns: dict[str, dict[str, Any]] | None = None,
    collection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    table: dict[str, Any] = {"name": "products", "columns": columns or {}}
    if collection is not None:
        table["x-typesense-collection"] = collection
    return table


def build(table: dict[str, Any]) -> dict[str, Any]:
    return collection_schema_from_table("ds_products", table, NORMALIZE)


def test_unhinted_table_matches_auto_schema() -> None:
    table = make_table({"title": {"name": "title", "data_type": "text"}})
    assert build(table) == collection_schema_auto("ds_products")


def test_hinted_columns_are_pinned_before_catch_all() -> None:
    table = make_table(
        {
            "category": {
                "name": "category",
                "data_type": "text",
                "nullable": True,
                "x-typesense-field": {"facet": True},
            },
            "title": {"name": "title", "data_type": "text"},
        }
    )
    schema = build(table)
    assert schema["fields"] == [
        {"name": "category", "facet": True, "type": "string", "optional": True},
        AUTO_FIELD,
    ]


def test_explicit_type_override_wins() -> None:
    table = make_table(
        {
            "embedding": {
                "name": "embedding",
                "data_type": "json",
                "x-typesense-field": {"type": "float[]", "num_dim": 3},
            }
        }
    )
    field = build(table)["fields"][0]
    assert field["type"] == "float[]"
    assert field["num_dim"] == 3


def test_hint_only_incomplete_column_pinned_as_auto() -> None:
    table = make_table({"vec": {"name": "vec", "x-typesense-field": {"facet": True}}})
    field = build(table)["fields"][0]
    assert field["type"] == "auto"
    assert field["optional"] is True


def test_optional_defaults_from_nullable() -> None:
    table = make_table(
        {
            "sku": {
                "name": "sku",
                "data_type": "text",
                "nullable": False,
                "x-typesense-field": {"index": True},
            }
        }
    )
    assert build(table)["fields"][0]["optional"] is False


def test_explicit_optional_wins_over_nullable() -> None:
    table = make_table(
        {
            "sku": {
                "name": "sku",
                "data_type": "text",
                "nullable": False,
                "x-typesense-field": {"optional": True},
            }
        }
    )
    assert build(table)["fields"][0]["optional"] is True


def test_collection_hints_merged() -> None:
    table = make_table(
        columns={},
        collection={
            "enable_nested_fields": False,
            "token_separators": ["-"],
            "symbols_to_index": ["+"],
            "metadata": {"team": "search"},
        },
    )
    schema = build(table)
    assert schema["enable_nested_fields"] is False
    assert schema["token_separators"] == ["-"]
    assert schema["symbols_to_index"] == ["+"]
    assert schema["metadata"] == {"team": "search"}
    assert schema["fields"] == [AUTO_FIELD]


def test_default_sorting_field_auto_pins_column() -> None:
    table = make_table(
        columns={"price": {"name": "price", "data_type": "double", "nullable": True}},
        collection={"default_sorting_field": "price"},
    )
    schema = build(table)
    assert schema["default_sorting_field"] == "price"
    assert schema["fields"][0] == {"name": "price", "type": "float", "optional": False}


def test_default_sorting_field_normalized() -> None:
    # Table-level hint values are not normalized by dlt; `id` maps to `__id`.
    table = make_table(
        columns={"__id": {"name": "__id", "data_type": "bigint"}},
        collection={"default_sorting_field": "id"},
    )
    schema = build(table)
    assert schema["default_sorting_field"] == "__id"
    assert schema["fields"][0]["name"] == "__id"


def test_default_sorting_field_forces_non_optional() -> None:
    table = make_table(
        columns={
            "price": {
                "name": "price",
                "data_type": "double",
                "nullable": True,
                "x-typesense-field": {"sort": True},
            }
        },
        collection={"default_sorting_field": "price"},
    )
    assert build(table)["fields"][0]["optional"] is False


def test_default_sorting_field_missing_column_raises() -> None:
    table = make_table(columns={}, collection={"default_sorting_field": "nope"})
    with pytest.raises(TypesenseSchemaError, match="does not match any column"):
        build(table)


def test_default_sorting_field_non_numeric_raises() -> None:
    table = make_table(
        columns={"title": {"name": "title", "data_type": "text"}},
        collection={"default_sorting_field": "title"},
    )
    with pytest.raises(TypesenseSchemaError, match="must be one of"):
        build(table)


def test_default_sorting_field_type_override_allows_numeric() -> None:
    table = make_table(
        columns={
            "price": {
                "name": "price",
                "data_type": "decimal",
                "x-typesense-field": {"type": "float"},
            }
        },
        collection={"default_sorting_field": "price"},
    )
    schema = build(table)
    assert schema["default_sorting_field"] == "price"
    field = schema["fields"][0]
    assert field["name"] == "price"
    assert field["type"] == "float"
    assert field["optional"] is False
