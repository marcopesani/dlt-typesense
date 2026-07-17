"""Tests for typesense_adapter hint attachment and validation."""

from __future__ import annotations

from typing import Any

import dlt
import pytest
from dlt.extract import DltResource

from dlt_typesense.typesense_adapter import (
    COLLECTION_HINT,
    COLLECTION_PARAMS,
    FIELD_HINT,
    FIELD_PARAMS,
    typesense_adapter,
)


def make_resource() -> DltResource:
    @dlt.resource(name="products")
    def products() -> Any:
        yield {"sku": "A1", "category": "tools", "price": 9.99}

    return products()


def column_hint(resource: DltResource, column: str) -> dict[str, Any]:
    columns = resource.compute_table_schema()["columns"]
    return columns[column][FIELD_HINT]  # type: ignore[typeddict-item]


def test_facet_convenience_str() -> None:
    resource = typesense_adapter(make_resource(), facet="category")
    assert column_hint(resource, "category") == {"facet": True}


def test_convenience_lists() -> None:
    resource = typesense_adapter(make_resource(), facet=["category"], sort=["price"], index=["sku"])
    assert column_hint(resource, "category") == {"facet": True}
    assert column_hint(resource, "price") == {"sort": True}
    assert column_hint(resource, "sku") == {"index": True}


def test_conveniences_merge_on_same_column() -> None:
    resource = typesense_adapter(make_resource(), facet="category", sort="category")
    assert column_hint(resource, "category") == {"facet": True, "sort": True}


def test_field_hints_win_over_convenience() -> None:
    resource = typesense_adapter(
        make_resource(), facet="category", field_hints={"category": {"facet": False}}
    )
    assert column_hint(resource, "category") == {"facet": False}


def test_field_hints_full_params() -> None:
    embed = {"from": ["title"], "model_config": {"model_name": "ts/e5-small"}}
    resource = typesense_adapter(
        make_resource(),
        field_hints={
            "embedding": {"type": "float[]", "num_dim": 3, "vec_dist": "cosine"},
            "title": {"locale": "de", "infix": True, "token_separators": ["-"]},
            "auto_embedding": {"embed": embed},
        },
    )
    assert column_hint(resource, "embedding") == {
        "type": "float[]",
        "num_dim": 3,
        "vec_dist": "cosine",
    }
    assert column_hint(resource, "title") == {
        "locale": "de",
        "infix": True,
        "token_separators": ["-"],
    }
    assert column_hint(resource, "auto_embedding") == {"embed": embed}


@pytest.mark.parametrize(
    "typesense_type", ["float[]", "string[]", "int64[]", "object", "object[]", "geopoint"]
)
def test_native_json_type_override_sets_dlt_json_type(typesense_type: str) -> None:
    # Array/object types must keep the raw JSON value inline: dlt would
    # otherwise normalize lists into child tables and flatten dicts.
    resource = typesense_adapter(make_resource(), field_hints={"vec": {"type": typesense_type}})
    columns = resource.compute_table_schema()["columns"]
    assert columns["vec"]["data_type"] == "json"


@pytest.mark.parametrize("typesense_type", ["string", "int64", "float", "bool", "auto"])
def test_scalar_type_override_leaves_dlt_type_alone(typesense_type: str) -> None:
    resource = typesense_adapter(make_resource(), field_hints={"col": {"type": typesense_type}})
    columns = resource.compute_table_schema()["columns"]
    assert "data_type" not in columns["col"]


def test_collection_hints_land_on_table() -> None:
    hints = {
        "default_sorting_field": "price",
        "token_separators": ["-", "/"],
        "symbols_to_index": ["+"],
        "enable_nested_fields": False,
        "metadata": {"team": "search"},
    }
    resource = typesense_adapter(make_resource(), collection_hints=hints)
    table = resource.compute_table_schema()
    assert table[COLLECTION_HINT] == hints  # type: ignore[typeddict-item]


def test_raw_data_is_wrapped() -> None:
    resource = typesense_adapter([{"category": "tools"}], facet="category")
    assert isinstance(resource, DltResource)
    assert column_hint(resource, "category") == {"facet": True}


def test_hints_survive_second_apply_hints() -> None:
    resource = typesense_adapter(make_resource(), facet="category")
    resource.apply_hints(write_disposition="merge", primary_key="sku")
    table = resource.compute_table_schema()
    assert table["columns"]["category"][FIELD_HINT] == {"facet": True}  # type: ignore[typeddict-item]
    assert table["write_disposition"] == "merge"


def test_repeat_adapter_replaces_field_dict() -> None:
    resource = typesense_adapter(make_resource(), field_hints={"category": {"facet": True}})
    typesense_adapter(resource, field_hints={"category": {"sort": True}})
    assert column_hint(resource, "category") == {"sort": True}


def test_empty_call_rejected() -> None:
    with pytest.raises(ValueError, match="at least one"):
        typesense_adapter(make_resource())


def test_unknown_field_param_rejected() -> None:
    with pytest.raises(ValueError, match="unknown Typesense field params.*facets"):
        typesense_adapter(make_resource(), field_hints={"category": {"facets": True}})


def test_unknown_collection_param_rejected() -> None:
    with pytest.raises(ValueError, match="unknown Typesense collection params"):
        typesense_adapter(make_resource(), collection_hints={"fields": []})


def test_non_dict_field_hint_rejected() -> None:
    with pytest.raises(ValueError, match="must be a dict"):
        typesense_adapter(make_resource(), field_hints={"category": True})  # type: ignore[dict-item]


def test_bool_param_type_checked() -> None:
    with pytest.raises(ValueError, match="must be a bool"):
        typesense_adapter(make_resource(), field_hints={"category": {"facet": "yes"}})


def test_num_dim_must_be_positive_int() -> None:
    with pytest.raises(ValueError, match="positive int"):
        typesense_adapter(make_resource(), field_hints={"embedding": {"num_dim": 0}})
    with pytest.raises(ValueError, match="positive int"):
        typesense_adapter(make_resource(), field_hints={"embedding": {"num_dim": True}})


def test_embed_requires_from() -> None:
    with pytest.raises(ValueError, match="'from'"):
        typesense_adapter(make_resource(), field_hints={"vec": {"embed": {"model_config": {}}}})


def test_separators_must_be_str_lists() -> None:
    with pytest.raises(ValueError, match="list of strings"):
        typesense_adapter(make_resource(), collection_hints={"token_separators": "-"})


def test_bad_convenience_value_rejected() -> None:
    with pytest.raises(ValueError, match="'facet' must be"):
        typesense_adapter(make_resource(), facet=[""])


def test_default_sorting_field_must_be_string() -> None:
    with pytest.raises(ValueError, match="default_sorting_field"):
        typesense_adapter(make_resource(), collection_hints={"default_sorting_field": 1})


def test_param_whitelists_cover_docs_surface() -> None:
    # Guard against accidental removals from the public whitelists.
    assert {
        "type",
        "facet",
        "sort",
        "num_dim",
        "embed",
        "reference",
        "hnsw_params",
        "async_reference",
        "cascade_delete",
    } <= FIELD_PARAMS
    assert {
        "default_sorting_field",
        "metadata",
        "enable_nested_fields",
        "synonym_sets",
        "curation_sets",
    } <= COLLECTION_PARAMS


def test_v30_field_params_accepted() -> None:
    resource = typesense_adapter(
        make_resource(),
        field_hints={
            "embedding": {
                "type": "float[]",
                "num_dim": 3,
                "hnsw_params": {"ef_construction": 200, "M": 16},
            },
            "author_id": {
                "type": "string",
                "reference": "authors.id",
                "async_reference": True,
                "cascade_delete": False,
            },
        },
    )
    assert column_hint(resource, "embedding")["hnsw_params"] == {
        "ef_construction": 200,
        "M": 16,
    }
    assert column_hint(resource, "author_id")["async_reference"] is True


def test_v30_collection_params_accepted() -> None:
    resource = typesense_adapter(
        make_resource(),
        collection_hints={"synonym_sets": ["common"], "curation_sets": ["promo"]},
    )
    table = resource.compute_table_schema()
    assert table[COLLECTION_HINT]["synonym_sets"] == ["common"]  # type: ignore[typeddict-item]
    assert table[COLLECTION_HINT]["curation_sets"] == ["promo"]  # type: ignore[typeddict-item]
