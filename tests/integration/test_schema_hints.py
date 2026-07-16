"""Adapter schema hints applied to live collections."""

from __future__ import annotations

import dlt
import pytest

from dlt_typesense import typesense_adapter

pytestmark = pytest.mark.integration


def retrieve_schema(probe, collection_name: str) -> dict:
    return probe.collections[collection_name].retrieve()


def field_map(probe, collection_name: str) -> dict[str, dict]:
    schema = retrieve_schema(probe, collection_name)
    return {field["name"]: field for field in schema["fields"]}


def test_facet_and_sort_hints_pin_typed_fields(make_pipeline, probe) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="products", write_disposition="append")
    def products():
        yield {"sku": "A1", "category": "tools", "price": 9.99, "title": "Widget"}

    pipeline.run(typesense_adapter(products(), facet="category", sort="price"))

    fields = field_map(probe, make_pipeline.qualified_name(pipeline, "products"))
    assert fields["category"]["facet"] is True
    assert fields["category"]["type"] == "string"
    assert fields["price"]["sort"] is True
    assert fields["price"]["type"] == "float"
    assert ".*" in fields  # auto catch-all still present
    # Unhinted columns are auto-detected by `.*` (no hint attributes applied).
    assert fields["title"]["facet"] is False


def test_default_sorting_field_end_to_end(make_pipeline, probe, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="ranked", write_disposition="append")
    def ranked():
        yield from ({"name": f"n{i}", "rank": i} for i in range(3))

    pipeline.run(typesense_adapter(ranked(), collection_hints={"default_sorting_field": "rank"}))

    collection = make_pipeline.qualified_name(pipeline, "ranked")
    schema = retrieve_schema(probe, collection)
    assert schema["default_sorting_field"] == "rank"
    fields = {field["name"]: field for field in schema["fields"]}
    assert fields["rank"]["optional"] is False
    assert fields["rank"]["type"] == "int64"
    assert len(documents(collection)) == 3


def test_vector_field_round_trip(make_pipeline, probe, documents) -> None:
    """float[] override suppresses json stringification; vectors stay lists."""
    pipeline = make_pipeline()

    @dlt.resource(name="embedded", write_disposition="append")
    def embedded():
        yield {"doc": "a", "embedding": [0.1, 0.2, 0.3]}
        yield {"doc": "b", "embedding": [0.4, 0.5, 0.6]}

    pipeline.run(
        typesense_adapter(embedded(), field_hints={"embedding": {"type": "float[]", "num_dim": 3}})
    )

    collection = make_pipeline.qualified_name(pipeline, "embedded")
    fields = field_map(probe, collection)
    assert fields["embedding"]["type"] == "float[]"
    assert fields["embedding"]["num_dim"] == 3

    doc = next(d for d in documents(collection) if d["doc"] == "a")
    assert isinstance(doc["embedding"], list)
    assert doc["embedding"] == pytest.approx([0.1, 0.2, 0.3])


def test_collection_level_params_applied(make_pipeline, probe) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="notes", write_disposition="append")
    def notes():
        yield {"body": "alpha-beta"}

    pipeline.run(
        typesense_adapter(
            notes(),
            collection_hints={
                "token_separators": ["-"],
                "symbols_to_index": ["+"],
                "metadata": {"team": "search"},
            },
        )
    )

    schema = retrieve_schema(probe, make_pipeline.qualified_name(pipeline, "notes"))
    assert schema["token_separators"] == ["-"]
    assert schema["symbols_to_index"] == ["+"]
    assert schema["metadata"] == {"team": "search"}


def test_replace_reapplies_hints_on_recreate(make_pipeline, probe) -> None:
    pipeline = make_pipeline()

    def run(facet_value: bool) -> None:
        @dlt.resource(name="items", write_disposition="replace")
        def items():
            yield {"kind": "x"}

        pipeline.run(typesense_adapter(items(), field_hints={"kind": {"facet": facet_value}}))

    run(True)
    collection = make_pipeline.qualified_name(pipeline, "items")
    assert field_map(probe, collection)["kind"]["facet"] is True
    # Replace drops + recreates, so changed hints take effect on the next run.
    run(False)
    assert field_map(probe, collection)["kind"]["facet"] is False


def test_hints_not_applied_to_existing_append_collection(make_pipeline, probe) -> None:
    """Create-time-only semantics: changed hints do not alter a live collection."""
    pipeline = make_pipeline()

    def run(**adapter_kwargs) -> None:
        @dlt.resource(name="logs", write_disposition="append")
        def logs():
            yield {"level": "info"}

        pipeline.run(typesense_adapter(logs(), **adapter_kwargs))

    run(facet="level")
    collection = make_pipeline.qualified_name(pipeline, "logs")
    assert field_map(probe, collection)["level"]["facet"] is True

    run(field_hints={"level": {"facet": False, "sort": True}})
    fields = field_map(probe, collection)
    assert fields["level"]["facet"] is True  # unchanged: schema was not altered
    assert "sort" not in fields["level"] or fields["level"]["sort"] is False


def test_default_sorting_field_on_text_column_fails_terminally(make_pipeline) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="bad_sort", write_disposition="append")
    def bad_sort():
        yield {"title": "abc"}

    with pytest.raises(Exception) as excinfo:
        pipeline.run(
            typesense_adapter(bad_sort(), collection_hints={"default_sorting_field": "title"})
        )
    assert "must be one of" in str(excinfo.value)
