"""End-to-end: typed field hints convert wire values (epoch timestamps, decimals)."""

from __future__ import annotations

from datetime import datetime, timezone

import dlt
import pytest

from dlt_typesense import typesense_adapter

pytestmark = pytest.mark.integration


def test_timestamp_hint_stores_epoch_and_supports_range_filter(
    make_pipeline, probe, documents
) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="events", write_disposition="replace")
    def events():
        yield {
            "name": "early",
            "last_update": datetime(2020, 1, 1, tzinfo=timezone.utc),
            "score": 1,
        }
        yield {
            "name": "late",
            "last_update": datetime(2022, 6, 15, tzinfo=timezone.utc),
            "score": 2,
        }

    pipeline.run(
        typesense_adapter(
            events(),
            field_hints={"last_update": {"type": "int64", "sort": True}},
        )
    )

    collection = make_pipeline.qualified_name(pipeline, "events")
    schema = probe.collections[collection].retrieve()
    fields = {f["name"]: f for f in schema["fields"]}
    assert fields["last_update"]["type"] == "int64"

    docs = {d["name"]: d for d in documents(collection)}
    assert docs["early"]["last_update"] == 1577836800
    assert docs["late"]["last_update"] == 1655251200

    # Range filter on the epoch field (impossible with ISO strings).
    filtered = documents(collection, filter_by="last_update:>1600000000")
    assert [d["name"] for d in filtered] == ["late"]

    ordered = documents(collection, sort_by="last_update:desc")
    assert [d["name"] for d in ordered] == ["late", "early"]


def test_epoch_int32_as_default_sorting_field(make_pipeline, probe, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="ranked", write_disposition="replace")
    def ranked():
        yield {"name": "a", "ts": datetime(2020, 1, 1, tzinfo=timezone.utc)}
        yield {"name": "b", "ts": datetime(2021, 1, 1, tzinfo=timezone.utc)}

    pipeline.run(
        typesense_adapter(
            ranked(),
            field_hints={"ts": {"type": "int32"}},
            collection_hints={"default_sorting_field": "ts"},
        )
    )

    collection = make_pipeline.qualified_name(pipeline, "ranked")
    schema = probe.collections[collection].retrieve()
    assert schema["default_sorting_field"] == "ts"
    fields = {f["name"]: f for f in schema["fields"]}
    assert fields["ts"]["type"] == "int32"
    assert fields["ts"]["optional"] is False
    docs = {d["name"]: d for d in documents(collection)}
    assert docs["a"]["ts"] == 1577836800
    assert docs["b"]["ts"] == 1609459200
    ordered = documents(collection, sort_by="ts:desc")
    assert [d["name"] for d in ordered] == ["b", "a"]


def test_decimal_hint_to_float(make_pipeline, documents) -> None:
    from decimal import Decimal

    pipeline = make_pipeline()

    @dlt.resource(name="prices", write_disposition="replace")
    def prices():
        yield {"sku": "A1", "price": Decimal("19.50")}

    pipeline.run(
        typesense_adapter(prices(), field_hints={"price": {"type": "float", "sort": True}})
    )

    collection = make_pipeline.qualified_name(pipeline, "prices")
    doc = documents(collection)[0]
    assert doc["price"] == pytest.approx(19.5)
