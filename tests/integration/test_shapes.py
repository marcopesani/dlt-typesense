"""Data-shape matrix across dispositions."""

from __future__ import annotations

from typing import Any

import dlt
import pytest

pytestmark = pytest.mark.integration


def test_flat_scalar_rows_load_one_to_one(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {"a": 1, "b": "x", "c": True}
        yield {"a": 2, "b": "y", "c": False}

    pipeline.run(rows())
    docs = sorted(documents(make_pipeline.qualified_name(pipeline, "rows")), key=lambda d: d["a"])
    assert [(d["a"], d["b"], d["c"]) for d in docs] == [(1, "x", True), (2, "y", False)]


@pytest.mark.parametrize("disposition", ["append", "replace", "merge"])
def test_nested_dicts_flatten_to_parent_child(make_pipeline, documents, disposition) -> None:
    pipeline = make_pipeline()
    resource_kwargs: dict[str, Any] = {"primary_key": "user_id"} if disposition == "merge" else {}

    @dlt.resource(name="users", write_disposition=disposition, **resource_kwargs)
    def users():
        yield {"user_id": "u1", "profile": {"name": "Ada", "address": {"city": "London"}}}

    pipeline.run(users())
    doc = documents(make_pipeline.qualified_name(pipeline, "users"))[0]
    assert doc["user_id"] == "u1"
    assert doc["profile__name"] == "Ada"
    assert doc["profile__address__city"] == "London"


def test_nested_lists_become_child_collections(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="orders", write_disposition="merge", primary_key="order_id")
    def orders():
        yield {"order_id": "o1", "lines": [{"sku": "a"}, {"sku": "b"}]}

    pipeline.run(orders())
    child = make_pipeline.qualified_name(pipeline, "orders__lines")
    child_docs = sorted(documents(child), key=lambda d: d["_dlt_list_idx"])
    assert [d["sku"] for d in child_docs] == ["a", "b"]  # content round-trips
    assert [d["_dlt_list_idx"] for d in child_docs] == [0, 1]
    for doc in child_docs:
        assert "_dlt_id" in doc and "_dlt_parent_id" in doc
        assert "_dlt_root_id" in doc  # merge propagates the root key


def test_nested_lists_under_append(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="orders", write_disposition="append")
    def orders():
        yield {"order_id": "o1", "lines": [{"sku": "a"}, {"sku": "b"}]}

    pipeline.run(orders())
    child_docs = sorted(
        documents(make_pipeline.qualified_name(pipeline, "orders__lines")),
        key=lambda d: d["_dlt_list_idx"],
    )
    assert [d["sku"] for d in child_docs] == ["a", "b"]
    for doc in child_docs:
        assert "_dlt_id" in doc and "_dlt_parent_id" in doc
        assert "_dlt_root_id" not in doc  # root propagation is a merge feature


def test_null_and_missing_values_are_optional(make_pipeline, documents, count_documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {"a": 1, "maybe": None}  # explicit null
        yield {"a": 2}  # missing column

    info = pipeline.run(rows())
    assert not info.has_failed_jobs
    collection = make_pipeline.qualified_name(pipeline, "rows")
    assert count_documents(collection) == 2
    for doc in documents(collection):
        assert "maybe" not in doc  # null/absent behaves as optional


@pytest.mark.parametrize("disposition", ["append", "merge"])
def test_schema_evolution_variant_column(make_pipeline, documents, disposition) -> None:
    pipeline = make_pipeline()
    resource_kwargs: dict[str, Any] = {"primary_key": "rid"} if disposition == "merge" else {}

    @dlt.resource(name="rows", write_disposition=disposition, **resource_kwargs)
    def rows():
        yield {"rid": 1, "val": 10}  # int -> `val`
        yield {"rid": 2, "val": "ten"}  # text -> variant `val__v_text`

    info = pipeline.run(rows())
    assert not info.has_failed_jobs
    columns = pipeline.default_schema.tables["rows"]["columns"]
    assert any(name.startswith("val__v_") for name in columns)
    docs = {d["rid"]: d for d in documents(make_pipeline.qualified_name(pipeline, "rows"))}
    assert len(docs) == 2
    assert docs[1]["val"] == 10 and "val__v_text" not in docs[1]
    assert docs[2]["val__v_text"] == "ten" and "val" not in docs[2]


def test_zero_row_resources_do_not_fail(make_pipeline) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="empty_append", write_disposition="append")
    def empty_append():
        return
        yield  # pragma: no cover

    @dlt.resource(name="empty_merge", write_disposition="merge", primary_key="id")
    def empty_merge():
        return
        yield  # pragma: no cover

    info = pipeline.run([empty_append(), empty_merge()])
    assert not info.has_failed_jobs


def test_unicode_and_special_characters_round_trip(make_pipeline, documents) -> None:
    pipeline = make_pipeline()
    tricky = 'héllo 😀 "quoted" \\ back \n newline \t tab 日本語'
    long_string = "x" * 20000

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {"row_id": "r1", "text": tricky, "long": long_string}

    pipeline.run(rows())
    doc = documents(make_pipeline.qualified_name(pipeline, "rows"))[0]
    assert doc["text"] == tricky
    assert doc["long"] == long_string
