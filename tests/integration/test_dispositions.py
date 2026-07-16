"""Append, replace and skip dispositions."""

from __future__ import annotations

import dlt
import pytest

pytestmark = pytest.mark.integration


def test_append_first_run_loads_all_with_dlt_fields(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="events", write_disposition="append")
    def events():
        yield from ({"event_id": f"e{i}", "name": "view"} for i in range(4))

    pipeline.run(events())
    docs = documents(make_pipeline.qualified_name(pipeline, "events"))
    assert len(docs) == 4
    for doc in docs:
        assert "_dlt_id" in doc and "_dlt_load_id" in doc
        assert doc["id"] == doc["_dlt_id"]  # id derives from _dlt_id
        assert doc["name"] == "view"


def test_append_accumulates_across_runs(make_pipeline, count_documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="events", write_disposition="append")
    def events():
        yield from ({"event_id": f"e{i}"} for i in range(3))

    pipeline.run(events())
    pipeline.run(events())
    assert count_documents(make_pipeline.qualified_name(pipeline, "events")) == 6


def test_whole_file_retry_is_idempotent(make_pipeline, documents, probe, count_documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="events", write_disposition="append")
    def events():
        yield from ({"event_id": f"e{i}"} for i in range(5))

    pipeline.run(events())
    collection = make_pipeline.qualified_name(pipeline, "events")
    loaded = documents(collection)
    assert len(loaded) == 5

    probe.collections[collection].documents.import_(loaded, {"action": "upsert"})
    assert count_documents(collection) == 5  # no duplicates


def test_append_across_schema_evolution(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="events", write_disposition="append")
    def events_before():
        yield {"event_id": "e1", "name": "a"}

    pipeline.run(events_before())

    @dlt.resource(name="events", write_disposition="append")
    def events_v2():
        yield {"event_id": "e2", "name": "b", "extra": "new-field"}

    pipeline.run(events_v2())
    docs = {d["event_id"]: d for d in documents(make_pipeline.qualified_name(pipeline, "events"))}
    assert len(docs) == 2
    assert "extra" not in docs["e1"]  # old document untouched
    assert docs["e2"]["extra"] == "new-field"  # new document carries new field


def test_replace_leaves_only_new_data(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="snapshot", write_disposition="replace")
    def snapshot_before():
        yield from ({"k": i} for i in range(5))

    pipeline.run(snapshot_before())

    @dlt.resource(name="snapshot", write_disposition="replace")
    def snapshot_v2():
        yield from ({"k": i} for i in range(100, 102))

    pipeline.run(snapshot_v2())
    docs = documents(make_pipeline.qualified_name(pipeline, "snapshot"))
    assert sorted(d["k"] for d in docs) == [100, 101]


def test_replace_survives_schema_change(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="snapshot", write_disposition="replace")
    def snapshot_before():
        yield {"keep": "x", "drop_me": 1, "changes": 5}

    pipeline.run(snapshot_before())

    @dlt.resource(name="snapshot", write_disposition="replace")
    def snapshot_v2():
        yield {"keep": "y", "changes": "now-text"}

    info = pipeline.run(snapshot_v2())
    assert not info.has_failed_jobs
    docs = documents(make_pipeline.qualified_name(pipeline, "snapshot"))
    assert len(docs) == 1
    assert "drop_me" not in docs[0]  # no stale field/document from run 1
    assert docs[0].get("changes") == "now-text" or docs[0].get("changes__v_text") == "now-text"


def test_replace_zero_rows_truncates(make_pipeline, count_documents, collection_exists) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="snapshot", write_disposition="replace")
    def snapshot_full():
        yield from ({"k": i} for i in range(4))

    pipeline.run(snapshot_full())
    collection = make_pipeline.qualified_name(pipeline, "snapshot")
    assert count_documents(collection) == 4

    @dlt.resource(name="snapshot", write_disposition="replace")
    def snapshot_empty():
        return
        yield  # pragma: no cover - makes this a generator

    pipeline.run(snapshot_empty())
    assert count_documents(collection) == 0  # emptied but exists
    assert collection_exists(collection)


def test_replace_replaces_child_tables(make_pipeline, count_documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="orders", write_disposition="replace")
    def orders_before():
        yield {"order_id": "o1", "items": [{"sku": "a"}, {"sku": "b"}, {"sku": "c"}]}

    pipeline.run(orders_before())
    child = make_pipeline.qualified_name(pipeline, "orders__items")
    assert count_documents(child) == 3

    @dlt.resource(name="orders", write_disposition="replace")
    def orders_v2():
        yield {"order_id": "o2", "items": [{"sku": "z"}]}

    pipeline.run(orders_v2())
    assert count_documents(make_pipeline.qualified_name(pipeline, "orders")) == 1
    assert count_documents(child) == 1  # no orphaned run-1 child docs


# NOTE: dlt core does not emit a completable load job for a *data-bearing* `skip`
# table — its loader raises LoadClientUnsupportedWriteDisposition for any disposition
# outside {append, replace, merge}, verified against dlt's own `dummy` destination.
# So the destination-level guarantee is that a `skip` table is never materialized;
# a row-less skip resource is the loadable case.


def test_skip_writes_nothing(make_pipeline, collection_exists) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="ignored", write_disposition="skip")
    def ignored():
        return
        yield  # pragma: no cover - makes this a generator

    info = pipeline.run(ignored())
    assert not info.has_failed_jobs
    assert not collection_exists(make_pipeline.qualified_name(pipeline, "ignored"))


def test_skip_does_not_affect_siblings(make_pipeline, count_documents, collection_exists) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="ignored", write_disposition="skip")
    def ignored():
        return
        yield  # pragma: no cover - makes this a generator

    @dlt.resource(name="kept", write_disposition="append")
    def kept():
        yield from ({"v": i} for i in range(2))

    info = pipeline.run([ignored(), kept()])
    assert not info.has_failed_jobs
    assert count_documents(make_pipeline.qualified_name(pipeline, "kept")) == 2
    assert not collection_exists(make_pipeline.qualified_name(pipeline, "ignored"))
