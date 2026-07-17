"""Merge disposition: upsert and insert-only."""

from __future__ import annotations

import dlt
import pytest
from dlt.common.destination.exceptions import DestinationCapabilitiesException
from dlt.pipeline.exceptions import PipelineStepFailed
from typesense.sync.documents import Documents

from dlt_typesense.exceptions import TypesenseTransientError
from dlt_typesense.load_jobs import merge_document_id

pytestmark = pytest.mark.integration


def _one(collection_documents: list[dict], **match) -> dict:
    hits = [d for d in collection_documents if all(d.get(k) == v for k, v in match.items())]
    assert len(hits) == 1, f"expected exactly one doc matching {match}, got {len(hits)}"
    return hits[0]


def test_unsupported_strategy_fails_writing_nothing(make_pipeline, collection_exists) -> None:
    pipeline = make_pipeline()

    @dlt.resource(
        name="products",
        write_disposition={"disposition": "merge", "strategy": "delete-insert"},
        primary_key="sku",
    )
    def products():
        yield {"sku": "A1", "title": "Widget"}

    with pytest.raises(PipelineStepFailed) as excinfo:
        pipeline.run(products())
    cause: BaseException | None = excinfo.value
    while cause is not None and not isinstance(cause, DestinationCapabilitiesException):
        cause = cause.__cause__
    assert isinstance(cause, DestinationCapabilitiesException)
    message = str(cause)
    assert "delete-insert" in message
    assert "upsert" in message and "insert-only" in message
    assert not collection_exists(make_pipeline.qualified_name(pipeline, "products"))


def test_deterministic_id_from_primary_key(make_pipeline, documents) -> None:
    pipeline_one = make_pipeline(dataset_name="catalog")

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def products():
        yield {"sku": "A1", "title": "Widget"}

    pipeline_one.run(products())
    collection = "catalog_products"
    expected_id = merge_document_id(collection, ["A1"])
    doc = _one(documents(collection), sku="A1")
    assert doc["id"] == expected_id

    pipeline_two = make_pipeline(dataset_name="catalog")
    pipeline_two.run(products())
    docs = documents(collection)
    assert len(docs) == 1
    assert docs[0]["id"] == expected_id


def test_updates_happen_in_place(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def initial():
        yield {"sku": "A1", "price": 10}

    pipeline.run(initial())
    collection = make_pipeline.qualified_name(pipeline, "products")
    after_first = documents(collection)
    assert len(after_first) == 1 and after_first[0]["price"] == 10

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def v2():
        yield {"sku": "A1", "price": 20}

    pipeline.run(v2())
    after_second = documents(collection)
    assert len(after_second) == 1  # count unchanged
    assert after_second[0]["price"] == 20  # updated in place


def test_double_run_is_identical(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def products():
        yield from ({"sku": f"S{i}", "n": i} for i in range(4))

    pipeline.run(products())
    collection = make_pipeline.qualified_name(pipeline, "products")
    first = {d["id"]: d["n"] for d in documents(collection)}
    pipeline.run(products())
    second = {d["id"]: d["n"] for d in documents(collection)}
    assert first == second


def test_compound_primary_keys(make_pipeline, documents) -> None:
    pipeline = make_pipeline(dataset_name="catalog")

    @dlt.resource(name="stock", write_disposition="merge", primary_key=["tenant", "sku"])
    def stock():
        yield {"tenant": "t1", "sku": "A1", "qty": 1}
        yield {"tenant": "t2", "sku": "A1", "qty": 2}

    pipeline.run(stock())
    collection = "catalog_stock"
    docs = documents(collection)
    assert len(docs) == 2  # distinct because the tenant differs
    t1 = _one(docs, tenant="t1")
    assert t1["id"] == merge_document_id(collection, ["t1", "A1"])

    @dlt.resource(name="stock", write_disposition="merge", primary_key=["tenant", "sku"])
    def stock_update():
        yield {"tenant": "t1", "sku": "A1", "qty": 99}

    pipeline.run(stock_update())
    docs = documents(collection)
    assert len(docs) == 2  # still two rows
    assert _one(docs, tenant="t1")["qty"] == 99  # right document updated


def test_unique_hint_keys_the_upsert(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(
        name="products",
        write_disposition="merge",
        columns={"code": {"unique": True}},
    )
    def initial():
        yield {"code": "X", "price": 1}

    pipeline.run(initial())
    collection = make_pipeline.qualified_name(pipeline, "products")

    @dlt.resource(
        name="products",
        write_disposition="merge",
        columns={"code": {"unique": True}},
    )
    def v2():
        yield {"code": "X", "price": 2}

    pipeline.run(v2())
    docs = documents(collection)
    assert len(docs) == 1  # keyed by the unique column -> updated in place
    assert docs[0]["price"] == 2


def test_merge_without_key_is_terminal(make_pipeline, count_documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="products", write_disposition="merge")
    def products():
        yield {"title": "no key"}

    with pytest.raises(PipelineStepFailed) as excinfo:
        pipeline.run(products())
    message = str(excinfo.value)
    assert "primary_key" in message or "unique" in message
    # Schema create may materialize the collection; the load must write nothing.
    assert count_documents(make_pipeline.qualified_name(pipeline, "products")) == 0


def test_mixed_insert_and_update_batch(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def initial():
        yield {"sku": "A1", "price": 1}
        yield {"sku": "B2", "price": 2}

    pipeline.run(initial())
    collection = make_pipeline.qualified_name(pipeline, "products")

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def v2():
        yield {"sku": "B2", "price": 20}  # update
        yield {"sku": "C3", "price": 3}  # insert

    pipeline.run(v2())
    docs = {d["sku"]: d["price"] for d in documents(collection)}
    assert docs == {"A1": 1, "B2": 20, "C3": 3}


def test_merge_retry_converges(make_pipeline, documents, monkeypatch) -> None:
    """Fail AFTER the first client chunk succeeds; whole-file retry must converge."""
    original = Documents.import_
    state = {"calls": 0, "failed": False}

    def flaky(self, documents, import_parameters=None, batch_size=None):
        result = original(self, documents, import_parameters, batch_size)
        state["calls"] += 1
        if state["calls"] == 1 and not state["failed"]:
            state["failed"] = True
            raise TypesenseTransientError("simulated after-chunk-1 failure")
        return result

    monkeypatch.setattr(Documents, "import_", flaky)

    pipeline = make_pipeline(destination_kwargs={"client_batch_size": 2})

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def products():
        yield from ({"sku": f"S{i}", "n": i} for i in range(5))

    info = pipeline.run(products())
    assert not info.has_failed_jobs
    assert state["failed"]
    collection = make_pipeline.qualified_name(pipeline, "products")
    docs = {d["sku"]: d["n"] for d in documents(collection)}
    assert docs == {f"S{i}": i for i in range(5)}


def test_insert_only_keys_by_dlt_id_not_primary_key(make_pipeline, documents) -> None:
    """insert-only must use _dlt_id (not uuid5 of PK); identical re-run is idempotent."""
    pipeline = make_pipeline()

    @dlt.resource(
        name="products",
        write_disposition={"disposition": "merge", "strategy": "insert-only"},
        primary_key="sku",
    )
    def products():
        yield {"sku": "A1", "n": 1}
        yield {"sku": "B2", "n": 2}

    pipeline.run(products())
    collection = make_pipeline.qualified_name(pipeline, "products")
    first = {d["sku"]: d for d in documents(collection)}
    assert set(first) == {"A1", "B2"}
    for doc in first.values():
        assert doc["id"] == doc["_dlt_id"]
        # Must not be the merge/upsert uuid5(collection, key) scheme.
        assert doc["id"] != merge_document_id(collection, [doc["sku"]])

    pipeline.run(products())  # identical rows → same _dlt_ids → no duplicates
    second = {d["sku"]: d for d in documents(collection)}
    assert set(second) == {"A1", "B2"}
    assert second["A1"]["id"] == first["A1"]["id"]
    assert second["B2"]["id"] == first["B2"]["id"]


def test_merge_leaves_stale_child_documents(make_pipeline, documents, count_documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="orders", write_disposition="merge", primary_key="order_id")
    def initial():
        yield {"order_id": "o1", "items": [{"sku": "a"}, {"sku": "b"}]}

    pipeline.run(initial())
    child = make_pipeline.qualified_name(pipeline, "orders__items")
    assert count_documents(child) == 2

    @dlt.resource(name="orders", write_disposition="merge", primary_key="order_id")
    def v2():
        yield {"order_id": "o1", "items": [{"sku": "a"}]}  # dropped "b"

    pipeline.run(v2())
    assert count_documents(make_pipeline.qualified_name(pipeline, "orders")) == 1
    # Documented limitation: orphan cleanup is not implemented.
    assert count_documents(child) == 2
    assert {d["sku"] for d in documents(child)} == {"a", "b"}


def test_upsert_drops_omitted_fields(make_pipeline, documents) -> None:
    """Default upsert is whole-document replace; omitted fields disappear."""
    pipeline = make_pipeline()

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def initial():
        yield {"sku": "A1", "a": 1, "b": 2}

    pipeline.run(initial())
    collection = make_pipeline.qualified_name(pipeline, "products")

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def v2():
        yield {"sku": "A1", "a": 10}  # `b` omitted

    pipeline.run(v2())
    doc = documents(collection)[0]
    assert doc["a"] == 10
    assert "b" not in doc


def test_append_then_merge_produces_duplicates(make_pipeline, documents, count_documents) -> None:
    """Append keys by _dlt_id; merge keys by uuid5(PK) — switching leaves duplicates."""
    pipeline = make_pipeline()

    @dlt.resource(name="products", write_disposition="append")
    def appended():
        yield {"sku": "A1", "price": 1}

    pipeline.run(appended())
    collection = make_pipeline.qualified_name(pipeline, "products")
    assert count_documents(collection) == 1

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def merged():
        yield {"sku": "A1", "price": 2}

    pipeline.run(merged())
    docs = documents(collection)
    assert len(docs) == 2
    assert {d["price"] for d in docs} == {1, 2}
