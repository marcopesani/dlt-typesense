"""Typesense-specific behaviors and non-functional guarantees."""

from __future__ import annotations

from typing import Any

import dlt
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.pipeline.exceptions import PipelineStepFailed
from typesense.sync.documents import Documents

from dlt_typesense.exceptions import TypesenseTransientError

pytestmark = pytest.mark.integration


def test_source_id_column_round_trips_without_collision(make_pipeline, documents) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {"id": "user-supplied", "name": "x"}

    pipeline.run(rows())
    doc = documents(make_pipeline.qualified_name(pipeline, "rows"))[0]
    assert doc["__id"] == "user-supplied"  # source value preserved
    assert doc["id"] != "user-supplied"  # reserved id is destination-managed
    assert doc["id"] == doc["_dlt_id"]


def test_client_chunking_is_exact_with_remainder(make_pipeline, count_documents) -> None:
    pipeline = make_pipeline(destination_kwargs={"client_batch_size": 7})

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield from ({"n": i} for i in range(25))  # 25 = 3*7 + 4

    pipeline.run(rows())
    assert count_documents(make_pipeline.qualified_name(pipeline, "rows")) == 25


def test_file_sharding_produces_exact_totals(make_pipeline, count_documents, monkeypatch) -> None:
    monkeypatch.setenv("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS", "10")
    pipeline = make_pipeline()

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield from ({"n": i} for i in range(25))

    pipeline.run(rows())
    assert count_documents(make_pipeline.qualified_name(pipeline, "rows")) == 25


def test_exact_client_batch_size_boundary(make_pipeline, count_documents) -> None:
    """N == k * client_batch_size must not emit an empty trailing import_."""
    pipeline = make_pipeline(destination_kwargs={"client_batch_size": 5})

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield from ({"n": i} for i in range(10))  # exactly 2 batches

    info = pipeline.run(rows())
    assert not info.has_failed_jobs
    assert count_documents(make_pipeline.qualified_name(pipeline, "rows")) == 10


def test_import_action_emplace_updates_partial(make_pipeline, documents) -> None:
    pipeline = make_pipeline(destination_kwargs={"import_action": "emplace"})

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def initial():
        yield {"sku": "A1", "a": 1, "b": 2}

    pipeline.run(initial())
    collection = make_pipeline.qualified_name(pipeline, "products")

    @dlt.resource(name="products", write_disposition="merge", primary_key="sku")
    def v2():
        yield {"sku": "A1", "a": 10}  # only `a` provided

    pipeline.run(v2())
    doc = documents(collection)[0]
    assert doc["a"] == 10  # updated
    assert doc["b"] == 2  # preserved by emplace


def test_bad_api_key_fails_terminally(require_server, dataset_name, tmp_path) -> None:
    from dlt_typesense import typesense
    from dlt_typesense.configuration import TypesenseCredentials

    bad = TypesenseCredentials()
    bad.host, bad.port, bad.protocol = (
        require_server.host,
        require_server.port,
        require_server.protocol,
    )
    bad.api_key = "definitely-wrong-key"
    pipeline = dlt.pipeline(
        pipeline_name="bad_key",
        destination=typesense(credentials=bad),
        dataset_name=dataset_name,
        pipelines_dir=str(tmp_path),
    )

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {"v": 1}

    with pytest.raises(PipelineStepFailed) as excinfo:
        pipeline.run(rows())
    cause = excinfo.value
    saw_terminal = False
    while cause is not None:
        if isinstance(cause, DestinationTerminalException):
            saw_terminal = True
            break
        cause = getattr(cause, "__cause__", None)
    assert saw_terminal, "bad api key must fail terminally, not transiently"


def test_dataset_separator_is_honored(make_pipeline, count_documents) -> None:
    pipeline = make_pipeline(dataset_name="catalog", destination_kwargs={"dataset_separator": "__"})

    @dlt.resource(name="products", write_disposition="append")
    def products():
        yield {"sku": "A1"}

    pipeline.run(products())
    assert make_pipeline.qualified_name(pipeline, "products") == "catalog__products"
    assert count_documents("catalog__products") == 1


@pytest.mark.parametrize("disposition", ["append", "replace", "merge"])
def test_rerun_recovers_after_injected_failure(
    make_pipeline, count_documents, monkeypatch, disposition
) -> None:
    """Fail after the first chunk is written; whole-file retry must leave exact counts."""
    original = Documents.import_
    state = {"calls": 0, "failed": False}

    def flaky(self, documents, import_parameters=None, batch_size=None):
        result = original(self, documents, import_parameters, batch_size)
        state["calls"] += 1
        if state["calls"] == 1 and not state["failed"]:
            state["failed"] = True
            raise TypesenseTransientError("injected after-chunk-1 failure")
        return result

    monkeypatch.setattr(Documents, "import_", flaky)

    pipeline = make_pipeline(destination_kwargs={"client_batch_size": 2})
    resource_kwargs: dict[str, Any] = {"primary_key": "k"} if disposition == "merge" else {}

    @dlt.resource(name="rows", write_disposition=disposition, **resource_kwargs)
    def rows():
        yield from ({"k": i, "v": i} for i in range(5))

    info = pipeline.run(rows())
    assert not info.has_failed_jobs
    assert state["failed"]
    assert count_documents(make_pipeline.qualified_name(pipeline, "rows")) == 5


def test_api_key_absent_from_error_messages(require_server, dataset_name, tmp_path) -> None:
    from dlt_typesense import typesense
    from dlt_typesense.configuration import TypesenseCredentials

    bad = TypesenseCredentials()
    bad.host, bad.port, bad.protocol = (
        require_server.host,
        require_server.port,
        require_server.protocol,
    )
    bad.api_key = "definitely-wrong-key-must-not-leak"
    pipeline = dlt.pipeline(
        pipeline_name="bad_key_logs",
        destination=typesense(credentials=bad),
        dataset_name=dataset_name,
        pipelines_dir=str(tmp_path),
    )

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {"v": 1}

    with pytest.raises(PipelineStepFailed) as excinfo:
        pipeline.run(rows())
    assert "definitely-wrong-key-must-not-leak" not in str(excinfo.value)
