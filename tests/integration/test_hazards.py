"""Typesense-specific behaviors and non-functional guarantees."""

from __future__ import annotations

from typing import Any

import dlt
import pytest
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt_typesense import load_jobs as _lj
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

    info = pipeline.run(rows())
    row_jobs = [
        job
        for package in info.load_packages
        for job in package.jobs["completed_jobs"]
        if job.job_file_info.table_name == "rows"
    ]
    assert len(row_jobs) >= 2  # actually sharded
    assert count_documents(make_pipeline.qualified_name(pipeline, "rows")) == 25


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
    original = _lj.import_documents
    state = {"failed": False}

    def flaky(ts, collection, docs, **kwargs):
        if not state["failed"]:
            state["failed"] = True
            list(docs)
            raise TypesenseTransientError("injected transient failure")
        return original(ts, collection, docs, **kwargs)

    monkeypatch.setattr(_lj, "import_documents", flaky)

    pipeline = make_pipeline()
    resource_kwargs: dict[str, Any] = {"primary_key": "k"} if disposition == "merge" else {}

    @dlt.resource(name="rows", write_disposition=disposition, **resource_kwargs)
    def rows():
        yield from ({"k": i, "v": i} for i in range(4))

    info = pipeline.run(rows())
    assert not info.has_failed_jobs
    assert state["failed"]  # a real failure was injected and recovered from
    assert count_documents(make_pipeline.qualified_name(pipeline, "rows")) == 4


def test_api_key_absent_from_logs_and_errors(make_pipeline, require_server, caplog) -> None:
    import logging

    pipeline = make_pipeline()

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {"v": 1}

    with caplog.at_level(logging.DEBUG):
        info = pipeline.run(rows())
    assert not info.has_failed_jobs
    assert require_server.api_key not in caplog.text
