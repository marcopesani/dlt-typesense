"""Data-type mapping and round-trip fidelity."""

from __future__ import annotations

import decimal
import json

import dlt
import pytest
from dlt.common.wei import Wei

pytestmark = pytest.mark.integration


def _load_one(make_pipeline, documents, row: dict, **resource_kwargs) -> dict:
    pipeline = make_pipeline()

    @dlt.resource(name="typed", write_disposition="append", **resource_kwargs)
    def typed():
        yield row

    info = pipeline.run(typed())
    assert not info.has_failed_jobs
    docs = documents(make_pipeline.qualified_name(pipeline, "typed"))
    assert len(docs) == 1
    return docs[0]


def test_native_scalars_round_trip(make_pipeline, documents) -> None:
    doc = _load_one(
        make_pipeline,
        documents,
        {"txt": "hello world", "count": 42, "ratio": 9.99, "flag": True},
    )
    assert doc["txt"] == "hello world"
    assert doc["count"] == 42
    assert doc["ratio"] == 9.99
    assert doc["flag"] is True


def test_bigint_boundaries_are_exact(make_pipeline, documents) -> None:
    hi, lo = 2**63 - 1, -(2**63 - 1)
    doc = _load_one(make_pipeline, documents, {"hi": hi, "lo": lo})
    assert doc["hi"] == hi
    assert doc["lo"] == lo


def test_decimal_keeps_full_precision_as_string(make_pipeline, documents) -> None:
    doc = _load_one(make_pipeline, documents, {"amount": decimal.Decimal("123456789.123456789")})
    assert doc["amount"] == "123456789.123456789"


def test_wei_beyond_int64_is_string(make_pipeline, documents) -> None:
    big = 2**80
    doc = _load_one(make_pipeline, documents, {"balance": Wei(big)})
    assert doc["balance"] == str(big)


def test_json_column_pinned_representation(make_pipeline, documents) -> None:
    doc = _load_one(
        make_pipeline,
        documents,
        {"payload": {"nested": {"a": 1}, "arr": [1, 2, 3]}},
        columns={"payload": {"data_type": "json"}},
    )
    assert isinstance(doc["payload"], str)
    assert json.loads(doc["payload"]) == {"nested": {"a": 1}, "arr": [1, 2, 3]}


def test_null_first_then_value_and_empty_string(make_pipeline, documents, count_documents) -> None:
    """Nulls are stripped; empty strings survive; later non-null values materialize the field."""
    pipeline = make_pipeline()

    @dlt.resource(name="rows", write_disposition="append")
    def rows():
        yield {"rid": 1, "maybe": None, "empty": ""}
        yield {"rid": 2, "maybe": "present", "empty": ""}

    info = pipeline.run(rows())
    assert not info.has_failed_jobs
    collection = make_pipeline.qualified_name(pipeline, "rows")
    assert count_documents(collection) == 2
    docs = {d["rid"]: d for d in documents(collection)}
    assert "maybe" not in docs[1]
    assert docs[1]["empty"] == ""
    assert docs[2]["maybe"] == "present"
    assert docs[2]["empty"] == ""
