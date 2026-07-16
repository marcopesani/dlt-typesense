"""Data-type mapping and round-trip fidelity."""

from __future__ import annotations

import base64
import datetime
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


def test_timestamp_and_date_are_iso_strings(make_pipeline, documents) -> None:
    ts = datetime.datetime(2026, 7, 16, 12, 34, 56, 789012, tzinfo=datetime.timezone.utc)
    d = datetime.date(2026, 7, 16)
    doc = _load_one(make_pipeline, documents, {"ts": ts, "d": d})
    assert isinstance(doc["ts"], str)
    assert datetime.datetime.fromisoformat(doc["ts"]) == ts
    assert "789012" in doc["ts"]  # microseconds preserved
    assert doc["d"] == "2026-07-16"


def test_time_round_trips_as_iso_string(make_pipeline, documents) -> None:
    t = datetime.time(12, 34, 56)
    doc = _load_one(make_pipeline, documents, {"t": t})
    assert isinstance(doc["t"], str)
    assert datetime.time.fromisoformat(doc["t"]) == t


def test_decimal_keeps_full_precision_as_string(make_pipeline, documents) -> None:
    doc = _load_one(make_pipeline, documents, {"amount": decimal.Decimal("123456789.123456789")})
    assert doc["amount"] == "123456789.123456789"


def test_wei_beyond_int64_is_string(make_pipeline, documents) -> None:
    big = 2**80
    doc = _load_one(make_pipeline, documents, {"balance": Wei(big)})
    assert doc["balance"] == str(big)


def test_binary_round_trips_as_base64(make_pipeline, documents) -> None:
    payload = b"\x00\x01\x02hello"
    doc = _load_one(make_pipeline, documents, {"blob": payload})
    assert base64.b64decode(doc["blob"]) == payload


def test_json_column_pinned_representation(make_pipeline, documents) -> None:
    doc = _load_one(
        make_pipeline,
        documents,
        {"payload": {"nested": {"a": 1}, "arr": [1, 2, 3]}},
        columns={"payload": {"data_type": "json"}},
    )
    assert isinstance(doc["payload"], str)
    assert json.loads(doc["payload"]) == {"nested": {"a": 1}, "arr": [1, 2, 3]}
