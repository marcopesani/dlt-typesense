"""Unit tests for typed field-hint value conversion."""

from __future__ import annotations

import json

import pytest

from dlt_typesense.exceptions import TypesenseImportError
from dlt_typesense.load_jobs import TypesenseLoadJob
from dlt_typesense.value_conversion import (
    apply_conversions,
    converted_fields,
)


def _job() -> TypesenseLoadJob:
    return TypesenseLoadJob("/tmp/t.abc.0.jsonl", "ds_events")


def test_timestamp_to_int64_epoch() -> None:
    table = {
        "columns": {
            "last_update": {
                "name": "last_update",
                "data_type": "timestamp",
                "x-typesense-field": {"type": "int64", "sort": True},
            }
        }
    }
    converters = converted_fields(table)
    data = {"last_update": "2020-01-01T00:00:00Z", "_dlt_id": "r"}
    apply_conversions(data, converters, collection_name="ds_events")
    assert data["last_update"] == 1577836800


def test_date_to_int64_midnight_utc() -> None:
    table = {
        "columns": {
            "day": {
                "name": "day",
                "data_type": "date",
                "x-typesense-field": {"type": "int64"},
            }
        }
    }
    converters = converted_fields(table)
    data = {"day": "2020-01-01"}
    apply_conversions(data, converters, collection_name="ds_events")
    assert data["day"] == 1577836800


def test_timestamp_to_int32_within_range() -> None:
    table = {
        "columns": {
            "ts": {
                "name": "ts",
                "data_type": "timestamp",
                "x-typesense-field": {"type": "int32"},
            }
        }
    }
    converters = converted_fields(table)
    data = {"ts": "2020-01-01T00:00:00Z"}
    apply_conversions(data, converters, collection_name="ds_events")
    assert data["ts"] == 1577836800


def test_timestamp_to_int32_overflow_raises() -> None:
    table = {
        "columns": {
            "ts": {
                "name": "ts",
                "data_type": "timestamp",
                "x-typesense-field": {"type": "int32"},
            }
        }
    }
    converters = converted_fields(table)
    data = {"ts": "2040-01-01T00:00:00Z"}
    with pytest.raises(TypesenseImportError, match="does not fit int32"):
        apply_conversions(data, converters, collection_name="ds_events")


def test_timestamp_to_float_rejected() -> None:
    table = {
        "columns": {
            "ts": {
                "name": "ts",
                "data_type": "timestamp",
                "x-typesense-field": {"type": "float"},
            }
        }
    }
    with pytest.raises(TypesenseImportError, match="cannot be hinted as Typesense 'float'"):
        converted_fields(table)


def test_decimal_to_float() -> None:
    table = {
        "columns": {
            "price": {
                "name": "price",
                "data_type": "decimal",
                "x-typesense-field": {"type": "float"},
            }
        }
    }
    converters = converted_fields(table)
    data = {"price": "19.50"}
    apply_conversions(data, converters, collection_name="ds_events")
    assert data["price"] == pytest.approx(19.5)


def test_decimal_to_int64() -> None:
    table = {
        "columns": {
            "cents": {
                "name": "cents",
                "data_type": "decimal",
                "x-typesense-field": {"type": "int64"},
            }
        }
    }
    converters = converted_fields(table)
    data = {"cents": "42"}
    apply_conversions(data, converters, collection_name="ds_events")
    assert data["cents"] == 42


def test_unconvertible_value_raises_with_sample() -> None:
    table = {
        "columns": {
            "ts": {
                "name": "ts",
                "data_type": "timestamp",
                "x-typesense-field": {"type": "int64"},
            }
        }
    }
    converters = converted_fields(table)
    data = {"ts": "not-a-date"}
    with pytest.raises(TypesenseImportError, match="cannot convert column 'ts'"):
        apply_conversions(data, converters, collection_name="ds_events")


def test_text_column_never_converted() -> None:
    table = {
        "columns": {
            "categories": {
                "name": "categories",
                "data_type": "text",
                "x-typesense-field": {"type": "string[]"},
            }
        }
    }
    assert converted_fields(table) == {}


def test_bigint_hinted_int64_no_converter() -> None:
    # Already numeric on the wire — nothing to convert.
    table = {
        "columns": {
            "count": {
                "name": "count",
                "data_type": "bigint",
                "x-typesense-field": {"type": "int64"},
            }
        }
    }
    assert converted_fields(table) == {}


def test_iter_documents_applies_converters() -> None:
    table = {
        "columns": {
            "last_update": {
                "name": "last_update",
                "data_type": "timestamp",
                "x-typesense-field": {"type": "int64"},
            }
        }
    }
    converters = converted_fields(table)
    docs = list(
        _job()._iter_documents(
            iter([json.dumps({"last_update": "2020-01-01T00:00:00Z", "_dlt_id": "r"})]),
            id_fields=None,
            json_fields=[],
            field_converters=converters,
        )
    )
    assert docs[0]["last_update"] == 1577836800
    assert docs[0]["id"] == "r"
