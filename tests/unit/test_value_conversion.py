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


def test_offset_aware_timestamp_keeps_absolute_epoch() -> None:
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
    data = {"ts": "2020-01-01T01:00:00+01:00"}
    apply_conversions(data, converters, collection_name="ds_events")
    assert data["ts"] == 1577836800  # absolute UTC, not wall-clock +01 as UTC


def test_noon_utc_timestamp_not_midnight() -> None:
    """Pins DateTime-before-Date isinstance order (DateTime subclasses Date in pendulum)."""
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
    data = {"ts": "2020-01-01T12:00:00Z"}
    apply_conversions(data, converters, collection_name="ds_events")
    assert data["ts"] == 1577880000


def test_int32_epoch_bounds() -> None:
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
    ok = {"ts": 2_147_483_647}
    apply_conversions(ok, converters, collection_name="ds_events")
    assert ok["ts"] == 2_147_483_647
    with pytest.raises(TypesenseImportError, match="does not fit int32"):
        apply_conversions({"ts": 2_147_483_648}, converters, collection_name="ds_events")


def test_fractional_decimal_to_int_raises() -> None:
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
    with pytest.raises(TypesenseImportError, match="not an integer"):
        apply_conversions({"cents": "19.50"}, converters, collection_name="ds_events")


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_decimal_to_float_raises(bad: str) -> None:
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
    with pytest.raises(TypesenseImportError, match="non-finite"):
        apply_conversions({"price": bad}, converters, collection_name="ds_events")


def test_scientific_notation_decimal_string() -> None:
    table = {
        "columns": {
            "qty": {
                "name": "qty",
                "data_type": "decimal",
                "x-typesense-field": {"type": "int64"},
            }
        }
    }
    converters = converted_fields(table)
    data = {"qty": "1E+2"}
    apply_conversions(data, converters, collection_name="ds_events")
    assert data["qty"] == 100
