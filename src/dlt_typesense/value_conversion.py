"""Convert JSONL wire values so they match a Typesense ``type`` field hint.

A ``field_hints`` ``type`` is a contract about *both* the collection schema and
the document value. dlt's JSONL wire keeps timestamps/dates as ISO-8601 strings
and decimals/wei as exact decimal strings; when the user pins those columns to
a numeric Typesense type, the load job converts the wire value here.

``text`` columns are never auto-parsed — that is data cleaning (``add_map``).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from dlt.common.pendulum import pendulum

from dlt_typesense.exceptions import ERROR_DETAIL_MAX_LEN, TypesenseImportError
from dlt_typesense.typesense_adapter import FIELD_HINT

# dlt data types whose wire representation is a string that may need converting.
_TEMPORAL_DLT = frozenset({"timestamp", "date"})
_DECIMAL_DLT = frozenset({"decimal", "wei"})
_NUMERIC_TYPESENSE = frozenset({"int32", "int64", "float"})

# Typesense float is IEEE-754 binary32; epoch seconds lose ~128 s of precision
# in 2026. Refuse the hint rather than silently corrupt timestamps.
_INT32_MAX = 2_147_483_647
_INT32_MIN = -2_147_483_648


def converted_fields(table: Mapping[str, Any]) -> dict[str, Callable[[Any], Any]]:
    """Map column name → converter for columns that need a wire-value change."""
    columns: Mapping[str, Mapping[str, Any]] = table.get("columns") or {}
    converters: dict[str, Callable[[Any], Any]] = {}
    for name, column in columns.items():
        hint_type = (column.get(FIELD_HINT) or {}).get("type")
        if not isinstance(hint_type, str) or hint_type not in _NUMERIC_TYPESENSE:
            continue
        dlt_type = column.get("data_type")
        if dlt_type in _TEMPORAL_DLT:
            if hint_type == "float":
                raise TypesenseImportError(
                    f"Column '{name}': timestamp/date cannot be hinted as Typesense "
                    "'float' (32-bit floats lose epoch-second precision). Use "
                    "'int64' (range filters / sort) or 'int32' (default_sorting_field; "
                    "valid until 2038)."
                )
            converters[name] = _make_temporal_converter(name, hint_type)
        elif dlt_type in _DECIMAL_DLT:
            converters[name] = _make_decimal_converter(name, hint_type)
    return converters


def apply_conversions(
    data: dict[str, Any],
    converters: Mapping[str, Callable[[Any], Any]],
    *,
    collection_name: str,
) -> None:
    """Mutate ``data`` in place, converting hinted fields. Raises terminal on failure."""
    for field_name, convert in converters.items():
        if field_name not in data:
            continue
        try:
            data[field_name] = convert(data[field_name])
        except TypesenseImportError:
            raise
        except Exception as exc:
            sample = str(data[field_name])[:ERROR_DETAIL_MAX_LEN]
            raise TypesenseImportError(
                f"Collection '{collection_name}': cannot convert column '{field_name}' "
                f"value {sample!r} to the hinted Typesense type ({exc})."
            ) from exc


def _make_temporal_converter(column: str, hint_type: str) -> Callable[[Any], Any]:
    def convert(value: Any) -> int:
        epoch = _to_epoch_seconds(value)
        if hint_type == "int32":
            if epoch < _INT32_MIN or epoch > _INT32_MAX:
                raise TypesenseImportError(
                    f"Column '{column}': epoch seconds {epoch} does not fit int32 "
                    f"(range {_INT32_MIN}..{_INT32_MAX}; overflows after 2038-01-19). "
                    "Use 'int64' instead."
                )
            return int(epoch)
        return int(epoch)

    return convert


def _make_decimal_converter(column: str, hint_type: str) -> Callable[[Any], Any]:
    def convert(value: Any) -> int | float:
        number = _to_decimal(value)
        if hint_type == "float":
            return float(number)
        as_int = int(number)
        if hint_type == "int32" and (as_int < _INT32_MIN or as_int > _INT32_MAX):
            raise TypesenseImportError(
                f"Column '{column}': value {as_int} does not fit int32 "
                f"(range {_INT32_MIN}..{_INT32_MAX})."
            )
        return as_int

    return convert


def _to_epoch_seconds(value: Any) -> int:
    if isinstance(value, bool):
        raise TypeError("boolean is not a temporal value")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        # Date-only ISO ("YYYY-MM-DD") → midnight UTC; full timestamps keep tz.
        parsed = pendulum.parse(value, tz="UTC")
        if isinstance(parsed, pendulum.DateTime):
            return int(parsed.int_timestamp)
        if isinstance(parsed, pendulum.Date):
            return int(
                pendulum.datetime(parsed.year, parsed.month, parsed.day, tz="UTC").int_timestamp
            )
        raise TypeError(f"unsupported temporal parse result {type(parsed).__name__}")
    # pendulum DateTime / datetime-like
    int_ts = getattr(value, "int_timestamp", None)
    if isinstance(int_ts, int):
        return int_ts
    timestamp = getattr(value, "timestamp", None)
    if callable(timestamp):
        result = timestamp()
        if isinstance(result, (int, float)):
            return int(result)
    raise TypeError(f"unsupported temporal value type {type(value).__name__}")


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise TypeError("boolean is not a numeric value")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise TypeError(f"not a decimal: {value!r}") from exc
    raise TypeError(f"unsupported numeric value type {type(value).__name__}")
