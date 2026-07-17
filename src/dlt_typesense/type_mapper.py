"""Build Typesense collection schemas from dlt table schemas.

Collections are created with a ``.*`` auto catch-all so unhinted columns and
schema evolution keep working. Columns carrying the ``x-typesense-field`` hint
(see ``typesense_adapter``) are pinned as explicit typed fields ahead of the
catch-all; collection-level parameters come from the ``x-typesense-collection``
table hint.

``DLT_TO_TYPESENSE_TYPE`` maps dlt column types to the Typesense type of the
value actually present on the JSONL wire: dlt serializes timestamps/dates/times
as ISO-8601 strings, decimals/wei as exact strings, binary as base64, and the
load job stringifies ``json`` columns.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dlt_typesense.exceptions import TypesenseSchemaError
from dlt_typesense.typesense_adapter import COLLECTION_HINT, FIELD_HINT

DLT_TO_TYPESENSE_TYPE: dict[str, str] = {
    "text": "string",
    "double": "float",
    "bool": "bool",
    "bigint": "int64",
    "timestamp": "string",
    "date": "string",
    "time": "string",
    "decimal": "string",
    "wei": "string",
    "binary": "string",
    "json": "string",
}

# Typesense docs list int32/float; the server also accepts int64 (verified
# against Typesense 30.x). Strings are never valid as default_sorting_field.
_SORTABLE_TYPES = ("int32", "int64", "float")


def collection_schema_auto(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "enable_nested_fields": True,
        "fields": [{"name": ".*", "type": "auto"}],
    }


def map_dlt_type(dlt_type: str | None) -> str:
    """Typesense type for a dlt column type; ``auto`` when unknown or missing."""
    return DLT_TO_TYPESENSE_TYPE.get(dlt_type or "", "auto")


def collection_schema_from_table(
    qualified_name: str,
    table: Mapping[str, Any],
    normalize: Callable[[str], str],
) -> dict[str, Any]:
    """Collection schema for a data table, honoring adapter hints if present.

    Hinted columns become pinned fields before the ``.*`` auto catch-all;
    ``normalize`` is the schema naming convention, needed because values inside
    the table-level hint (``default_sorting_field``) are not normalized by dlt.
    """
    columns: Mapping[str, Mapping[str, Any]] = table.get("columns") or {}
    pinned: dict[str, dict[str, Any]] = {}
    for column_name, column in columns.items():
        params = column.get(FIELD_HINT)
        if params is None:
            continue
        pinned[column_name] = _pinned_field(column_name, column, params)

    schema: dict[str, Any] = {"name": qualified_name, "enable_nested_fields": True}
    collection_params = dict(table.get(COLLECTION_HINT) or {})
    sorting_field = collection_params.pop("default_sorting_field", None)
    schema.update(collection_params)

    if sorting_field is not None:
        normalized = normalize(sorting_field)
        _pin_sorting_field(qualified_name, normalized, pinned, columns)
        schema["default_sorting_field"] = normalized

    schema["fields"] = [*pinned.values(), {"name": ".*", "type": "auto"}]
    return schema


def _pinned_field(
    column_name: str, column: Mapping[str, Any], params: Mapping[str, Any]
) -> dict[str, Any]:
    field: dict[str, Any] = {"name": column_name}
    field.update(params)
    if "type" not in field:
        field["type"] = map_dlt_type(column.get("data_type"))
    if "optional" not in field:
        field["optional"] = bool(column.get("nullable", True))
    return field


def _pin_sorting_field(
    qualified_name: str,
    field_name: str,
    pinned: dict[str, dict[str, Any]],
    columns: Mapping[str, Mapping[str, Any]],
) -> None:
    column = columns.get(field_name)
    field = pinned.get(field_name)
    if field is None:
        if column is None:
            raise TypesenseSchemaError(
                f"Collection '{qualified_name}': default_sorting_field '{field_name}' does not "
                "match any column. Add the column or a field_hints entry for it."
            )
        field = _pinned_field(field_name, column, {})
        pinned[field_name] = field
    # Typesense rejects an optional default sorting field. Force non-optional
    # unless the user explicitly set `optional` via field_hints (then let the
    # server report the conflict rather than silently overriding the user).
    explicit_params = (column or {}).get(FIELD_HINT) or {}
    if "optional" not in explicit_params:
        field["optional"] = False
    if field.get("type") not in _SORTABLE_TYPES:
        raise TypesenseSchemaError(
            f"Collection '{qualified_name}': default_sorting_field '{field_name}' resolves to "
            f"Typesense type '{field.get('type')}', but must be one of {list(_SORTABLE_TYPES)}. "
            "Use an int32/int64/float column (for timestamps, pin "
            '`field_hints={...: {"type": "int64"}}` so the load job stores Unix epoch '
            "seconds), or drop default_sorting_field and sort with per-field "
            "`sort: true` + an explicit `sort_by` at query time."
        )
