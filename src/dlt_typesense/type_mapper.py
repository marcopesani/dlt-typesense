"""Map dlt column types to Typesense collection field definitions.

Collections are created with Typesense auto schema
(``fields: [{"name": ".*", "type": "auto"}]``). ``DLT_TO_TYPESENSE_TYPE``
documents the typed mapping for when explicit field schemas are used.
"""

from __future__ import annotations

from typing import Any

# Typesense has no native datetime type; timestamps would be int64 (unix) or string.
DLT_TO_TYPESENSE_TYPE: dict[str, str] = {
    "text": "string",
    "double": "float",
    "bool": "bool",
    "timestamp": "int64",
    "date": "int64",
    "bigint": "int64",
    "binary": "string",
    "decimal": "float",
    "wei": "string",
    "json": "object",
}


def collection_schema_auto(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "enable_nested_fields": True,
        "fields": [{"name": ".*", "type": "auto"}],
    }


def map_dlt_type(dlt_type: str) -> str:
    raise NotImplementedError(
        "Typed field mapping is unused; collections use collection_schema_auto()."
    )
