"""Map dlt column types to Typesense collection field definitions.

v1 approach: create collections with auto schema
(`fields: [{"name": ".*", "type": "auto"}]`, `enable_nested_fields: true`).

This module reserves the typed mapping used once auto-schema is replaced.
"""

from __future__ import annotations

from typing import Any

# Planned mapping (phase 2). Typesense has no native datetime type.
DLT_TO_TYPESENSE_TYPE: dict[str, str] = {
    "text": "string",
    "double": "float",
    "bool": "bool",
    "timestamp": "int64",  # unix epoch; alternative: string ISO-8601
    "date": "int64",
    "bigint": "int64",
    "binary": "string",
    "decimal": "float",  # or string for precision
    "wei": "string",
    "json": "object",
}


def collection_schema_auto(name: str) -> dict[str, Any]:
    """Return a Typesense collection schema using auto field detection."""
    return {
        "name": name,
        "enable_nested_fields": True,
        "fields": [{"name": ".*", "type": "auto"}],
    }


def map_dlt_type(dlt_type: str) -> str:
    """Map a dlt data type name to a Typesense field type.

    Raises:
        NotImplementedError: Typed mapping is not implemented in the scaffold.
    """
    raise NotImplementedError(
        "Typed field mapping is phase 2; use collection_schema_auto() for v1."
    )
