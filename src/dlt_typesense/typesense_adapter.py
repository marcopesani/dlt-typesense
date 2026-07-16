"""Resource adapter for Typesense collection schema hints.

Attaches per-field and collection-level Typesense schema parameters to a dlt
resource. Hints are applied when the collection is created (first load, or
every ``replace`` run); see the Typesense collections API:
https://typesense.org/docs/30.2/api/collections.html
"""

from __future__ import annotations

from typing import Any

from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource

# Column-level hint: dict of Typesense field parameters, stored verbatim.
FIELD_HINT = "x-typesense-field"
# Table-level hint: dict of Typesense collection parameters, stored verbatim.
COLLECTION_HINT = "x-typesense-collection"

FIELD_PARAMS: frozenset[str] = frozenset(
    {
        "type",
        "facet",
        "index",
        "optional",
        "sort",
        "infix",
        "locale",
        "stem",
        "stem_dictionary",
        "num_dim",
        "vec_dist",
        "reference",
        "range_index",
        "store",
        "truncate_len",
        "token_separators",
        "symbols_to_index",
        "embed",
    }
)

COLLECTION_PARAMS: frozenset[str] = frozenset(
    {
        "default_sorting_field",
        "token_separators",
        "symbols_to_index",
        "enable_nested_fields",
        "metadata",
    }
)

_BOOL_FIELD_PARAMS = frozenset(
    {"facet", "index", "optional", "sort", "infix", "stem", "range_index", "store"}
)
_POSITIVE_INT_FIELD_PARAMS = frozenset({"num_dim", "truncate_len"})
_STR_LIST_PARAMS = frozenset({"token_separators", "symbols_to_index"})


def typesense_adapter(
    data: Any,
    *,
    facet: str | list[str] | None = None,
    sort: str | list[str] | None = None,
    index: str | list[str] | None = None,
    field_hints: dict[str, dict[str, Any]] | None = None,
    collection_hints: dict[str, Any] | None = None,
) -> DltResource:
    """Attach Typesense schema hints to a resource or in-memory items.

    Hinted columns are pinned as explicit typed fields in the collection
    schema; everything else falls through to the ``.*`` auto field. Hints
    take effect when the collection is created.

    Args:
        data: A dlt resource or in-memory items.
        facet: Column(s) to mark facetable (shorthand for ``{"facet": True}``).
        sort: Column(s) to mark sortable (shorthand for ``{"sort": True}``).
        index: Column(s) to mark indexed (shorthand for ``{"index": True}``).
        field_hints: Full per-column Typesense field parameters, e.g.
            ``{"embedding": {"type": "float[]", "num_dim": 384}}``. Merged over
            the shorthand params; explicit entries win. See ``FIELD_PARAMS``.
        collection_hints: Collection-level parameters, e.g.
            ``{"default_sorting_field": "price"}``. See ``COLLECTION_PARAMS``.
            (``synonym_sets``/``curation_sets`` are managed via their own
            Typesense APIs and are out of scope here.)

    Returns:
        The dlt resource with Typesense hints applied.

    Example:
        >>> typesense_adapter(
        ...     products,
        ...     facet="category",
        ...     sort=["price", "rating"],
        ...     field_hints={"description": {"locale": "de", "infix": True}},
        ...     collection_hints={"default_sorting_field": "price"},
        ... )
    """
    resource = get_resource_for_adapter(data)

    column_params: dict[str, dict[str, Any]] = {}
    for param_name, columns in (("facet", facet), ("sort", sort), ("index", index)):
        for column in _column_name_list(param_name, columns):
            column_params.setdefault(column, {})[param_name] = True

    if field_hints is not None:
        if not isinstance(field_hints, dict):
            raise ValueError("field_hints must be a dict of column name -> field params.")
        for column, params in field_hints.items():
            _validate_field_params(column, params)
            column_params.setdefault(column, {}).update(params)

    if collection_hints is not None:
        _validate_collection_params(collection_hints)

    if not column_params and collection_hints is None:
        raise ValueError(
            "typesense_adapter requires at least one of: facet, sort, index, "
            "field_hints, collection_hints."
        )

    column_hints: TTableSchemaColumns = {}
    for column, params in column_params.items():
        column_schema: TColumnSchema = {"name": column}
        # Array/object Typesense types need the raw JSON value in the document.
        # Typing the dlt column as `json` keeps lists inline (instead of being
        # normalized into child tables) and dicts unflattened.
        if _is_native_json_type(params.get("type")):
            column_schema["data_type"] = "json"
        column_schema[FIELD_HINT] = params  # type: ignore[typeddict-unknown-key]
        column_hints[column] = column_schema

    resource.apply_hints(
        columns=column_hints or None,
        additional_table_hints=(
            {COLLECTION_HINT: dict(collection_hints)} if collection_hints is not None else None
        ),
    )
    return resource


def _is_native_json_type(typesense_type: Any) -> bool:
    """Types whose document value is a JSON array/object (not a scalar)."""
    return isinstance(typesense_type, str) and (
        typesense_type.endswith("[]") or typesense_type in ("object", "geopoint")
    )


def _column_name_list(param_name: str, value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    names = [value] if isinstance(value, str) else value
    if not isinstance(names, list) or not all(
        isinstance(name, str) and name.strip() for name in names
    ):
        raise ValueError(
            f"'{param_name}' must be a column name or a list of non-empty column names."
        )
    return names


def _validate_field_params(column: str, params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError(f"field_hints['{column}'] must be a dict of Typesense field params.")
    unknown = set(params) - FIELD_PARAMS
    if unknown:
        raise ValueError(
            f"field_hints['{column}'] has unknown Typesense field params {sorted(unknown)}; "
            f"allowed: {sorted(FIELD_PARAMS)}."
        )
    for key, value in params.items():
        if key in _BOOL_FIELD_PARAMS and not isinstance(value, bool):
            raise ValueError(f"field_hints['{column}']['{key}'] must be a bool.")
        if key in _POSITIVE_INT_FIELD_PARAMS and (
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
        ):
            raise ValueError(f"field_hints['{column}']['{key}'] must be a positive int.")
        if key in _STR_LIST_PARAMS:
            _require_str_list(f"field_hints['{column}']['{key}']", value)
    embed = params.get("embed")
    if embed is not None and (not isinstance(embed, dict) or "from" not in embed):
        raise ValueError(
            f"field_hints['{column}']['embed'] must be a dict with a 'from' key "
            "(and typically 'model_config')."
        )


def _validate_collection_params(params: dict[str, Any]) -> None:
    if not isinstance(params, dict):
        raise ValueError("collection_hints must be a dict of Typesense collection params.")
    unknown = set(params) - COLLECTION_PARAMS
    if unknown:
        raise ValueError(
            f"collection_hints has unknown Typesense collection params {sorted(unknown)}; "
            f"allowed: {sorted(COLLECTION_PARAMS)}."
        )
    sorting_field = params.get("default_sorting_field")
    if sorting_field is not None and (not isinstance(sorting_field, str) or not sorting_field):
        raise ValueError("collection_hints['default_sorting_field'] must be a non-empty string.")
    nested = params.get("enable_nested_fields")
    if nested is not None and not isinstance(nested, bool):
        raise ValueError("collection_hints['enable_nested_fields'] must be a bool.")
    metadata = params.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        raise ValueError("collection_hints['metadata'] must be a dict.")
    for key in _STR_LIST_PARAMS:
        if key in params:
            _require_str_list(f"collection_hints['{key}']", params[key])


def _require_str_list(label: str, value: Any) -> None:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{label} must be a list of strings.")
