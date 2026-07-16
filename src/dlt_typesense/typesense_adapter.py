"""Resource adapter for Typesense field hints (facet, sort, index)."""

from __future__ import annotations

from typing import Any

from dlt.common.schema.typing import TColumnSchema
from dlt.common.typing import TDataItem
from dlt.extract import DltResource

FACET_HINT = "x-typesense-facet"
SORT_HINT = "x-typesense-sort"
INDEX_HINT = "x-typesense-index"
EMBED_HINT = "x-typesense-embed"


def typesense_adapter(
    data: DltResource | list[TDataItem] | Any,
    *,
    facet: str | list[str] | None = None,
    sort: str | list[str] | None = None,
    index: str | list[str] | None = None,
    embed: str | list[str] | None = None,
) -> DltResource:
    """Attach Typesense field hints to a resource or list of items.

    Mirrors ``qdrant_adapter`` / ``weaviate_adapter``. Not wired: collections
    use auto-schema, so these hints are not applied.

    Args:
        data: A dlt resource or in-memory items.
        facet: Field(s) to mark as facetable.
        sort: Field(s) to mark as sortable.
        index: Field(s) to include in the index (default: all).
        embed: Field(s) reserved for embedding / vector features.

    Returns:
        A dlt resource with Typesense column hints applied.

    Raises:
        NotImplementedError: Always — adapter is not implemented.
    """
    _ = (facet, sort, index, embed, TColumnSchema)
    raise NotImplementedError(
        "typesense_adapter is not implemented. "
        "Pass resources directly to pipeline.run; collections use auto-schema."
    )
