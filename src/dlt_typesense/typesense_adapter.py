"""Resource adapter for Typesense field hints (facet, sort, index)."""

from __future__ import annotations

from typing import Any

from dlt.common.schema.typing import TColumnSchema
from dlt.common.typing import TDataItem
from dlt.extract import DltResource

# Column hint names reserved for Typesense collection field flags (phase 2)
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

    Mirrors ``qdrant_adapter`` / ``weaviate_adapter``. Hints are applied once
    typed collection schemas replace auto-schema.

    Args:
        data: A dlt resource or in-memory items.
        facet: Field(s) to mark as facetable.
        sort: Field(s) to mark as sortable.
        index: Field(s) to include in the index (default: all).
        embed: Field(s) reserved for future embedding / vector features.

    Returns:
        A dlt resource with Typesense column hints applied.

    Raises:
        NotImplementedError: Adapter wiring is scaffold-only for now.
    """
    _ = (facet, sort, index, embed, TColumnSchema)  # reserved for implementation
    raise NotImplementedError(
        "typesense_adapter hint application is not implemented yet. "
        "Pass resources directly to pipeline.run for now."
    )
