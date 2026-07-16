"""Naming convention for the Typesense destination.

Typesense reserves the top-level string field ``id`` as a document's primary key,
which the destination manages itself (from ``_dlt_id`` or a merge key). A *source*
column named ``id`` is therefore renamed at normalization time — the same
mechanism dlt's Weaviate destination uses for its reserved properties — so the
value is preserved as a distinct field and the stored dlt schema always matches
what physically exists in Typesense (AC-TS-01). All other identifiers use dlt's
standard snake_case normalization, which already yields Typesense-safe names for
dots, spaces, dashes, and leading digits (AC-TS-02).
"""

from __future__ import annotations

from typing import ClassVar

from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)


class NamingConvention(SnakeCaseNamingConvention):
    """snake_case, but reserved names are remapped away from Typesense's ``id``."""

    # Applied to the *already normalized* identifier. If a source also contains the
    # escape target (e.g. both `id` and `__id`), both normalize to `__id` and dlt's
    # identifier-collision detection raises a terminal error — loud, never silent.
    RESERVED_PROPERTIES: ClassVar[dict[str, str]] = {"id": "__id"}

    def normalize_identifier(self, identifier: str) -> str:
        normalized = super().normalize_identifier(identifier)
        return self.RESERVED_PROPERTIES.get(normalized, normalized)
