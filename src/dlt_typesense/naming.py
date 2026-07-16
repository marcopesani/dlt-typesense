"""Naming convention: snake_case with Typesense's reserved ``id`` remapped to ``__id``."""

from __future__ import annotations

from typing import ClassVar

from dlt.common.normalizers.naming.snake_case import (
    NamingConvention as SnakeCaseNamingConvention,
)


class NamingConvention(SnakeCaseNamingConvention):
    """snake_case, but remaps Typesense's reserved document ``id`` field."""

    # Applied after snake_case. Collisions with an existing `__id` raise via dlt.
    RESERVED_PROPERTIES: ClassVar[dict[str, str]] = {"id": "__id"}

    def normalize_identifier(self, identifier: str) -> str:
        normalized = super().normalize_identifier(identifier)
        return self.RESERVED_PROPERTIES.get(normalized, normalized)
