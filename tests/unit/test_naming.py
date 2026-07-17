"""Naming convention: reserved-id rename (the only repo-specific rule)."""

from __future__ import annotations

from dlt_typesense.naming import NamingConvention


def _naming() -> NamingConvention:
    return NamingConvention(255)


def test_source_id_is_renamed_away_from_reserved() -> None:
    naming = _naming()
    assert naming.normalize_identifier("id") == "__id"
    # Parent snake_case lowercases first; remap must still hit after that.
    assert naming.normalize_identifier("Id") == "__id"
    assert naming.normalize_identifier("ID") == "__id"


def test_non_reserved_identifiers_unchanged_semantics() -> None:
    naming = _naming()
    assert naming.normalize_identifier("_dlt_id") == "_dlt_id"
