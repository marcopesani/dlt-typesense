"""Naming convention: reserved-id rename and deterministic field names."""

from __future__ import annotations

from dlt_typesense.naming import NamingConvention


def _naming() -> NamingConvention:
    return NamingConvention(255)


def test_source_id_is_renamed_away_from_reserved() -> None:
    naming = _naming()
    assert naming.normalize_identifier("id") == "__id"


def test_non_reserved_identifiers_unchanged_semantics() -> None:
    naming = _naming()
    assert naming.normalize_identifier("_dlt_id") == "_dlt_id"
    assert naming.normalize_identifier("sku") == "sku"


def test_field_names_normalized_deterministically() -> None:
    naming = _naming()
    expected = {
        "my.field": "my_field",
        "my field": "my_field",
        "my-field": "my_field",
        "1col": "_1col",
        "CamelCase": "camel_case",
    }
    for source, want in expected.items():
        assert naming.normalize_identifier(source) == want
    for source, want in expected.items():
        assert naming.normalize_identifier(source) == want


def test_no_collisions_on_realistic_inputs() -> None:
    naming = _naming()
    names = [
        naming.normalize_identifier(c)
        for c in ["sku", "price_usd", "created_at", "user.email", "id", "_dlt_id"]
    ]
    assert len(names) == len(set(names))
