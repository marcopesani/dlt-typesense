"""Naming convention: reserved-id rename and deterministic field names."""

from __future__ import annotations

from dlt_typesense.naming import NamingConvention


def _naming() -> NamingConvention:
    # 255 matches the destination's max_identifier_length capability.
    return NamingConvention(255)


def test_source_id_is_renamed_away_from_reserved() -> None:
    """Covers: AC-TS-01 — a source column `id` is renamed, never Typesense's reserved id."""
    naming = _naming()
    assert naming.normalize_identifier("id") == "__id"
    # The destination is then free to own the top-level `id` field.


def test_non_reserved_identifiers_unchanged_semantics() -> None:
    """Covers: AC-TS-01 — only `id` is remapped; `_dlt_id`/others pass through."""
    naming = _naming()
    assert naming.normalize_identifier("_dlt_id") == "_dlt_id"
    assert naming.normalize_identifier("sku") == "sku"


def test_field_names_normalized_deterministically() -> None:
    """Covers: AC-TS-02 — dots, spaces, dashes, leading digits map to concrete names."""
    naming = _naming()
    # Concrete expected outputs (dlt snake_case): non-word chars collapse to `_`,
    # a leading digit is escaped. Asserting the actual values, not a tautology.
    expected = {
        "my.field": "my_field",
        "my field": "my_field",
        "my-field": "my_field",
        "1col": "_1col",
        "CamelCase": "camel_case",
    }
    for source, want in expected.items():
        assert naming.normalize_identifier(source) == want
    # Determinism: a second call yields the same result.
    for source, want in expected.items():
        assert naming.normalize_identifier(source) == want


def test_no_collisions_on_realistic_inputs() -> None:
    """Covers: AC-TS-02 — distinct realistic columns keep distinct names."""
    naming = _naming()
    names = [
        naming.normalize_identifier(c)
        for c in ["sku", "price_usd", "created_at", "user.email", "id", "_dlt_id"]
    ]
    assert len(names) == len(set(names))
