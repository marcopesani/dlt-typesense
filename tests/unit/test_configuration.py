"""Credential resolution and secret-masking tests (no server required)."""

from __future__ import annotations

import pytest
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.destination.exceptions import DestinationCapabilitiesException

from dlt_typesense.configuration import TypesenseClientConfiguration, TypesenseCredentials

_SECRET = "SUPER_SECRET_API_KEY_9f8e7d"
_CRED_SECTIONS = ("destination", "typesense")


@pytest.fixture(autouse=True)
def _clean_credential_env(monkeypatch):
    """Ensure no ambient DESTINATION__TYPESENSE__* env leaks into resolution tests."""
    for key in list(__import__("os").environ):
        if key.startswith("DESTINATION__TYPESENSE__") or key.startswith("TYPESENSE_"):
            monkeypatch.delenv(key, raising=False)


def _configured() -> TypesenseClientConfiguration:
    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol, creds.api_key = "h.example", 7777, "https", _SECRET
    config = TypesenseClientConfiguration()
    config.credentials = creds
    return config


def test_credentials_defaults() -> None:
    """Covers: AC-CAP-06 — defaults host/port/protocol."""
    creds = TypesenseCredentials()
    assert creds.host == "localhost"
    assert creds.port == 8108
    assert creds.protocol == "http"


def test_fingerprint_stable_and_key_independent() -> None:
    """Covers: AC-CAP-07 — fingerprint stable per protocol/host/port, key-independent."""
    config_a = _configured()
    config_b = _configured()
    config_b.credentials.api_key = "a-totally-different-key"
    assert config_a.fingerprint() == config_b.fingerprint()
    assert config_a.fingerprint() != ""


def test_fingerprint_discriminates_connection_tuple() -> None:
    """Covers: AC-CAP-07 — distinct protocol/host/port yield distinct fingerprints.

    Guards against a degenerate, location-insensitive fingerprint (which would let
    state/schema from one server be reused against another undetected).
    """
    base = _configured()
    for field, value in (("host", "other.example"), ("port", 1234), ("protocol", "http")):
        variant = _configured()
        setattr(variant.credentials, field, value)
        assert variant.fingerprint() != base.fingerprint(), f"fingerprint ignores {field}"


@pytest.mark.parametrize(
    "rendered",
    [
        lambda c: str(c.credentials),
        lambda c: repr(c.credentials),
        lambda c: str(c),
        lambda c: c.fingerprint(),
        lambda c: c.physical_location(),
    ],
)
def test_api_key_never_leaks(rendered) -> None:
    """Covers: AC-CAP-07, AC-NF-03 — the api_key value never appears in reprs/output."""
    config = _configured()
    assert _SECRET not in rendered(config)


def test_physical_location_shows_connection_only() -> None:
    """Covers: AC-CAP-07"""
    config = _configured()
    assert config.physical_location() == "https://h.example:7777"
    assert str(config) == "https://h.example:7777"


def test_missing_api_key_makes_config_partial() -> None:
    """Covers: AC-CAP-06 — a missing api_key is detected as unresolved (dlt raises).

    ``api_key`` is a non-optional secret, so a ``None`` value leaves the
    credentials partial; dlt surfaces this as ConfigFieldMissingException naming
    the field before any load starts.
    """
    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol = "localhost", 8108, "http"
    # api_key deliberately left as None
    assert creds.is_partial() is True
    resolvable = creds.get_resolvable_fields()
    assert "api_key" in resolvable
    assert creds.is_field_resolved(None, resolvable["api_key"]) is False


def test_present_api_key_resolves() -> None:
    """Covers: AC-CAP-06"""
    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol, creds.api_key = "localhost", 8108, "http", "k"
    resolvable = creds.get_resolvable_fields()
    assert creds.is_field_resolved("k", resolvable["api_key"]) is True


def test_credentials_resolve_from_env_with_defaults(monkeypatch) -> None:
    """Covers: AC-CAP-06 — real dlt config resolution from env, with defaults."""
    monkeypatch.setenv("DESTINATION__TYPESENSE__CREDENTIALS__API_KEY", "env-key")
    monkeypatch.setenv("DESTINATION__TYPESENSE__CREDENTIALS__HOST", "env-host")
    resolved = resolve_configuration(TypesenseCredentials(), sections=_CRED_SECTIONS)
    assert resolved.api_key == "env-key"
    assert resolved.host == "env-host"
    assert resolved.port == 8108  # default
    assert resolved.protocol == "http"  # default


def test_missing_api_key_raises_naming_the_field() -> None:
    """Covers: AC-CAP-06 — real resolution with no api_key raises, naming the field."""
    with pytest.raises(ConfigFieldMissingException) as excinfo:
        resolve_configuration(TypesenseCredentials(), sections=_CRED_SECTIONS)
    assert "api_key" in excinfo.value.fields
    assert "api_key" in str(excinfo.value)


def test_unsupported_replace_strategy_rejected() -> None:
    """Covers: AC-CAP-03 — a staging replace strategy fails, naming strategy + supported list."""
    for strategy in ("staging-optimized", "insert-from-staging"):
        config = _configured()
        config.replace_strategy = strategy  # type: ignore[attr-defined]
        with pytest.raises(DestinationCapabilitiesException) as excinfo:
            config.on_resolved()
        message = str(excinfo.value)
        assert strategy in message
        assert "truncate-and-insert" in message
    _configured().on_resolved()  # default truncate-and-insert is accepted


def test_create_import_action_rejected() -> None:
    """Covers: AC-TS-07 — `create` is rejected at config resolution."""
    config = _configured()
    config.import_action = "create"
    with pytest.raises(DestinationCapabilitiesException):
        config.on_resolved()
