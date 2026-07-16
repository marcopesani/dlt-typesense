"""Credential resolution and secret-masking tests (no server required)."""

from __future__ import annotations

import os

import pytest
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.destination.exceptions import DestinationCapabilitiesException

from dlt_typesense.configuration import TypesenseClientConfiguration, TypesenseCredentials

_SECRET = "SUPER_SECRET_API_KEY_9f8e7d"
_CRED_SECTIONS = ("destination", "typesense")


@pytest.fixture(autouse=True)
def _clean_credential_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("DESTINATION__TYPESENSE__") or key.startswith("TYPESENSE_"):
            monkeypatch.delenv(key, raising=False)


def _configured() -> TypesenseClientConfiguration:
    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol, creds.api_key = "h.example", 7777, "https", _SECRET
    config = TypesenseClientConfiguration()
    config.credentials = creds
    return config


def test_credentials_defaults() -> None:
    creds = TypesenseCredentials()
    assert creds.host == "localhost"
    assert creds.port == 8108
    assert creds.protocol == "http"


def test_fingerprint_stable_and_key_independent() -> None:
    config_a = _configured()
    config_b = _configured()
    config_b.credentials.api_key = "a-totally-different-key"
    assert config_a.fingerprint() == config_b.fingerprint()
    assert config_a.fingerprint() != ""


def test_fingerprint_discriminates_connection_tuple() -> None:
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
    config = _configured()
    assert _SECRET not in rendered(config)


def test_physical_location_shows_connection_only() -> None:
    config = _configured()
    assert config.physical_location() == "https://h.example:7777"
    assert str(config) == "https://h.example:7777"


def test_missing_api_key_makes_config_partial() -> None:
    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol = "localhost", 8108, "http"
    assert creds.is_partial() is True
    resolvable = creds.get_resolvable_fields()
    assert "api_key" in resolvable
    assert creds.is_field_resolved(None, resolvable["api_key"]) is False


def test_present_api_key_resolves() -> None:
    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol, creds.api_key = "localhost", 8108, "http", "k"
    resolvable = creds.get_resolvable_fields()
    assert creds.is_field_resolved("k", resolvable["api_key"]) is True


def test_credentials_resolve_from_env_with_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DESTINATION__TYPESENSE__CREDENTIALS__API_KEY", "env-key")
    monkeypatch.setenv("DESTINATION__TYPESENSE__CREDENTIALS__HOST", "env-host")
    resolved = resolve_configuration(TypesenseCredentials(), sections=_CRED_SECTIONS)
    assert resolved.api_key == "env-key"
    assert resolved.host == "env-host"
    assert resolved.port == 8108
    assert resolved.protocol == "http"


def test_missing_api_key_raises_naming_the_field() -> None:
    with pytest.raises(ConfigFieldMissingException) as excinfo:
        resolve_configuration(TypesenseCredentials(), sections=_CRED_SECTIONS)
    assert "api_key" in excinfo.value.fields
    assert "api_key" in str(excinfo.value)


def test_unsupported_replace_strategy_rejected() -> None:
    for strategy in ("staging-optimized", "insert-from-staging"):
        config = _configured()
        config.replace_strategy = strategy  # type: ignore[attr-defined]
        with pytest.raises(DestinationCapabilitiesException) as excinfo:
            config.on_resolved()
        message = str(excinfo.value)
        assert strategy in message
        assert "truncate-and-insert" in message
    _configured().on_resolved()


def test_create_import_action_rejected() -> None:
    config = _configured()
    config.import_action = "create"
    with pytest.raises(DestinationCapabilitiesException):
        config.on_resolved()
