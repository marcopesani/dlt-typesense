"""dlt JobClientBase signature compatibility."""

from __future__ import annotations

import inspect

from dlt.common.destination.client import JobClientBase

from dlt_typesense.typesense_client import _BASE_ACCEPTS_FORCE, TypesenseClient


def test_base_force_probe_matches_installed_dlt() -> None:
    params = inspect.signature(JobClientBase.update_stored_schema).parameters
    assert ("force" in params) == _BASE_ACCEPTS_FORCE


def test_update_stored_schema_accepts_force_kwarg() -> None:
    # Our override keeps the 1.28+ signature so loaders that pass force= work.
    params = inspect.signature(TypesenseClient.update_stored_schema).parameters
    assert "force" in params
