"""Integration tests against a live Typesense server.

Skipped by default (`addopts = -m 'not integration'`). Enable with:

    uv run pytest -m integration
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def test_typesense_reachable_placeholder() -> None:
    """Placeholder until rest client + service container are wired."""
    pytest.skip("Typesense integration not implemented yet")
