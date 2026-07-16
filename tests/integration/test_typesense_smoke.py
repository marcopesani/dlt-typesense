"""Integration smoke test: the server is reachable and a pipeline runs.

Skipped by default (`addopts = -m 'not integration'`). Enable with:

    uv run pytest -m integration
"""

from __future__ import annotations

import dlt
import pytest

pytestmark = pytest.mark.integration


def test_server_reachable(probe) -> None:
    """The configured Typesense server answers (fails, never skips — §1)."""
    assert probe.list_collections() is not None


def test_minimal_pipeline_runs(make_pipeline, count_documents) -> None:
    """A trivial append pipeline loads end-to-end."""
    pipeline = make_pipeline()

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield from ({"value": i} for i in range(3))

    info = pipeline.run(items())
    assert not info.has_failed_jobs
    assert count_documents(make_pipeline.qualified_name(pipeline, "items")) == 3
