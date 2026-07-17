"""``destination=\"typesense\"`` short-name resolution via the dlt plugin entry point."""

from __future__ import annotations

import dlt
import pytest

pytestmark = pytest.mark.integration


def test_string_destination_resolves_and_loads(
    require_server, dataset_name, tmp_path, documents
) -> None:
    # Force credentials via env so the string destination can resolve config.
    import os

    os.environ["DESTINATION__TYPESENSE__CREDENTIALS__HOST"] = require_server.host
    os.environ["DESTINATION__TYPESENSE__CREDENTIALS__PORT"] = str(require_server.port)
    os.environ["DESTINATION__TYPESENSE__CREDENTIALS__PROTOCOL"] = require_server.protocol
    os.environ["DESTINATION__TYPESENSE__CREDENTIALS__API_KEY"] = require_server.api_key

    pipeline = dlt.pipeline(
        pipeline_name=f"str_dest_{dataset_name}",
        destination="typesense",
        dataset_name=dataset_name,
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    try:

        @dlt.resource(name="items", write_disposition="replace")
        def items():
            yield {"sku": "A1", "title": "Widget"}

        info = pipeline.run(items())
        assert not info.has_failed_jobs
        with pipeline.destination_client() as client:
            collection = client.make_qualified_collection_name("items")  # type: ignore[attr-defined]
        docs = documents(collection)
        assert len(docs) == 1
        assert docs[0]["sku"] == "A1"
        assert docs[0]["title"] == "Widget"
    finally:
        try:
            with pipeline.destination_client() as client:
                client.drop_storage()  # type: ignore[attr-defined]
        except Exception:
            pass
