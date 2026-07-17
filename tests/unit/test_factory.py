"""Smoke tests for destination factory and capabilities."""

from __future__ import annotations

import dlt
import pytest
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.exceptions import (
    DestinationIncompatibleLoaderFileFormatException,
)
from dlt.common.schema import Schema

from dlt_typesense import typesense
from dlt_typesense.configuration import TypesenseClientConfiguration, TypesenseCredentials
from dlt_typesense.typesense_client import TypesenseClient


def test_non_jsonl_loader_format_rejected_before_load(tmp_path) -> None:
    creds = TypesenseCredentials()
    creds.host, creds.port, creds.protocol, creds.api_key = "localhost", 8108, "http", "x"
    pipeline = dlt.pipeline(
        pipeline_name="fmt_check",
        destination=typesense(credentials=creds),
        dataset_name="d",
        pipelines_dir=str(tmp_path),
    )
    with pytest.raises(DestinationIncompatibleLoaderFileFormatException):
        pipeline.extract([{"a": 1}], table_name="x", loader_file_format="parquet")


def test_factory_capabilities() -> None:
    dest = typesense()
    caps = dest._raw_capabilities()
    assert caps.preferred_loader_file_format == "jsonl"
    # "reference" is internal: it routes the merge orphan-cleanup follow-up job.
    assert caps.supported_loader_file_formats == ["jsonl", "reference"]
    assert caps.supported_merge_strategies == ["upsert", "insert-only"]
    assert caps.supported_replace_strategies == ["truncate-and-insert"]
    assert caps.max_identifier_length == 255
    assert caps.max_column_identifier_length == 255


def test_qualified_collection_name_helper() -> None:
    schema = Schema("test")
    config = TypesenseClientConfiguration()
    config.dataset_name = "catalog"  # type: ignore[attr-defined]
    client = TypesenseClient(schema, config, DestinationCapabilitiesContext.generic_capabilities())
    assert client.make_qualified_collection_name("products") == "catalog_products"


def test_empty_dataset_yields_bare_names() -> None:
    config = TypesenseClientConfiguration()
    config.dataset_name = ""  # type: ignore[attr-defined]
    client = TypesenseClient(
        Schema("test"), config, DestinationCapabilitiesContext.generic_capabilities()
    )
    assert client.make_qualified_collection_name("products") == "products"


def test_qualified_collection_name_bounded_to_255() -> None:
    config = TypesenseClientConfiguration()
    config.dataset_name = "d" * 200  # type: ignore[attr-defined]
    client = TypesenseClient(
        Schema("test"), config, DestinationCapabilitiesContext.generic_capabilities()
    )
    long_name = client.make_qualified_collection_name("t" * 200)
    assert len(long_name) <= 255
    assert long_name == client.make_qualified_collection_name("t" * 200)
    # Truncation appends _{16-hex sha1 digest}.
    assert "_" in long_name[-17:]
