"""Smoke tests for destination factory and capabilities."""

from __future__ import annotations

import dlt
import pytest
from dlt.common.destination.exceptions import (
    DestinationIncompatibleLoaderFileFormatException,
)

from dlt_typesense import __version__, typesense
from dlt_typesense.configuration import TypesenseClientConfiguration, TypesenseCredentials
from dlt_typesense.load_jobs import TypesenseLoadJob, TypesenseRemoveOrphansJob
from dlt_typesense.typesense_client import TypesenseClient


def test_version() -> None:
    assert __version__ == "0.0.1"


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
    assert caps.supported_loader_file_formats == ["jsonl"]
    assert caps.supported_merge_strategies == ["upsert", "insert-only"]
    assert caps.supported_replace_strategies == ["truncate-and-insert"]
    assert caps.max_identifier_length == 255
    assert caps.max_column_identifier_length == 255
    assert caps.supports_ddl_transactions is False
    assert caps.recommended_file_size == 64_000_000
    assert caps.has_case_sensitive_identifiers is True


def test_client_class() -> None:
    assert typesense().client_class is TypesenseClient


def test_spec() -> None:
    assert typesense.spec is TypesenseClientConfiguration


def test_load_job_construction() -> None:
    job = TypesenseLoadJob("/tmp/products.abc123.0.jsonl", "catalog_products")
    assert job._collection_name == "catalog_products"


def test_orphan_job_stub_exists() -> None:
    job = TypesenseRemoveOrphansJob(
        "/tmp/products__items.abc123.0.jsonl", "catalog_products__items"
    )
    assert job._collection_name == "catalog_products__items"


def test_qualified_collection_name_helper() -> None:
    from dlt.common.destination import DestinationCapabilitiesContext
    from dlt.common.schema import Schema

    schema = Schema("test")
    config = TypesenseClientConfiguration()
    config.dataset_name = "catalog"  # type: ignore[attr-defined]
    client = TypesenseClient(schema, config, DestinationCapabilitiesContext.generic_capabilities())
    name = client.make_qualified_collection_name("products")
    assert name.endswith("products")
    assert "products" in name


def test_empty_dataset_yields_bare_names() -> None:
    from dlt.common.destination import DestinationCapabilitiesContext
    from dlt.common.schema import Schema

    config = TypesenseClientConfiguration()
    config.dataset_name = ""  # type: ignore[attr-defined]
    client = TypesenseClient(
        Schema("test"), config, DestinationCapabilitiesContext.generic_capabilities()
    )
    assert client.make_qualified_collection_name("products") == "products"


def test_qualified_collection_name_bounded_to_255() -> None:
    from dlt.common.destination import DestinationCapabilitiesContext
    from dlt.common.schema import Schema

    config = TypesenseClientConfiguration()
    config.dataset_name = "d" * 200  # type: ignore[attr-defined]
    client = TypesenseClient(
        Schema("test"), config, DestinationCapabilitiesContext.generic_capabilities()
    )
    long_name = client.make_qualified_collection_name("t" * 200)
    assert len(long_name) <= 255
    assert long_name == client.make_qualified_collection_name("t" * 200)
