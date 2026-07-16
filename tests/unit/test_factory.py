"""Smoke tests for destination factory and capabilities."""

from __future__ import annotations

from dlt_typesense import __version__, typesense
from dlt_typesense.configuration import TypesenseClientConfiguration
from dlt_typesense.load_jobs import TypesenseLoadJob, TypesenseRemoveOrphansJob
from dlt_typesense.typesense_client import TypesenseClient


def test_version() -> None:
    assert __version__ == "0.0.1"


def test_factory_capabilities() -> None:
    dest = typesense()
    caps = dest._raw_capabilities()
    assert caps.preferred_loader_file_format == "jsonl"
    assert caps.supported_loader_file_formats == ["jsonl"]
    assert caps.supported_merge_strategies == ["upsert"]
    assert caps.supported_replace_strategies == ["truncate-and-insert"]
    assert caps.supports_ddl_transactions is False
    assert caps.recommended_file_size == 64_000_000
    assert caps.has_case_sensitive_identifiers is True


def test_client_class() -> None:
    assert typesense().client_class is TypesenseClient


def test_spec() -> None:
    assert typesense.spec is TypesenseClientConfiguration


def test_load_job_construction() -> None:
    # dlt load job filenames: table.file_id.retry_count.format
    job = TypesenseLoadJob("/tmp/products.abc123.0.jsonl", "catalog_products")
    assert job._collection_name == "catalog_products"


def test_orphan_job_stub_exists() -> None:
    job = TypesenseRemoveOrphansJob(
        "/tmp/products__items.abc123.0.jsonl", "catalog_products__items"
    )
    assert job._collection_name == "catalog_products__items"


def test_qualified_collection_name_helper() -> None:
    # Lightweight check without opening a real Typesense connection
    from dlt.common.destination import DestinationCapabilitiesContext
    from dlt.common.schema import Schema

    schema = Schema("test")
    config = TypesenseClientConfiguration()
    config.dataset_name = "catalog"  # type: ignore[attr-defined]
    client = TypesenseClient(schema, config, DestinationCapabilitiesContext.generic_capabilities())
    # dataset_name may be normalized by config; separator seam must exist
    name = client.make_qualified_collection_name("products")
    assert name.endswith("products")
    assert "products" in name
