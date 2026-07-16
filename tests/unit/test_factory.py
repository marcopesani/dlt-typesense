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
    """Covers: AC-CAP-01 — a parquet run fails before any load job starts."""
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
    """Covers: AC-CAP-01, AC-CAP-02, AC-CAP-03, AC-CAP-04, AC-CAP-05"""
    dest = typesense()
    caps = dest._raw_capabilities()
    # AC-CAP-01: jsonl is the only loader format.
    assert caps.preferred_loader_file_format == "jsonl"
    assert caps.supported_loader_file_formats == ["jsonl"]
    # AC-CAP-02: "upsert" first is semantic (dlt's default merge strategy).
    assert caps.supported_merge_strategies == ["upsert", "insert-only"]
    # AC-CAP-03: replace strategy is truncate-and-insert only.
    assert caps.supported_replace_strategies == ["truncate-and-insert"]
    # AC-CAP-04: identifier limits declared.
    assert caps.max_identifier_length == 255
    assert caps.max_column_identifier_length == 255
    # AC-CAP-05: no DDL transactions, 64 MB sharding hint.
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


def test_empty_dataset_yields_bare_names() -> None:
    """Covers: AC-PROTO-06 — an empty dataset yields bare (unqualified) table names."""
    from dlt.common.destination import DestinationCapabilitiesContext
    from dlt.common.schema import Schema

    config = TypesenseClientConfiguration()
    config.dataset_name = ""  # type: ignore[attr-defined]
    client = TypesenseClient(
        Schema("test"), config, DestinationCapabilitiesContext.generic_capabilities()
    )
    assert client.make_qualified_collection_name("products") == "products"


def test_qualified_collection_name_bounded_to_255() -> None:
    """Covers: AC-TS-09 — qualified collection names never exceed 255 chars."""
    from dlt.common.destination import DestinationCapabilitiesContext
    from dlt.common.schema import Schema

    config = TypesenseClientConfiguration()
    config.dataset_name = "d" * 200  # type: ignore[attr-defined]
    client = TypesenseClient(
        Schema("test"), config, DestinationCapabilitiesContext.generic_capabilities()
    )
    long_name = client.make_qualified_collection_name("t" * 200)
    assert len(long_name) <= 255
    # Deterministic: same input -> same shortened name.
    assert long_name == client.make_qualified_collection_name("t" * 200)
