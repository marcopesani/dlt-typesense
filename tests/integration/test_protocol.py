"""Storage lifecycle protocol against a live Typesense."""

from __future__ import annotations

from contextlib import suppress

import dlt
import pytest

from dlt_typesense.typesense_client import TypesenseClient

pytestmark = pytest.mark.integration


def _establish_schema(pipeline: dlt.Pipeline, table: str = "items") -> None:
    """Extract locally so the pipeline has a default schema (no load)."""
    pipeline.extract([{"value": 1}], table_name=table)


def _system_collections(client: TypesenseClient) -> list[str]:
    return [
        client.make_qualified_collection_name(name)
        for name in (
            client.schema.version_table_name,
            client.schema.loads_table_name,
            client.schema.state_table_name,
        )
    ]


def test_initialize_storage_creates_system_collections_idempotently(
    make_pipeline, open_client, probe, collection_exists
) -> None:
    pipeline = make_pipeline()
    _establish_schema(pipeline)
    with open_client(pipeline) as client:
        client.initialize_storage()
        for collection in _system_collections(client):
            assert collection_exists(collection)
        version_collection = client.make_qualified_collection_name(client.schema.version_table_name)
        probe.collections[version_collection].documents.upsert(
            {
                "id": "sentinel",
                "version": 1,
                "engine_version": 1,
                "schema_name": "s",
                "version_hash": "h",
                "inserted_at": "2020-01-01T00:00:00+00:00",
                "schema": "{}",
            }
        )
        client.initialize_storage()  # second run must not error or lose data
        for collection in _system_collections(client):
            assert collection_exists(collection)
        assert probe.collections[version_collection].documents["sentinel"].retrieve() is not None


def test_is_storage_initialized_transitions(make_pipeline, open_client) -> None:
    pipeline = make_pipeline()
    _establish_schema(pipeline)
    with open_client(pipeline) as client:
        assert client.is_storage_initialized() is False
        client.initialize_storage()
        assert client.is_storage_initialized() is True


def test_truncate_tables_empties_only_listed(
    make_pipeline, open_client, collection_exists, count_documents
) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="a", write_disposition="append")
    def a():
        yield from ({"v": i} for i in range(3))

    @dlt.resource(name="b", write_disposition="append")
    def b():
        yield from ({"v": i} for i in range(5))

    pipeline.run([a(), b()])
    coll_a = make_pipeline.qualified_name(pipeline, "a")
    coll_b = make_pipeline.qualified_name(pipeline, "b")
    assert count_documents(coll_a) == 3
    assert count_documents(coll_b) == 5

    with open_client(pipeline) as client:
        client.initialize_storage(truncate_tables=["a"])
    assert count_documents(coll_a) == 0  # emptied
    assert collection_exists(coll_a)  # but still exists
    assert count_documents(coll_b) == 5  # untouched


def test_drop_storage_removes_everything_and_allows_restart(
    make_pipeline, open_client, count_documents, collection_exists
) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield from ({"v": i} for i in range(4))

    pipeline.run(items())
    collection = make_pipeline.qualified_name(pipeline, "items")
    assert count_documents(collection) == 4

    with open_client(pipeline) as client:
        client.drop_storage()
        assert client.is_storage_initialized() is False
        for system_collection in _system_collections(client):
            assert not collection_exists(system_collection)
        assert not collection_exists(collection)

    info = pipeline.run(items())
    assert not info.has_failed_jobs
    assert count_documents(collection) == 4


def test_update_stored_schema_creates_tables_and_noops_on_unchanged_hash(
    make_pipeline, open_client, collection_exists, documents
) -> None:
    pipeline = make_pipeline()
    _establish_schema(pipeline, table="products")
    with open_client(pipeline) as client:
        client.initialize_storage()
        client.update_stored_schema()
        assert collection_exists(client.make_qualified_collection_name("products"))
        stored = client.get_stored_schema_by_hash(client.schema.stored_version_hash)
        assert stored is not None
        assert stored.version_hash == client.schema.stored_version_hash

        version_collection = client.make_qualified_collection_name(client.schema.version_table_name)
        before_docs = documents(version_collection)
        assert len(before_docs) == 1
        inserted_before = before_docs[0]["inserted_at"]
        client.update_stored_schema()  # unchanged hash -> genuinely no write
        after_docs = documents(version_collection)
        assert len(after_docs) == 1
        assert after_docs[0]["inserted_at"] == inserted_before


def test_dataset_qualification_and_isolation(make_pipeline, open_client, count_documents) -> None:
    pipeline_one = make_pipeline(dataset_name="catalog")

    @dlt.resource(name="products", write_disposition="append")
    def products():
        yield {"sku": "A1"}

    pipeline_one.run(products())
    assert make_pipeline.qualified_name(pipeline_one, "products") == "catalog_products"
    assert count_documents("catalog_products") == 1

    pipeline_two = make_pipeline(dataset_name="warehouse")
    pipeline_two.run(products())
    assert count_documents("warehouse_products") == 1

    with open_client(pipeline_two) as client:
        client.drop_storage()
    assert count_documents("catalog_products") == 1
    assert count_documents("warehouse_products") == 0


def test_drop_storage_empty_dataset_only_matches_schema_tables(
    make_pipeline, open_client, probe, collection_exists
) -> None:
    """Empty dataset_name deletes bare schema-table names only — not unrelated collections."""
    pipeline = make_pipeline(dataset_name="")
    sibling = f"unrelated_{pipeline.pipeline_name}"
    probe.collections.create(
        {"name": sibling, "fields": [{"name": ".*", "type": "auto"}]}
    )
    try:

        @dlt.resource(name="items", write_disposition="append")
        def items():
            yield {"v": 1}

        pipeline.run(items())
        assert collection_exists("items")
        with open_client(pipeline) as client:
            client.drop_storage()
        assert not collection_exists("items")
        assert collection_exists(sibling)
    finally:
        with suppress(Exception):
            probe.collections[sibling].delete()


def test_complete_load_records_load_id(make_pipeline, open_client, probe) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield {"v": 1}

    info = pipeline.run(items())
    load_id = info.loads_ids[0]
    with open_client(pipeline) as client:
        loads_collection = client.make_qualified_collection_name(client.schema.loads_table_name)
        doc = probe.collections[loads_collection].documents[load_id].retrieve()
        assert doc is not None
        n = client.schema.naming.normalize_identifier
        assert doc[n("load_id")] == load_id
        assert doc[n("status")] == 0  # completed
        assert doc[n("schema_name")] == client.schema.name
        assert doc[n("schema_version_hash")] == client.schema.version_hash
        # Retries of complete_load upsert by id — no duplicate, no error.
        client.complete_load(load_id)
        client.complete_load(load_id)
        found = probe.collections[loads_collection].documents.search(
            {"q": "*", "filter_by": f"{n('load_id')}:=`{load_id}`", "per_page": 0}
        )["found"]
        assert found == 1
