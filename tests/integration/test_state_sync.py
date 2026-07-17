"""State & schema sync via WithStateSync."""

from __future__ import annotations

import dlt
import pytest

pytestmark = pytest.mark.integration


def test_stored_schema_round_trips_by_name_and_hash(make_pipeline, open_client) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield {"v": 1}

    pipeline.run(items())
    version_hash = pipeline.default_schema.stored_version_hash
    with open_client(pipeline) as client:
        by_name = client.get_stored_schema(pipeline.default_schema.name)
        by_hash = client.get_stored_schema_by_hash(version_hash)
        assert by_name is not None and by_hash is not None
        assert by_name.version_hash == by_hash.version_hash == version_hash
        assert by_name.schema == by_hash.schema  # identical stored content


def test_state_visible_only_after_complete_load(make_pipeline, open_client, probe) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield {"v": 1}

    pipeline.run(items())
    with open_client(pipeline) as client:
        committed = client.get_stored_state(pipeline.pipeline_name)
        assert committed is not None

        state_collection = client.make_qualified_collection_name(client.schema.state_table_name)
        naming = client.schema.naming
        forged = {
            "id": "forged-state",
            naming.normalize_identifier("version"): committed.version + 1,
            naming.normalize_identifier("engine_version"): committed.engine_version,
            naming.normalize_identifier("pipeline_name"): pipeline.pipeline_name,
            naming.normalize_identifier("state"): "not-committed",
            naming.normalize_identifier("created_at"): "2099-01-01T00:00:00+00:00",
            naming.normalize_identifier("_dlt_load_id"): "load-that-never-completed",
        }
        probe.collections[state_collection].documents.upsert(forged)

        still = client.get_stored_state(pipeline.pipeline_name)
        assert still is not None
        assert still.state != "not-committed"
        assert still.version == committed.version
        assert still.state == committed.state


def test_get_stored_state_returns_newest_committed(make_pipeline, open_client) -> None:
    data: list[dict] = [{"seq": 1}]

    def source():
        @dlt.resource(name="events", write_disposition="append")
        def events(cursor=dlt.sources.incremental("seq", initial_value=0)):  # noqa: B008
            yield from data

        return events

    pipeline = make_pipeline()
    pipeline.run(source())
    with open_client(pipeline) as client:
        first = client.get_stored_state(pipeline.pipeline_name)
        assert first is not None
        version_after_run1 = first.version

    data.append({"seq": 2})  # cursor advances -> a new committed state version
    pipeline.run(source())
    with open_client(pipeline) as client:
        newest = client.get_stored_state(pipeline.pipeline_name)
        assert newest is not None
        assert newest.version > version_after_run1  # the newer committed state wins


def test_incremental_second_run_loads_only_delta(make_pipeline, count_documents) -> None:
    data: list[dict] = [{"seq": i, "payload": f"row-{i}"} for i in range(1, 4)]

    def source():
        @dlt.resource(name="events", write_disposition="append")
        def events(cursor=dlt.sources.incremental("seq", initial_value=0)):  # noqa: B008
            yield from data

        return events

    pipeline = make_pipeline()
    pipeline.run(source())
    collection = make_pipeline.qualified_name(pipeline, "events")
    assert count_documents(collection) == 3

    data.extend([{"seq": 4, "payload": "row-4"}, {"seq": 5, "payload": "row-5"}])
    pipeline.run(source())
    assert count_documents(collection) == 5  # 8 would mean the delta was ignored


def test_second_machine_restore(make_pipeline, count_documents, tmp_path) -> None:
    data: list[dict] = [{"seq": i} for i in range(1, 4)]

    def source():
        @dlt.resource(name="events", write_disposition="append")
        def events(cursor=dlt.sources.incremental("seq", initial_value=0)):  # noqa: B008
            yield from data

        return events

    dataset = f"restore_{tmp_path.name}"
    pipeline_one = make_pipeline(
        pipeline_name="restore_pipe", dataset_name=dataset, pipelines_dir=str(tmp_path / "m1")
    )
    pipeline_one.run(source())
    collection = make_pipeline.qualified_name(pipeline_one, "events")
    assert count_documents(collection) == 3

    pipeline_two = make_pipeline(
        pipeline_name="restore_pipe", dataset_name=dataset, pipelines_dir=str(tmp_path / "m2")
    )
    pipeline_two.sync_destination()

    data.extend([{"seq": 4}, {"seq": 5}])
    pipeline_two.run(source())
    assert count_documents(collection) == 5


def test_get_stored_state_pages_past_incomplete(make_pipeline, open_client, probe) -> None:
    """>50 incomplete newer states must not hide the committed state on page 2."""
    pipeline = make_pipeline()

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield {"v": 1}

    pipeline.run(items())
    with open_client(pipeline) as client:
        committed = client.get_stored_state(pipeline.pipeline_name)
        assert committed is not None
        state_collection = client.make_qualified_collection_name(client.schema.state_table_name)
        naming = client.schema.naming
        p_version = naming.normalize_identifier("version")
        p_pipeline = naming.normalize_identifier("pipeline_name")
        p_state = naming.normalize_identifier("state")
        p_created = naming.normalize_identifier("created_at")
        p_load = naming.normalize_identifier("_dlt_load_id")
        p_engine = naming.normalize_identifier("engine_version")
        # page_size is 50; forge 50 incomplete states with higher versions.
        for i in range(50):
            probe.collections[state_collection].documents.upsert(
                {
                    "id": f"forged-{i}",
                    p_version: committed.version + 1 + i,
                    p_engine: committed.engine_version,
                    p_pipeline: pipeline.pipeline_name,
                    p_state: "incomplete",
                    p_created: "2099-01-01T00:00:00+00:00",
                    p_load: f"load-never-completed-{i}",
                }
            )
        still = client.get_stored_state(pipeline.pipeline_name)
        assert still is not None
        assert still.version == committed.version
        assert still.state == committed.state


def test_pipeline_state_isolated_by_pipeline_name(make_pipeline, open_client) -> None:
    dataset = "shared_state_ds"
    pipe_a = make_pipeline(pipeline_name="pipe_a", dataset_name=dataset)
    pipe_b = make_pipeline(pipeline_name="pipe_b", dataset_name=dataset)

    @dlt.resource(name="items", write_disposition="append")
    def items_a():
        yield {"v": 1}

    @dlt.resource(name="items", write_disposition="append")
    def items_b():
        yield {"v": 2}

    pipe_a.run(items_a())
    pipe_b.run(items_b())
    with open_client(pipe_a) as client:
        state_a = client.get_stored_state("pipe_a")
        state_b = client.get_stored_state("pipe_b")
        assert state_a is not None and state_b is not None
        assert state_a.pipeline_name == "pipe_a"
        assert state_b.pipeline_name == "pipe_b"


def test_backtick_in_pipeline_name_rejected(make_pipeline, open_client) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="items", write_disposition="append")
    def items():
        yield {"v": 1}

    pipeline.run(items())
    with open_client(pipeline) as client, pytest.raises(ValueError, match="backtick"):
        client.get_stored_state("bad`pipe")


def test_schema_evolution_bumps_version(make_pipeline, open_client) -> None:
    pipeline = make_pipeline()

    @dlt.resource(name="items", write_disposition="append")
    def items_before():
        yield {"a": 1}

    pipeline.run(items_before())
    hash_before = pipeline.default_schema.stored_version_hash
    version_before = pipeline.default_schema.version

    @dlt.resource(name="items", write_disposition="append")
    def items_v2():
        yield {"a": 2, "b": "added-column"}

    pipeline.run(items_v2())
    hash_after = pipeline.default_schema.stored_version_hash
    version_after = pipeline.default_schema.version

    assert hash_before != hash_after
    assert version_after > version_before
    with open_client(pipeline) as client:
        stored_before = client.get_stored_schema_by_hash(hash_before)
        stored_after = client.get_stored_schema_by_hash(hash_after)
        assert stored_before is not None and stored_after is not None
        assert stored_after.version > stored_before.version
        assert stored_before.version == version_before and stored_after.version == version_after
        newest = client.get_stored_schema(pipeline.default_schema.name)
        assert newest is not None
        assert newest.version_hash == hash_after
        assert newest.version == version_after
