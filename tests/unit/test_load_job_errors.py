"""Load-job import chunking and error reporting (no server required)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest
from typesense.exceptions import ObjectNotFound

from dlt_typesense.exceptions import TypesenseImportError, TypesensePartialImportError
from dlt_typesense.load_jobs import TypesenseLoadJob


class FakeTs:
    """Stands in for typesense.Client: supports ts.collections[name].documents.import_()."""

    def __init__(self, respond=None) -> None:
        self.calls: list[tuple[str, list[dict[str, Any]], dict[str, Any]]] = []
        self._respond = respond or (lambda chunk: [{"success": True} for _ in chunk])
        self.collections = self  # ts.collections[name] resolves via __getitem__ below

    def __getitem__(self, name: str):
        def _import(documents, params):
            chunk = list(documents)
            self.calls.append((name, chunk, dict(params)))
            return self._respond(chunk)

        return SimpleNamespace(documents=SimpleNamespace(import_=_import))


class _FakeConfig:
    client_batch_size = 1000
    server_batch_size = 40
    import_action = "upsert"


class _FakeClient:
    def __init__(self, ts: FakeTs) -> None:
        self.config = _FakeConfig()
        self.ts = ts


def _write_jsonl(tmp_path, rows: list[dict[str, Any]], name: str = "rows.abc123.0.jsonl"):
    file_path = tmp_path / name
    file_path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return file_path


def _make_job(
    tmp_path,
    collection: str,
    ts: FakeTs,
    *,
    rows: list[dict[str, Any]] | None = None,
    load_id: str = "1",
) -> TypesenseLoadJob:
    if rows is None:
        rows = [{"_dlt_id": "r1", "v": 1}]
    file_path = _write_jsonl(tmp_path, rows)
    job = TypesenseLoadJob(str(file_path), collection)
    job._job_client = _FakeClient(ts)  # type: ignore[assignment]
    job._load_table = {"name": "rows", "write_disposition": "append", "columns": {}}
    job._load_id = load_id
    return job


def test_create_action_is_rejected(tmp_path) -> None:
    ts = FakeTs()
    job = _make_job(tmp_path, "c", ts)
    job._job_client.config.import_action = "create"  # type: ignore[attr-defined]
    with pytest.raises(TypesenseImportError):
        job.run()
    assert ts.calls == []


def test_partial_import_error_is_diagnosable(tmp_path) -> None:
    def respond(chunk):
        return [
            {"success": False, "error": "Field `v` type mismatch", "document": '{"v":1}'}
            for _ in chunk
        ]

    job = _make_job(tmp_path, "catalog_rows", FakeTs(respond), load_id="1700000000.42")

    with pytest.raises(TypesensePartialImportError) as excinfo:
        job.run()
    error = excinfo.value
    assert error.failed_count == 1
    assert error.total_count == 1
    message = str(error)
    assert "catalog_rows" in message
    assert "1700000000.42" in message
    assert "type mismatch" in message


def test_config_batch_settings_reach_the_import(tmp_path) -> None:
    ts = FakeTs()
    job = _make_job(tmp_path, "c", ts)
    job._job_client.config.server_batch_size = 17  # type: ignore[attr-defined]
    job._job_client.config.client_batch_size = 500  # type: ignore[attr-defined]
    job.run()
    collection, chunk, params = ts.calls[0]
    assert collection == "c"
    assert len(chunk) == 1
    assert params == {"action": "upsert", "batch_size": 17}


def test_import_chunks_and_forwards_server_batch_size(tmp_path) -> None:
    ts = FakeTs()
    rows = [{"_dlt_id": str(i), "v": i} for i in range(5)]
    job = _make_job(tmp_path, "c", ts, rows=rows)
    job._job_client.config.client_batch_size = 2  # type: ignore[attr-defined]
    job._job_client.config.server_batch_size = 40  # type: ignore[attr-defined]
    job.run()
    assert [len(chunk) for _, chunk, _ in ts.calls] == [2, 2, 1]
    assert all(params == {"action": "upsert", "batch_size": 40} for _, _, params in ts.calls)


def test_import_streams_lazily_not_buffered_whole(tmp_path) -> None:
    # Write enough rows that a naive full-buffer would pull everything before the
    # first SDK call; assert the first import_ sees only one client batch.
    rows = [{"_dlt_id": str(i), "v": i} for i in range(25)]
    file_path = _write_jsonl(tmp_path, rows)
    pulled_at_first_call: dict[str, int] = {}

    class CountingFakeTs(FakeTs):
        def __getitem__(self, name: str):
            def _import(documents, params):
                chunk = list(documents)
                if "n" not in pulled_at_first_call:
                    # Lines already consumed from the file equal the first chunk size
                    # when streaming; prove we did not buffer all 25 first.
                    pulled_at_first_call["n"] = len(chunk)
                self.calls.append((name, chunk, dict(params)))
                return [{"success": True} for _ in chunk]

            return SimpleNamespace(documents=SimpleNamespace(import_=_import))

    ts = CountingFakeTs()
    job = TypesenseLoadJob(str(file_path), "c")
    job._job_client = _FakeClient(ts)  # type: ignore[assignment]
    job._job_client.config.client_batch_size = 10  # type: ignore[attr-defined]
    job._load_table = {"name": "rows", "write_disposition": "append", "columns": {}}
    job._load_id = "1"
    job.run()
    assert sum(len(chunk) for _, chunk, _ in ts.calls) == 25
    assert pulled_at_first_call["n"] == 10


def test_import_collects_per_line_failures(tmp_path) -> None:
    def respond(chunk):
        return [
            {"success": True},
            {"success": False, "error": "bad type", "document": '{"x":1}'},
            {"success": True},
        ]

    rows = [{"_dlt_id": str(i), "v": i} for i in range(3)]
    job = _make_job(tmp_path, "c", FakeTs(respond), rows=rows)
    with pytest.raises(TypesensePartialImportError) as excinfo:
        job.run()
    assert excinfo.value.total_count == 3
    assert excinfo.value.failed_count == 1
    assert "bad type" in str(excinfo.value)


def test_import_empty_file_makes_no_requests(tmp_path) -> None:
    ts = FakeTs()
    job = _make_job(tmp_path, "c", ts, rows=[])
    job.run()
    assert ts.calls == []


def test_import_maps_sdk_errors(tmp_path) -> None:
    def respond(chunk):
        raise ObjectNotFound(404, "collection missing")

    job = _make_job(tmp_path, "c", FakeTs(respond))
    with pytest.raises(TypesenseImportError):
        job.run()
