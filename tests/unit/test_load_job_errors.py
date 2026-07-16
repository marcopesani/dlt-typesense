"""Load-job error reporting (no server required)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from dlt_typesense.exceptions import TypesenseImportError, TypesensePartialImportError
from dlt_typesense.load_jobs import TypesenseLoadJob


class _FakeConfig:
    client_batch_size = 1000
    server_batch_size = 40
    import_action = "upsert"


class _FakeTs:
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


class _FakeClient:
    def __init__(self, ts: _FakeTs) -> None:
        self.config = _FakeConfig()
        self.ts = ts


def _make_job(tmp_path, collection: str, ts: _FakeTs, load_id: str = "1") -> TypesenseLoadJob:
    file_path = tmp_path / "rows.abc123.0.jsonl"
    file_path.write_text(json.dumps({"_dlt_id": "r1", "v": 1}) + "\n")
    job = TypesenseLoadJob(str(file_path), collection)
    job._job_client = _FakeClient(ts)  # type: ignore[assignment]
    job._load_table = {"name": "rows", "write_disposition": "append", "columns": {}}
    job._load_id = load_id
    return job


def test_create_action_is_rejected(tmp_path) -> None:
    ts = _FakeTs()
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

    job = _make_job(tmp_path, "catalog_rows", _FakeTs(respond), load_id="1700000000.42")

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
    ts = _FakeTs()
    job = _make_job(tmp_path, "c", ts)
    job._job_client.config.server_batch_size = 17  # type: ignore[attr-defined]
    job._job_client.config.client_batch_size = 500  # type: ignore[attr-defined]
    job.run()
    collection, chunk, params = ts.calls[0]
    assert collection == "c"
    assert len(chunk) == 1
    assert params == {"action": "upsert", "batch_size": 17}
