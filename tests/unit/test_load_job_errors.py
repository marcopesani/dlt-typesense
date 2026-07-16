"""Load-job error reporting (no server required)."""

from __future__ import annotations

import json

import pytest

from dlt_typesense.exceptions import TypesensePartialImportError
from dlt_typesense.load_jobs import TypesenseLoadJob
from dlt_typesense.rest_client import ImportSummary


class _FakeConfig:
    client_batch_size = 1000
    server_batch_size = 40
    import_action = "upsert"


def test_create_action_is_rejected(tmp_path) -> None:
    """Covers: AC-TS-07 — `create` breaks retry idempotency and is rejected."""
    from dlt_typesense.exceptions import TypesenseImportError

    file_path = tmp_path / "rows.abc.0.jsonl"
    file_path.write_text(json.dumps({"_dlt_id": "r1"}) + "\n")
    job = TypesenseLoadJob(str(file_path), "c")
    config = _FakeConfig()
    config.import_action = "create"  # type: ignore[misc]
    job._job_client = _FakeClient(_FakeRest(ImportSummary()))  # type: ignore[assignment]
    job._job_client.config = config  # type: ignore[attr-defined]
    job._load_table = {"name": "rows", "write_disposition": "append", "columns": {}}
    job._load_id = "1"
    with pytest.raises(TypesenseImportError):
        job.run()


class _FakeRest:
    def __init__(self, summary: ImportSummary) -> None:
        self._summary = summary
        self.calls: list[str] = []
        self.last_kwargs: dict = {}

    def import_documents(self, collection_name, documents, **kwargs) -> ImportSummary:
        # drain the generator so streaming is exercised
        list(documents)
        self.calls.append(collection_name)
        self.last_kwargs = kwargs
        return self._summary


class _FakeClient:
    def __init__(self, rest: _FakeRest) -> None:
        self.config = _FakeConfig()
        self.rest = rest


def test_partial_import_error_is_diagnosable(tmp_path) -> None:
    """Covers: AC-TS-03, AC-NF-02 — error names the collection, load id, first errors."""
    file_path = tmp_path / "rows.abc123.0.jsonl"
    file_path.write_text(json.dumps({"_dlt_id": "r1", "v": 1}) + "\n")

    summary = ImportSummary(total_count=1)
    summary.add_failure({"error": "Field `v` type mismatch", "document": '{"v":1}'})

    job = TypesenseLoadJob(str(file_path), "catalog_rows")
    job._job_client = _FakeClient(_FakeRest(summary))  # type: ignore[assignment]
    job._load_table = {"name": "rows", "write_disposition": "append", "columns": {}}
    job._load_id = "1700000000.42"

    with pytest.raises(TypesensePartialImportError) as excinfo:
        job.run()
    error = excinfo.value
    assert error.failed_count == 1
    assert error.total_count == 1
    message = str(error)
    assert "catalog_rows" in message
    assert "1700000000.42" in message
    assert "type mismatch" in message


def test_config_server_batch_size_reaches_rest(tmp_path) -> None:
    """Covers: AC-TS-09 — the configured server_batch_size flows job -> rest import."""
    file_path = tmp_path / "rows.abc.0.jsonl"
    file_path.write_text(json.dumps({"_dlt_id": "r1", "v": 1}) + "\n")
    config = _FakeConfig()
    config.server_batch_size = 17  # type: ignore[misc]
    config.client_batch_size = 500  # type: ignore[misc]
    rest = _FakeRest(ImportSummary())
    job = TypesenseLoadJob(str(file_path), "c")
    client = _FakeClient(rest)
    client.config = config  # type: ignore[attr-defined]
    job._job_client = client  # type: ignore[assignment]
    job._load_table = {"name": "rows", "write_disposition": "append", "columns": {}}
    job._load_id = "1"
    job.run()
    assert rest.last_kwargs.get("server_batch_size") == 17
    assert rest.last_kwargs.get("client_batch_size") == 500
    assert rest.last_kwargs.get("action") == "upsert"
