"""Orphan-cleanup scheduling gate, reference-job routing, and filter building."""

from __future__ import annotations

from typing import Any, cast

import pytest
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema import Schema
from dlt.common.storages import LoadJobInfo, ParsedLoadJobFileName
from dlt.destinations.job_impl import ReferenceFollowupJobRequest

from dlt_typesense import typesense
from dlt_typesense.configuration import TypesenseClientConfiguration
from dlt_typesense.exceptions import TypesenseImportError
from dlt_typesense.load_jobs import TypesenseRemoveOrphansJob, _in_filter
from dlt_typesense.typesense_adapter import NO_REMOVE_ORPHANS_HINT
from dlt_typesense.typesense_client import TypesenseClient


def make_client() -> TypesenseClient:
    config = TypesenseClientConfiguration()
    config.dataset_name = "ds"  # type: ignore[attr-defined]
    caps: DestinationCapabilitiesContext = typesense()._raw_capabilities()
    return TypesenseClient(Schema("test"), config, caps)


def root_table(**overrides: Any) -> Any:
    table: dict[str, Any] = {"name": "orders", "write_disposition": "merge", "columns": {}}
    table.update(overrides)
    return cast("Any", table)


def child_table() -> Any:
    return cast(
        "Any",
        {
            "name": "orders__items",
            "parent": "orders",
            "write_disposition": "merge",
            "columns": {},
        },
    )


def job_info(file_name: str) -> LoadJobInfo:
    return LoadJobInfo(
        "completed_jobs",
        f"/tmp/{file_name}",
        0,
        None,  # type: ignore[arg-type]
        0.0,
        ParsedLoadJobFileName.parse(file_name),
        None,  # type: ignore[arg-type]
    )


def test_merge_upsert_chain_schedules_reference_job() -> None:
    client = make_client()
    chain = [root_table(), child_table()]
    infos = [job_info("orders.aaa.0.jsonl"), job_info("orders__items.bbb.0.jsonl")]
    jobs = client.create_table_chain_completed_followup_jobs(chain, infos)
    assert len(jobs) == 1
    request = jobs[0]
    assert isinstance(request, ReferenceFollowupJobRequest)
    assert request.new_file_path().endswith(".reference")
    references = ReferenceFollowupJobRequest.resolve_references(request.new_file_path())
    assert references == ["/tmp/orders.aaa.0.jsonl", "/tmp/orders__items.bbb.0.jsonl"]


def test_single_table_chain_schedules_nothing() -> None:
    client = make_client()
    jobs = client.create_table_chain_completed_followup_jobs(
        [root_table()], [job_info("orders.aaa.0.jsonl")]
    )
    assert jobs == []


def test_append_chain_schedules_nothing() -> None:
    client = make_client()
    chain = [root_table(write_disposition="append"), child_table()]
    infos = [job_info("orders.aaa.0.jsonl"), job_info("orders__items.bbb.0.jsonl")]
    assert client.create_table_chain_completed_followup_jobs(chain, infos) == []


def test_insert_only_chain_schedules_nothing() -> None:
    client = make_client()
    chain = [root_table(**{"x-merge-strategy": "insert-only"}), child_table()]
    infos = [job_info("orders.aaa.0.jsonl"), job_info("orders__items.bbb.0.jsonl")]
    assert client.create_table_chain_completed_followup_jobs(chain, infos) == []


def test_opt_out_hint_schedules_nothing() -> None:
    client = make_client()
    chain = [root_table(**{NO_REMOVE_ORPHANS_HINT: True}), child_table()]
    infos = [job_info("orders.aaa.0.jsonl"), job_info("orders__items.bbb.0.jsonl")]
    assert client.create_table_chain_completed_followup_jobs(chain, infos) == []


def test_chain_without_root_files_schedules_nothing() -> None:
    client = make_client()
    chain = [root_table(), child_table()]
    infos = [job_info("orders__items.bbb.0.jsonl")]
    assert client.create_table_chain_completed_followup_jobs(chain, infos) == []


def test_reference_job_routed_to_orphan_removal(tmp_path: Any) -> None:
    reference = tmp_path / "orders.aaa.0.reference"
    reference.write_text("/tmp/orders.aaa.0.jsonl")
    client = make_client()
    job = client.create_load_job(root_table(), str(reference), "load1")
    assert isinstance(job, TypesenseRemoveOrphansJob)
    assert job.references == ["/tmp/orders.aaa.0.jsonl"]


def test_in_filter_quotes_values() -> None:
    # dlt ids are base64 and may contain + and /; both must be quoted literally.
    assert _in_filter("_dlt_id", ["a+b", "c/d"]) == "_dlt_id:=[`a+b`,`c/d`]"


def test_in_filter_rejects_backticks() -> None:
    with pytest.raises(TypesenseImportError, match="backtick"):
        _in_filter("_dlt_id", ["a`b"])
