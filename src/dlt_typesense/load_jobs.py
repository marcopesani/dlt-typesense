"""Load jobs that push JSONL packages into Typesense collections."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import typesense
from dlt.common.destination.client import HasFollowupJobs, RunnableLoadJob
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.json import json as dlt_json
from dlt.common.schema.utils import get_columns_names_with_prop, is_nested_table
from dlt.common.storages import FileStorage

from dlt_typesense.exceptions import (
    ERROR_DETAIL_MAX_LEN,
    TYPESENSE_ERRORS,
    TypesenseImportError,
    TypesensePartialImportError,
    map_typesense_error,
)

if TYPE_CHECKING:
    from dlt_typesense.typesense_client import TypesenseClient

_MAX_ERROR_SAMPLES = 5

# Fixed uuid5 namespace — must never change or merge ids stop matching.
_ID_NAMESPACE = uuid.NAMESPACE_DNS

# Typesense document primary key; source columns named `id` are renamed in naming.py.
RESERVED_ID_FIELD = "id"

# `create` is rejected: whole-file retry would fail on already-existing documents.
_RETRY_SAFE_ACTIONS = ("upsert", "emplace")


def _user_columns(names: Sequence[str]) -> list[str]:
    """Drop dlt system columns (``_dlt_*``) from a hint-derived column list."""
    return [name for name in names if not name.startswith("_dlt")]


@dataclass
class ImportSummary:
    """Outcome of a chunked bulk import (counts + bounded failure samples)."""

    total_count: int = 0
    failed_count: int = 0
    first_errors: list[str] = field(default_factory=list)

    def add_failure(self, line: dict[str, Any]) -> None:
        self.failed_count += 1
        if len(self.first_errors) < _MAX_ERROR_SAMPLES:
            error = line.get("error", "unknown error")
            document = line.get("document")
            sample = f"{error}"
            if document is not None:
                sample = f"{error} | document: {str(document)[:ERROR_DETAIL_MAX_LEN]}"
            self.first_errors.append(sample[:ERROR_DETAIL_MAX_LEN])


def _chunked(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    chunk: list[Any] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def import_documents(
    ts: typesense.Client,
    collection_name: str,
    documents: Iterable[dict[str, Any]],
    *,
    action: str = "upsert",
    client_batch_size: int = 1000,
    server_batch_size: int = 40,
) -> ImportSummary:
    """Import ``documents`` in client-sized chunks; per-line failures accumulate.

    Typesense returns HTTP 200 even when individual documents fail, so the
    per-line results parsed by the SDK feed the summary instead of raising.
    """
    summary = ImportSummary()
    # `batch_size` here is the server-side import<->search interleave knob (a
    # query param), not the SDK's own client-side list slicer.
    params = {"action": action, "batch_size": server_batch_size}
    docs_api = ts.collections[collection_name].documents
    for chunk in _chunked(documents, client_batch_size):
        try:
            results = docs_api.import_(chunk, cast("Any", params))
        except TYPESENSE_ERRORS as exc:
            raise map_typesense_error(exc, "document import") from exc
        for line in results:
            summary.total_count += 1
            if not line.get("success", False):
                summary.add_failure(cast("dict[str, Any]", line))
    return summary


def merge_document_id(collection_name: str, key_values: Sequence[Any]) -> str:
    """Deterministic Typesense document id for a merge/upsert row.

    Key components are JSON-encoded so compound keys cannot collide ambiguously
    (e.g. ``["a_b", "c"]`` vs ``["a", "b_c"]``).
    """
    key = json.dumps([str(value) for value in key_values])
    return str(uuid.uuid5(_ID_NAMESPACE, f"{collection_name}:{key}"))


class TypesenseLoadJob(RunnableLoadJob, HasFollowupJobs):
    """Load one JSONL file into a Typesense collection via streamed bulk import.

    - append / replace / merge(insert-only): ``id`` from ``_dlt_id``, ``action=upsert``
    - merge (upsert): ``id`` = uuid5 of the primary/unique key, ``action=upsert``
    """

    def __init__(self, file_path: str, collection_name: str) -> None:
        super().__init__(file_path)
        self._collection_name = collection_name

    @property
    def _client(self) -> TypesenseClient:
        return self._job_client  # type: ignore[return-value]

    def run(self) -> None:
        config = self._client.config
        if config.import_action not in _RETRY_SAFE_ACTIONS:
            raise TypesenseImportError(
                f"import_action='{config.import_action}' is not supported; use one of "
                f"{list(_RETRY_SAFE_ACTIONS)}. 'create' breaks dlt's whole-file retry idempotency."
            )
        id_fields = self._id_fields(self._load_table)
        json_fields = self._json_fields(self._load_table)
        ts = self._client.ts
        assert ts is not None, "Typesense client must be open while a job runs"

        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            summary = import_documents(
                ts,
                self._collection_name,
                self._iter_documents(f, id_fields, json_fields),
                action=config.import_action,
                client_batch_size=config.client_batch_size,
                server_batch_size=config.server_batch_size,
            )

        if summary.failed_count:
            raise TypesensePartialImportError(
                f"{summary.failed_count} of {summary.total_count} documents failed to import "
                f"into collection '{self._collection_name}' (load {self._load_id}). "
                f"First errors: {summary.first_errors}",
                failed_count=summary.failed_count,
                total_count=summary.total_count,
            )

    def _iter_documents(
        self,
        lines: Iterator[str],
        id_fields: Sequence[str] | None,
        json_fields: Sequence[str],
    ) -> Iterator[dict[str, Any]]:
        for line in lines:
            line = line.strip()
            if not line:
                continue
            raw: dict[str, Any] = json.loads(line)
            # Typesense auto-schema rejects nulls; omit the field instead.
            data = {key: value for key, value in raw.items() if value is not None}
            data[RESERVED_ID_FIELD] = self._document_id(raw, id_fields)
            for field_name in json_fields:
                if field_name in data:
                    data[field_name] = dlt_json.dumps(data[field_name])
            yield data

    def _document_id(self, data: dict[str, Any], id_fields: Sequence[str] | None) -> str:
        if id_fields:
            key_values: list[Any] = []
            for field in id_fields:
                value = data.get(field)
                if value is None:
                    raise TypesenseImportError(
                        f"Collection '{self._collection_name}': merge key column '{field}' is "
                        "missing or null in a row, so a deterministic document id cannot be built."
                    )
                key_values.append(value)
            return merge_document_id(self._collection_name, key_values)
        dlt_id = data.get("_dlt_id")
        if dlt_id is not None:
            return str(dlt_id)
        return str(uuid.uuid4())

    @staticmethod
    def _json_fields(table: Any) -> list[str]:
        columns = table.get("columns") or {}
        return [name for name, column in columns.items() if column.get("data_type") == "json"]

    def _id_fields(self, table: Any) -> Sequence[str] | None:
        """Columns that key the Typesense ``id``, or ``None`` to use ``_dlt_id``."""
        if table.get("write_disposition") != "merge":
            return None

        merge_strategy = resolve_merge_strategy({table["name"]: table}, table)
        if merge_strategy == "insert-only":
            return None

        if is_nested_table(table):
            return None

        primary_keys = _user_columns(get_columns_names_with_prop(table, "primary_key"))
        if primary_keys:
            return primary_keys
        # dlt marks `_dlt_id` unique on every table — ignore system columns or
        # every re-run would key on a fresh id and duplicate rows.
        unique_keys = _user_columns(get_columns_names_with_prop(table, "unique"))
        if unique_keys:
            return unique_keys

        raise TypesenseImportError(
            f"Collection '{self._collection_name}': merge (upsert) requires a primary_key or a "
            "column hinted 'unique' to build a deterministic document id, but the table "
            f"'{table.get('name')}' declares neither. Add a primary_key, mark a column unique, or "
            "use the insert-only merge strategy."
        )


class TypesenseRemoveOrphansJob(RunnableLoadJob):
    """Delete child documents orphaned after a root merge/upsert.

    Not implemented: merge updates root documents only; stale child rows remain.
    """

    def __init__(self, file_path: str, collection_name: str) -> None:
        super().__init__(file_path)
        self._collection_name = collection_name

    def run(self) -> None:
        raise NotImplementedError(
            "Child-table orphan cleanup is not implemented; "
            "merge leaves stale nested-list documents in place."
        )
