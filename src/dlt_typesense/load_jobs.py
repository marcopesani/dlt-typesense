"""Load jobs that push JSONL packages into Typesense collections.

Merge (upsert) updates root documents in place. Once every job of a merge
table chain has completed, a follow-up ``TypesenseRemoveOrphansJob`` deletes
orphaned nested-table (child) documents — rows that disappeared from a
re-loaded root row's nested lists (lancedb precedent).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any, cast

from dlt.common import logger
from dlt.common.destination.client import HasFollowupJobs, RunnableLoadJob
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.json import json as dlt_json
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_nested_tables,
    get_root_table,
    is_nested_table,
)
from dlt.common.storages import FileStorage, ParsedLoadJobFileName
from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from typesense.exceptions import ObjectNotFound

from dlt_typesense.exceptions import (
    ERROR_DETAIL_MAX_LEN,
    TYPESENSE_ERRORS,
    TypesenseImportError,
    TypesensePartialImportError,
    map_typesense_error,
)
from dlt_typesense.typesense_adapter import FIELD_HINT
from dlt_typesense.value_conversion import apply_conversions, converted_fields

if TYPE_CHECKING:
    from dlt_typesense.typesense_client import TypesenseClient

_MAX_ERROR_SAMPLES = 5

# Ids per orphan-cleanup filter. dlt ids are ~14 chars (~18 bytes quoted), so
# each `filter_by` stays under ~4 KB — well below URL length limits (export and
# delete-by-filter are GET/DELETE requests; there is no request-body variant).
_ORPHAN_BATCH_SIZE = 200

# Fixed uuid5 namespace — must never change or merge ids stop matching.
_ID_NAMESPACE = uuid.NAMESPACE_DNS

# Typesense document primary key; source columns named `id` are renamed in naming.py.
RESERVED_ID_FIELD = "id"

# `create` is rejected: whole-file retry would fail on already-existing documents.
_RETRY_SAFE_ACTIONS = ("upsert", "emplace")


def _user_columns(names: Sequence[str]) -> list[str]:
    """Drop dlt system columns (``_dlt_*``) from a hint-derived column list."""
    return [name for name in names if not name.startswith("_dlt")]


def _chunked(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    chunk: list[Any] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


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
        field_converters = converted_fields(self._load_table)
        ts = self._client.ts
        assert ts is not None, "Typesense client must be open while a job runs"

        # `batch_size` here is the server-side import↔search interleave knob (a
        # query param), not the SDK's own client-side list slicer. We stream
        # client-sized chunks ourselves so a 64 MB JSONL file is never fully
        # materialized (the SDK's import_ requires a list and raises on empty).
        params = {"action": config.import_action, "batch_size": config.server_batch_size}
        docs_api = ts.collections[self._collection_name].documents
        total_count = 0
        failed_count = 0
        first_errors: list[str] = []

        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            for chunk in _chunked(
                self._iter_documents(f, id_fields, json_fields, field_converters),
                config.client_batch_size,
            ):
                try:
                    results = docs_api.import_(chunk, cast("Any", params))
                except TYPESENSE_ERRORS as exc:
                    raise map_typesense_error(exc, "document import") from exc
                for line in results:
                    total_count += 1
                    if line.get("success", False):
                        continue
                    failed_count += 1
                    if len(first_errors) >= _MAX_ERROR_SAMPLES:
                        continue
                    error = line.get("error", "unknown error")
                    document = line.get("document")
                    sample = f"{error}"
                    if document is not None:
                        sample = f"{error} | document: {str(document)[:ERROR_DETAIL_MAX_LEN]}"
                    first_errors.append(sample[:ERROR_DETAIL_MAX_LEN])

        if failed_count:
            raise TypesensePartialImportError(
                f"{failed_count} of {total_count} documents failed to import "
                f"into collection '{self._collection_name}' (load {self._load_id}). "
                f"First errors: {first_errors}",
                failed_count=failed_count,
                total_count=total_count,
            )

    def _iter_documents(
        self,
        lines: Iterator[str],
        id_fields: Sequence[str] | None,
        json_fields: Sequence[str],
        field_converters: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        converters = field_converters or {}
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
            if converters:
                apply_conversions(data, converters, collection_name=self._collection_name)
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
        """``json`` columns to stringify — except those pinned to a Typesense
        type that expects the native JSON value (``float[]`` vectors must stay
        lists, ``string[]`` must stay string arrays, ``object`` stays a dict)."""
        columns = table.get("columns") or {}
        fields = []
        for name, column in columns.items():
            if column.get("data_type") != "json":
                continue
            override = (column.get(FIELD_HINT) or {}).get("type")
            if override is not None and override != "string":
                continue
            fields.append(name)
        return fields

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
    """Delete orphaned nested-table documents after a merge (upsert) chain load.

    Scheduled by ``TypesenseClient.create_table_chain_completed_followup_jobs``
    as a ``reference`` job whose payload is the list of the chain's completed
    JSONL job files. The cleanup contract:

    - Root ids are the ``_dlt_id`` values found in the root-table files. Under
      the upsert strategy dlt derives them from the primary key (``key_hash``),
      so a re-loaded source row always produces the same root id.
    - For every nested table, a document is an orphan when its ``_dlt_root_id``
      belongs to a root row of this load but its ``_dlt_id`` was not re-written
      by it — the element disappeared from the parent's nested list.
    - Root rows *not* part of this load are never touched, so incremental
      loads only clean up the parents they actually re-synced.

    Orphans are found by exporting the current child ids per batch of root ids
    and diffing against the loaded ids, then deleted with id-list filters. All
    requests are bounded by ``_ORPHAN_BATCH_SIZE``, and re-running the whole
    job after a partial failure converges (deletes are idempotent).
    """

    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self.references = ReferenceFollowupJobRequest.resolve_references(file_path)

    @property
    def _client(self) -> TypesenseClient:
        return self._job_client  # type: ignore[return-value]

    def run(self) -> None:
        files_by_table: dict[str, list[str]] = {}
        for file_path in self.references:
            table_name = ParsedLoadJobFileName.parse(file_path).table_name
            files_by_table.setdefault(table_name, []).append(file_path)

        root_table = get_root_table(self._schema.tables, self._load_table["name"] or "")
        root_name = root_table["name"] or ""
        root_ids = self._loaded_root_ids(root_name, files_by_table.get(root_name, []))
        if not root_ids:
            return

        for table in get_nested_tables(self._schema.tables, root_name):
            table_name = table["name"] or ""
            if table_name == root_name:
                continue
            self._remove_table_orphans(
                root_name, table_name, root_ids, files_by_table.get(table_name, [])
            )

    def _loaded_root_ids(self, root_name: str, file_paths: list[str]) -> set[str]:
        """Row keys (``_dlt_id``) of every root row written by this load."""
        prepared_root = self._client.prepare_load_table(root_name)
        dataset_name = self._client.dataset_name
        row_key_col = SqlMergeFollowupJob.get_row_key_col(
            [prepared_root], prepared_root, dataset_name, dataset_name
        )
        root_ids: set[str] = set()
        for file_path in file_paths:
            for row in _iter_jsonl_rows(file_path):
                value = row.get(row_key_col)
                if value is None:
                    raise TypesenseImportError(
                        f"Root table '{root_name}': row is missing '{row_key_col}', so orphaned "
                        "nested documents cannot be identified for this load."
                    )
                root_ids.add(str(value))
        return root_ids

    def _remove_table_orphans(
        self,
        root_name: str,
        table_name: str,
        root_ids: set[str],
        file_paths: list[str],
    ) -> None:
        client = self._client
        dataset_name = client.dataset_name
        prepared_root = client.prepare_load_table(root_name)
        prepared = client.prepare_load_table(table_name)
        chain = [prepared_root, prepared]
        # `_dlt_root_id` (root_key) requires dlt's root key propagation, which is
        # on by default for merge. If a pipeline disabled it, this raises a
        # terminal MergeDispositionException — opt out of cleanup instead with
        # `typesense_adapter(resource, no_remove_orphans=True)`.
        root_key_col = SqlMergeFollowupJob.get_root_key_col(
            chain, prepared, dataset_name, dataset_name
        )
        row_key_col = SqlMergeFollowupJob.get_row_key_col(
            chain, prepared, dataset_name, dataset_name
        )

        # ids written by this load, grouped by the root row they belong to;
        # tables whose lists were emptied have no files and therefore no ids.
        loaded_ids: dict[str, set[str]] = {}
        for file_path in file_paths:
            for row in _iter_jsonl_rows(file_path):
                root_id = row.get(root_key_col)
                row_id = row.get(row_key_col)
                if root_id is None or row_id is None:
                    raise TypesenseImportError(
                        f"Nested table '{table_name}': row is missing '{root_key_col}' or "
                        f"'{row_key_col}', so orphaned documents cannot be identified."
                    )
                loaded_ids.setdefault(str(root_id), set()).add(str(row_id))

        collection_name = client.make_qualified_collection_name(table_name)
        ts = client.ts
        assert ts is not None, "Typesense client must be open while a job runs"
        docs_api = ts.collections[collection_name].documents

        deleted = 0
        for root_batch in _chunked(sorted(root_ids), _ORPHAN_BATCH_SIZE):
            try:
                export = docs_api.export(
                    cast(
                        "Any",
                        {
                            "filter_by": _in_filter(root_key_col, root_batch),
                            "include_fields": row_key_col,
                        },
                    )
                )
            except ObjectNotFound:
                # Collection missing, or no document ever defined the filter
                # field — either way there is nothing to clean up.
                return
            except TYPESENSE_ERRORS as exc:
                raise map_typesense_error(exc, f"orphan export from '{collection_name}'") from exc

            existing: set[str] = set()
            for line in export.splitlines():
                line = line.strip()
                if not line:
                    continue
                value = json.loads(line).get(row_key_col)
                if value is not None:
                    existing.add(str(value))

            loaded_in_batch: set[str] = set()
            for root_id in root_batch:
                loaded_in_batch.update(loaded_ids.get(root_id, ()))

            for stale_batch in _chunked(sorted(existing - loaded_in_batch), _ORPHAN_BATCH_SIZE):
                try:
                    response = docs_api.delete(
                        cast("Any", {"filter_by": _in_filter(row_key_col, stale_batch)})
                    )
                except TYPESENSE_ERRORS as exc:
                    raise map_typesense_error(
                        exc, f"orphan delete from '{collection_name}'"
                    ) from exc
                deleted += int(response.get("num_deleted", 0))

        if deleted:
            logger.info(
                f"Removed {deleted} orphaned document(s) from collection "
                f"'{collection_name}' (load {self._load_id})."
            )


def _iter_jsonl_rows(file_path: str) -> Iterator[dict[str, Any]]:
    with FileStorage.open_zipsafe_ro(file_path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _in_filter(field: str, values: Sequence[str]) -> str:
    """Typesense exact-match IN filter with backtick-quoted literals.

    dlt ids are base64 and may contain ``+``/``/``, so every value is quoted.
    Backticks cannot be escaped inside ``field:=[`…`]`` literals; a value
    containing one is rejected rather than silently matching nothing.
    """
    for value in values:
        if "`" in value:
            raise TypesenseImportError(
                f"Cannot build a Typesense filter on '{field}': value {value!r} contains a "
                "backtick, which cannot be escaped in filter literals."
            )
    joined = ",".join(f"`{value}`" for value in values)
    return f"{field}:=[{joined}]"
