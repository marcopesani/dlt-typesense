"""Load jobs that push JSONL packages into Typesense collections."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from dlt.common.destination.client import HasFollowupJobs, RunnableLoadJob
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.json import json as dlt_json
from dlt.common.schema.utils import get_columns_names_with_prop, is_nested_table
from dlt.common.storages import FileStorage

from dlt_typesense.exceptions import (
    TypesenseImportError,
    TypesensePartialImportError,
)

if TYPE_CHECKING:
    from dlt_typesense.typesense_client import TypesenseClient

# Namespace for deterministic merge document ids (uuid5). Any fixed namespace works;
# the value must never change or existing merge ids would stop matching.
_ID_NAMESPACE = uuid.NAMESPACE_DNS

# Typesense reserves the top-level string field `id` as a document's primary key,
# which this destination sets itself. A source column named `id` is renamed away
# from `id` by the naming convention (see dlt_typesense.naming), so by the time a
# row reaches this job it no longer collides (AC-TS-01).
RESERVED_ID_FIELD = "id"

# Import actions that keep dlt's whole-file retry idempotent. `create` is rejected
# because a retried file would fail on already-existing documents (AC-TS-07).
_RETRY_SAFE_ACTIONS = ("upsert", "emplace")


def _user_columns(names: Sequence[str]) -> list[str]:
    """Drop dlt system columns (``_dlt_*``) from a hint-derived column list."""
    return [name for name in names if not name.startswith("_dlt")]


def merge_document_id(collection_name: str, key_values: Sequence[Any]) -> str:
    """Deterministic Typesense document id for a merge/upsert row.

    Stable across independent runs for the same collection and ordered key
    tuple, so the same source row always maps to the same document (AC-MERGE-02,
    AC-MERGE-04). The key is JSON-encoded so compound components can never run
    together ambiguously: ``["a_b", "c"]`` and ``["a", "b_c"]`` stay distinct.
    """
    key = json.dumps([str(value) for value in key_values])
    return str(uuid.uuid5(_ID_NAMESPACE, f"{collection_name}:{key}"))


class TypesenseLoadJob(RunnableLoadJob, HasFollowupJobs):
    """Load one JSONL file into a Typesense collection via streamed bulk import.

    Disposition → document ``id`` → import ``action``:

    - append / replace: ``id`` from ``_dlt_id``, ``action=upsert``
    - merge (upsert): ``id`` = uuid5 of the primary/unique key, ``action=upsert``
    - merge (insert-only): ``id`` from ``_dlt_id`` (shares the append path)

    ``action=upsert`` (never ``create``) keeps dlt's whole-file retry idempotent:
    re-importing the same file with the same ids is a no-op for already-loaded rows.
    """

    def __init__(self, file_path: str, collection_name: str) -> None:
        super().__init__(file_path)
        self._collection_name = collection_name

    @property
    def _client(self) -> TypesenseClient:
        # Set by the loader (RunnableLoadJob.run_managed) before run() is called.
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
        rest = self._client.rest
        assert rest is not None, "REST client must be open while a job runs"

        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            summary = rest.import_documents(
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
        """Parse JSONL lines, assign the Typesense ``id``, stream documents out.

        Documents are yielded one at a time so the REST client can chunk them
        without ever holding the whole file in memory.
        """
        for line in lines:
            line = line.strip()
            if not line:
                continue
            raw: dict[str, Any] = json.loads(line)
            # Drop nulls: Typesense auto-schema rejects null values, so an absent
            # field is how "optional / no value" is represented (AC-SHAPE-04).
            data = {key: value for key, value in raw.items() if value is not None}
            data[RESERVED_ID_FIELD] = self._document_id(raw, id_fields)
            # `json`-typed columns are stored as one canonical JSON string so their
            # representation is deterministic regardless of row order (AC-TYPE-07).
            for field in json_fields:
                if field in data:
                    data[field] = dlt_json.dumps(data[field])
            yield data

    def _document_id(self, data: dict[str, Any], id_fields: Sequence[str] | None) -> str:
        """Return the deterministic Typesense document id for a row."""
        if id_fields:
            key_values: list[Any] = []
            for field in id_fields:
                value = data.get(field)
                if value is None:
                    # A null/missing key component would collapse distinct rows into
                    # one document (or KeyError into a retry loop) — fail loudly.
                    raise TypesenseImportError(
                        f"Collection '{self._collection_name}': merge key column '{field}' is "
                        "missing or null in a row, so a deterministic document id cannot be built."
                    )
                key_values.append(value)
            return merge_document_id(self._collection_name, key_values)
        # append / replace / insert-only: the dlt row id is already unique and stable
        # across whole-file retries, so use it directly.
        dlt_id = data.get("_dlt_id")
        if dlt_id is not None:
            return str(dlt_id)
        # Should not happen for dlt-normalized data; keep the load moving with a
        # random id rather than corrupting another document.
        return str(uuid.uuid4())

    @staticmethod
    def _json_fields(table: Any) -> list[str]:
        """Names of columns typed as dlt ``json`` (kept complex through normalization)."""
        columns = table.get("columns") or {}
        return [name for name, column in columns.items() if column.get("data_type") == "json"]

    def _id_fields(self, table: Any) -> Sequence[str] | None:
        """Columns used to build the Typesense document ``id``.

        Returns ``None`` when the row id (``_dlt_id``) should be used: append,
        replace, the ``insert-only`` merge strategy, and merge *child* tables
        (nested rows have no user key — they are keyed by ``_dlt_id`` with root
        propagation). For an ``upsert`` merge *root* table the primary key (or a
        ``unique`` column fallback) keys the document; a root merge with neither
        is a terminal, non-silent error (AC-MERGE-05).
        """
        if table.get("write_disposition") != "merge":
            return None

        merge_strategy = resolve_merge_strategy({table["name"]: table}, table)
        if merge_strategy == "insert-only":
            return None

        # Nested child tables have no user primary key; key them by _dlt_id.
        if is_nested_table(table):
            return None

        primary_keys = _user_columns(get_columns_names_with_prop(table, "primary_key"))
        if primary_keys:
            return primary_keys
        # dlt stamps `unique: true` on `_dlt_id` in every table, so the unique
        # fallback must ignore dlt system columns — otherwise the random per-run
        # `_dlt_id` would key the upsert and every re-run would duplicate rows.
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
    """Phase 2: delete child documents orphaned after a root merge/upsert.

    Mirrors the lancedb orphan-follow-up pattern. Not used in v1 (root docs only).
    """

    def __init__(self, file_path: str, collection_name: str) -> None:
        super().__init__(file_path)
        self._collection_name = collection_name

    def run(self) -> None:
        raise NotImplementedError(
            "Child-table orphan cleanup is phase 2; flatten documents for v1."
        )
