"""Load jobs that push JSONL packages into Typesense collections."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

from dlt.common.destination.client import HasFollowupJobs, RunnableLoadJob
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TTableSchema
from dlt.common.schema.utils import get_columns_names_with_prop


class TypesenseLoadJob(RunnableLoadJob, HasFollowupJobs):
    """Load one JSONL file into a Typesense collection.

    Disposition → id → action seams (to implement):

    - append: id from `_dlt_id`, action=`upsert`
    - replace: collection truncated by client; id from `_dlt_id`, action=`upsert`
    - merge: id from primary_key (uuid5), action=`upsert`/`emplace`

    Never use `action=create` — dlt retries whole files on transient failure.
    """

    def __init__(self, file_path: str, collection_name: str) -> None:
        super().__init__(file_path)
        self._collection_name = collection_name

    def run(self) -> None:
        """Stream the load file into Typesense via bulk import.

        Planned steps:
        1. Read JSONL from ``self._file_path`` in client-sized chunks.
        2. Assign each row a Typesense ``id`` from :meth:`_id_fields`.
        3. POST `/collections/{c}/documents/import?action=upsert`.
        4. Parse per-line JSONL response; raise terminal/transient errors.
        """
        raise NotImplementedError("TypesenseLoadJob.run is not implemented yet.")

    def _id_fields(self, table: PreparedTableSchema) -> Sequence[str]:
        """Return columns used to build the Typesense document ``id``."""
        schema_table = cast(TTableSchema, table)
        if table.get("write_disposition") == "merge":
            primary_keys = get_columns_names_with_prop(schema_table, "primary_key")
            if primary_keys:
                return primary_keys
        return get_columns_names_with_prop(schema_table, "unique")


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
