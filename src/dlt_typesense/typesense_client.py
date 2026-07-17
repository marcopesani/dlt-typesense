"""Job client: storage lifecycle, schema/state sync, and load-job creation.

Templated on dlt's Qdrant destination. Tables map to Typesense collections;
system tables (``_dlt_version``, ``_dlt_loads``, ``_dlt_pipeline_state``) are
dataset-qualified collections. Newest lookups sort by the integer ``version``
column. System collections use a hybrid schema: sort/filter fields are declared
explicitly, everything else falls through to ``.*`` auto.
"""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Iterable
from contextlib import suppress
from types import TracebackType
from typing import Any, cast

import typesense
from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    JobClientBase,
    LoadJob,
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
)
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import C_DLT_LOAD_ID, C_DLT_LOADS_TABLE_LOAD_ID
from dlt.common.schema.utils import (
    loads_table,
    normalize_table_identifiers,
    version_table,
)
from typesense.exceptions import ObjectAlreadyExists, ObjectNotFound

from dlt_typesense.configuration import TypesenseClientConfiguration
from dlt_typesense.exceptions import wrap_typesense_error
from dlt_typesense.load_jobs import TypesenseLoadJob
from dlt_typesense.type_mapper import collection_schema_auto, collection_schema_from_table
from dlt_typesense.typesense_adapter import COLLECTION_HINT, FIELD_HINT

# dlt 1.28.0 added `force` to JobClientBase.update_stored_schema; older releases
# reject the kwarg. Probe once so we stay compatible across the declared range.
_BASE_ACCEPTS_FORCE = "force" in inspect.signature(JobClientBase.update_stored_schema).parameters


class TypesenseClient(JobClientBase, WithStateSync):
    """Typesense destination handler (document collections + state sync)."""

    def __init__(
        self,
        schema: Schema,
        config: TypesenseClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: TypesenseClientConfiguration = config
        self.ts: typesense.Client | None = None

        version_table_ = normalize_table_identifiers(version_table(), schema.naming)
        self.version_collection_properties = list(version_table_["columns"].keys())
        loads_table_ = normalize_table_identifiers(loads_table(), schema.naming)
        self.loads_collection_properties = list(loads_table_["columns"].keys())

    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)

    def make_qualified_collection_name(self, table_name: str) -> str:
        """Dataset-prefixed collection name, truncated with a hash if over 255 chars."""
        dataset_separator = self.config.dataset_separator
        if self.dataset_name:
            name = f"{self.dataset_name}{dataset_separator}{table_name}"
        else:
            name = table_name
        if len(name) > 255:
            digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:16]
            name = f"{name[:238]}_{digest}"
        return name

    @property
    def _system_table_names(self) -> set[str]:
        return {
            self.schema.version_table_name,
            self.schema.loads_table_name,
            self.schema.state_table_name,
        }

    def _collection_schema(self, qualified_name: str, table_name: str) -> dict[str, Any]:
        n = self.schema.naming.normalize_identifier
        if table_name == self.schema.version_table_name:
            return self._hybrid_schema(
                qualified_name,
                [
                    {"name": n("version"), "type": "int64", "sort": True, "optional": True},
                    {"name": n("schema_name"), "type": "string", "optional": True},
                    {"name": n("version_hash"), "type": "string", "optional": True},
                    {"name": n("schema"), "type": "string", "index": False, "optional": True},
                ],
            )
        if table_name == self.schema.state_table_name:
            return self._hybrid_schema(
                qualified_name,
                [
                    {"name": n("version"), "type": "int64", "sort": True, "optional": True},
                    {"name": n("pipeline_name"), "type": "string", "optional": True},
                    {"name": n("state"), "type": "string", "index": False, "optional": True},
                ],
            )
        if table_name == self.schema.loads_table_name:
            return self._hybrid_schema(
                qualified_name,
                [{"name": n(C_DLT_LOADS_TABLE_LOAD_ID), "type": "string", "optional": True}],
            )
        table = self.schema.tables.get(table_name)
        if table is None:
            return collection_schema_auto(qualified_name)
        return collection_schema_from_table(qualified_name, table, n)

    @staticmethod
    def _hybrid_schema(qualified_name: str, pinned_fields: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "name": qualified_name,
            "enable_nested_fields": True,
            "fields": [*pinned_fields, {"name": ".*", "type": "auto"}],
        }

    def _collection_exists(self, name: str) -> bool:
        try:
            self._ts.collections[name].retrieve()
        except ObjectNotFound:
            return False
        return True

    def _create_collection(self, schema: dict[str, Any]) -> None:
        # Concurrent creation is fine — the collection is there, which is all we need.
        with suppress(ObjectAlreadyExists):
            self._ts.collections.create(cast("Any", schema))

    def _delete_collection(self, name: str) -> None:
        with suppress(ObjectNotFound):
            self._ts.collections[name].delete()

    def _search_documents(
        self,
        collection_name: str,
        *,
        filter_by: str | None = None,
        sort_by: str | None = None,
        per_page: int = 250,
        page: int = 1,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"q": "*", "per_page": per_page, "page": page}
        if filter_by:
            params["filter_by"] = filter_by
        if sort_by:
            params["sort_by"] = sort_by
        response = self._ts.collections[collection_name].documents.search(cast("Any", params))
        return [cast("dict[str, Any]", hit["document"]) for hit in response.get("hits", [])]

    def _ensure_collection(self, table_name: str) -> str:
        qualified_name = self.make_qualified_collection_name(table_name)
        if not self._collection_exists(qualified_name):
            self._create_collection(self._collection_schema(qualified_name, table_name))
        elif self._table_has_hints(table_name):
            logger.info(
                f"Collection '{qualified_name}' already exists; Typesense schema hints apply "
                "only at collection creation and were not re-applied. Recreate the collection "
                "(e.g. replace disposition or drop) to apply changed hints."
            )
        return qualified_name

    def _table_has_hints(self, table_name: str) -> bool:
        table = self.schema.tables.get(table_name)
        if not table:
            return False
        if table.get(COLLECTION_HINT):
            return True
        columns = table.get("columns") or {}
        return any(FIELD_HINT in column for column in columns.values())

    def _recreate_collection(self, table_name: str) -> None:
        qualified_name = self.make_qualified_collection_name(table_name)
        self._delete_collection(qualified_name)
        self._create_collection(self._collection_schema(qualified_name, table_name))

    @wrap_typesense_error
    def initialize_storage(self, truncate_tables: Iterable[str] | None = None) -> None:
        for table_name in self._system_table_names:
            self._ensure_collection(table_name)
        for table_name in truncate_tables or []:
            # _delete_collection already tolerates 404; no pre-exists probe needed.
            self._recreate_collection(table_name)

    @wrap_typesense_error
    def is_storage_initialized(self) -> bool:
        version_collection = self.make_qualified_collection_name(self.schema.version_table_name)
        return self._collection_exists(version_collection)

    @wrap_typesense_error
    def drop_storage(self) -> None:
        """Delete collections owned by this dataset.

        With a non-empty ``dataset_name``, deletes every collection whose name
        starts with ``{dataset}{separator}``. With an empty dataset name there
        is no prefix: only collections whose bare name matches a table in the
        current schema are deleted. Unrelated collections that happen to share
        those bare names are therefore also removed — prefer a non-empty
        dataset name in shared Typesense clusters.
        """
        collections = self._ts.collections.retrieve()
        existing = {c["name"] for c in collections}
        if self.dataset_name:
            prefix = f"{self.dataset_name}{self.config.dataset_separator}"
            targets = [name for name in existing if name.startswith(prefix)]
        else:
            targets = [name for name in self.schema.tables if name in existing]
        for name in targets:
            self._delete_collection(name)

    @wrap_typesense_error
    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,  # type: ignore[assignment]
        expected_update: TSchemaTables = None,  # type: ignore[assignment]
        force: bool = False,
    ) -> TSchemaTables | None:
        if _BASE_ACCEPTS_FORCE:
            applied_update = super().update_stored_schema(only_tables, expected_update, force)
        else:
            applied_update = super().update_stored_schema(only_tables, expected_update)
        schema_info = self.get_stored_schema_by_hash(self.schema.stored_version_hash)
        if schema_info is None or force:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} not found in Typesense "
                "(or update enforced); creating collections and storing schema."
            )
            self._execute_schema_update(only_tables, store_schema=schema_info is None)
        else:
            logger.info(
                f"Schema with hash {self.schema.stored_version_hash} found in Typesense "
                f"(inserted at {schema_info.inserted_at}); no upgrade required."
            )
        return applied_update

    def _execute_schema_update(
        self, only_tables: Iterable[str] | None, store_schema: bool = True
    ) -> None:
        for table_name in only_tables or self.schema.tables:
            self._ensure_collection(table_name)
        if store_schema:
            self._update_schema_in_storage(self.schema)

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> LoadJob:
        table_name = table["name"] or ""
        return TypesenseLoadJob(
            file_path,
            collection_name=self.make_qualified_collection_name(table_name),
        )

    @wrap_typesense_error
    def complete_load(self, load_id: str) -> None:
        values: list[Any] = [
            load_id,
            self.schema.name,
            0,
            str(pendulum.now()),
            self.schema.version_hash,
        ]
        assert len(values) == len(self.loads_collection_properties)
        document = dict(zip(self.loads_collection_properties, values, strict=True))
        # Document id = load id so retries are idempotent and visibility is a GET.
        document["id"] = load_id
        loads_collection = self.make_qualified_collection_name(self.schema.loads_table_name)
        self._ts.collections[loads_collection].documents.upsert(document)

    def _update_schema_in_storage(self, schema: Schema) -> None:
        schema_str = json.dumps(schema.to_dict())
        values: list[Any] = [
            schema.version,
            schema.ENGINE_VERSION,
            str(pendulum.now().isoformat()),
            schema.name,
            schema.stored_version_hash,
            schema_str,
        ]
        assert len(values) == len(self.version_collection_properties)
        document = dict(zip(self.version_collection_properties, values, strict=True))
        document["id"] = _doc_id("version", schema.stored_version_hash)
        version_collection = self.make_qualified_collection_name(self.schema.version_table_name)
        self._ts.collections[version_collection].documents.upsert(document)

    @wrap_typesense_error
    def get_stored_schema(self, schema_name: str = None) -> StorageSchemaInfo | None:  # type: ignore[assignment]
        version_collection = self.make_qualified_collection_name(self.schema.version_table_name)
        if not self._collection_exists(version_collection):
            return None
        n = self.schema.naming.normalize_identifier
        filter_by = _eq_filter(n("schema_name"), schema_name) if schema_name else None
        documents = self._search_documents(
            version_collection,
            filter_by=filter_by,
            sort_by=f"{n('version')}:desc",
            per_page=1,
        )
        if not documents:
            return None
        return StorageSchemaInfo.from_normalized_mapping(documents[0], self.schema.naming)

    @wrap_typesense_error
    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo | None:
        version_collection = self.make_qualified_collection_name(self.schema.version_table_name)
        if not self._collection_exists(version_collection):
            return None
        n = self.schema.naming.normalize_identifier
        documents = self._search_documents(
            version_collection,
            filter_by=_eq_filter(n("version_hash"), version_hash),
            per_page=1,
        )
        if not documents:
            return None
        return StorageSchemaInfo.from_normalized_mapping(documents[0], self.schema.naming)

    @wrap_typesense_error
    def get_stored_state(self, pipeline_name: str) -> StateInfo | None:
        state_collection = self.make_qualified_collection_name(self.schema.state_table_name)
        loads_collection = self.make_qualified_collection_name(self.schema.loads_table_name)
        if not (
            self._collection_exists(state_collection) and self._collection_exists(loads_collection)
        ):
            raise DestinationUndefinedEntity(
                f"State or loads collection missing for dataset '{self.dataset_name}'."
            )

        n = self.schema.naming.normalize_identifier
        p_pipeline_name = n("pipeline_name")
        p_dlt_load_id = n(C_DLT_LOAD_ID)
        page = 1
        page_size = 50
        while True:
            documents = self._search_documents(
                state_collection,
                filter_by=_eq_filter(p_pipeline_name, pipeline_name),
                sort_by=f"{n('version')}:desc",
                per_page=page_size,
                page=page,
            )
            if not documents:
                return None
            for state in documents:
                load_id = state.get(p_dlt_load_id)
                if load_id is None:
                    continue
                # Visible only after its load completed (loads doc id == load id).
                try:
                    self._ts.collections[loads_collection].documents[str(load_id)].retrieve()
                except ObjectNotFound:
                    continue
                return StateInfo.from_normalized_mapping(state, self.schema.naming)
            if len(documents) < page_size:
                return None
            page += 1

    @property
    def _ts(self) -> typesense.Client:
        if self.ts is None:
            raise RuntimeError("TypesenseClient is used outside of its context manager.")
        return self.ts

    def __enter__(self) -> TypesenseClient:
        self.ts = self.config.credentials.get_client(
            read_timeout_seconds=self.config.read_timeout_seconds
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.ts is not None:
            self.ts.api_call.close()
            self.ts = None


def _doc_id(*parts: str) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def _eq_filter(field: str, value: str) -> str:
    """Build a Typesense equality filter with a backtick-quoted literal.

    Typesense has no escape for backticks inside ``field:=`…` `` literals, so
    values containing a backtick are rejected rather than silently no-matching
    (which would make state/schema lookups look empty and reset incremental
    cursors).
    """
    if "`" in value:
        raise ValueError(
            f"Typesense filter value for '{field}' contains a backtick, which cannot "
            "be escaped in field:=`…` literals. Rename the pipeline/schema so the "
            "value has no backticks."
        )
    return f"{field}:=`{value}`"
