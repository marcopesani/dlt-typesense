"""Job client: storage lifecycle, schema/state sync, and load-job creation.

Templated on dlt's Qdrant destination. Tables map to Typesense collections;
the dlt system tables live as dataset-qualified collections:

- ``_dlt_version``  — one document per stored schema version
- ``_dlt_loads``    — one document per completed load id
- ``_dlt_pipeline_state`` — pipeline state documents (loaded via a normal job)

"Newest" lookups sort by dlt's monotonically increasing integer ``version``
column (an int64 Typesense sort field) rather than a timestamp string, so the
ordering is unambiguous and does not depend on string-sort semantics. System
collections use a *hybrid* schema: the fields we sort/filter on are declared
explicitly (typed, sortable) while everything else falls through to a ``.*``
auto field, exactly like data collections.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from types import TracebackType
from typing import Any

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

from dlt_typesense.configuration import TypesenseClientConfiguration
from dlt_typesense.load_jobs import TypesenseLoadJob
from dlt_typesense.rest_client import TypesenseRestClient
from dlt_typesense.type_mapper import collection_schema_auto


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
        self.rest: TypesenseRestClient | None = None

        # Normalized column orders for the system tables (mirrors qdrant).
        version_table_ = normalize_table_identifiers(version_table(), schema.naming)
        self.version_collection_properties = list(version_table_["columns"].keys())
        loads_table_ = normalize_table_identifiers(loads_table(), schema.naming)
        self.loads_collection_properties = list(loads_table_["columns"].keys())

    # --- naming ---------------------------------------------------------------

    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)

    def make_qualified_collection_name(self, table_name: str) -> str:
        """Dataset-prefixed collection name (qdrant-style separator), bounded to 255.

        dlt caps individual identifiers at ``max_identifier_length`` (255), but the
        dataset+separator+table concatenation could in principle exceed it; a
        deterministic hash suffix keeps the qualified name within bounds (AC-TS-09).
        """
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

    # --- collection schema ----------------------------------------------------

    def _collection_schema(self, qualified_name: str, table_name: str) -> dict[str, Any]:
        """Return the Typesense schema for a collection.

        Data tables use auto schema; system tables get a hybrid schema whose
        sort/filter fields are declared explicitly.
        """
        n = self.schema.naming.normalize_identifier
        if table_name == self.schema.version_table_name:
            return self._hybrid_schema(
                qualified_name,
                [
                    {"name": n("version"), "type": "int64", "sort": True, "optional": True},
                    {"name": n("schema_name"), "type": "string", "optional": True},
                    {"name": n("version_hash"), "type": "string", "optional": True},
                    # The schema JSON blob is stored but not indexed.
                    {"name": n("schema"), "type": "string", "index": False, "optional": True},
                ],
            )
        if table_name == self.schema.state_table_name:
            return self._hybrid_schema(
                qualified_name,
                [
                    {"name": n("version"), "type": "int64", "sort": True, "optional": True},
                    {"name": n("pipeline_name"), "type": "string", "optional": True},
                    # The compressed state blob is stored but never indexed.
                    {"name": n("state"), "type": "string", "index": False, "optional": True},
                ],
            )
        if table_name == self.schema.loads_table_name:
            return self._hybrid_schema(
                qualified_name,
                [{"name": n(C_DLT_LOADS_TABLE_LOAD_ID), "type": "string", "optional": True}],
            )
        return collection_schema_auto(qualified_name)

    @staticmethod
    def _hybrid_schema(qualified_name: str, pinned_fields: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "name": qualified_name,
            "enable_nested_fields": True,
            "fields": [*pinned_fields, {"name": ".*", "type": "auto"}],
        }

    def _ensure_collection(self, table_name: str) -> str:
        """Create the collection for a dlt table if missing; return its name."""
        qualified_name = self.make_qualified_collection_name(table_name)
        if not self._rest.collection_exists(qualified_name):
            self._rest.create_collection(self._collection_schema(qualified_name, table_name))
        return qualified_name

    def _recreate_collection(self, table_name: str) -> None:
        """Drop and recreate a collection so it ends up empty but existing."""
        qualified_name = self.make_qualified_collection_name(table_name)
        self._rest.delete_collection(qualified_name)
        self._rest.create_collection(self._collection_schema(qualified_name, table_name))

    # --- storage lifecycle ----------------------------------------------------

    def initialize_storage(self, truncate_tables: Iterable[str] | None = None) -> None:
        # System collections must exist right after initialize_storage (AC-PROTO-01),
        # not only after update_stored_schema.
        for table_name in self._system_table_names:
            self._ensure_collection(table_name)
        for table_name in truncate_tables or []:
            qualified_name = self.make_qualified_collection_name(table_name)
            if self._rest.collection_exists(qualified_name):
                self._recreate_collection(table_name)

    def is_storage_initialized(self) -> bool:
        version_collection = self.make_qualified_collection_name(self.schema.version_table_name)
        return self._rest.collection_exists(version_collection)

    def drop_storage(self) -> None:
        """Delete every collection belonging to this dataset (system + data)."""
        existing = {c["name"] for c in self._rest.list_collections()}
        if self.dataset_name:
            prefix = f"{self.dataset_name}{self.config.dataset_separator}"
            targets = [name for name in existing if name.startswith(prefix)]
        else:
            targets = [name for name in self.schema.tables if name in existing]
        for name in targets:
            self._rest.delete_collection(name)

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,  # type: ignore[assignment]
        expected_update: TSchemaTables = None,  # type: ignore[assignment]
        force: bool = False,
    ) -> TSchemaTables | None:
        applied_update = super().update_stored_schema(only_tables, expected_update, force)
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
        # A prepared table always carries a name.
        table_name = table["name"] or ""
        return TypesenseLoadJob(
            file_path,
            collection_name=self.make_qualified_collection_name(table_name),
        )

    def complete_load(self, load_id: str) -> None:
        """Record a completed load in the `_dlt_loads` collection (AC-PROTO-07)."""
        values: list[Any] = [
            load_id,
            self.schema.name,
            0,
            str(pendulum.now()),
            self.schema.version_hash,
        ]
        assert len(values) == len(self.loads_collection_properties)
        document = dict(zip(self.loads_collection_properties, values, strict=True))
        # Use the load id as the Typesense document id: a retried complete_load is an
        # idempotent upsert, and the state-visibility check becomes a GET-by-id.
        document["id"] = load_id
        loads_collection = self.make_qualified_collection_name(self.schema.loads_table_name)
        self._rest.upsert_document(loads_collection, document)

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
        self._rest.upsert_document(version_collection, document)

    # --- state & schema sync (WithStateSync) ----------------------------------

    def get_stored_schema(self, schema_name: str = None) -> StorageSchemaInfo | None:  # type: ignore[assignment]
        version_collection = self.make_qualified_collection_name(self.schema.version_table_name)
        if not self._rest.collection_exists(version_collection):
            return None
        n = self.schema.naming.normalize_identifier
        filter_by = _eq_filter(n("schema_name"), schema_name) if schema_name else None
        documents = self._rest.search_documents(
            version_collection,
            filter_by=filter_by,
            sort_by=f"{n('version')}:desc",
            per_page=1,
        )
        if not documents:
            return None
        return StorageSchemaInfo.from_normalized_mapping(documents[0], self.schema.naming)

    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo | None:
        version_collection = self.make_qualified_collection_name(self.schema.version_table_name)
        if not self._rest.collection_exists(version_collection):
            return None
        n = self.schema.naming.normalize_identifier
        documents = self._rest.search_documents(
            version_collection,
            filter_by=_eq_filter(n("version_hash"), version_hash),
            per_page=1,
        )
        if not documents:
            return None
        return StorageSchemaInfo.from_normalized_mapping(documents[0], self.schema.naming)

    def get_stored_state(self, pipeline_name: str) -> StateInfo | None:
        state_collection = self.make_qualified_collection_name(self.schema.state_table_name)
        loads_collection = self.make_qualified_collection_name(self.schema.loads_table_name)
        if not (
            self._rest.collection_exists(state_collection)
            and self._rest.collection_exists(loads_collection)
        ):
            # Pipeline never ran against this dataset.
            raise DestinationUndefinedEntity(
                f"State or loads collection missing for dataset '{self.dataset_name}'."
            )

        n = self.schema.naming.normalize_identifier
        p_pipeline_name = n("pipeline_name")
        p_dlt_load_id = n(C_DLT_LOAD_ID)
        page = 1
        page_size = 50
        while True:
            documents = self._rest.search_documents(
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
                # State is only visible once its load has completed (AC-STATE-02).
                # The loads document id *is* the load id, so this is a GET-by-id.
                if self._rest.get_document(loads_collection, str(load_id)) is not None:
                    return StateInfo.from_normalized_mapping(state, self.schema.naming)
            if len(documents) < page_size:
                return None
            page += 1

    # --- connection -----------------------------------------------------------

    @property
    def _rest(self) -> TypesenseRestClient:
        if self.rest is None:
            raise RuntimeError("TypesenseClient is used outside of its context manager.")
        return self.rest

    def __enter__(self) -> TypesenseClient:
        self.rest = TypesenseRestClient(
            self.config.credentials,
            read_timeout_seconds=self.config.read_timeout_seconds,
        )
        self.rest.open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self.rest is not None:
            self.rest.close()
            self.rest = None


def _doc_id(*parts: str) -> str:
    """Deterministic, URL-safe id for a housekeeping document."""
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def _eq_filter(field: str, value: str) -> str:
    """Typesense exact-match filter; the value is backtick-wrapped for safety."""
    return f"{field}:=`{value}`"
