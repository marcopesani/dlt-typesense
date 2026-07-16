"""Job client: schema/state sync and load-job creation for Typesense."""

from __future__ import annotations

from collections.abc import Iterable
from types import TracebackType

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    JobClientBase,
    LoadJob,
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
)
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema import Schema, TSchemaTables

from dlt_typesense.configuration import TypesenseClientConfiguration
from dlt_typesense.load_jobs import TypesenseLoadJob
from dlt_typesense.rest_client import TypesenseRestClient


class TypesenseClient(JobClientBase, WithStateSync):
    """Typesense destination handler (document collections + state sync).

    Templated on dlt's Qdrant client: sentinel collection marks initialization;
    `_dlt_version`, `_dlt_loads`, and `_dlt_pipeline_state` live as collections.
    """

    def __init__(
        self,
        schema: Schema,
        config: TypesenseClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: TypesenseClientConfiguration = config
        self.rest: TypesenseRestClient | None = None

    @property
    def dataset_name(self) -> str:
        return self.config.normalize_dataset_name(self.schema)

    def make_qualified_collection_name(self, table_name: str) -> str:
        """Dataset-prefixed collection name (qdrant-style separator)."""
        dataset_separator = self.config.dataset_separator
        if self.dataset_name:
            return f"{self.dataset_name}{dataset_separator}{table_name}"
        return table_name

    def initialize_storage(self, truncate_tables: Iterable[str] | None = None) -> None:
        """Create sentinel collection; truncate means drop+recreate collections."""
        raise NotImplementedError("initialize_storage is not implemented yet.")

    def is_storage_initialized(self) -> bool:
        raise NotImplementedError("is_storage_initialized is not implemented yet.")

    def drop_storage(self) -> None:
        raise NotImplementedError("drop_storage is not implemented yet.")

    def update_stored_schema(
        self,
        only_tables: Iterable[str] = None,  # type: ignore[assignment]
        expected_update: TSchemaTables = None,  # type: ignore[assignment]
        force: bool = False,
    ) -> TSchemaTables | None:
        """Ensure collections exist for tables and persist schema version docs."""
        _ = super().update_stored_schema(only_tables, expected_update, force)
        raise NotImplementedError(
            "update_stored_schema collection create + version write not implemented."
        )

    def create_load_job(
        self,
        table: PreparedTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False,
    ) -> LoadJob:
        table_name = table.get("name") or "unknown"
        return TypesenseLoadJob(
            file_path,
            collection_name=self.make_qualified_collection_name(table_name),
        )

    def complete_load(self, load_id: str) -> None:
        """Record a completed load in the `_dlt_loads` collection."""
        raise NotImplementedError("complete_load is not implemented yet.")

    def get_stored_schema(self, schema_name: str = None) -> StorageSchemaInfo | None:  # type: ignore[assignment]
        raise NotImplementedError("get_stored_schema is not implemented yet.")

    def get_stored_schema_by_hash(self, version_hash: str) -> StorageSchemaInfo | None:
        # Base protocol omits Optional; qdrant returns None when missing.
        raise NotImplementedError("get_stored_schema_by_hash is not implemented yet.")

    def get_stored_state(self, pipeline_name: str) -> StateInfo | None:
        raise NotImplementedError("get_stored_state is not implemented yet.")

    def __enter__(self) -> TypesenseClient:
        self.rest = TypesenseRestClient(self.config.credentials)
        # Open connection when rest client is implemented
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.rest = None
