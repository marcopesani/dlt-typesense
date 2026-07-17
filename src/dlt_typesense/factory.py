"""Destination factory entry point for Typesense."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt_typesense.configuration import TypesenseClientConfiguration, TypesenseCredentials

if TYPE_CHECKING:
    from dlt_typesense.typesense_client import TypesenseClient
else:
    TypesenseClient = Any  # type: ignore[misc, assignment]


class typesense(Destination[TypesenseClientConfiguration, "TypesenseClient"]):
    """Typesense document destination for dlt.

    Collections map to tables; documents are loaded via JSONL bulk import.
    Supports append, replace (truncate-and-insert), and merge (upsert, insert-only).
    """

    spec = TypesenseClientConfiguration  # type: ignore[assignment]

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        # "reference" is dlt's internal format for follow-up jobs; it routes the
        # merge orphan-cleanup job (lancedb precedent), never user data.
        caps.supported_loader_file_formats = ["jsonl", "reference"]
        caps.naming_convention = "dlt_typesense.naming"
        caps.has_case_sensitive_identifiers = True
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 8 * 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 8 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = False
        caps.supports_ddl_transactions = False
        caps.supported_replace_strategies = ["truncate-and-insert"]
        # "upsert" first: dlt resolves the first entry as the default merge strategy.
        caps.supported_merge_strategies = ["upsert", "insert-only"]
        caps.recommended_file_size = 64_000_000
        return caps

    @property
    def client_class(self) -> type[TypesenseClient]:
        from dlt_typesense.typesense_client import TypesenseClient

        return TypesenseClient

    def __init__(
        self,
        credentials: TypesenseCredentials | dict[str, Any] | None = None,
        destination_name: str | None = None,
        environment: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


typesense.register()
