"""dlt short-name resolution: destination="typesense".

dlt's DestinationReference.find looks up ``<plugin>.destinations.<name>`` for
every installed ``dlt`` entry-point plugin. Re-exporting the factory here lets
``dlt.pipeline(..., destination="typesense")`` resolve without an import.
"""

from dlt_typesense.factory import typesense

__all__ = ["typesense"]
