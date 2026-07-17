"""Typesense document destination for dlt."""

from dlt_typesense.factory import typesense
from dlt_typesense.typesense_adapter import typesense_adapter

__all__ = ["typesense", "typesense_adapter", "__version__"]
__version__ = "0.2.0"
