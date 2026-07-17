"""Schema hints: facets, sort, vectors, epoch timestamps, collection options.

A ``type`` in ``field_hints`` is a contract about both the schema and the
document value. Pinning a dlt ``timestamp``/``date`` to ``int64``/``int32``
stores Unix epoch seconds (range-filterable and sortable in Typesense).

Run a local Typesense first (repo root):

    docker compose up -d
    # or: .local/typesense/typesense-server --data-dir=.local/typesense/data \\
    #        --api-key=local-dev-key --api-port=8108

Credentials resolve from ``.dlt/secrets.toml`` or ``TYPESENSE_*`` /
``DESTINATION__TYPESENSE__CREDENTIALS__*`` env vars:

```toml
[destination.typesense.credentials]
host = "localhost"
port = 8108
protocol = "http"
api_key = "local-dev-key"
```
"""

from __future__ import annotations

from datetime import datetime, timezone

import dlt

from dlt_typesense import typesense, typesense_adapter


@dlt.resource(name="books", write_disposition="replace")
def books():
    yield {
        "title": "Dune",
        "genre": "sci-fi",
        "rating": 4.5,
        "published_at": datetime(1965, 8, 1, tzinfo=timezone.utc),
        "embedding": [0.1, 0.2, 0.3],
    }
    yield {
        "title": "Emma",
        "genre": "classic",
        "rating": 4.1,
        "published_at": datetime(1815, 12, 23, tzinfo=timezone.utc),
        "embedding": [0.4, 0.5, 0.6],
    }


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="typesense_schema_hints_example",
        destination=typesense(),
        dataset_name="library",
    )
    print(
        pipeline.run(
            typesense_adapter(
                books(),
                facet="genre",
                sort="rating",
                field_hints={
                    # timestamp → int64 Unix epoch seconds on the wire
                    "published_at": {"type": "int64", "sort": True},
                    "embedding": {"type": "float[]", "num_dim": 3},
                },
                collection_hints={
                    "default_sorting_field": "rating",
                    "metadata": {"example": "schema_hints"},
                },
            )
        )
    )


if __name__ == "__main__":
    main()
