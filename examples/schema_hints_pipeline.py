"""Schema hints: faceting, sorting, vectors, and collection options.

Run a local Typesense first (repo root):

    docker compose up -d

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

import dlt

from dlt_typesense import typesense, typesense_adapter


@dlt.resource(name="books", write_disposition="replace")
def books():
    yield {"title": "Dune", "genre": "sci-fi", "rating": 4.5, "embedding": [0.1, 0.2, 0.3]}
    yield {"title": "Emma", "genre": "classic", "rating": 4.1, "embedding": [0.4, 0.5, 0.6]}


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
                field_hints={"embedding": {"type": "float[]", "num_dim": 3}},
                collection_hints={
                    "default_sorting_field": "rating",
                    "metadata": {"example": "schema_hints"},
                },
            )
        )
    )


if __name__ == "__main__":
    main()
