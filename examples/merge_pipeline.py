"""Merge/upsert products into Typesense keyed by a primary key.

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

from dlt_typesense import typesense


@dlt.resource(name="products", write_disposition="merge", primary_key="sku")
def products():
    yield {"sku": "A1", "title": "Widget", "price": 9.99}
    yield {"sku": "B2", "title": "Gadget", "price": 19.5}


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="typesense_merge_example",
        destination=typesense(),
        dataset_name="catalog",
    )
    # First run inserts both products; re-running upserts them in place
    # (same deterministic document id from the `sku` primary key).
    info = pipeline.run(products())
    print(info)


if __name__ == "__main__":
    main()
