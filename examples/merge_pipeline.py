"""Merge/upsert into Typesense, including nested-list orphan cleanup.

Root documents are keyed by ``primary_key`` (deterministic Typesense ``id``).
Nested lists become child collections (``orders__items``). On a re-load under
merge (``upsert``), child documents that disappeared from a parent's list are
deleted automatically — see README "Orphan cleanup for nested tables".

Opt out per resource with ``typesense_adapter(resource, no_remove_orphans=True)``.

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

import dlt

from dlt_typesense import typesense


@dlt.resource(name="products", write_disposition="merge", primary_key="sku")
def products():
    yield {"sku": "A1", "title": "Widget", "price": 9.99}
    yield {"sku": "B2", "title": "Gadget", "price": 19.5}


@dlt.resource(name="orders", write_disposition="merge", primary_key="order_id")
def orders_with_two_items():
    yield {"order_id": "o1", "customer": "alice", "items": [{"sku": "a"}, {"sku": "b"}]}


@dlt.resource(name="orders", write_disposition="merge", primary_key="order_id")
def orders_with_one_item():
    # Dropped "b" — orphan cleanup deletes the stale orders__items document.
    yield {"order_id": "o1", "customer": "alice", "items": [{"sku": "a"}]}


def main() -> None:
    # destination="typesense" also works once credentials are in secrets/env.
    pipeline = dlt.pipeline(
        pipeline_name="typesense_merge_example",
        destination=typesense(),
        dataset_name="catalog",
    )
    print("products:", pipeline.run(products()))
    print("orders (2 items):", pipeline.run(orders_with_two_items()))
    print("orders (1 item, orphan 'b' removed):", pipeline.run(orders_with_one_item()))


if __name__ == "__main__":
    main()
