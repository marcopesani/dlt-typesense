"""Append vs replace write dispositions against Typesense.

- ``append``: documents keyed by ``_dlt_id``; re-runs add new documents.
- ``replace``: drop + recreate the collection, then import (``truncate-and-insert``).

For upsert-by-primary-key and nested-list orphan cleanup, see ``merge_pipeline.py``.

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


@dlt.resource(name="events", write_disposition="append")
def events():
    yield {"event_id": "e1", "name": "page_view"}
    yield {"event_id": "e2", "name": "click"}


@dlt.resource(name="snapshot", write_disposition="replace")
def snapshot():
    yield {"metric": "dau", "value": 1200}
    yield {"metric": "mau", "value": 34000}


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="typesense_disposition_example",
        destination=typesense(),
        dataset_name="demo",
    )
    print(pipeline.run(events()))
    print(pipeline.run(snapshot()))


if __name__ == "__main__":
    main()
