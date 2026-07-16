"""Append vs replace write dispositions against Typesense.

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
    # append: every run adds the rows again.
    print(pipeline.run(events()))
    # replace: each run truncates and reloads, leaving only the latest snapshot.
    print(pipeline.run(snapshot()))


if __name__ == "__main__":
    main()
