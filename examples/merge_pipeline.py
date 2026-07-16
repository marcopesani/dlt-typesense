"""Example skeleton: merge/upsert products into Typesense.

This example will fail until load jobs are implemented. It documents the
intended API for contributors and early adopters.

Secrets (`.dlt/secrets.toml`):

```toml
[destination.typesense.credentials]
host = "localhost"
port = 8108
protocol = "http"
api_key = "xyz"
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
    # Will raise NotImplementedError until TypesenseLoadJob.run is implemented.
    info = pipeline.run(products())
    print(info)


if __name__ == "__main__":
    main()
