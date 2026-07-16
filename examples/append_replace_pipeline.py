"""Example skeleton: append vs replace write dispositions.

Import is not implemented yet; this file documents intended usage.
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
    yield {"id": 1, "label": "current"}


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
