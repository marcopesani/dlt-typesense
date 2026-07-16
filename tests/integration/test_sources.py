"""Real-source smoke tests: sql_database, filesystem, rest_api."""

from __future__ import annotations

import csv
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

pytestmark = pytest.mark.integration


def test_sql_database_over_sqlite(make_pipeline, count_documents, documents, tmp_path) -> None:
    import sqlalchemy as sa
    from dlt.sources.sql_database import sql_database

    db_path = tmp_path / "shop.db"
    engine = sa.create_engine(f"sqlite:///{db_path}")
    metadata = sa.MetaData()
    products = sa.Table(
        "products",
        metadata,
        sa.Column("sku", sa.String, primary_key=True),
        sa.Column("price", sa.Integer),
    )
    events = sa.Table(
        "events",
        metadata,
        sa.Column("event_id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(products.insert(), [{"sku": "A1", "price": 10}, {"sku": "B2", "price": 20}])
        conn.execute(events.insert(), [{"event_id": 1, "name": "x"}, {"event_id": 2, "name": "y"}])

    def build_source():
        source = sql_database(engine).with_resources("products", "events")
        source.products.apply_hints(write_disposition="merge")
        source.events.apply_hints(write_disposition="append")
        return source

    pipeline = make_pipeline(dataset_name="shop")
    pipeline.run(build_source())
    assert count_documents("shop_products") == 2
    assert count_documents("shop_events") == 2

    with engine.begin() as conn:
        conn.execute(sa.update(products).where(products.c.sku == "A1").values(price=999))
    pipeline.run(build_source())

    assert count_documents("shop_products") == 2
    product_rows = documents("shop_products")
    assert len(product_rows) == 2
    product_docs = {d["sku"]: d["price"] for d in product_rows}
    assert product_docs == {"A1": 999, "B2": 20}  # in-place PK update
    assert count_documents("shop_events") == 4  # append accumulates


def test_filesystem_csv_jsonl_parquet(make_pipeline, count_documents, documents, tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    with open(data_dir / "data.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "label"])
        writer.writerows([["1", "a"], ["2", "b"], ["3", "c"]])

    with open(data_dir / "data.jsonl", "w") as handle:
        for i in range(2):
            handle.write(json.dumps({"id": i, "label": f"j{i}"}) + "\n")

    import decimal

    table = pa.table(
        {
            "n": pa.array([1, 2, 3, 4], type=pa.int64()),
            "ratio": pa.array([1.5, 2.5, 3.5, 4.5], type=pa.float64()),
            "ts": pa.array([1, 2, 3, 4], type=pa.timestamp("us", tz="UTC")),
            "amount": pa.array(
                [
                    decimal.Decimal("10.25"),
                    decimal.Decimal("20.50"),
                    decimal.Decimal("30.75"),
                    decimal.Decimal("40.00"),
                ],
                type=pa.decimal128(18, 2),
            ),
        }
    )
    pq.write_table(table, data_dir / "data.parquet")

    bucket_url = data_dir.as_uri()
    csv_res = (filesystem(bucket_url=bucket_url, file_glob="data.csv") | read_csv()).with_name(
        "csv_rows"
    )
    jsonl_res = (
        filesystem(bucket_url=bucket_url, file_glob="data.jsonl") | read_jsonl()
    ).with_name("jsonl_rows")
    parquet_res = (
        filesystem(bucket_url=bucket_url, file_glob="data.parquet") | read_parquet()
    ).with_name("parquet_rows")

    pipeline = make_pipeline(dataset_name="fs")
    info = pipeline.run([csv_res, jsonl_res, parquet_res])
    assert not info.has_failed_jobs
    assert count_documents("fs_csv_rows") == 3
    assert count_documents("fs_jsonl_rows") == 2
    assert count_documents("fs_parquet_rows") == 4

    parquet_docs = sorted(documents("fs_parquet_rows"), key=lambda d: d["n"])
    assert [d["n"] for d in parquet_docs] == [1, 2, 3, 4]  # int64 exact
    assert parquet_docs[0]["ratio"] == 1.5  # float
    assert parquet_docs[0]["amount"] == "10.25"
    assert isinstance(parquet_docs[0]["ts"], str) and parquet_docs[0]["ts"].startswith("1970-01-01")


class _MockAPIState:
    def __init__(self) -> None:
        self.items = [{"id": i, "updated_at": i, "name": f"n{i}"} for i in range(1, 6)]
        self.since_seen: list[int] = []


def _make_handler(state: _MockAPIState, host_port: list[str]):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args) -> None:  # silence
            pass

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            since = int(query.get("since", ["0"])[0])
            state.since_seen.append(since)
            page = int(query.get("page", ["1"])[0])
            page_size = 2
            matching = [item for item in state.items if item["updated_at"] > since]
            start = (page - 1) * page_size
            page_items = matching[start : start + page_size]
            has_next = start + page_size < len(matching)
            next_url = (
                f"http://{host_port[0]}{parsed.path}?since={since}&page={page + 1}"
                if has_next
                else None
            )
            body = json.dumps({"items": page_items, "paging": {"next": next_url}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def test_rest_api_paginated_incremental(make_pipeline, count_documents, tmp_path) -> None:
    from dlt.sources.rest_api import rest_api_source

    state = _MockAPIState()
    host_port: list[str] = ["127.0.0.1:0"]
    server = HTTPServer(("127.0.0.1", 0), _make_handler(state, host_port))
    host_port[0] = f"127.0.0.1:{server.server_address[1]}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://{host_port[0]}/"

        def build_source():
            return rest_api_source(
                {
                    "client": {
                        "base_url": base_url,
                        "paginator": {"type": "json_link", "next_url_path": "paging.next"},
                    },
                    "resources": [
                        {
                            "name": "api_items",
                            "endpoint": {
                                "path": "items",
                                "data_selector": "items",
                                "params": {
                                    "since": {
                                        "type": "incremental",
                                        "cursor_path": "updated_at",
                                        "initial_value": 0,
                                    }
                                },
                            },
                            "primary_key": "id",
                            "write_disposition": "merge",
                        }
                    ],
                }
            )

        pipeline = make_pipeline(dataset_name="api")
        pipeline.run(build_source())
        assert count_documents("api_api_items") == 5  # all pages loaded
        assert 0 in state.since_seen  # run 1 started from initial_value=0

        state.since_seen.clear()
        state.items.extend(
            [
                {"id": 6, "updated_at": 6, "name": "n6"},
                {"id": 7, "updated_at": 7, "name": "n7"},
            ]
        )
        pipeline.run(build_source())
        assert count_documents("api_api_items") == 7
        assert state.since_seen, "run 2 issued no request"
        assert all(since == 5 for since in state.since_seen)
    finally:
        server.shutdown()
        thread.join(timeout=5)
