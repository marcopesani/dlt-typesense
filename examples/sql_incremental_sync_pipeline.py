"""Incremental SQL → Typesense sync (the production shape).

Declarative multi-collection config, incremental cursor, merge on primary key,
typed field hints (timestamps → Unix epoch), and ``add_map`` for row cleaning.

Uses a temp SQLite database so the example runs without warehouse creds.
Swap the SQLAlchemy URL for Redshift/Postgres/etc. in real pipelines.

If a synced table has nested lists, merge (``upsert``) automatically removes
orphaned child documents for parents present in the load — see README
"Orphan cleanup for nested tables". Opt out with
``typesense_adapter(..., no_remove_orphans=True)``.

Run a local Typesense first (repo root):

    docker compose up -d
    # or: .local/typesense/typesense-server --data-dir=.local/typesense/data \\
    #        --api-key=local-dev-key --api-port=8108

Credentials resolve from ``.dlt/secrets.toml`` or
``DESTINATION__TYPESENSE__CREDENTIALS__*`` / ``TYPESENSE_*`` env vars:

```toml
[destination.typesense.credentials]
host = "localhost"
port = 8108
protocol = "http"
api_key = "local-dev-key"
```

Usage::

    python examples/sql_incremental_sync_pipeline.py
    python examples/sql_incremental_sync_pipeline.py --target users --limit 10
    python examples/sql_incremental_sync_pipeline.py --full-refresh
"""

from __future__ import annotations

import argparse
import json
import tempfile
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

import dlt
import sqlalchemy as sa
from dlt.sources.sql_database import sql_database

from dlt_typesense import typesense, typesense_adapter


class SourceConfig(TypedDict):
    table: str


class SyncConfig(TypedDict, total=False):
    collection: str
    source: SourceConfig
    primary_key: str
    timestamp_field: str
    field_hints: dict[str, dict[str, Any]]
    facet: list[str]
    sort: list[str]
    transform: Callable[[dict[str, Any]], dict[str, Any]]


def decode_categories(row: dict[str, Any]) -> dict[str, Any]:
    """Decode a JSON-encoded array column (e.g. Redshift SUPER → string).

    ``add_map`` sees whatever shape the source yields. This example uses
    ``backend=\"sqlalchemy\"`` so items are dicts. With ``backend=\"pyarrow\"``,
    items are Arrow tables — either switch backend or use ``add_yield_map``
    and ``table.to_pylist()``.
    """
    raw = row.get("categories")
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
            row["categories"] = decoded if isinstance(decoded, list) else []
        except json.JSONDecodeError:
            row["categories"] = []
    elif raw is None:
        row["categories"] = []
    return row


COLLECTIONS: list[SyncConfig] = [
    {
        "collection": "users",
        "source": {"table": "user_stats"},
        "primary_key": "user_id",
        "timestamp_field": "last_update",
        "facet": ["account_type"],
        "sort": ["order_count"],
        "field_hints": {
            # dlt timestamp → Typesense int64 Unix epoch (range-filterable).
            "last_update": {"type": "int64", "sort": True},
            "categories": {"type": "string[]", "facet": True},
        },
        "transform": decode_categories,
    },
]


def _seed_sqlite(path: Path) -> None:
    """Create a demo table with a real DATETIME column so dlt types it as timestamp."""
    engine = sa.create_engine(f"sqlite:///{path}")
    metadata = sa.MetaData()
    user_stats = sa.Table(
        "user_stats",
        metadata,
        sa.Column("user_id", sa.String, primary_key=True),
        sa.Column("email", sa.String),
        sa.Column("account_type", sa.String),
        sa.Column("order_count", sa.Integer),
        sa.Column("last_update", sa.DateTime(timezone=True)),
        sa.Column("categories", sa.String),  # JSON-encoded string (SUPER-like)
    )
    metadata.create_all(engine)
    rows = [
        {
            "user_id": "u1",
            "email": "a@example.com",
            "account_type": "consumer",
            "order_count": 3,
            "last_update": datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
            "categories": '["gift-cards","esim"]',
        },
        {
            "user_id": "u2",
            "email": "b@example.com",
            "account_type": "business",
            "order_count": 12,
            "last_update": datetime(2024, 6, 1, 8, 30, tzinfo=timezone.utc),
            "categories": '["refills"]',
        },
        {
            "user_id": "u3",
            "email": "c@example.com",
            "account_type": "consumer",
            "order_count": 1,
            "last_update": datetime(2023, 11, 20, tzinfo=timezone.utc),
            "categories": None,
        },
    ]
    with engine.begin() as conn:
        conn.execute(user_stats.insert(), rows)


def make_resource(
    config: SyncConfig,
    engine: Any,
    *,
    limit: int | None = None,
) -> Any:
    table = config["source"]["table"]
    # sqlalchemy backend → dict rows (friendly for add_map). Use pyarrow for
    # throughput in production and wrap transforms with to_pylist() if needed.
    source = sql_database(engine, table_names=[table], backend="sqlalchemy", chunk_size=1000)
    resource = source.resources[table]
    resource.apply_hints(
        table_name=config["collection"],
        primary_key=config["primary_key"],
        # SQLite returns naive datetimes; keep the cursor naive to avoid
        # tz-awareness mismatches on the first run.
        incremental=dlt.sources.incremental(
            cursor_path=config["timestamp_field"],
            initial_value=datetime(1970, 1, 1),
        ),
    )
    if limit is not None:
        resource.add_limit(limit, count_rows=True)

    transform = config.get("transform")
    if transform is not None:
        resource.add_map(transform)

    return typesense_adapter(
        resource,
        facet=config.get("facet"),
        sort=config.get("sort"),
        field_hints=config.get("field_hints"),
    )


def run(
    *,
    target: str = "all",
    limit: int | None = None,
    full_refresh: bool = False,
    dev_mode: bool = False,
    db_path: Path | None = None,
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        sqlite_path = db_path or Path(tmp) / "demo.db"
        if not sqlite_path.exists():
            _seed_sqlite(sqlite_path)
        engine = sa.create_engine(f"sqlite:///{sqlite_path}")

        # destination="typesense" also works once credentials are in secrets/env.
        pipeline = dlt.pipeline(
            pipeline_name="sql_incremental_typesense",
            destination=typesense(),
            dataset_name="sql_sync",
            dev_mode=dev_mode,
        )

        write_disposition = "replace" if full_refresh else "merge"
        for config in COLLECTIONS:
            if target != "all" and target != config["collection"]:
                continue
            adapted = make_resource(config, engine, limit=limit)
            info = pipeline.run(adapted, write_disposition=write_disposition)
            print(f"{config['collection']}: {info}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Incremental SQL → Typesense sync")
    choices = ["all"] + [c["collection"] for c in COLLECTIONS]
    parser.add_argument("--target", choices=choices, default="all")
    parser.add_argument("--limit", type=int, default=None, help="Cap rows (test runs)")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Replace the collection instead of merging",
    )
    parser.add_argument(
        "--dev-mode",
        action="store_true",
        help="Reset pipeline state between runs",
    )
    args = parser.parse_args()
    run(
        target=args.target,
        limit=args.limit,
        full_refresh=args.full_refresh,
        dev_mode=args.dev_mode,
    )
