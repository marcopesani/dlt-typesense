# dlt-typesense

[![CI](https://github.com/marcopesani/dlt-typesense/actions/workflows/ci.yml/badge.svg)](https://github.com/marcopesani/dlt-typesense/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dlt-typesense.svg)](https://pypi.org/project/dlt-typesense/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/marcopesani/dlt-typesense/blob/main/LICENSE)

Typesense document destination for [dlt](https://dlthub.com). Load data into
Typesense collections with proper write dispositions (`append`, `replace`,
`merge`/`upsert`), not as a blind reverse-ETL sink.

## Install dlt with Typesense

```bash
pip install dlt-typesense
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add dlt-typesense
```

For local development:

```bash
git clone https://github.com/marcopesani/dlt-typesense.git
cd dlt-typesense
uv sync --group dev
```

## Destination capabilities

| Feature | Value |
|---------|-------|
| Preferred loader file format | `jsonl` |
| Supported loader file formats | `jsonl` (plus dlt's internal `reference` format for cleanup follow-up jobs) |
| Has case sensitive identifiers | True |
| Supported merge strategies | `upsert` (default), `insert-only` |
| Supported replace strategies | `truncate-and-insert` |
| Max identifier length | 255 |

`delete-insert` and `scd2` merge strategies and staging replace strategies
(`insert-from-staging`, `staging-optimized`) are **not supported** — there is
no SQL layer or staging dataset.

## Setup guide

1. Install the package (see above).
2. Add credentials to `.dlt/secrets.toml` (or the matching
   `DESTINATION__TYPESENSE__CREDENTIALS__*` env vars). The `api_key` is
   required; without it the run fails before any load starts. The key is never
   written to logs, reprs, or error messages.

```toml
[destination.typesense.credentials]
host = "localhost"      # default: localhost
port = 8108             # default: 8108
protocol = "http"       # default: http
api_key = "local-dev-key"
```

3. Define a resource and a pipeline. The destination resolves by short name
   (`destination="typesense"`) or by importing the factory:

```python
import dlt
from dlt_typesense import typesense


@dlt.resource(name="products", write_disposition="merge", primary_key="sku")
def products():
    yield {"sku": "A1", "title": "Widget", "price": 9.99}


pipeline = dlt.pipeline(
    pipeline_name="shop",
    destination=typesense(),  # or destination="typesense"
    dataset_name="catalog",
)
pipeline.run(products())
```

4. Start a local Typesense with `docker compose up -d` and run the examples in
   [`examples/`](https://github.com/marcopesani/dlt-typesense/tree/main/examples):
   `merge_pipeline.py` (upsert + nested orphan cleanup),
   `append_replace_pipeline.py`,
   `schema_hints_pipeline.py` (facets, vectors, epoch timestamps),
   `sql_incremental_sync_pipeline.py` (production-shaped SQL sync).

## `typesense_adapter`

By default collections use Typesense auto schema (a `.*` catch-all field).
`typesense_adapter` pins explicit typed fields for hinted columns and sets
collection-level options. Unhinted columns still fall through to `.*` auto.

**A `type` hint is a contract about both the schema and the document value.**
When you pin a dlt `timestamp`/`date` column to `int64`/`int32`, the load job
stores Unix epoch seconds (Typesense's recommended date representation —
range-filterable and sortable). When you pin a `decimal`/`wei` column to
`float`/`int64`/`int32`, the exact decimal string on the wire is parsed to that
numeric type. Apply the adapter to individual resources, not to a whole source.

```python
from dlt_typesense import typesense, typesense_adapter

pipeline.run(
    typesense_adapter(
        products,
        facet="category",  # shorthand for {"facet": True}
        sort=["price", "rating"],  # shorthand for {"sort": True}
        field_hints={
            "description": {"locale": "de", "infix": True},
            "last_update": {"type": "int64", "sort": True},  # timestamp → epoch
            "gltv_eur": {"type": "float", "sort": True},  # decimal → float
            "embedding": {"type": "float[]", "num_dim": 384},
            "summary_vec": {
                "type": "float[]",
                "embed": {
                    "from": ["title", "description"],
                    "model_config": {"model_name": "ts/e5-small"},
                },
            },
        },
        collection_hints={
            "default_sorting_field": "price",
            "token_separators": ["-"],
            "metadata": {"owner": "search-team"},
        },
    )
)
```

- `field_hints` accepts, per column: `type`, `facet`, `index`, `optional`,
  `sort`, `infix`, `locale`, `stem`, `stem_dictionary`, `num_dim`, `vec_dist`,
  `hnsw_params`, `reference`, `async_reference`, `cascade_delete`,
  `range_index`, `store`, `truncate_len`, `token_separators`,
  `symbols_to_index`, `embed`.
- `collection_hints` accepts: `default_sorting_field`, `token_separators`,
  `symbols_to_index`, `enable_nested_fields`, `metadata`, `synonym_sets`,
  `curation_sets`.
- `no_remove_orphans=True` disables orphan cleanup of nested-table documents
  under merge (see [Orphan cleanup](#orphan-cleanup-for-nested-tables)).
- Unknown parameter names raise `ValueError` when the adapter is called; value
  errors (e.g. a bad `locale` or `vec_dist`) surface as terminal errors from
  the server at load time.

### Cleaning rows with `add_map`

The type contract never auto-parses `text` columns into arrays or numbers —
that is data cleaning. Decode JSON-encoded strings (e.g. Redshift SUPER) with
dlt's native `add_map` before loading:

```python
import json as pyjson


def clean_row(row: dict) -> dict:
    raw = row.get("categories")
    row["categories"] = pyjson.loads(raw) if isinstance(raw, str) else (raw or [])
    row.setdefault("receive_marketing", False)
    return row


resource.add_map(clean_row)

pipeline.run(
    typesense_adapter(
        resource,
        field_hints={"categories": {"type": "string[]", "facet": True}},
    )
)
```

`add_map` sees whatever shape the source yields. With
`sql_database(backend="pyarrow")` items are Arrow tables, not dicts — either
use `backend="sqlalchemy"` for row-shaped items, or map the table
(`table.to_pylist()` inside an `add_yield_map`). See
[`examples/sql_incremental_sync_pipeline.py`](examples/sql_incremental_sync_pipeline.py).

## Write disposition

### Replace

`truncate-and-insert`: drop and recreate the collection, then import. A
concurrent reader can observe an empty collection window mid-replace.

### Merge

- `upsert` (default): document `id` is a deterministic uuid5 of the
  `primary_key` (or user `unique` columns). Re-runs update existing documents.
- `insert-only`: document `id` from `_dlt_id`; existing documents are never
  modified.

Set merge (and the primary key) from the first run; otherwise later merges
cannot reconcile documents created under append semantics. A merge table with
neither a `primary_key` nor a `unique` column fails terminally rather than
silently loading duplicates.

#### Orphan cleanup for nested tables

Nested lists become child collections (`orders` → `orders__items` →
`orders__items__parts`). Under merge (`upsert`), a follow-up job runs after
every load and deletes **orphaned child documents** — children whose parent
row was re-loaded but which were not re-written, i.e. elements that
disappeared from the parent's nested list:

```python
pipeline.run(orders())  # o1 has items [a, b]
pipeline.run(orders())  # o1 now has items [a]  → the "b" document is deleted
```

Semantics:

- **Scoped to the load.** Only root rows present in the load are cleaned up;
  children of parents that were not re-synced are never touched, so
  incremental loads stay incremental.
- **Emptied lists are handled.** Re-loading a parent whose list became empty
  deletes all of its child documents.
- **All nesting levels.** Grandchild collections (and deeper) are cleaned the
  same way.
- **Merge `upsert` only.** `insert-only` merge, `append`, and `replace` never
  run cleanup (`replace` recreates collections instead).
- **Opt out per resource** with
  `typesense_adapter(resource, no_remove_orphans=True)` — re-loaded parents
  then leave stale child documents behind, as in versions before cleanup
  existed.

How it works: after all files of a merge table chain finish loading, a
follow-up job collects the loaded root ids (`_dlt_id`) and, per child
collection, exports the current child ids for those roots
(`_dlt_root_id` filter), diffs them against the ids just loaded, and deletes
the stale ones with batched `filter_by` requests (200 ids per request). Cost
is proportional to the number of children of the re-loaded parents.

Cleanup keys on dlt's system columns only (`_dlt_id`, `_dlt_root_id`), which
requires root-key propagation — dlt enables it automatically for merge. If
your pipeline explicitly disables it, the cleanup job fails terminally; opt
out with `no_remove_orphans=True` instead.

### Append

Bulk import with `action=upsert`; document `id` from `_dlt_id`.

`skip` writes nothing for that table.

## Data loading

Tables map to Typesense collections (dataset-qualified, e.g. `catalog_products`);
rows become documents loaded via streamed JSONL
[bulk import](https://typesense.org/docs/latest/api/documents.html#import-documents).

### Data types

| dlt type | Typesense (unhinted) | With numeric `type` hint |
|----------|----------------------|--------------------------|
| `text` | `string` | no auto-parse (use `add_map`) |
| `bigint` | `int64` | as-is |
| `double` | `float` | as-is |
| `bool` | `bool` | as-is |
| `timestamp` / `date` | ISO-8601 `string` | `int64`/`int32` → Unix epoch seconds |
| `time` | ISO-8601 `string` | — |
| `decimal` / `wei` | exact `string` | `float`/`int64`/`int32` → parsed number |
| `binary` | base64 `string` | — |
| `json` | JSON `string` | native JSON when hinted `float[]`/`string[]`/`object`/… |

Pinning a timestamp to `float` is rejected (Typesense floats are 32-bit and
lose epoch-second precision). Use `int64` for range filters / sort, or `int32`
when the column must be `default_sorting_field` (valid until 2038).

### Dataset name

Collection names are `{dataset_name}{dataset_separator}{table_name}`
(separator defaults to `_`). Empty `dataset_name` yields the bare table name.
Names longer than 255 characters are truncated with a hash suffix.

### Reserved `id`

Typesense reserves the top-level document `id`, which this destination manages
(from `_dlt_id` or the merge key). A source column named `id` is renamed to
`__id` by the naming convention, so its value is preserved and never silently
overwritten.

### Schema hints lifecycle

Hints apply at collection creation only: first load, every `replace` run, and
dev-mode/full-refresh runs. Changing hints on an existing `append`/`merge`
collection has no effect until the collection is recreated (a log line notes
this). There is no schema PATCH/alter support — the safe path for production
schema changes is recreate under a new name and switch a collection alias.

## Additional destination options

Passed to `typesense(...)` or resolved from config (`destination.typesense.*`):

| Knob | Default | Purpose |
|------|---------|---------|
| `dataset_separator` | `"_"` | Separator between dataset and table in collection names |
| `client_batch_size` | `1000` | Documents per HTTP import request (files are streamed, never buffered whole) |
| `server_batch_size` | `40` | Typesense `batch_size` import query parameter |
| `import_action` | `"upsert"` | Import action. `emplace` updates only provided fields. **Do not use `create`** — it breaks dlt's whole-file retry idempotency |
| `connection_timeout_seconds` | `5.0` | Connect timeout (on credentials) |
| `read_timeout_seconds` | `180.0` | Read timeout for long import requests |
| `max_parallel_load_jobs` | `None` | Optional loader parallelism override |

### Run Typesense locally

```bash
docker compose up -d   # Typesense 30.2, API key local-dev-key
```

## Production pattern: incremental SQL sync

Real syncs look like `sql_database` + incremental cursor + merge + hints +
optional `add_map`. A declarative multi-collection config keeps each sync
spec in one place:

```python
COLLECTIONS = [
    {
        "collection": "users",
        "source": {"table": "user_stats"},
        "primary_key": "user_id",
        "timestamp_field": "last_update",
        "field_hints": {"last_update": {"type": "int64", "sort": True}},
        "transform": decode_categories,
    },
]
```

See [`examples/sql_incremental_sync_pipeline.py`](examples/sql_incremental_sync_pipeline.py)
for a runnable SQLite version with `--target`, `--limit`, `--full-refresh`,
and `--dev-mode`.

### dbt support

The Typesense destination does not support dbt.

### Syncing of `dlt` state

The Typesense destination supports syncing of the `dlt` state (incremental
cursors survive clean runners).

## Development

```bash
uv sync --group dev
uv run ruff check .
uv run ruff format --check .
uv run basedpyright
uv run pytest                 # unit tests (no server)

docker compose up -d          # start Typesense for integration tests
uv run pytest -m integration  # integration tests (fail, never skip, if unreachable)
```

See [docs/architecture.md](docs/architecture.md) for module seams and scale notes.

## Contributing

See [CONTRIBUTING.md](https://github.com/marcopesani/dlt-typesense/blob/main/CONTRIBUTING.md).
By contributing you agree to the [Developer Certificate of Origin](https://developercertificate.org/)
(sign off commits with `Signed-off-by`).

## License

[Apache License 2.0](https://github.com/marcopesani/dlt-typesense/blob/main/LICENSE)
