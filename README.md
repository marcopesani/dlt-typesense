# dlt-typesense

[![CI](https://github.com/marcopesani/dlt-typesense/actions/workflows/ci.yml/badge.svg)](https://github.com/marcopesani/dlt-typesense/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**Typesense document destination for [dlt](https://dlthub.com)** — load data into Typesense collections with proper write dispositions (`append`, `replace`, `merge`/`upsert`), not as a blind reverse-ETL sink.

## Why a full destination?

Typesense is treated as a **document database**. dlt tables map to Typesense
collections (dataset-qualified, e.g. `catalog_products`); rows become documents
loaded via streamed JSONL [bulk import](https://typesense.org/docs/latest/api/documents.html#import-documents).

| Disposition | Behavior |
|-------------|----------|
| `append` | Bulk import; document `id` from `_dlt_id`; `action=upsert` |
| `replace` (`truncate-and-insert`) | Drop + recreate the collection, then import |
| `merge` (`upsert`) | Upsert keyed by `primary_key` → deterministic Typesense `id` (uuid5) |
| `merge` (`insert-only`) | Insert keyed by `_dlt_id`; existing documents never modified |
| `skip` | Nothing is written for that table |

Merge strategies are `["upsert", "insert-only"]` (upsert is the default). The
`delete-insert` and `scd2` merge strategies and the staging replace strategies
(`insert-from-staging`, `staging-optimized`) are **not supported** and are
rejected with a clear capabilities error — there is no SQL layer or staging
dataset. This matches dlt's non-SQL destinations (Qdrant; `insert-only` follows
LanceDB): `JobClientBase` + JSONL load jobs + `WithStateSync` for incremental
pipelines. Suitable for multi-million-row syncs via file sharding and parallel
import jobs.

See [docs/architecture.md](docs/architecture.md) for module seams and scale notes.

## Install

Requires [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/marcopesani/dlt-typesense.git
cd dlt-typesense
uv sync --group dev
```

## Usage

```python
import dlt
from dlt_typesense import typesense

@dlt.resource(name="products", write_disposition="merge", primary_key="sku")
def products():
    yield {"sku": "A1", "title": "Widget", "price": 9.99}

pipeline = dlt.pipeline(
    pipeline_name="shop",
    destination=typesense(),
    dataset_name="catalog",
)
pipeline.run(products())
```

Runnable examples are in [`examples/`](examples/). Start a local server with
`docker compose up -d` first.

### Schema customization (`typesense_adapter`)

By default collections use Typesense auto schema (a `.*` catch-all field).
`typesense_adapter` pins explicit typed fields for hinted columns and sets
collection-level options, covering the full
[Typesense collections API](https://typesense.org/docs/30.2/api/collections.html)
surface. Unhinted columns still fall through to `.*` auto, so schema evolution
keeps working.

```python
from dlt_typesense import typesense, typesense_adapter

pipeline.run(
    typesense_adapter(
        products,
        facet="category",                      # shorthand for {"facet": True}
        sort=["price", "rating"],              # shorthand for {"sort": True}
        field_hints={
            "description": {"locale": "de", "infix": True},
            "embedding": {"type": "float[]", "num_dim": 384},   # vector field
            "summary_vec": {                                    # auto-embedding
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
  `reference`, `range_index`, `store`, `truncate_len`, `token_separators`,
  `symbols_to_index`, `embed`.
- `collection_hints` accepts: `default_sorting_field`, `token_separators`,
  `symbols_to_index`, `enable_nested_fields`, `metadata`.
- Unknown parameter names raise `ValueError` when the adapter is called; value
  errors (e.g. a bad `locale` or `vec_dist`) surface as terminal errors from
  the server at load time.

### Credentials

Credentials resolve from `.dlt/secrets.toml` (or the matching
`DESTINATION__TYPESENSE__CREDENTIALS__*` env vars). The `api_key` is required;
without it the run fails with dlt's missing-config error before any load starts.
The key is never written to logs, reprs, or error messages.

```toml
[destination.typesense.credentials]
host = "localhost"      # default: localhost
port = 8108             # default: 8108
protocol = "http"       # default: http
api_key = "local-dev-key"
```

### Configuration knobs

Passed to `typesense(...)` or resolved from config (`destination.typesense.*`):

| Knob | Default | Purpose |
|------|---------|---------|
| `dataset_separator` | `"_"` | Separator between dataset and table in collection names (≤ 255 chars) |
| `client_batch_size` | `1000` | Documents per HTTP import request (client-side chunking; files are streamed, never buffered whole) |
| `server_batch_size` | `40` | Typesense `batch_size` import query parameter |
| `import_action` | `"upsert"` | Import action. `emplace` updates only provided fields. **Do not use `create`** — it breaks dlt's whole-file retry idempotency |
| `connection_timeout_seconds` | `5.0` | Connect timeout |
| `read_timeout_seconds` | `180.0` | Read timeout for long import requests |

## Behavior notes

- **Auto schema + pinned fields.** Collections are created with a `.*` auto
  catch-all; columns hinted via `typesense_adapter` are pinned as explicit
  typed fields ahead of it. Pinned types follow the wire format below unless
  overridden with a `type` field hint.
- **Hints apply at collection creation only.** First load, every `replace`
  run, and dev-mode/full-refresh runs (re)create collections with the current
  hints. Changing hints on an existing `append`/`merge` collection has no
  effect until the collection is recreated (a log line notes this). There is
  no schema PATCH/alter support yet.
- **`default_sorting_field`** must resolve to a numeric (`int32`/`int64`/
  `float`) non-optional field — the destination pins it, forces it
  non-optional, and fails terminally otherwise. Note dlt `decimal`/`timestamp`
  columns are stored as strings; use a `double`/`bigint` column or override
  the field `type`. Rows with a null in that column fail at import.
- **Vector/native fields.** An array or object `type` hint (`float[]`,
  `string[]`, `object`, `geopoint`, …) marks the dlt column as `json` so lists
  stay inline instead of becoming child collections, and the value is imported
  as native JSON rather than a string — this is how `num_dim` vectors work
  end-to-end. With `import_action="emplace"`, partial updates that omit a
  non-optional pinned field are rejected by the server.
- **Auto-embedding (`embed`) fields** make the server download the model at
  collection creation — expect a slow first create.
- **Child tables under merge.** Merge updates root documents in place. When
  nested-list items disappear from the source, orphaned child-table documents
  are not deleted. Root documents stay correct.
- **Reserved `id`.** Typesense reserves the top-level document `id`, which this
  destination manages (from `_dlt_id` or the merge key). A *source* column named
  `id` is renamed to `__id` by the naming convention, so its value is preserved
  and never silently overwritten.
- **Type representations.** dlt's JSONL wire format is stored as-is under auto
  schema: `decimal`/`wei` as exact strings, `timestamp`/`date`/`time` as
  ISO-8601 strings, `binary` as base64, `json` columns as one canonical JSON
  string. Bigints round-trip exactly as Typesense int64.
- **Merge without a key.** A `merge` table with neither a `primary_key` nor a
  `unique` column cannot form a deterministic id and fails with a terminal
  error rather than silently loading duplicates.
- **Replace is drop + recreate.** A concurrent reader can observe an empty
  collection window mid-replace.

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

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). By contributing you agree to the [Developer Certificate of Origin](https://developercertificate.org/) (sign off commits with `Signed-off-by`).

## License

[Apache License 2.0](LICENSE)
