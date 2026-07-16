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

- **Auto collection schema.** Collections are created with Typesense auto
  schema (`.*` field). Explicit typed field maps and `typesense_adapter`
  facet/sort/index hints are not applied.
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
