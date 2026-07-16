# dlt-typesense

[![CI](https://github.com/marcopesani/dlt-typesense/actions/workflows/ci.yml/badge.svg)](https://github.com/marcopesani/dlt-typesense/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**Typesense document destination for [dlt](https://dlthub.com)** — load data into Typesense collections with proper write dispositions (`append`, `replace`, `merge`/`upsert`), not as a blind reverse-ETL sink.

> Status: **pre-alpha scaffold**. Package layout, capabilities, and job-client seams are in place; document import is not implemented yet.

## Why a full destination?

Typesense is treated as a **document database**:

| Disposition | Behavior (planned) |
|-------------|--------------------|
| `append` | Bulk import; document `id` from `_dlt_id`; `action=upsert` |
| `replace` | Truncate collection (drop/recreate; later alias-swap), then import |
| `merge` | Upsert by `primary_key` → deterministic Typesense `id` |

This matches dlt’s non-SQL destinations (e.g. Qdrant): `JobClientBase` + JSONL load jobs + `WithStateSync` for incremental pipelines. Suitable for multi-million-row syncs via file sharding and parallel import jobs.

See [docs/architecture.md](docs/architecture.md) for module seams and scale notes.

## Install (development)

Requires [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/marcopesani/dlt-typesense.git
cd dlt-typesense
uv sync --group dev
```

## Intended usage (once implemented)

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

Secrets (planned) in `.dlt/secrets.toml`:

```toml
[destination.typesense.credentials]
host = "localhost"
port = 8108
protocol = "http"
api_key = "xyz"
```

## Development

```bash
uv sync --group dev
uv run ruff check .
uv run ruff format --check .
uv run basedpyright
uv run pytest
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). By contributing you agree to the [Developer Certificate of Origin](https://developercertificate.org/) (sign off commits with `Signed-off-by`).

## License

[Apache License 2.0](LICENSE)
