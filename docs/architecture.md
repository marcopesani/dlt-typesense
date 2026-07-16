# Architecture

`dlt-typesense` is a **document-database destination** for [dlt](https://dlthub.com), not a reverse-ETL sink.

## Why not `@dlt.destination`?

The custom sink decorator is for blind API pushes. It does not provide:

- write dispositions (`append` / `replace` / `merge`)
- load-job file sharding and parallel imports
- stored schema / pipeline state (`WithStateSync`)
- retry-safe whole-file job semantics

For multi-million-row syncs into Typesense collections, those seams are required.

## Template

The package mirrors dlt’s **qdrant** destination:

| Concern | Approach |
|---------|----------|
| Factory | `Destination` subclass in `factory.py` |
| Client | `JobClientBase` + `WithStateSync` |
| Loads | `RunnableLoadJob` reading JSONL packages |
| Format | Preferred loader format: `jsonl` |
| Merge | Strategies: `upsert` (default, list-first), `insert-only` (lancedb precedent) |
| Replace | Strategy: `truncate-and-insert` (drop + recreate) |

Secondary references:

- **weaviate** — batch insert with per-object failure aggregation (Typesense import returns HTTP 200 with per-line errors)
- **lancedb** — orphan follow-up jobs for nested child tables (not implemented here)

## Module map

```
factory.py           → Destination entry + capabilities
configuration.py     → credentials + batch/timeout knobs + official client factory
typesense_client.py  → storage init, schema update, state sync, create_load_job
load_jobs.py         → TypesenseLoadJob + chunked import over the official client
type_mapper.py       → auto-schema collection create
typesense_adapter.py → facet/sort/index hints (not wired)
exceptions.py        → terminal vs transient error taxonomy + SDK error mapping
```

All Typesense I/O goes through the official [`typesense`](https://pypi.org/project/typesense/)
client directly. Its built-in retries are disabled (`num_retries=0`): retry
ownership stays with dlt's whole-job load retry, which is why the import
action is restricted to idempotent `upsert`/`emplace`.

## Write dispositions

| Disposition | Typesense mechanism | Document `id` | Import `action` |
|-------------|---------------------|---------------|-----------------|
| append | bulk JSONL import | `_dlt_id` | `upsert` |
| replace | drop + recreate collection, then import | `_dlt_id` | `upsert` |
| merge (`upsert`) | bulk import keyed on PK | uuid5 of `primary_key` | `upsert` / `emplace` |
| merge (`insert-only`) | bulk import, append code path | `_dlt_id` | `upsert` |

Always prefer `upsert`/`emplace` over `create` so dlt’s whole-file retry is idempotent.

## Scale

- Set `recommended_file_size` so large tables split into parallel load jobs
- Stream JSONL in client-sized chunks; do not buffer entire files
- Parse every import response line (HTTP 200 ≠ full success)
- Replace uses drop/recreate — not delete-by-filter over millions of rows
