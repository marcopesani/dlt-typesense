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
| Merge | Strategy: `upsert` |
| Replace | Strategy: `truncate-and-insert` |

Secondary references:

- **weaviate** — batch insert with per-object failure aggregation (Typesense import returns HTTP 200 with per-line errors)
- **lancedb** — orphan follow-up jobs for nested child tables (phase 2)

## Module map

```
factory.py           → Destination entry + capabilities
configuration.py     → credentials + batch/timeout knobs
typesense_client.py  → storage init, schema update, state sync, create_load_job
load_jobs.py         → TypesenseLoadJob (+ RemoveOrphansJob stub)
rest_client.py       → streaming HTTP import / collection CRUD
type_mapper.py       → auto-schema first; typed map later
typesense_adapter.py → facet/sort/index hints
exceptions.py        → terminal vs transient import errors
```

## Write dispositions

| Disposition | Typesense mechanism | Document `id` | Import `action` |
|-------------|---------------------|---------------|-----------------|
| append | bulk JSONL import | `_dlt_id` | `upsert` |
| replace | drop+recreate collection (later: alias-swap), then import | `_dlt_id` | `upsert` |
| merge | bulk import keyed on PK | uuid5 / hash of `primary_key` | `upsert` / `emplace` |

Always prefer `upsert`/`emplace` over `create` so dlt’s whole-file retry is idempotent.

## Scale

- Set `recommended_file_size` so large tables split into parallel load jobs
- Stream JSONL in client-sized chunks; do not buffer entire files
- Parse every import response line (HTTP 200 ≠ full success)
- Replace via drop/recreate or alias-swap — not delete-by-filter over millions

## v1 vs phase 2

**v1:** root-level documents; auto collection schema; append / replace / merge upsert.

**Phase 2:** typed field map + `typesense_adapter` hints; alias-swap replace; `TypesenseRemoveOrphansJob` for nested child tables.
