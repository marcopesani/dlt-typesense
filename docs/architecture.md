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
- **lancedb** — orphan follow-up jobs for nested child tables (the template for `TypesenseRemoveOrphansJob`)

## Module map

```
factory.py           → Destination entry + capabilities
configuration.py     → credentials + batch/timeout knobs + official client factory
typesense_client.py  → storage init, schema update, state sync, create_load_job,
                       orphan-cleanup follow-up scheduling
load_jobs.py         → TypesenseLoadJob (chunked import) +
                       TypesenseRemoveOrphansJob (merge orphan cleanup)
value_conversion.py  → wire-value converters for typed field hints (epoch, decimal)
type_mapper.py       → collection schema build (typed pinned fields + `.*` auto)
typesense_adapter.py → per-field + collection-level schema hints
destinations/        → short-name resolution (destination="typesense")
exceptions.py        → terminal vs transient error taxonomy + SDK error mapping
```

All Typesense I/O goes through the official [`typesense`](https://pypi.org/project/typesense/)
client directly. Its built-in retries are disabled (`num_retries=0`): retry
ownership stays with dlt's whole-job load retry, which is why the import
action is restricted to idempotent `upsert`/`emplace`.

## Schema hints

`typesense_adapter` stores hints in the dlt schema and returns the resource:

- `x-typesense-field` (per column) — a dict of Typesense field params
  (`type`, `facet`, `sort`, `locale`, `num_dim`, `embed`, …), attached via
  `apply_hints(columns=...)` so it travels with the normalized column name.
- `x-typesense-collection` (per table) — collection params
  (`default_sorting_field`, `token_separators`, `symbols_to_index`,
  `enable_nested_fields`, `metadata`, `synonym_sets`, `curation_sets`),
  attached via `additional_table_hints`.

A `type` in the field hint is a contract about both the schema and the
document value: the load job converts dlt `timestamp`/`date` wire strings to
Unix epoch seconds when pinned to `int64`/`int32`, and parses `decimal`/`wei`
strings when pinned to a numeric Typesense type.

`typesense_client._collection_schema` builds data-table schemas with
`type_mapper.collection_schema_from_table`: hinted columns become pinned typed
fields (dlt type → wire-format Typesense type unless a `type` hint overrides),
followed by the `.*` auto catch-all. Hints apply at collection **create** time
only (first load, `replace` recreates); there is no schema alter path.
`load_jobs` skips JSON-stringification for `json` columns whose type override
is non-string, so `float[]` vector fields arrive as native lists.

## Write dispositions

| Disposition | Typesense mechanism | Document `id` | Import `action` |
|-------------|---------------------|---------------|-----------------|
| append | bulk JSONL import | `_dlt_id` | `upsert` |
| replace | drop + recreate collection, then import | `_dlt_id` | `upsert` |
| merge (`upsert`) | bulk import keyed on PK | uuid5 of `primary_key` | `upsert` / `emplace` |
| merge (`insert-only`) | bulk import, append code path | `_dlt_id` | `upsert` |

Always prefer `upsert`/`emplace` over `create` so dlt’s whole-file retry is idempotent.

### Orphan cleanup (merge `upsert`)

Nested (child) tables keep their documents keyed on `_dlt_id`, so re-loading a
root row whose nested list shrank would leave stale children behind. Following
the lancedb template:

- `TypesenseClient.create_table_chain_completed_followup_jobs` emits one
  `ReferenceFollowupJobRequest` per merge (`upsert`) chain with nested tables,
  pointing at the chain's completed JSONL files. The `reference` loader format
  is declared in the factory capabilities for exactly this internal routing.
- `create_load_job` routes `.reference` files to `TypesenseRemoveOrphansJob`,
  which is scheduled/retried by dlt like any other load job (idempotent:
  re-running after a partial failure converges).
- The job reads root ids (`_dlt_id`) from the root files; per nested
  collection it exports current child ids for those roots (filter on
  `_dlt_root_id`, deterministic because merge propagates the root key), diffs
  against the loaded ids, and deletes stale ids with bounded `filter_by`
  batches — including grandchildren, and children of roots whose lists were
  emptied (jobless nested tables are resolved from the schema, not the files).
- Gate: merge + `upsert` strategy only, skipped for `insert-only`, single-table
  chains, and resources adapted with `no_remove_orphans=True`
  (`x-typesense-no-remove-orphans` table hint).

## Scale

- Set `recommended_file_size` so large tables split into parallel load jobs
- Stream JSONL in client-sized chunks; do not buffer entire files
- Parse every import response line (HTTP 200 ≠ full success)
- Replace uses drop/recreate — not delete-by-filter over millions of rows
- Orphan cleanup batches every export/delete filter to 200 ids so `filter_by`
  strings stay far below URL length limits; its cost scales with the number of
  children of the re-loaded roots, not with collection size
