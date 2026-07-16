# Acceptance Criteria

This document is the behavioral contract for `dlt-typesense`. Implementation is done when every
`v1` criterion below is `Verified`; `phase-2` criteria gate the second milestone (see
[architecture.md](architecture.md) for the v1 / phase-2 split).

Scope: the destination must (1) load data from many different types of sources — covered by
expressing criteria over the *data shapes, types, and behaviors* that vary between sources, plus
three real-source smoke criteria — and (2) support every update strategy dlt offers document
databases: `append`, `replace` (`truncate-and-insert`), `merge` (`upsert` and `insert-only`),
and `skip`. The `delete-insert` and `scd2` merge strategies and the staging replace strategies
are deliberately unsupported (no SQL layer, no staging dataset); rejecting them cleanly is
itself a criterion.

## 0. Conventions

- **IDs** are `AC-<GROUP>-<NN>`, assigned once, never renumbered or reused. A removed criterion
  stays in this document with status `Withdrawn` and a one-line reason.
- **Tags**: `v1` or `phase-2`, consistent with [architecture.md](architecture.md).
- **Status**: `Proposed` → `Accepted` → `Verified`. A criterion becomes `Verified` only when an
  automated test (or, exceptionally, a documented manual procedure) covers it.
- **Verified by**: pytest node ids, filled in as tests land. Test docstrings declare coverage
  with the grep-able token `Covers: AC-MERGE-02` (comma-separate multiple IDs). Many-to-many is
  allowed: one test may cover several criteria; hazard criteria may be covered twice (unit test
  with mocked responses + integration test).
- Each criterion is written **Given / When / Then**. "Run the pipeline" means
  `pipeline.run(...)` with `destination=typesense(...)` against the test server from §1.
- Current totals: **80 criteria — 71 `v1`, 9 `phase-2`**.

## 1. Test environment contract

Integration criteria run against a disposable local Typesense started from the repo-root
[docker-compose.yml](../docker-compose.yml) (`typesense/typesense:29.0` — typesense-py 2.0.0
requires server ≥ v28).

- Tests discover the server via plain env vars with defaults matching the compose file:
  `TYPESENSE_HOST=localhost`, `TYPESENSE_PORT=8108`, `TYPESENSE_PROTOCOL=http`,
  `TYPESENSE_API_KEY=local-dev-key`. Tests build `TypesenseCredentials` from these explicitly —
  never via dlt config resolution — so a contributor's own `.dlt/` files cannot leak in.
- Every test uses a unique `dataset_name` (uuid suffix) and tears down via `drop_storage`
  (or `pipeline.drop()`), so tests can run in parallel against one server.
- When `pytest -m integration` is explicitly requested and the server is unreachable, tests
  **fail** — they never silently skip. (Default `pytest` excludes the `integration` marker.)
- CI runs the integration suite in a dedicated job with a Typesense **service container**
  configured purely via `TYPESENSE_*` env vars; readiness is polled from the runner
  (the image ships no curl/wget).

---

## 2. AC-CAP — Factory & capabilities contract *(unit-testable, no server)*

#### AC-CAP-01 — jsonl is the only loader format `v1` `Verified`
**Given** the `typesense` factory, **when** capabilities are read, **then**
`preferred_loader_file_format == "jsonl"` and `supported_loader_file_formats == ["jsonl"]`,
and a pipeline run with `loader_file_format="parquet"` fails before any load job starts.
Verified by: tests/unit/test_factory.py::test_factory_capabilities, tests/unit/test_factory.py::test_non_jsonl_loader_format_rejected_before_load

#### AC-CAP-02 — merge strategies are `["upsert", "insert-only"]`, upsert first `v1` `Verified`
**Given** the factory capabilities, **then**
`supported_merge_strategies == ["upsert", "insert-only"]`. Ordering is semantic: dlt's
`resolve_merge_strategy` defaults to the **first** entry when a merge table declares no
explicit strategy, so `upsert` must stay first or default merge behavior silently changes.
Verified by: tests/unit/test_factory.py::test_factory_capabilities

#### AC-CAP-03 — replace strategy is `truncate-and-insert` only `v1` `Verified`
**Given** the factory capabilities, **then**
`supported_replace_strategies == ["truncate-and-insert"]`, and configuring
`replace_strategy="staging-optimized"` or `"insert-from-staging"` fails with a clear
capabilities error naming the requested strategy and the supported list.
Verified by: tests/unit/test_configuration.py::test_unsupported_replace_strategy_rejected, tests/unit/test_factory.py::test_factory_capabilities

#### AC-CAP-04 — identifier limits are declared and enforced `v1` `Verified`
**Given** the factory capabilities, **then** `max_identifier_length == 255` and
`max_column_identifier_length == 255`, and the naming convention emits Typesense-safe
collection/field names within those bounds.
Verified by: tests/unit/test_factory.py::test_factory_capabilities

#### AC-CAP-05 — no DDL transactions, no staging, 64 MB sharding hint `v1` `Verified`
**Given** the factory capabilities, **then** `supports_ddl_transactions is False`, no staging
dataset is required or used, and `recommended_file_size == 64_000_000` so the normalizer
shards large tables into multiple load-job files.
Verified by: tests/unit/test_factory.py::test_factory_capabilities

#### AC-CAP-06 — credentials resolve from dlt config/env `v1` `Verified`
**Given** credentials supplied via `.dlt/secrets.toml` or env vars
(`DESTINATION__TYPESENSE__CREDENTIALS__API_KEY` etc.), **when** a pipeline is created,
**then** they resolve with defaults `host="localhost"`, `port=8108`, `protocol="http"`;
**and given** no `api_key` anywhere, **then** the run fails with dlt's missing-config error
naming the field, before any load starts.
Verified by: tests/unit/test_configuration.py::test_credentials_defaults, tests/unit/test_configuration.py::test_credentials_resolve_from_env_with_defaults, tests/unit/test_configuration.py::test_missing_api_key_makes_config_partial, tests/unit/test_configuration.py::test_missing_api_key_raises_naming_the_field, tests/unit/test_configuration.py::test_present_api_key_resolves

#### AC-CAP-07 — reprs never leak the API key `v1` `Verified`
**Given** a resolved configuration, **then** `fingerprint()` is stable for the same
protocol/host/port, and `fingerprint()`, `physical_location()`, `__str__`, and exception
messages never contain the `api_key` value.
Verified by: tests/unit/test_configuration.py::test_api_key_never_leaks, tests/unit/test_configuration.py::test_fingerprint_discriminates_connection_tuple, tests/unit/test_configuration.py::test_fingerprint_stable_and_key_independent, tests/unit/test_configuration.py::test_physical_location_shows_connection_only

## 3. AC-PROTO — Storage lifecycle protocol *(integration)*

#### AC-PROTO-01 — `initialize_storage` creates system collections, idempotently `v1` `Verified`
**Given** a fresh server and dataset, **when** `initialize_storage()` runs, **then** the dlt
system collections (version, loads, state — dataset-qualified) exist; **and when** it runs a
second time, **then** it succeeds without error or data loss.
Verified by: tests/integration/test_protocol.py::test_initialize_storage_creates_system_collections_idempotently

#### AC-PROTO-02 — `is_storage_initialized` transitions correctly `v1` `Verified`
**Given** a fresh dataset, **then** `is_storage_initialized()` is `False`; **when** storage is
initialized, **then** it returns `True`.
Verified by: tests/integration/test_protocol.py::test_is_storage_initialized_transitions

#### AC-PROTO-03 — `truncate_tables` empties exactly the listed collections `v1` `Verified`
**Given** two populated collections A and B, **when**
`initialize_storage(truncate_tables=["A"])` runs, **then** A is empty (but exists) and B is
untouched.
Verified by: tests/integration/test_protocol.py::test_truncate_tables_empties_only_listed

#### AC-PROTO-04 — `drop_storage` removes everything and allows a clean restart `v1` `Verified`
**Given** a dataset with data and system collections, **when** `drop_storage()` runs, **then**
all the dataset's collections (including system collections) are gone and
`is_storage_initialized()` is `False`; **and when** the same pipeline runs again from scratch,
**then** it succeeds.
Verified by: tests/integration/test_protocol.py::test_drop_storage_removes_everything_and_allows_restart

#### AC-PROTO-05 — `update_stored_schema` creates tables and no-ops on unchanged hash `v1` `Verified`
**Given** a new schema, **when** `update_stored_schema()` runs, **then** collections for new
tables exist and the schema version is recorded; **and when** it runs again with an unchanged
schema hash, **then** no writes occur.
Verified by: tests/integration/test_protocol.py::test_update_stored_schema_creates_tables_and_noops_on_unchanged_hash

#### AC-PROTO-06 — dataset qualification and isolation `v1` `Verified`
**Given** `dataset_name="catalog"` and `dataset_separator="_"`, **then** table `products` maps
to collection `catalog_products`; an empty dataset yields bare table names; **and given** two
datasets on one server, **when** one is loaded and dropped, **then** the other's collections
and documents are untouched.
Verified by: tests/unit/test_factory.py::test_empty_dataset_yields_bare_names, tests/integration/test_protocol.py::test_dataset_qualification_and_isolation

#### AC-PROTO-07 — `complete_load` records the load id `v1` `Verified`
**Given** a successful `pipeline.run`, **then** the loads collection contains a completed
record for the load id, queryable from Typesense.
Verified by: tests/integration/test_protocol.py::test_complete_load_records_load_id

## 4. AC-STATE — State & schema sync (`WithStateSync`)

#### AC-STATE-01 — stored schema round-trips by name and hash `v1` `Verified`
**Given** a schema stored by `update_stored_schema`, **then** `get_stored_schema()` and
`get_stored_schema_by_hash(version_hash)` return it with identical content and version info.
Verified by: tests/integration/test_protocol.py::test_update_stored_schema_creates_tables_and_noops_on_unchanged_hash, tests/integration/test_state_sync.py::test_stored_schema_round_trips_by_name_and_hash

#### AC-STATE-02 — state is visible only after `complete_load` `v1` `Verified`
**Given** pipeline state written during a load package, **then** `get_stored_state(pipeline_name)`
does not return it until the load is completed; after completion it returns the newest state.
Verified by: tests/integration/test_state_sync.py::test_get_stored_state_returns_newest_committed, tests/integration/test_state_sync.py::test_state_visible_only_after_complete_load

#### AC-STATE-03 — incremental second run loads only the delta `v1` `Verified`
**Given** a resource with `dlt.sources.incremental` over a cursor column, **when** run 1 loads
N rows and the source then gains M new rows, **then** run 2 loads exactly M documents.
Verified by: tests/integration/test_state_sync.py::test_incremental_second_run_loads_only_delta

#### AC-STATE-04 — second-machine restore `v1` `Verified`
**Given** a completed incremental pipeline and a **wiped local pipeline working directory**
(simulating a new machine), **when** the pipeline is recreated and `sync_destination()` runs,
**then** state and schema are restored from Typesense and the next incremental run continues
from the stored cursor without re-loading old rows.
Verified by: tests/integration/test_state_sync.py::test_second_machine_restore

#### AC-STATE-05 — schema evolution bumps the stored version `v1` `Verified`
**Given** run 2 adds a column, **then** the stored schema version increases and **both** the
old and new schema versions are retrievable by their hashes.
Verified by: tests/integration/test_state_sync.py::test_schema_evolution_bumps_version

## 5. AC-APPEND — Append disposition

#### AC-APPEND-01 — first run loads all rows with dlt system fields `v1` `Verified`
**Given** a flat resource with `write_disposition="append"`, **when** run once, **then** the
collection's document count equals the source row count and every document carries `_dlt_id`
and `_dlt_load_id`; document `id` derives from `_dlt_id`.
Verified by: tests/unit/test_load_job_id.py::test_id_fields_append_returns_none, tests/unit/test_load_job_id.py::test_id_from_dlt_id_when_no_key, tests/unit/test_load_job_id.py::test_iter_documents_sets_id_from_dlt_id, tests/integration/test_dispositions.py::test_append_first_run_loads_all_with_dlt_fields

#### AC-APPEND-02 — re-running appends again (accumulation is the contract) `v1` `Verified`
**Given** the same source data, **when** the pipeline runs twice, **then** the count doubles —
append never deduplicates across runs.
Verified by: tests/integration/test_dispositions.py::test_append_accumulates_across_runs

#### AC-APPEND-03 — same-package retry is idempotent `v1` `Verified`
**Given** a load package whose job already imported (fully or partially) before a simulated
failure, **when** dlt retries the same job file, **then** the final count equals the source row
count — `id=_dlt_id` + `action=upsert` make whole-file retries safe.
Verified by: tests/integration/test_dispositions.py::test_whole_file_retry_is_idempotent

#### AC-APPEND-04 — append across schema evolution `v1` `Verified`
**Given** run 2 adds a new column, **when** both runs complete, **then** old documents are
untouched, new documents carry the new field, and the load succeeds under auto-schema.
Verified by: tests/integration/test_dispositions.py::test_append_across_schema_evolution

## 6. AC-REPLACE — Replace disposition

#### AC-REPLACE-01 — second run leaves only new data `v1` `Verified`
**Given** `write_disposition="replace"`, **when** run 1 loads N rows and run 2 loads M
different rows, **then** exactly the M run-2 documents exist afterwards.
Verified by: tests/integration/test_dispositions.py::test_replace_leaves_only_new_data

#### AC-REPLACE-02 — schema change survives the recreate `v1` `Verified`
**Given** run 2 drops a column and changes another's type, **when** replace runs (drop +
recreate + import), **then** the load succeeds and no stale field definitions or documents
survive from run 1.
Verified by: tests/integration/test_dispositions.py::test_replace_survives_schema_change

#### AC-REPLACE-03 — zero-row source still truncates `v1` `Verified`
**Given** a populated collection, **when** a replace run yields zero rows, **then** the
collection ends empty (but exists).
Verified by: tests/integration/test_dispositions.py::test_replace_zero_rows_truncates

#### AC-REPLACE-04 — child tables are replaced too `v1` `Verified`
**Given** a resource with nested lists (child collections), **when** replace runs again with
different data, **then** child collections contain only run-2 documents — no orphaned child
docs from run 1.
Verified by: tests/integration/test_dispositions.py::test_replace_replaces_child_tables

#### AC-REPLACE-05 — alias-swap replace is atomic for readers `phase-2` `Proposed`
**Given** alias-based replace, **when** a replace load is in progress, **then** searches
against the alias see the complete old data until the new collection is fully imported, then
atomically the new; **and given** a failed load, **then** the alias keeps serving the old
collection.
Verified by: —

## 7. AC-MERGE — Merge disposition

#### AC-MERGE-01 — unsupported merge strategies fail fast, writing nothing `v1` `Verified`
**Given** a resource with `write_disposition={"disposition": "merge", "strategy": "scd2"}` (or
`"delete-insert"`), **when** the pipeline runs, **then** it fails with dlt's
`DestinationCapabilitiesException` naming the strategy and the supported list
(`['upsert', 'insert-only']`), and no collection or document was written.
Verified by: tests/integration/test_merge.py::test_unsupported_strategy_fails_writing_nothing

#### AC-MERGE-02 — deterministic document id from primary key `v1` `Verified`
**Given** `primary_key="sku"`, **when** the same row loads in two independent pipeline runs,
**then** it produces the same Typesense document `id` (uuid5/hash of the PK value) both times.
Verified by: tests/unit/test_load_job_id.py::test_id_fields_merge_uses_primary_key, tests/unit/test_load_job_id.py::test_merge_id_depends_on_collection, tests/unit/test_load_job_id.py::test_merge_id_is_deterministic_across_runs, tests/integration/test_merge.py::test_deterministic_id_from_primary_key

#### AC-MERGE-03 — updates happen in place `v1` `Verified`
**Given** run 2 re-yields the same PKs with changed non-key fields, **when** it completes,
**then** the documents show the new values and the total count is unchanged.
Verified by: tests/integration/test_merge.py::test_updates_happen_in_place

#### AC-MERGE-04 — compound primary keys `v1` `Verified`
**Given** `primary_key=["tenant", "sku"]`, **then** the document id derives from the full
ordered key tuple: rows differing in any component are distinct documents, and updates match
the right document.
Verified by: tests/unit/test_load_job_id.py::test_compound_primary_key, tests/integration/test_merge.py::test_compound_primary_keys

#### AC-MERGE-05 — key fallback and no-key behavior `v1` `Verified`
**Given** a merge resource with no `primary_key` but a column hinted `unique`, **then** that
column keys the upsert (`_id_fields` fallback); **and given** neither hint, **then** the run
fails with a clear terminal error — never a silent full-duplicate load.
**Pinned:** merge (upsert) with neither a `primary_key` nor a `unique` column raises a terminal
`TypesenseImportError` (surfaced by dlt as `PipelineStepFailed`). This deliberately diverges
from dlt-core's SQL append-fallback: Typesense merge is upsert-only and duplicate documents are
especially harmful in a search index, so the safer contract is to fail loudly and force the
user to declare a key (or `write_disposition="append"`).
Verified by: tests/unit/test_load_job_id.py::test_document_id_rejects_null_merge_key, tests/unit/test_load_job_id.py::test_id_fields_ignores_dlt_id_unique_hint, tests/unit/test_load_job_id.py::test_id_fields_merge_no_key_raises, tests/unit/test_load_job_id.py::test_id_fields_merge_unique_fallback, tests/unit/test_load_job_id.py::test_id_fields_unique_fallback_excludes_dlt_id, tests/integration/test_merge.py::test_merge_without_key_is_terminal, tests/integration/test_merge.py::test_unique_hint_keys_the_upsert

#### AC-MERGE-06 — double run is byte-identical `v1` `Verified`
**Given** an identical merge pipeline, **when** run twice, **then** collection state (count and
document content) is identical after run 1 and run 2.
Verified by: tests/integration/test_merge.py::test_double_run_is_identical, tests/integration/test_merge.py::test_updates_happen_in_place

#### AC-MERGE-07 — retry after partial failure converges `v1` `Verified`
**Given** a merge load interrupted mid-import (some batches applied, some not), **when** dlt
retries the load package, **then** the final collection state equals a single clean run — no
duplicates, no missing rows.
Verified by: tests/integration/test_merge.py::test_merge_retry_converges

#### AC-MERGE-08 — mixed insert + update batch `v1` `Verified`
**Given** run 2 yields some new PKs and some existing PKs with changes, **then** new documents
are inserted and existing ones updated within the same run.
Verified by: tests/integration/test_merge.py::test_mixed_insert_and_update_batch

#### AC-MERGE-09 — `insert-only` never modifies existing documents `v1` `Verified`
**Given** `write_disposition={"disposition": "merge", "strategy": "insert-only"}`, **when**
run 2 re-yields overlapping data, **then** rows import keyed by `_dlt_id` and previously loaded
documents are never modified; same-package retries remain idempotent.
Verified by: tests/unit/test_load_job_id.py::test_id_fields_insert_only_uses_dlt_id, tests/integration/test_merge.py::test_insert_only_never_modifies_and_retry_idempotent

#### AC-MERGE-10 — v1 child-table limitation is documented `v1` `Verified`
**Given** a merge resource with nested lists, **when** run 2 removes items from a row's list,
**then** stale child documents remain (no orphan removal in v1) while the root collection stays
correct — and this limitation is stated in the README and this document (resolved by
AC-ORPHAN-01 in phase 2).
Verified by: tests/integration/test_merge.py::test_v1_child_table_orphans_remain_under_merge

## 8. AC-SKIP — Skip disposition

#### AC-SKIP-01 — skip writes nothing `v1` `Verified`
**Given** a resource with `write_disposition="skip"`, **when** the pipeline runs, **then** it
succeeds and no collection or document is created for that table.
**Note:** dlt core does not emit a completable load job for a *data-bearing* `skip` table — its
loader raises `LoadClientUnsupportedWriteDisposition` for any disposition outside
`{append, replace, merge}` (verified against dlt's own `dummy` destination), so this is a
dlt-wide constraint, not a destination limitation. The destination-level guarantee is that a
`skip` table is never materialized; the loadable case is a row-less skip resource.
Verified by: tests/integration/test_dispositions.py::test_skip_writes_nothing

#### AC-SKIP-02 — skip does not affect sibling resources `v1` `Verified`
**Given** a source with one skipped and one appended resource, **when** the pipeline runs,
**then** the appended resource loads normally.
Verified by: tests/integration/test_dispositions.py::test_skip_does_not_affect_siblings

## 9. AC-SHAPE — Data-shape matrix *(what actually varies between sources)*

#### AC-SHAPE-01 — flat scalar rows load 1:1 `v1` `Verified`
**Given** flat rows of scalars, **when** loaded (any disposition), **then** each row is one
document with all values intact. This is the base case for AC-APPEND/AC-REPLACE/AC-MERGE.
Verified by: tests/integration/test_dispositions.py::test_append_first_run_loads_all_with_dlt_fields, tests/integration/test_shapes.py::test_flat_scalar_rows_load_one_to_one

#### AC-SHAPE-02 — nested dicts flatten to `parent__child` fields `v1` `Verified`
**Given** rows containing nested dicts, **when** loaded under append, replace, and
merge-upsert, **then** nested keys appear as flattened `parent__child` fields on the root
document, per dlt's normalizer.
Verified by: tests/integration/test_shapes.py::test_nested_dicts_flatten_to_parent_child

#### AC-SHAPE-03 — nested lists become child collections `v1` `Verified`
**Given** rows containing lists of objects, **when** loaded under append and merge-upsert,
**then** child collections exist with `_dlt_id`, `_dlt_parent_id`, `_dlt_list_idx`, and — under
merge with root-key propagation — `_dlt_root_id`; child row counts match the source lists.
Verified by: tests/integration/test_dispositions.py::test_replace_replaces_child_tables, tests/integration/test_shapes.py::test_nested_lists_become_child_collections, tests/integration/test_shapes.py::test_nested_lists_under_append

#### AC-SHAPE-04 — null and missing values are fine `v1` `Verified`
**Given** rows where columns are `None` or absent, **when** loaded, **then** the import
succeeds and fields behave as optional under auto-schema.
Verified by: tests/integration/test_shapes.py::test_null_and_missing_values_are_optional

#### AC-SHAPE-05 — schema evolution and variant columns `v1` `Verified`
**Given** a column that changes type across rows/runs (producing a dlt variant column such as
`col__v_text`), **when** loaded under append and merge-upsert, **then** the load succeeds and
both the original and variant fields are present per dlt's schema contract.
Verified by: tests/integration/test_dispositions.py::test_append_across_schema_evolution, tests/integration/test_shapes.py::test_schema_evolution_variant_column

#### AC-SHAPE-06 — zero-row resources don't fail `v1` `Verified`
**Given** a resource yielding no rows, **when** the pipeline runs (append and merge), **then**
it completes successfully (for replace, see AC-REPLACE-03).
Verified by: tests/integration/test_shapes.py::test_zero_row_resources_do_not_fail

#### AC-SHAPE-07 — unicode and special characters round-trip `v1` `Verified`
**Given** values with unicode, emoji, very long strings, and JSON-special characters
(quotes, backslashes, newlines), **when** loaded, **then** they round-trip byte-identical
through JSONL import and Typesense retrieval.
Verified by: tests/integration/test_shapes.py::test_unicode_and_special_characters_round_trip

## 10. AC-TYPE — Data-type mapping

dlt's JSONL encoder pins the wire format: `Decimal`/`Wei` → string, `datetime`/`date`/`time` →
ISO-8601 string, `bytes` → base64 string. v1 uses Typesense auto-schema (`.*` auto field);
phase 2 adds a typed field map.

#### AC-TYPE-01 — native scalars round-trip natively `v1` `Verified`
**Given** `text`, `bigint`, `double`, and `bool` columns, **then** they are stored and
retrieved as Typesense string/int64/float/bool with values intact.
Verified by: tests/integration/test_types.py::test_native_scalars_round_trip

#### AC-TYPE-02 — timestamp and date arrive as ISO-8601 strings `v1` `Verified`
**Given** `timestamp` (tz-aware) and `date` columns, **then** under auto-schema they are stored
as the ISO-8601 strings dlt emits and round-trip losslessly as strings.
Verified by: tests/integration/test_types.py::test_timestamp_and_date_are_iso_strings

#### AC-TYPE-03 — time round-trips as ISO string `v1` `Verified`
**Given** a `time` column, **then** it round-trips as its ISO-8601 string form.
Verified by: tests/integration/test_types.py::test_time_round_trips_as_iso_string

#### AC-TYPE-04 — decimal keeps full precision as string `v1` `Verified`
**Given** `Decimal` values (e.g. `123456789.123456789`), **then** they are stored as the exact
string dlt emits — no float coercion, no precision loss — and the representation is documented.
Verified by: tests/integration/test_types.py::test_decimal_keeps_full_precision_as_string

#### AC-TYPE-05 — wei beyond int64 does not overflow `v1` `Verified`
**Given** `wei` values exceeding 2^63−1, **then** the load succeeds with string representation
(Typesense int64 cannot hold them) and values round-trip exactly.
Verified by: tests/integration/test_types.py::test_wei_beyond_int64_is_string

#### AC-TYPE-06 — binary round-trips as base64 `v1` `Verified`
**Given** a `binary` column, **then** it is stored as dlt's base64 string and decodes back to
the original bytes.
Verified by: tests/integration/test_types.py::test_binary_round_trips_as_base64

#### AC-TYPE-07 — json columns have one pinned representation `v1` `Verified`
**Given** a `json` column (kept complex via `columns={...: {"data_type": "json"}}`), **then**
it loads deterministically in one documented representation and round-trips accordingly.
**Pinned:** `json` columns are stored as one canonical JSON **string** (dlt's `json.dumps`),
not a nested object. Under auto-schema a nested object's field types would be inferred from
whichever row arrives first, making imports order-dependent; a string is deterministic, never
fails import, and round-trips byte-for-byte after `json.loads`.
Verified by: tests/unit/test_load_job_id.py::test_json_field_is_serialized_to_string, tests/unit/test_load_job_id.py::test_json_fields_detected_from_schema, tests/integration/test_types.py::test_json_column_pinned_representation

#### AC-TYPE-08 — bigint at the int64 boundaries is exact `v1` `Verified`
**Given** values `±(2^63−1)`, **then** they survive exactly — auto-schema must not route them
through float.
Verified by: tests/integration/test_types.py::test_bigint_boundaries_are_exact

#### AC-TYPE-09 — typed field map replaces auto-schema `phase-2` `Proposed`
**Given** the phase-2 typed mapper, **then** collection schemas declare explicit Typesense
field types per dlt data type (per `DLT_TO_TYPESENSE_TYPE`) instead of the `.*` auto field.
Verified by: —

#### AC-TYPE-10 — typed timestamps enable sort/range `phase-2` `Proposed`
**Given** the typed mapper, **then** `timestamp` columns are stored as int64 epoch values and
support Typesense `sort_by` and range filtering.
Verified by: —

## 11. AC-TS — Typesense-specific behaviors & hazards

#### AC-TS-01 — source column named `id` must not collide silently `v1` `Verified`
**Given** source rows containing an `id` column, **when** loaded, **then** the destination
applies a deterministic, documented handling — Typesense's reserved string document `id` must
never silently swallow or corrupt source data, under any disposition.
**Pinned:** a source column `id` is renamed to `__id` by the destination naming convention
(`dlt_typesense.naming`, following the Weaviate reserved-property precedent), so its value is
preserved as a distinct field and the stored dlt schema matches Typesense exactly. The
top-level `id` is destination-managed (from `_dlt_id` or the merge key). A pathological source
that also contains `__id` collides on normalization and dlt raises a terminal
identifier-collision error — loud, never silent.
Verified by: tests/unit/test_naming.py::test_non_reserved_identifiers_unchanged_semantics, tests/unit/test_naming.py::test_source_id_is_renamed_away_from_reserved, tests/integration/test_hazards.py::test_source_id_column_round_trips_without_collision

#### AC-TS-02 — field names are normalized deterministically `v1` `Verified`
**Given** source columns with dots, spaces, dashes, or leading digits, **then** the naming
convention maps them to Typesense-safe identifiers deterministically (same input → same name)
and without collisions on realistic inputs.
Verified by: tests/unit/test_naming.py::test_field_names_normalized_deterministically, tests/unit/test_naming.py::test_no_collisions_on_realistic_inputs

#### AC-TS-03 — HTTP 200 with per-line failures raises `TypesensePartialImportError` `v1` `Verified`
**Given** an import response with status 200 whose JSONL body contains failed lines, **then**
the job raises `TypesensePartialImportError` with accurate `failed_count` and `total_count` —
HTTP 200 is never treated as proof of success.
Verified by: tests/unit/test_load_job_errors.py::test_partial_import_error_is_diagnosable, tests/unit/test_rest_client.py::test_import_parses_per_line_failures

#### AC-TS-04 — error taxonomy routes retries correctly `v1` `Verified`
**Given** auth failures, schema/validation 4xx errors, **then** the job raises a
`DestinationTerminalException` subclass (`TypesenseImportError`) and dlt does not retry;
**given** timeouts, connection resets, or 5xx, **then** it raises `TypesenseTransientError`
and dlt retries.
Verified by: tests/unit/test_rest_client.py::test_network_error_is_transient, tests/unit/test_rest_client.py::test_terminal_statuses_raise_terminal, tests/unit/test_rest_client.py::test_transient_statuses_raise_transient

#### AC-TS-05 — client-side chunking is exact and streaming `v1` `Verified`
**Given** N source rows with N > several × `client_batch_size` (default 1000) including a
non-divisible remainder, **then** exactly N documents arrive, and job memory stays bounded —
files are streamed in chunks, never buffered whole.
Verified by: tests/unit/test_rest_client.py::test_import_streams_in_client_batches_and_passes_server_batch_size, tests/unit/test_rest_client.py::test_import_streams_lazily_not_buffered_whole, tests/integration/test_hazards.py::test_client_chunking_is_exact_with_remainder

#### AC-TS-06 — files shard into parallel jobs with exact totals `v1` `Verified`
**Given** a table whose normalized data exceeds `recommended_file_size` (64 MB), **then** the
load produces multiple job files, all jobs complete (in parallel), and the final document count
is exact.
Verified by: tests/integration/test_hazards.py::test_file_sharding_produces_exact_totals

#### AC-TS-07 — `import_action` is honored end-to-end `v1` `Verified`
**Given** the default config, **then** imports use `action=upsert`; **given**
`import_action="emplace"`, **then** emplace semantics apply (partial documents update only the
provided fields); `action=create` is rejected or explicitly documented as breaking retry
idempotency.
Verified by: tests/unit/test_configuration.py::test_create_import_action_rejected, tests/unit/test_load_job_errors.py::test_create_action_is_rejected, tests/integration/test_hazards.py::test_import_action_emplace_updates_partial

#### AC-TS-08 — bad API key fails terminally `v1` `Verified`
**Given** a wrong `api_key`, **when** the pipeline runs, **then** it fails with a terminal,
actionable error (no infinite retry loop).
Verified by: tests/unit/test_rest_client.py::test_terminal_statuses_raise_terminal, tests/integration/test_hazards.py::test_bad_api_key_fails_terminally

#### AC-TS-09 — separator and server batch knobs are honored `v1` `Verified`
**Given** `dataset_separator` (default `"_"`), **then** qualified collection names use it and
never exceed 255 chars; **given** `server_batch_size` (default 40), **then** it is passed as
the import `batch_size` query parameter.
Verified by: tests/unit/test_factory.py::test_qualified_collection_name_bounded_to_255, tests/unit/test_load_job_errors.py::test_config_server_batch_size_reaches_rest, tests/unit/test_rest_client.py::test_import_streams_in_client_batches_and_passes_server_batch_size, tests/integration/test_hazards.py::test_dataset_separator_is_honored

## 12. AC-SRC — Real-source smoke criteria

#### AC-SRC-01 — `sql_database` over SQLite `v1` `Verified`
**Given** a seeded SQLite file with ≥ 2 tables consumed via dlt's `sql_database` source — one
table merge-with-PK, one append — **when** the pipeline runs twice (with row updates in
between), **then** Typesense counts match, and the merge table shows in-place PK updates.
Verified by: tests/integration/test_sources.py::test_sql_database_over_sqlite

#### AC-SRC-02 — `filesystem` over CSV, JSONL, and Parquet `v1` `Verified`
**Given** local CSV, JSONL, and Parquet fixtures consumed via dlt's `filesystem` source,
**when** loaded, **then** all three formats land with correct counts and Parquet-typed columns
(int, float, timestamp, decimal) behave per AC-TYPE.
Verified by: tests/integration/test_sources.py::test_filesystem_csv_jsonl_parquet

#### AC-SRC-03 — `rest_api` against a local mock `v1` `Verified`
**Given** a local HTTP mock with paginated responses and an incremental cursor consumed via
dlt's `rest_api` source, **when** run 1 executes, **then** all pages load; **when** the mock
gains new records and run 2 executes, **then** only the delta loads.
Verified by: tests/integration/test_sources.py::test_rest_api_paginated_incremental

## 13. AC-NF — Non-functional

#### AC-NF-01 — every failure is recoverable by re-running `v1` `Verified`
**Given** any failed run (transient network error, interrupted load) under any disposition,
**when** the user simply calls `pipeline.run()` again, **then** the pipeline completes and
converges to the correct state — no manual cleanup ever required.
Verified by: tests/integration/test_hazards.py::test_rerun_recovers_after_injected_failure, tests/integration/test_merge.py::test_merge_retry_converges

#### AC-NF-02 — failures are diagnosable `v1` `Verified`
**Given** an import failure, **then** the error reports the collection name, load id, and the
first N per-line errors from the Typesense response.
Verified by: tests/unit/test_load_job_errors.py::test_partial_import_error_is_diagnosable

#### AC-NF-03 — the API key never appears in output `v1` `Verified`
**Given** normal runs and failing runs with logging enabled, **then** the `api_key` value never
appears in logs, exception messages, or object reprs (see AC-CAP-07).
Verified by: tests/unit/test_configuration.py::test_api_key_never_leaks, tests/unit/test_rest_client.py::test_error_message_never_contains_api_key, tests/integration/test_hazards.py::test_api_key_absent_from_logs_and_errors

#### AC-NF-04 — documentation covers the contract `v1` `Verified`
**Given** the README and docs, **then** they state the supported dispositions and strategies,
all configuration knobs, and known limitations (auto-schema in v1, child-table orphans under
merge, reserved-`id` handling, type representations).
Verified by: tests/unit/test_docs.py::test_readme_documents_contract, tests/unit/test_docs.py::test_readme_documents_unsupported_strategies

#### AC-NF-05 — the shipped examples run green `v1` `Verified`
**Given** the §1 harness, **then** `examples/append_replace_pipeline.py` and
`examples/merge_pipeline.py` run successfully end-to-end.
Verified by: tests/integration/test_examples.py::test_examples_run_green

## 14. AC-ADAPT — Adapter hints `phase-2`

#### AC-ADAPT-01 — facet hint `phase-2` `Proposed`
**Given** `typesense_adapter(resource, facet=["category"])`, **then** the created collection
schema marks `category` with `facet: true`.
Verified by: —

#### AC-ADAPT-02 — default sorting field `phase-2` `Proposed`
**Given** a `default_sorting_field` hint on a numeric field, **then** the collection is created
with it; **given** an invalid field type, **then** collection creation fails terminally with
the Typesense error surfaced.
Verified by: —

#### AC-ADAPT-03 — index/no-index hints `phase-2` `Proposed`
**Given** index/no-index hints, **then** the field definitions carry the corresponding
`index` flags.
Verified by: —

## 15. AC-ORPHAN — Child-table orphan removal `phase-2`

#### AC-ORPHAN-01 — orphaned child docs are deleted under merge `phase-2` `Proposed`
**Given** a merge-upsert resource with nested lists, **when** run 2 removes items from a row's
list, **then** the follow-up orphan job deletes child documents whose `_dlt_root_id` matches a
re-loaded root but whose `_dlt_id` was not re-yielded.
Verified by: —

#### AC-ORPHAN-02 — orphan job only runs for upsert `phase-2` `Proposed`
**Given** `insert-only` merge, **then** no orphan-removal job is scheduled (lancedb precedent).
Verified by: —

#### AC-ORPHAN-03 — orphan job retry is idempotent `phase-2` `Proposed`
**Given** an orphan-removal job interrupted and retried, **then** the final child-collection
state equals a single clean execution.
Verified by: —

---

## Appendix A — Coverage matrix (disposition × data shape)

One criterion may cover several cells; shape criteria state the dispositions they run under.

| Shape ↓ / Disposition → | append | replace | merge (upsert) | merge (insert-only) | skip |
|---|---|---|---|---|---|
| Flat scalar rows | AC-APPEND-01..04 | AC-REPLACE-01..03 | AC-MERGE-02..08 | AC-MERGE-09 | AC-SKIP-01..02 |
| Nested dicts (flattened) | AC-SHAPE-02 | AC-SHAPE-02, AC-REPLACE-02 | AC-SHAPE-02 | AC-MERGE-09 ¹ | AC-SKIP-01 ¹ |
| Nested lists (child tables) | AC-SHAPE-03 | AC-REPLACE-04 | AC-SHAPE-03, AC-MERGE-10 (AC-ORPHAN-01 ²) | AC-ORPHAN-02 ² | AC-SKIP-01 ¹ |
| Evolving schema / variants | AC-APPEND-04, AC-SHAPE-05 | AC-REPLACE-02 | AC-SHAPE-05 | AC-MERGE-09 ¹ | AC-SKIP-01 ¹ |
| Zero rows | AC-SHAPE-06 | AC-REPLACE-03 | AC-SHAPE-06 | AC-SHAPE-06 ¹ | AC-SKIP-01 ¹ |

¹ Covered by the referenced criterion because the disposition's code path does not branch on
shape (skip writes nothing; insert-only shares the append import path).
² phase-2.

Cross-cutting groups apply to every cell: AC-TYPE (types), AC-TS (transport/hazards),
AC-PROTO/AC-STATE (lifecycle), AC-SRC (real sources), AC-NF (non-functional).

## Appendix B — Traceability rules

1. IDs are permanent. Never renumber, never reuse. New criteria take the next free number in
   their group.
2. Withdrawn criteria keep their entry with status `Withdrawn: <reason>`.
3. Tests declare coverage in their docstring with `Covers: AC-XXX-NN[, AC-YYY-MM…]`. Audit gap
   check: compare `grep -ohE 'AC-[A-Z]+-[0-9]{2}' docs/acceptance-criteria.md | sort -u`
   against the same grep over `tests/`.
4. When a test lands, add its pytest node id to the criterion's `Verified by:` line and move
   status to `Verified`.
5. Changes to criterion *meaning* require a PR touching this file; status/`Verified by` updates
   may ride along with test PRs.
