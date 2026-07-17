# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-07-17

### Added

- **Orphan cleanup for nested tables under merge (`upsert`).** After a merge
  table chain finishes loading, a follow-up job deletes child documents whose
  parent was re-synced but which were not re-written (elements that disappeared
  from a nested list). Cleanup is scoped to the load (untouched parents keep
  their children), covers emptied lists and all nesting levels, and batches
  Typesense `filter_by` requests. Opt out per resource with
  `typesense_adapter(resource, no_remove_orphans=True)`.
- **Wire-value conversion for typed field hints.** Pinning a dlt `timestamp` /
  `date` column to `int64`/`int32` stores Unix epoch seconds; pinning
  `decimal`/`wei` to a numeric Typesense type parses the exact wire string.
  Array/object types keep native JSON values.
- **Short-name destination resolution.** `destination="typesense"` works via
  the `dlt` entry point and `dlt_typesense.destinations`.
- **Incremental SQL sync example** (`examples/sql_incremental_sync_pipeline.py`)
  with a declarative multi-collection config, `--target` / `--limit` /
  `--full-refresh` / `--dev-mode` flags.
- CI matrix coverage for `dlt` 1.20.0, locked, latest, and pre-release, and
  Typesense 30.2.

### Changed

- README reshaped to the canonical dlt destination layout, with a dedicated
  **Orphan cleanup for nested tables** section and clearer type-hint contracts.
- `update_stored_schema` stays compatible across dlt 1.20–1.29 (probes for the
  `force` kwarg instead of assuming it).

### Fixed

- Refuse silent decimal truncation and backtick filter values that would make
  state/schema lookups look empty.
- Large-import retry and state-sync edge cases pinned by the overhauled test
  suite.

## [0.1.0] - 2026-07-16

### Added

- First PyPI release of `dlt-typesense`: a full dlt document destination for
  Typesense with `append` / `replace` / `merge` (`upsert`, `insert-only`),
  schema hints via `typesense_adapter`, and state sync.

[0.2.0]: https://github.com/marcopesani/dlt-typesense/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/marcopesani/dlt-typesense/releases/tag/v0.1.0
