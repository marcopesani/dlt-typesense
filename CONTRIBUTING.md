# Contributing

Thanks for helping improve `dlt-typesense`.

## Setup

1. Install [uv](https://docs.astral.sh/uv/).
2. Clone the repo and sync:

```bash
uv sync --group dev
```

3. Run checks before opening a PR:

```bash
uv run ruff check .
uv run ruff format .
uv run basedpyright
uv run pytest
```

## Integration tests

Integration tests (marker `integration`, excluded by default) run against a local Typesense
started via Docker:

```bash
docker compose up -d --wait
uv run pytest -m integration
docker compose down
```

The compose file pins `typesense/typesense:29.0` with API key `local-dev-key`; tests discover
it via `TYPESENSE_HOST` / `TYPESENSE_PORT` / `TYPESENSE_PROTOCOL` / `TYPESENSE_API_KEY` env
vars (defaults match the compose file). When `-m integration` is requested and the server is
unreachable, tests fail rather than skip.

## Workflow

1. Open an issue for non-trivial changes when possible.
2. Fork and create a feature branch from `main`.
3. Keep PRs focused; prefer small, reviewable diffs.
4. Fill in the PR template.
5. Ensure CI is green.

## Developer Certificate of Origin (DCO)

This project uses the [DCO](https://developercertificate.org/) instead of a CLA.

Sign off every commit:

```bash
git commit -s -m "Your message"
```

Git adds a `Signed-off-by: Your Name <email>` trailer. Your sign-off certifies you have the right to submit the contribution under the Apache-2.0 license.

## Architecture notes

- This is a **full dlt destination** (`JobClientBase`), templated on Qdrant — not an `@dlt.destination` sink.
- Prefer stub seams in `load_jobs.py` / `rest_client.py` over ad-hoc scripts.
- See [docs/architecture.md](docs/architecture.md).

## Code style

- Python 3.10+, typed public APIs
- Ruff for lint/format; basedpyright for types
- No secrets in the repo (use `.dlt/secrets.toml` locally; it is gitignored)

## Releasing to PyPI

Maintainers: bump `__version__` in `src/dlt_typesense/__init__.py`, merge to `main`,
then publish a GitHub Release tagged `vX.Y.Z` matching that version. See
[docs/github-setup.md](docs/github-setup.md#pypi-publishing-trusted-publisher) for
Trusted Publisher setup and the publish workflow.

## Reporting security issues

See [SECURITY.md](SECURITY.md).
