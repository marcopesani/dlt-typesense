# GitHub repository setup

Apply after the first push to `main` (and preferably after CI has run once so status check names exist).

## Repo metadata and merge hygiene

```bash
gh repo edit marcopesani/dlt-typesense \
  --description "Typesense document destination for dlt" \
  --add-topic dlt --add-topic typesense --add-topic document-database \
  --add-topic elt --add-topic search --add-topic python \
  --enable-issues --enable-discussions \
  --enable-squash-merge --enable-merge-commit=false \
  --enable-rebase-merge=false --delete-branch-on-merge \
  --allow-update-branch

gh label create dependencies -c "#0366d6" -f --repo marcopesani/dlt-typesense
```

## Branch ruleset

```bash
gh api repos/marcopesani/dlt-typesense/rulesets \
  --method POST \
  --input .github/ruleset-main.json
```

Ruleset summary:

- PRs required into `main`
- Conversation resolution required
- 0 approvals (solo-maintainer friendly; still review external PRs)
- Required check: `ci-success` (aggregates lint, typecheck, and the Python matrix)
- No force-push / deletion
- Repository admins may bypass during bootstrap

Apply the ruleset only after CI has run once so the `ci-success` check name exists.

## License on GitHub

Ensure the repo license is detected (Apache-2.0 `LICENSE` at repo root). Refresh
with:

```bash
gh api repos/marcopesani/dlt-typesense --jq .license
```

## PyPI publishing (Trusted Publisher)

Releases publish to PyPI via OIDC Trusted Publishing — no long-lived API tokens.
Workflow: [`.github/workflows/publish.yml`](../.github/workflows/publish.yml)
(triggered when a GitHub Release is published).

### One-time setup

1. **GitHub Environment** named `pypi` (Settings → Environments → New environment).
   Optional but recommended: require a reviewer before deploy jobs run.
2. **Pending publisher on PyPI** (project does not exist yet):
   - Open [https://pypi.org/manage/account/publishing/](https://pypi.org/manage/account/publishing/)
   - Owner: `marcopesani`, Repository: `dlt-typesense`
   - Workflow: `publish.yml`, Environment: `pypi`
   - Project name: `dlt-typesense`
3. Optional **TestPyPI** pending publisher with the same fields if you want a dry run
   (point a temporary workflow/`repository-url` at TestPyPI before the first real upload).

### Cut a release

1. Bump `__version__` in `src/dlt_typesense/__init__.py` (single source of truth;
   hatchling reads it for the wheel metadata).
2. Merge to `main`.
3. Create a GitHub Release whose tag is `v` + that version (e.g. version `0.1.0` → tag
   `v0.1.0`). Publishing the release runs the workflow.
4. The workflow builds, runs `twine check`, asserts tag == package version, smoke-imports
   the wheel, then uploads with `pypa/gh-action-pypi-publish` (Trusted Publishing).

The first successful Trusted Publishing upload creates the PyPI project and binds the
pending publisher. Version uploads are immutable — dry-run on TestPyPI if unsure.
