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
