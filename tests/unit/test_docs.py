"""README documents the destination contract."""

from __future__ import annotations

import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
README = (REPO_ROOT / "README.md").read_text().lower()


@pytest.mark.parametrize(
    "phrase",
    [
        "append",
        "replace",
        "merge",
        "skip",
        "upsert",
        "insert-only",
        "truncate-and-insert",
        "dataset_separator",
        "client_batch_size",
        "server_batch_size",
        "import_action",
        "auto",
        "orphan",
        "__id",
        "base64",
    ],
)
def test_readme_documents_contract(phrase: str) -> None:
    assert phrase in README, f"README should document '{phrase}'"


def test_readme_documents_unsupported_strategies() -> None:
    assert "delete-insert" in README
    assert "scd2" in README
