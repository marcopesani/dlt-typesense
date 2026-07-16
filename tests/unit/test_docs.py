"""Documentation covers the contract (AC-NF-04). No server required."""

from __future__ import annotations

import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
README = (REPO_ROOT / "README.md").read_text().lower()


@pytest.mark.parametrize(
    "phrase",
    [
        # supported dispositions
        "append",
        "replace",
        "merge",
        "skip",
        # strategies
        "upsert",
        "insert-only",
        "truncate-and-insert",
        # configuration knobs
        "dataset_separator",
        "client_batch_size",
        "server_batch_size",
        "import_action",
        # known limitations
        "auto",  # auto schema
        "orphan",  # child-table orphans
        "__id",  # reserved-id handling
        "base64",  # type representations
    ],
)
def test_readme_documents_contract(phrase: str) -> None:
    """Covers: AC-NF-04"""
    assert phrase in README, f"README should document '{phrase}'"


def test_readme_documents_unsupported_strategies() -> None:
    """Covers: AC-NF-04"""
    assert "delete-insert" in README
    assert "scd2" in README
