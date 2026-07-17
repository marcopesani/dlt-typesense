"""Unit tests for Typesense filter literal construction."""

from __future__ import annotations

import pytest

from dlt_typesense.typesense_client import _eq_filter


def test_eq_filter_quotes_value() -> None:
    assert _eq_filter("pipeline_name", "my_pipe") == "pipeline_name:=`my_pipe`"


def test_eq_filter_rejects_backtick() -> None:
    with pytest.raises(ValueError, match="backtick"):
        _eq_filter("pipeline_name", "bad`name")
