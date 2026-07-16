"""The shipped examples run green end-to-end."""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import pytest

from dlt_typesense.rest_client import TypesenseRestClient

pytestmark = pytest.mark.integration

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
EXAMPLES_DIR = REPO_ROOT / "examples"


def _drop_dataset(credentials, dataset: str) -> None:
    with TypesenseRestClient(credentials) as client:
        prefix = f"{dataset}_"
        for collection in client.list_collections():
            if collection["name"].startswith(prefix):
                client.delete_collection(collection["name"])


def _run_example(name: str, env: dict, cwd: pathlib.Path) -> None:
    result = subprocess.run(
        [sys.executable, str(EXAMPLES_DIR / f"{name}.py")],
        env=env,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"{name} failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def test_examples_run_green(require_server, tmp_path) -> None:
    env = dict(os.environ)
    env.update(
        {
            "DESTINATION__TYPESENSE__CREDENTIALS__HOST": require_server.host,
            "DESTINATION__TYPESENSE__CREDENTIALS__PORT": str(require_server.port),
            "DESTINATION__TYPESENSE__CREDENTIALS__PROTOCOL": require_server.protocol,
            "DESTINATION__TYPESENSE__CREDENTIALS__API_KEY": require_server.api_key,
            "DLT_DATA_DIR": str(tmp_path / "dlt"),
            "PYTHONPATH": str(REPO_ROOT / "src"),
        }
    )
    try:
        _run_example("append_replace_pipeline", env, tmp_path)
        _run_example("merge_pipeline", env, tmp_path)
        _run_example("schema_hints_pipeline", env, tmp_path)
    finally:
        _drop_dataset(require_server, "demo")
        _drop_dataset(require_server, "catalog")
        _drop_dataset(require_server, "library")
