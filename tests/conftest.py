"""Shared test fixtures for the Typesense destination."""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from typing import Any, cast

import dlt
import httpx
import pytest

from dlt_typesense import typesense
from dlt_typesense.configuration import TypesenseCredentials
from dlt_typesense.rest_client import TypesenseRestClient
from dlt_typesense.typesense_client import TypesenseClient


def env_credentials() -> TypesenseCredentials:
    """Build credentials explicitly from env vars (never via dlt config)."""
    creds = TypesenseCredentials()
    creds.host = os.environ.get("TYPESENSE_HOST", "localhost")
    creds.port = int(os.environ.get("TYPESENSE_PORT", "8108"))
    creds.protocol = os.environ.get("TYPESENSE_PROTOCOL", "http")
    creds.api_key = os.environ.get("TYPESENSE_API_KEY", "local-dev-key")
    creds.__is_resolved__ = True
    return creds


def server_reachable(creds: TypesenseCredentials) -> bool:
    try:
        resp = httpx.get(
            f"{creds.protocol}://{creds.host}:{creds.port}/health",
            headers={"X-TYPESENSE-API-KEY": creds.api_key},
            timeout=3.0,
        )
        return resp.status_code == 200 and resp.json().get("ok") is True
    except Exception:
        return False


@pytest.fixture
def credentials() -> TypesenseCredentials:
    return env_credentials()


@pytest.fixture
def require_server(credentials: TypesenseCredentials) -> TypesenseCredentials:
    if not server_reachable(credentials):
        location = f"{credentials.protocol}://{credentials.host}:{credentials.port}"
        pytest.fail(
            f"Typesense is not reachable at {location}. Start it with "
            "`docker compose up -d` from the repo root."
        )
    return credentials


@pytest.fixture
def probe(require_server: TypesenseCredentials) -> Iterator[TypesenseRestClient]:
    """A direct REST client for asserting Typesense state independently."""
    with TypesenseRestClient(require_server) as client:
        yield client


@pytest.fixture
def dataset_name() -> str:
    return f"ds_{uuid.uuid4().hex[:12]}"


class PipelineFactory:
    """Creates pipelines against the Typesense destination and drops their data."""

    def __init__(
        self, credentials: TypesenseCredentials, default_dataset: str, pipelines_dir: str
    ) -> None:
        self._credentials = credentials
        self._default_dataset = default_dataset
        self._pipelines_dir = pipelines_dir
        self.created: list[dlt.Pipeline] = []

    def __call__(self, **kwargs: Any) -> dlt.Pipeline:
        destination_kwargs = kwargs.pop("destination_kwargs", {})
        pipeline = dlt.pipeline(
            pipeline_name=kwargs.pop("pipeline_name", f"pipe_{uuid.uuid4().hex[:8]}"),
            destination=typesense(credentials=self._credentials, **destination_kwargs),
            dataset_name=kwargs.pop("dataset_name", self._default_dataset),
            pipelines_dir=kwargs.pop("pipelines_dir", self._pipelines_dir),
            dev_mode=kwargs.pop("dev_mode", False),
            **kwargs,
        )
        self.created.append(pipeline)
        return pipeline

    def qualified_name(self, pipeline: dlt.Pipeline, table_name: str) -> str:
        with pipeline.destination_client() as client:
            return client.make_qualified_collection_name(table_name)  # type: ignore[attr-defined]

    def teardown(self) -> None:
        for pipeline in self.created:
            try:
                with pipeline.destination_client() as client:
                    client.drop_storage()  # type: ignore[attr-defined]
            except Exception:
                pass


@pytest.fixture
def make_pipeline(
    require_server: TypesenseCredentials, dataset_name: str, tmp_path: Any
) -> Iterator[PipelineFactory]:
    factory = PipelineFactory(require_server, dataset_name, str(tmp_path / "pipelines"))
    yield factory
    factory.teardown()


@pytest.fixture
def count_documents(probe: TypesenseRestClient) -> Callable[[str], int]:
    def _count(collection_name: str) -> int:
        if not probe.collection_exists(collection_name):
            return 0
        return probe.count_documents(collection_name)

    return _count


@pytest.fixture
def open_client() -> Callable[[dlt.Pipeline], AbstractContextManager[TypesenseClient]]:
    """Open the pipeline's destination client, typed as TypesenseClient."""

    def _open(pipeline: dlt.Pipeline) -> AbstractContextManager[TypesenseClient]:
        return cast("AbstractContextManager[TypesenseClient]", pipeline.destination_client())

    return _open


@pytest.fixture
def documents(probe: TypesenseRestClient) -> Callable[..., list[dict[str, Any]]]:
    def _documents(collection_name: str, **kwargs: Any) -> list[dict[str, Any]]:
        if not probe.collection_exists(collection_name):
            return []
        kwargs.setdefault("per_page", 250)
        return probe.search_documents(collection_name, **kwargs)

    return _documents
