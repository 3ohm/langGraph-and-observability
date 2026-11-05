"""Shared pytest fixtures for the test suite: ElasticMQ and the test graph.

The fixtures that every test directory needs are registered by the repo-root
conftest.py: ``_isolate_settings`` and ``strict_msgpack`` there, and the Postgres
fixtures ``pg_dsn``, ``fresh_dsn`` and ``pool`` in ``tests/fixtures_pg.py``.
"""

import os
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from langgraph_and_observability.persistence.migrate import run_migrate
from langgraph_and_observability.persistence.resources import set_pool
from langgraph_and_observability.settings import load_settings

ELASTICMQ_IMAGE = "softwaremill/elasticmq-native:1.7.1"
ELASTICMQ_PORT = 9324

# Read once, at import: _isolate_settings (repo-root conftest.py) removes every APP_*
# variable before each test.
TEST_SQS_ENDPOINT = os.environ.get("APP_TEST_SQS_ENDPOINT") or None

# The developer's AWS variables that could reach a test client; the sqs fixture removes them.
_AWS_IDENTITY_VARS = (
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
)


@pytest.fixture(scope="session")
def sqs_endpoint() -> Iterator[str]:
    """An SQS API for the session: APP_TEST_SQS_ENDPOINT, or a new ElasticMQ container.

    As with pg_dsn (``tests/fixtures_pg.py``), the container starts on first use and is
    removed when the session ends. Tests create their own queues.
    """
    if TEST_SQS_ENDPOINT:
        yield TEST_SQS_ENDPOINT
        return
    # Imported here: unit test runs never load testcontainers.
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    container = (
        DockerContainer(ELASTICMQ_IMAGE)
        .with_exposed_ports(ELASTICMQ_PORT)
        .waiting_for(LogMessageWaitStrategy(r"ElasticMQ server .* started"))
    )
    with container:
        host = container.get_container_host_ip()
        yield f"http://{host}:{container.get_exposed_port(ELASTICMQ_PORT)}"


@pytest.fixture
def sqs(sqs_endpoint: str, monkeypatch: pytest.MonkeyPatch) -> Any:
    """A boto3 SQS client for sqs_endpoint, configured as ``worker.intake.make_sqs_client``.

    It is built here, not by that function, so this conftest imports nothing from the
    worker, which a fork may delete (spec 13.9). ElasticMQ accepts any credentials. The
    fixture sets dummy ones and removes the developer's session token and profile, so a
    test never sends real credentials.
    """
    # Imported here: loading this conftest does not import boto3.
    import boto3
    from botocore.config import Config

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    for name in _AWS_IDENTITY_VARS:
        monkeypatch.delenv(name, raising=False)
    return boto3.session.Session().client(
        "sqs",
        region_name=load_settings().aws_region,
        endpoint_url=sqs_endpoint,
        config=Config(retries={"mode": "standard", "max_attempts": 3}),
    )


@pytest.fixture
async def steps_pool(
    pool: AsyncConnectionPool[AsyncConnection[DictRow]], fresh_dsn: str
) -> AsyncIterator[AsyncConnectionPool[AsyncConnection[DictRow]]]:
    """The test pool, ready to run the ``test_steps`` graph under the worker.

    Applies the core, langgraph and worker migrations, creates ``test_step_calls`` and
    registers the pool with ``set_pool``, so the steps record their marks in Postgres.
    Afterwards it clears the registered pool and ``tests.graphs.steps.FAULTS``.
    """
    # Imported here, so that loading this conftest does not import the test-only graph.
    # Always as tests.graphs.steps: the tests change this module's FAULTS.
    from tests.graphs import steps

    await run_migrate(
        load_settings(database_url=fresh_dsn), components={"core", "langgraph", "worker"}
    )
    await steps.ensure_table(pool)
    set_pool(pool)
    try:
        yield pool
    finally:
        set_pool(None)
        steps.FAULTS.clear()
