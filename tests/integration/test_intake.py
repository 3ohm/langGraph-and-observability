"""SQS intake against ElasticMQ and a real Postgres (spec 8.1, 9.11; P8)."""

import asyncio
import dataclasses
import json
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import structlog
from pydantic import BaseModel

from langgraph_postgres_starter.persistence.migrate import run_migrate
from langgraph_postgres_starter.settings import load_settings
from langgraph_postgres_starter.worker.intake import SqsIntake
from langgraph_postgres_starter.worker.runs import RunRepo, RunStatus
from tests.graphs.steps import STEPS_SPEC

pytestmark = pytest.mark.integration

# Short long polls: an empty poll waits this long for a message.
WAIT_S = 1


@pytest.fixture
async def repo(pool, fresh_dsn):
    """A RunRepo on the test pool, after the core and worker migrations."""
    await run_migrate(load_settings(database_url=fresh_dsn), components={"core", "worker"})
    return RunRepo(pool)


@pytest.fixture
def make_intake(sqs, repo):
    """Build an SqsIntake for ``queue_url``, by default for the steps agent."""

    def make(queue_url, agent=STEPS_SPEC):
        return SqsIntake(
            client=sqs, queue_url=queue_url, agent=agent, repo=repo, wait_seconds=WAIT_S
        )

    return make


def _queue(sqs, **attributes: str) -> str:
    """Create a queue with a new name and ``attributes``; return its URL."""
    name = f"intake-{uuid.uuid4().hex}"
    url: str = sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    return url


def _counts(sqs, queue_url: str) -> tuple[int, int]:
    """The queue's visible and in-flight message counts."""
    names = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=names)["Attributes"]
    return int(attrs[names[0]]), int(attrs[names[1]])


def _send(sqs, queue_url: str, body: object) -> None:
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(body))


async def test_valid_message_creates_run_and_is_deleted(make_intake, repo, sqs):
    sent_at: list[datetime] = []

    def dedup_key(model: BaseModel, at: datetime) -> str:
        sent_at.append(at)
        return STEPS_SPEC.dedup_key(model, at)

    queue = _queue(sqs)
    intake = make_intake(queue, agent=dataclasses.replace(STEPS_SPEC, dedup_key=dedup_key))
    before = datetime.now(UTC)
    _send(sqs, queue, {"key": "k1"})
    started = time.monotonic()

    run_ids = await intake.poll(10)

    assert len(run_ids) == 1
    run = await repo.get(run_ids[0])
    assert run is not None
    assert (run.agent, run.dedup_key, run.status, run.input) == (
        STEPS_SPEC.name,
        "test_steps:k1",
        RunStatus.PENDING,
        {"key": "k1"},
    )
    # sent_at is the message's SentTimestamp, as an aware UTC datetime. The margin
    # allows for a difference between the container's clock and the test's.
    [at] = sent_at
    assert at.tzinfo is UTC
    assert abs(at - before) < timedelta(seconds=30)
    assert _counts(sqs, queue) == (0, 0)
    assert intake.last_poll_monotonic >= started

    # A poll that receives nothing still counts as a poll.
    polled = time.monotonic()
    assert await intake.poll(10) == []
    assert intake.last_poll_monotonic >= polled


async def test_duplicate_message_creates_one_run(make_intake, repo, sqs):
    queue = _queue(sqs)
    intake = make_intake(queue)
    _send(sqs, queue, {"key": "dup"})
    [run_id] = await intake.poll(10)

    _send(sqs, queue, {"key": "dup"})
    with structlog.testing.capture_logs() as logs:
        assert await intake.poll(10) == []

    [duplicate] = [entry for entry in logs if entry["event"] == "intake.duplicate"]
    assert duplicate["run_id"] == str(run_id)
    assert [run.run_id for run in await repo.list()] == [run_id]
    assert _counts(sqs, queue) == (0, 0)


@pytest.mark.parametrize(
    ("body", "error"),
    [
        pytest.param(
            '{"key": "s3cr3t-value',
            {"error": "JSONDecodeError", "line": 1, "column": 9},
            id="not-json",
        ),
        pytest.param(
            '{"secret": "s3cr3t-value"}',
            {"error": "ValidationError", "errors": [{"type": "missing", "loc": ["key"]}]},
            id="not-valid",
        ),
    ],
)
async def test_invalid_message_reaches_dlq(make_intake, repo, sqs, body, error):
    # A body too deep for json.loads is a unit test (tests/unit/test_intake_invalid.py):
    # the depth at which the parser gives up depends on the platform.
    dlq = _queue(sqs)
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    redrive = {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}
    queue = _queue(sqs, VisibilityTimeout="1", RedrivePolicy=json.dumps(redrive))
    intake = make_intake(queue)
    sqs.send_message(QueueUrl=queue, MessageBody=body)

    with structlog.testing.capture_logs() as logs:
        for _ in range(3):
            assert await intake.poll(10) == []
            await asyncio.sleep(1.1)
        # ElasticMQ moves the message to the DLQ on the receive after the third.
        assert await intake.poll(10) == []

    assert await repo.list() == []
    assert _counts(sqs, queue) == (0, 0)
    [moved] = sqs.receive_message(QueueUrl=dlq, MaxNumberOfMessages=10)["Messages"]
    assert moved["Body"] == body
    invalid = [entry for entry in logs if entry["event"] == "intake.invalid"]
    assert [entry["receive_count"] for entry in invalid] == [1, 2, 3]
    assert {entry["message_id"] for entry in invalid} == {moved["MessageId"]}
    # The log names the error and where it is, never the body: it can carry secrets.
    known = {"event", "log_level", "message_id", "receive_count"}
    assert [{k: v for k, v in entry.items() if k not in known} for entry in invalid] == [error] * 3
    assert "s3cr3t" not in repr(logs)


async def test_poll_respects_max_messages(make_intake, repo, sqs):
    queue = _queue(sqs)
    intake = make_intake(queue)
    for i in range(5):
        _send(sqs, queue, {"key": f"m{i}"})

    first = await intake.poll(2)

    assert len(first) == 2
    assert _counts(sqs, queue) == (3, 0)
    # SQS returns at most 10 messages per receive; poll asks for no more than that.
    rest = await intake.poll(25)
    assert len(rest) == 3
    assert {run.run_id for run in await repo.list()} == {*first, *rest}
