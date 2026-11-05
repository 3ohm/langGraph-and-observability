"""SQS intake of a body that json.loads gives up on for its depth (spec 8.1; P8).

How deep a body must be for json.loads to raise RecursionError depends on the platform
and the Python build, so the test makes json.loads raise it. The queue is a fake: the
message must stay on it (no delete), so SQS moves it to the dead-letter queue.
"""

import json
from typing import Any, cast

import structlog

from langgraph_and_observability.worker.intake import SqsIntake
from langgraph_and_observability.worker.runs import RunRepo
from tests.graphs.steps import STEPS_SPEC

MESSAGE = {
    "MessageId": "m-1",
    "ReceiptHandle": "r-1",
    "Body": "[[[[",
    "Attributes": {"ApproximateReceiveCount": "2", "SentTimestamp": "1793000000000"},
}


class FakeSqs:
    """Returns one message per receive, and records the deletes."""

    def __init__(self) -> None:
        self.deleted: list[str] = []

    def receive_message(self, **kwargs: Any) -> dict[str, Any]:
        return {"Messages": [MESSAGE]}

    def delete_message(self, *, QueueUrl: str, ReceiptHandle: str) -> None:  # noqa: N803
        self.deleted.append(ReceiptHandle)


class NoRuns:
    async def create(self, *args: Any) -> None:
        raise AssertionError("an invalid message created a run")


async def test_too_deep_body_is_invalid_and_stays_on_the_queue(monkeypatch):
    def too_deep(text: str) -> object:
        raise RecursionError("maximum recursion depth exceeded while decoding a JSON array")

    monkeypatch.setattr(json, "loads", too_deep)
    sqs = FakeSqs()
    intake = SqsIntake(
        client=cast(Any, sqs),
        queue_url="http://sqs.local/q",
        agent=STEPS_SPEC,
        repo=cast(RunRepo, NoRuns()),
        wait_seconds=0,
    )

    with structlog.testing.capture_logs() as logs:
        assert await intake.poll(10) == []

    assert sqs.deleted == []
    [invalid] = [entry for entry in logs if entry["event"] == "intake.invalid"]
    assert invalid == {
        "event": "intake.invalid",
        "log_level": "warning",
        "message_id": "m-1",
        "receive_count": 2,
        "error": "RecursionError",
    }
