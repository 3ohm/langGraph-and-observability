"""SQS intake: validate, dedup, record, then delete (spec §8.1, §9.11; P8).

The worker calls ``SqsIntake.poll`` with its number of free run slots. One poll
long-polls the queue once and handles the messages it receives, in order:

1. Parse the body as JSON and validate it with the agent's ``input_model``. An invalid
   message is logged as ``intake.invalid`` and is **not deleted**: SQS delivers it
   again when its visibility timeout ends, and moves it to the dead-letter queue after
   the queue's ``maxReceiveCount``. The log names the error's type and location, never
   the body, because the body can carry secrets.
2. Compute the run's dedup key with the agent's ``dedup_key``, from the input and the
   time the message was sent, and insert the run unless one has that key
   (``RunRepo.create``).
3. Delete the message. From here on Postgres, not SQS, owns the run, so the visibility
   timeout only has to cover steps 1-3. A message that already had a run is logged as
   ``intake.duplicate``.

boto3 is synchronous, so each SQS call runs in a thread (``asyncio.to_thread``).
"""

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import boto3
from botocore.config import Config
from pydantic import ValidationError

from langgraph_postgres_starter.agent_spec import AgentSpec
from langgraph_postgres_starter.logging import get_logger
from langgraph_postgres_starter.settings import Settings
from langgraph_postgres_starter.worker.runs import RunRepo

if TYPE_CHECKING:
    # The stubs are a dev dependency: only annotations name these types.
    from mypy_boto3_sqs import SQSClient
    from mypy_boto3_sqs.type_defs import MessageTypeDef

_log = get_logger(__name__)

# SQS returns at most this many messages per receive.
_MAX_MESSAGES_PER_RECEIVE = 10

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def make_sqs_client(settings: Settings) -> Any:
    """Return a boto3 SQS client for ``aws_region`` and ``sqs_endpoint_url``.

    ``sqs_endpoint_url`` is None for AWS, or ElasticMQ's URL for local runs and tests.
    Credentials come from boto3's own chain (environment, web identity, instance role);
    nothing here sets them. Each client gets its own session, which reads that chain
    when the client is built: boto3's shared default session keeps the credentials it
    found first. botocore retries a failed call in standard mode, at most 3 times
    after the first attempt (in client config, ``max_attempts`` counts retries); it is
    the only retry layer for SQS.
    """
    return boto3.session.Session().client(
        "sqs",
        region_name=settings.aws_region,
        endpoint_url=settings.sqs_endpoint_url,
        config=Config(retries={"mode": "standard", "max_attempts": 3}),
    )


class SqsIntake:
    """Turns the messages on ``queue_url`` into pending runs of ``agent`` (spec §8.1).

    ``client`` is a boto3 SQS client (``make_sqs_client``). ``wait_seconds`` is how
    long one receive waits for a message (``APP_SQS_WAIT_SECONDS``, default 20).

    ``last_poll_monotonic`` is the ``time.monotonic()`` at which the last receive
    returned, with or without messages; the worker's readiness check reads it. Until
    the first receive returns, it is the time the intake was built.
    """

    def __init__(
        self,
        *,
        client: SQSClient,
        queue_url: str,
        agent: AgentSpec,
        repo: RunRepo,
        wait_seconds: int,
    ) -> None:
        self._client = client
        self._queue_url = queue_url
        self._agent = agent
        self._repo = repo
        self._wait_seconds = wait_seconds
        self.last_poll_monotonic: float = time.monotonic()

    async def poll(self, max_messages: int) -> list[UUID]:
        """Receive up to ``max_messages`` messages (at least 1; at most 10 are asked for).

        Records a run for each valid message and deletes the message. Returns the ids
        of the runs that this poll created, in message order: a duplicate or an
        invalid message adds none.
        """
        response = await asyncio.to_thread(
            self._client.receive_message,
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=min(max_messages, _MAX_MESSAGES_PER_RECEIVE),
            WaitTimeSeconds=self._wait_seconds,
            MessageSystemAttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
        )
        self.last_poll_monotonic = time.monotonic()
        run_ids: list[UUID] = []
        # SQS leaves out "Messages" when it returns none.
        for message in response.get("Messages", []):
            run_id = await self._handle(message)
            if run_id is not None:
                run_ids.append(run_id)
        return run_ids

    async def _handle(self, message: MessageTypeDef) -> UUID | None:
        """Record one message's run and delete it; return the run's id if it is new."""
        message_id = message["MessageId"]
        attributes = message["Attributes"]
        try:
            model = self._agent.input_model.model_validate(json.loads(message["Body"]))
        # json.loads raises RecursionError for a body that is nested too deeply.
        except (json.JSONDecodeError, RecursionError, ValidationError) as exc:
            _log.warning(
                "intake.invalid",
                message_id=message_id,
                receive_count=int(attributes["ApproximateReceiveCount"]),
                **_error_fields(exc),
            )
            return None
        sent_at = _EPOCH + timedelta(milliseconds=int(attributes["SentTimestamp"]))
        dedup_key = self._agent.dedup_key(model, sent_at)
        run_id, created = await self._repo.create(
            self._agent.name, dedup_key, model.model_dump(mode="json")
        )
        await asyncio.to_thread(
            self._client.delete_message,
            QueueUrl=self._queue_url,
            ReceiptHandle=message["ReceiptHandle"],
        )
        if not created:
            _log.info("intake.duplicate", message_id=message_id, run_id=str(run_id))
            return None
        return run_id


def _error_fields(exc: json.JSONDecodeError | RecursionError | ValidationError) -> dict[str, Any]:
    """What ``intake.invalid`` logs about ``exc``: its type and location, never input.

    ``error`` is the exception's type. A JSON error adds the ``line`` and ``column``
    where parsing failed; a validation error adds ``errors``, the ``type`` and ``loc``
    of each failure.
    """
    match exc:
        case json.JSONDecodeError():
            return {"error": "JSONDecodeError", "line": exc.lineno, "column": exc.colno}
        case ValidationError():
            failures = exc.errors(include_url=False, include_context=False, include_input=False)
            return {
                "error": "ValidationError",
                "errors": [{"type": f["type"], "loc": list(f["loc"])} for f in failures],
            }
        case RecursionError():
            return {"error": "RecursionError"}
