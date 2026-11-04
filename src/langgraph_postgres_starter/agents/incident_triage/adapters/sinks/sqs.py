"""The ``sqs`` sink: each result as one message on an output queue (spec §9.9, §9.10).

The queue is ``APP_RESULT_QUEUE_URL``. A message's body is the result's JSON, and its
``idempotency_key`` attribute is the send's ledger key, ``publish:<run_id>:sqs``, so a
consumer can drop a repeated message. On a FIFO queue (its URL ends in ``.fifo``) the
key is also the ``MessageDeduplicationId``, so SQS drops a repeat that comes within its
5-minute deduplication window, and the target's namespace is the ``MessageGroupId``.
The IAM permission is ``sqs:SendMessage`` on that queue.

boto3 is synchronous, so the call runs in a thread (``asyncio.to_thread``). botocore
retries it in standard mode, the only retry layer (``make_sqs_client``). The agent
builds its own client: it imports nothing from the worker, which a fork may delete
(spec §13.9).
"""

import asyncio
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config

from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.interface import ResultSink
from langgraph_postgres_starter.agents.incident_triage.schemas import TriageResult

if TYPE_CHECKING:
    # The stubs are a dev dependency: only annotations name these types.
    from mypy_boto3_sqs import SQSClient
    from mypy_boto3_sqs.type_defs import SendMessageRequestTypeDef

_FIFO_SUFFIX = ".fifo"


def make_sqs_client(region: str, endpoint_url: str | None) -> SQSClient:
    """A boto3 SQS client for ``region``, with its own session.

    ``endpoint_url`` is None for AWS, or ElasticMQ's URL for local runs and tests
    (``APP_SQS_ENDPOINT_URL``). As for the worker's intake client, credentials come from
    boto3's own chain, and botocore retries in standard mode, at most 3 times. boto3
    looks for credentials when it builds the client, which can mean a network call, so
    ``build_adapters`` passes this function, not a client, to ``SqsSink``.
    """
    return boto3.session.Session().client(
        "sqs",
        region_name=region,
        endpoint_url=endpoint_url,
        config=Config(retries={"mode": "standard", "max_attempts": 3}),
    )


class SqsSink(ResultSink):
    """Sends each result to the SQS queue ``queue_url``.

    ``client`` is a boto3 SQS client, or a function that returns one (such as
    ``functools.partial(make_sqs_client, region, endpoint_url)``). The sink calls the
    function once, on its first send, so building the sink touches no network.
    """

    name = "sqs"

    def __init__(self, client: SQSClient | Callable[[], SQSClient], queue_url: str) -> None:
        self._make_client: Callable[[], SQSClient] = (
            client if callable(client) else (lambda: client)
        )
        self._client: SQSClient | None = None
        self._client_lock = threading.Lock()
        self._queue_url = queue_url

    async def send(self, result: TriageResult, idempotency_key: str) -> dict[str, Any]:
        """Send ``result`` as one message. Returns the sink's name and the message's ID.

        The client's errors are raised as they are.
        """
        request: SendMessageRequestTypeDef = {
            "QueueUrl": self._queue_url,
            "MessageBody": result.model_dump_json(),
            "MessageAttributes": {
                "idempotency_key": {"DataType": "String", "StringValue": idempotency_key}
            },
        }
        if self._queue_url.endswith(_FIFO_SUFFIX):
            request["MessageDeduplicationId"] = idempotency_key
            request["MessageGroupId"] = result.target.namespace
        message_id = await asyncio.to_thread(self._send, request)
        return {"sink": self.name, "message_id": message_id}

    def _send(self, request: SendMessageRequestTypeDef) -> str:
        """Send the message and return its ID (runs in a thread; builds the client once)."""
        with self._client_lock:
            if self._client is None:
                self._client = self._make_client()
            client = self._client
        return client.send_message(**request)["MessageId"]
