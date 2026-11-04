"""Send one incident to the queue, as an alert would (spec §10.2). ``make incident`` runs this.

Usage::

    uv run scripts/send_incident.py --fixture python-keyerror
    uv run scripts/send_incident.py --message-file incident.json --queue-url URL

The message is an incident fixture's ``incident.json`` (``--fixture <id>``, a folder in
``src/langgraph_postgres_starter/agents/incident_triage/fixtures/incidents/``) or a JSON
``IncidentMessage`` in a file (``--message-file <path>``). A message without
``fired_at`` gets the current time, in UTC. The script validates the message as an
``IncidentMessage``, sends it to ``--queue-url`` and prints the SQS message ID. An
invalid message, or a failed send, exits 1 with an error message on stderr.

The defaults reach the local stack's queue (compose.yaml): ElasticMQ on localhost:9324.
For a queue in AWS, pass its URL and ``--endpoint-url ''``: boto3 then uses the AWS
endpoint of the configured region (us-east-1 when none is configured). Credentials
come from boto3's usual chain. Only when the endpoint is local (this machine, or
Compose's ``elasticmq``) and the chain finds none does the script sign with dummy
credentials, which ElasticMQ accepts; for a local endpoint the chain skips the EC2
metadata endpoint (``make_client``).
"""

import argparse
import json
import os
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from langgraph_postgres_starter.agents.incident_triage.fixtures.loader import load_fixture
from langgraph_postgres_starter.agents.incident_triage.schemas import IncidentMessage

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

# The queue of the local stack, as the host sees it (compose.yaml).
DEFAULT_QUEUE_URL = "http://localhost:9324/000000000000/incidents"
DEFAULT_ENDPOINT_URL = "http://localhost:9324"
# The region when boto3's configuration names none: the default of APP_AWS_REGION.
DEFAULT_REGION = "us-east-1"
# The local queue's hosts: this machine, and Compose's ElasticMQ service. Only for them
# may the script use dummy credentials.
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "elasticmq"})
# ElasticMQ accepts any credentials; botocore needs some to sign a request.
_DUMMY_KEY_ID = "test"
_DUMMY_SECRET = "test"  # noqa: S105 - not a secret: a placeholder that ElasticMQ ignores


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="send_incident.py",
        description="Send one IncidentMessage to an SQS queue and print its message ID.",
    )
    message = parser.add_mutually_exclusive_group(required=True)
    message.add_argument(
        "--fixture", metavar="ID", help="an incident fixture, such as python-keyerror"
    )
    message.add_argument("--message-file", type=Path, metavar="PATH", help="a JSON IncidentMessage")
    parser.add_argument(
        "--queue-url", default=DEFAULT_QUEUE_URL, help="the queue (default: %(default)s)"
    )
    parser.add_argument(
        "--endpoint-url",
        default=DEFAULT_ENDPOINT_URL,
        help="the SQS endpoint; '' for AWS's own (default: %(default)s)",
    )
    return parser


def load_message(fixture: str | None, message_file: Path | None) -> IncidentMessage:
    """The message to send: the fixture's incident, else the file's JSON, with ``fired_at``.

    Raises OSError when a file cannot be read, and ValueError (pydantic's
    ValidationError and json's JSONDecodeError are ValueErrors) when the fixture or
    the message is not valid.
    """
    data: Any
    if fixture is not None:
        data = load_fixture(fixture).incident
    elif message_file is not None:
        data = json.loads(message_file.read_text(encoding="utf-8"))
    else:
        raise ValueError("give a fixture ID or a message file")
    # A message that is not an object fails the validation below.
    if isinstance(data, dict) and data.get("fired_at") is None:
        data = {**data, "fired_at": datetime.now(UTC)}
    return IncidentMessage.model_validate(data)


def make_client(endpoint_url: str | None) -> SQSClient:
    """A boto3 SQS client for ``endpoint_url`` (None: AWS's endpoint for the region).

    It uses boto3's credential chain, or dummy credentials when the endpoint is local
    and the chain finds none. botocore retries a failed call in standard mode, at most
    3 times, as the worker's client does.

    For a local endpoint, the script first sets ``AWS_EC2_METADATA_DISABLED=true`` in
    its own environment. The chain asks the EC2 metadata endpoint last, after the
    configured credentials; off EC2 that probe takes seconds, and a local queue needs no
    instance role. botocore reads the variable from the environment only, when the
    chain is first used.
    """
    local = endpoint_url is not None and urlsplit(endpoint_url).hostname in _LOCAL_HOSTS
    if local:
        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    session = boto3.session.Session()
    dummy = local and session.get_credentials() is None
    return session.client(
        "sqs",
        region_name=session.region_name or DEFAULT_REGION,
        endpoint_url=endpoint_url,
        aws_access_key_id=_DUMMY_KEY_ID if dummy else None,
        aws_secret_access_key=_DUMMY_SECRET if dummy else None,
        config=Config(retries={"mode": "standard", "max_attempts": 3}),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        message = load_message(args.fixture, args.message_file)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"{parser.prog}: error: {exc}\n")
    try:
        response = make_client(args.endpoint_url or None).send_message(
            QueueUrl=args.queue_url, MessageBody=message.model_dump_json()
        )
    except (BotoCoreError, ClientError) as exc:
        parser.exit(1, f"{parser.prog}: error: the message was not sent: {exc}\n")
    print(response["MessageId"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
