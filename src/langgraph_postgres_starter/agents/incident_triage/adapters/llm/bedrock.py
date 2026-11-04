"""Claude on Amazon Bedrock through boto3's ``invoke_model`` (spec §6.4, §9.7; P16, P18).

The request body is Anthropic's Messages body for ``bedrock-runtime`` (verified against
the AWS and Anthropic documentation on 2026-11-04; docs/practices/P16.md lists the
pages):

- ``anthropic_version`` ``bedrock-2023-05-31``, ``max_tokens`` (``APP_MAX_OUTPUT_TOKENS``);
- ``system``: one text block, the fixed system prompt, with the cache breakpoint
  ``cache_control: {"type": "ephemeral"}``;
- ``messages``: one user message, the incident's evidence. No assistant prefill;
- ``output_config``: ``effort`` (``APP_MODEL_EFFORT``) and the structured-output
  ``format``, ``{"type": "json_schema", "schema": ...}``.

It has no ``temperature``, ``top_p``, ``top_k``, ``tools`` or ``tool_choice``: Claude
Opus 5.5 rejects sampling parameters and forced tool choice. It has no ``thinking``
either: Opus 5.5 always thinks adaptively, and effort is the control.

The model ID is ``APP_BEDROCK_MODEL_ID``. On ``bedrock-runtime``, the bare ID
``anthropic.claude-opus-5-5`` runs in-Region only in ``eu-west-2``; elsewhere it takes
an inference-profile prefix, such as ``global.anthropic.claude-opus-5-5`` or
``us.anthropic.claude-opus-5-5``. The IAM permission is ``bedrock:InvokeModel``.

boto3 is synchronous, so the call runs in a thread (``asyncio.to_thread``). botocore
retries a failed call in standard mode: it is the only retry layer, and the graph
does not retry botocore's errors (``policies.is_transient``).
"""

import asyncio
import functools
import json
import threading
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config

from langgraph_postgres_starter.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmAdapter,
    LlmResponse,
)
from langgraph_postgres_starter.agents.incident_triage.schemas import Usage
from langgraph_postgres_starter.settings import Settings

if TYPE_CHECKING:
    # The stubs are a dev dependency: only annotations name these types.
    from mypy_boto3_bedrock_runtime import BedrockRuntimeClient

ANTHROPIC_VERSION = "bedrock-2023-05-31"
# The first request with a new schema compiles a grammar, which can take about 90 s.
READ_TIMEOUT_S = 300


def make_bedrock_client(settings: Settings) -> BedrockRuntimeClient:
    """A boto3 ``bedrock-runtime`` client for ``aws_region``, with its own session.

    As for SQS (``worker.intake.make_sqs_client``), credentials come from boto3's own
    chain, and botocore retries in standard mode, at most 3 times after the first
    attempt (in client config, ``max_attempts`` counts retries). The read timeout is
    ``READ_TIMEOUT_S``. boto3 looks for credentials when it builds the client, which
    can mean a network call, so ``BedrockLlm`` builds its client on its first call.
    """
    return boto3.session.Session().client(
        "bedrock-runtime",
        region_name=settings.aws_region,
        config=Config(
            read_timeout=READ_TIMEOUT_S,
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )


def build_request_body(req: AnalysisRequest, settings: Settings) -> dict[str, Any]:
    """The ``invoke_model`` body of ``req``: the module docstring lists its fields."""
    return {
        "anthropic_version": ANTHROPIC_VERSION,
        "max_tokens": settings.max_output_tokens,
        "system": [{"type": "text", "text": req.system, "cache_control": {"type": "ephemeral"}}],
        "messages": [{"role": "user", "content": [{"type": "text", "text": req.user}]}],
        "output_config": {
            "effort": settings.model_effort,
            "format": {"type": "json_schema", "schema": req.schema},
        },
    }


class BedrockLlm(LlmAdapter):
    """The model of ``APP_LLM_MODE=bedrock``: one ``invoke_model`` call per request.

    ``client`` is a boto3 ``bedrock-runtime`` client, or a function that returns one.
    Without it, the adapter builds ``make_bedrock_client(settings)`` on its first call,
    so building the adapter touches no network.
    """

    def __init__(
        self,
        settings: Settings,
        client: BedrockRuntimeClient | Callable[[], BedrockRuntimeClient] | None = None,
    ) -> None:
        self._settings = settings
        if client is None:
            self._make_client: Callable[[], BedrockRuntimeClient] = functools.partial(
                make_bedrock_client, settings
            )
        elif callable(client):
            self._make_client = client
        else:
            self._make_client = lambda: client
        self._client: BedrockRuntimeClient | None = None
        self._client_lock = threading.Lock()

    async def analyze(self, req: AnalysisRequest) -> LlmResponse:
        """Send ``req`` to Bedrock and return the reply.

        The reply's text is its text blocks, joined; thinking blocks are skipped. Its
        ``model_id`` is the model ID that was called. botocore's errors, such as a
        ``ClientError`` for a throttled call that botocore gave up retrying, are
        raised as they are. A reply without a stop reason or token usage raises
        ValueError.
        """
        body = json.dumps(build_request_body(req, self._settings))
        reply = await asyncio.to_thread(self._invoke, body)
        return _response(reply, self._settings.bedrock_model_id)

    def _invoke(self, body: str) -> object:
        """Call ``invoke_model`` (runs in a thread) and return the decoded reply."""
        response = self._client_for_use().invoke_model(
            modelId=self._settings.bedrock_model_id,
            body=body,
            contentType="application/json",
            accept="application/json",
        )
        return json.loads(response["body"].read())

    def _client_for_use(self) -> BedrockRuntimeClient:
        with self._client_lock:
            if self._client is None:
                self._client = self._make_client()
            return self._client


def _response(reply: object, model_id: str) -> LlmResponse:
    """The ``LlmResponse`` of a decoded Messages reply."""
    if not isinstance(reply, Mapping):
        raise ValueError("Bedrock sent an unexpected reply: it is not a JSON object")
    stop_reason = reply.get("stop_reason")
    usage = reply.get("usage")
    content = reply.get("content")
    if not isinstance(stop_reason, str):
        raise ValueError("Bedrock sent an unexpected reply: it has no stop_reason")
    if not isinstance(usage, Mapping):
        raise ValueError("Bedrock sent an unexpected reply: it has no usage")
    counts = {
        field: usage.get(field, 0)
        for field in ("input_tokens", "output_tokens", "cache_read_input_tokens")
    }
    if not all(isinstance(count, int) for count in counts.values()):
        raise ValueError("Bedrock sent an unexpected reply: its token usage is not a number")
    blocks = content if isinstance(content, list) else []
    text = "".join(
        block["text"]
        for block in blocks
        if isinstance(block, Mapping)
        and block.get("type") == "text"
        and isinstance(block.get("text"), str)
    )
    return LlmResponse(
        text=text,
        stop_reason=stop_reason,
        model_id=model_id,
        usage=Usage(**counts),
    )
