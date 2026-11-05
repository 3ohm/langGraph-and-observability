"""The Bedrock model adapter: request body, client and reply parsing (spec §6.4, §9.7; P16, P18).

No test calls Bedrock. botocore's ``Stubber`` answers ``invoke_model``, and every client
is built with keys in the environment or in the call, so boto3 makes no network call
to find credentials.
"""

import io
import json
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import Stubber

from langgraph_and_observability.agents.incident_triage.adapters.llm.bedrock import (
    BedrockLlm,
    build_request_body,
    make_bedrock_client,
)
from langgraph_and_observability.agents.incident_triage.adapters.llm.interface import (
    AnalysisRequest,
    LlmResponse,
)
from langgraph_and_observability.agents.incident_triage.policies import is_transient
from langgraph_and_observability.agents.incident_triage.prompts import (
    PROMPT_VERSION,
    SYSTEM_PROMPT,
    analysis_schema,
)
from langgraph_and_observability.agents.incident_triage.schemas import Usage
from langgraph_and_observability.settings import load_settings

PROFILE = "global.anthropic.claude-opus-5-5"
ANSWER = '{"category": "application"}'


def _request(user: str = "## Target\nnamespace: shop") -> AnalysisRequest:
    return AnalysisRequest(
        system=SYSTEM_PROMPT,
        user=user,
        schema=analysis_schema(),
        fixture_id=None,
        attempt=0,
        prompt_version=PROMPT_VERSION,
    )


@pytest.fixture
def aws_keys(monkeypatch):
    # Keys in the environment: boto3 finds them first, without a network call.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)


def _reply_body(reply: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(reply).encode()
    return {
        "body": StreamingBody(io.BytesIO(payload), len(payload)),
        "contentType": "application/json",
    }


def test_body_has_no_sampling_or_forced_tools():  # P18
    body = build_request_body(_request(), load_settings())

    assert not {"temperature", "top_p", "top_k", "tool_choice"} & body.keys()
    # No tools, and no assistant prefill: the one message is the user's.
    assert "tools" not in body
    assert [message["role"] for message in body["messages"]] == ["user"]


def test_body_uses_settings():
    req = _request()

    body = build_request_body(req, load_settings())

    assert body == {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 16000,
        "system": [{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        "messages": [{"role": "user", "content": [{"type": "text", "text": req.user}]}],
        "output_config": {
            "effort": "medium",
            "format": {"type": "json_schema", "schema": req.schema},
        },
    }
    other = build_request_body(req, load_settings(model_effort="high", max_output_tokens=8000))
    assert (other["max_tokens"], other["output_config"]["effort"]) == (8000, "high")


def test_throttling_left_to_botocore(aws_keys):  # Review Focus 5
    client = make_bedrock_client(load_settings(aws_region="eu-west-1"))

    assert client.meta.region_name == "eu-west-1"
    config = cast(Any, client.meta.config)  # the stubs leave Config.retries out
    # The Config literal is retries={"mode": "standard", "max_attempts": 3}: 3 retries.
    # botocore stores it as 4 attempts in all.
    assert config.retries == {"mode": "standard", "total_max_attempts": 4}
    assert config.read_timeout == 300
    # What botocore gives up on, the graph does not retry.
    assert (
        is_transient(ClientError({"Error": {"Code": "ThrottlingException"}}, "InvokeModel"))
        is False
    )


async def test_bedrock_invoke_shape_with_stubber(aws_keys):
    settings = load_settings(bedrock_model_id=PROFILE)
    client = make_bedrock_client(settings)
    req = _request()
    reply = {
        "id": "msg_bdrk_01",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5-5",
        "content": [
            # Opus 5.5 always thinks; the thinking text is empty unless it is asked for.
            {"type": "thinking", "thinking": "", "signature": "c2lnbmF0dXJl"},
            {"type": "text", "text": ANSWER},
        ],
        "stop_reason": "end_turn",
        "stop_sequence": None,
        "usage": {
            "input_tokens": 1200,
            "output_tokens": 300,
            "cache_read_input_tokens": 900,
            "cache_creation_input_tokens": 0,
        },
    }
    sent = json.dumps(build_request_body(req, settings))
    with Stubber(client) as stubber:
        stubber.add_response(
            "invoke_model",
            _reply_body(reply),
            expected_params={
                "modelId": PROFILE,
                "body": sent,
                "contentType": "application/json",
                "accept": "application/json",
            },
        )

        response = await BedrockLlm(settings, client).analyze(req)

        stubber.assert_no_pending_responses()
    assert response == LlmResponse(
        text=ANSWER,
        stop_reason="end_turn",
        model_id=PROFILE,
        usage=Usage(input_tokens=1200, output_tokens=300, cache_read_input_tokens=900),
    )
    assert set(json.loads(sent)) == {
        "anthropic_version",
        "max_tokens",
        "system",
        "messages",
        "output_config",
    }


async def test_bedrock_reads_a_refusal(aws_keys):
    settings = load_settings()
    client = make_bedrock_client(settings)
    refusal = {
        "stop_reason": "refusal",
        "stop_details": {"type": "refusal", "category": "cyber", "explanation": "blocked"},
        "content": [],
        "usage": {"input_tokens": 106, "output_tokens": 1},
    }
    with Stubber(client) as stubber:
        stubber.add_response("invoke_model", _reply_body(refusal))

        response = await BedrockLlm(settings, client).analyze(_request())

    assert response == LlmResponse(
        text="",
        stop_reason="refusal",
        model_id="anthropic.claude-opus-5-5",
        usage=Usage(input_tokens=106, output_tokens=1),
    )


async def test_bedrock_errors_are_raised_after_one_call(aws_keys):
    settings = load_settings()
    client = make_bedrock_client(settings)
    with Stubber(client) as stubber:
        stubber.add_client_error(
            "invoke_model", service_error_code="ThrottlingException", http_status_code=429
        )

        with pytest.raises(ClientError, match="ThrottlingException"):
            await BedrockLlm(settings, client).analyze(_request())

        # The adapter adds no retry loop of its own: botocore's is the one layer.
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    ("usage", "expected"),
    [
        # The Messages API types the cache counters as integer or null.
        (
            {
                "input_tokens": 5,
                "output_tokens": 2,
                "cache_read_input_tokens": None,
                "cache_creation_input_tokens": None,
            },
            Usage(input_tokens=5, output_tokens=2, cache_read_input_tokens=0),
        ),
        (
            {"input_tokens": 5, "output_tokens": 2},
            Usage(input_tokens=5, output_tokens=2, cache_read_input_tokens=0),
        ),
        (
            {"input_tokens": 5, "output_tokens": 2, "cache_read_input_tokens": 3},
            Usage(input_tokens=5, output_tokens=2, cache_read_input_tokens=3),
        ),
    ],
    ids=["cache-null", "cache-absent", "cache-count"],
)
async def test_bedrock_reads_usage(aws_keys, usage, expected):
    settings = load_settings()
    client = make_bedrock_client(settings)
    reply = {"stop_reason": "end_turn", "content": [], "usage": usage}
    with Stubber(client) as stubber:
        stubber.add_response("invoke_model", _reply_body(reply))

        response = await BedrockLlm(settings, client).analyze(_request())

    assert response.usage == expected


def _reply_with_usage(**usage: object) -> dict[str, Any]:
    return {"stop_reason": "end_turn", "content": [], "usage": usage}


@pytest.mark.parametrize(
    "reply",
    [
        {"content": [], "usage": {"input_tokens": 1, "output_tokens": 1}},
        {"stop_reason": "end_turn", "content": []},
        # The input and output counts are always integers in a valid reply.
        _reply_with_usage(input_tokens=None, output_tokens=1),
        _reply_with_usage(input_tokens=1, output_tokens=None),
        _reply_with_usage(input_tokens=1),
        _reply_with_usage(input_tokens="1", output_tokens=1),
        _reply_with_usage(input_tokens=True, output_tokens=1),
        _reply_with_usage(input_tokens=1, output_tokens=1, cache_read_input_tokens="3"),
        _reply_with_usage(input_tokens=1, output_tokens=1, cache_read_input_tokens=False),
    ],
    ids=[
        "no-stop-reason",
        "no-usage",
        "input-null",
        "output-null",
        "output-missing",
        "input-string",
        "input-bool",
        "cache-string",
        "cache-bool",
    ],
)
async def test_bedrock_unexpected_reply_raises(aws_keys, reply):
    settings = load_settings()
    client = make_bedrock_client(settings)
    with Stubber(client) as stubber:
        stubber.add_response("invoke_model", _reply_body(reply))

        with pytest.raises(ValueError, match="unexpected reply"):
            await BedrockLlm(settings, client).analyze(_request())
