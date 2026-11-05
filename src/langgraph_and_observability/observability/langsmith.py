"""LangSmith tracing: one redacting client, content modes and LLM runs (spec §9.7, §12.1; P15).

Tracing is off unless langsmith's own switch is on and an API key is set. The switch is
``LANGSMITH_TRACING=true`` (or ``LANGSMITH_TRACING_V2``, ``LANGCHAIN_TRACING_V2``,
``LANGCHAIN_TRACING``), read as ``langsmith.utils.tracing_is_enabled`` reads it. The key
is ``LANGSMITH_API_KEY`` or ``LANGCHAIN_API_KEY``. Then ``configure_tracing`` builds one
``langsmith.Client`` and makes it the default client of every trace with
``langsmith.configure``. ``traceable`` runs and LangGraph's tracer (langchain-core's
``LangChainTracer``) both take their client from there. With the switch on and no key,
``configure_tracing`` turns tracing off, so that langsmith never traces through its
default client, which does not redact.

``APP_TRACE_CONTENT`` sets what the client sends:

- ``redacted`` (the default): inputs, outputs, metadata and errors, with every string
  passed through ``redact``;
- ``none``: no inputs and no outputs; metadata and errors are redacted;
- ``full`` (``dev`` only, see ``Settings.require_runtime_secrets``): all of it, as it is.

The project is ``LANGSMITH_PROJECT`` (or ``LANGCHAIN_PROJECT``) when set, else
``langgraph-and-observability-<env>``: one project per environment.

``log_eval_results`` records the eval test's scores in its LangSmith experiment (spec
§12.3), so the agent's evals import no langsmith code.
"""

import asyncio
import functools
import inspect
import os
from collections.abc import Callable, Mapping
from typing import Any, Literal, cast

import langsmith
from langsmith import utils as ls_utils
from langsmith.run_helpers import get_current_run_tree
from langsmith.schemas import ExtractedUsageMetadata

from langgraph_and_observability.logging import get_logger
from langgraph_and_observability.redaction import redact, redact_obj
from langgraph_and_observability.settings import Settings

type TraceContent = Literal["full", "redacted", "none"]
type ContentHook = Callable[[dict[str, Any]], dict[str, Any]]
# The client that configure_tracing returns, for modules that do not import langsmith.
type TracingClient = langsmith.Client

# The project is <PROJECT_PREFIX>-<env> when no project variable is set.
PROJECT_PREFIX = "langgraph-and-observability"
# How long close_tracing waits for pending traces at shutdown.
FLUSH_TIMEOUT_S = 5.0

# The variables that langsmith's Client reads its API key from.
_API_KEYS = ("LANGSMITH_API_KEY", "LANGCHAIN_API_KEY")
_PROJECTS = ("LANGSMITH_PROJECT", "LANGCHAIN_PROJECT")
# The token counts of an LlmResponse's usage.
_USAGE_FIELDS = ("input_tokens", "output_tokens", "cache_read_input_tokens")
# The longest error message in a log line.
_MAX_ERROR_MESSAGE = 2000

_log = get_logger(__name__)


def configure_tracing(settings: Settings) -> TracingClient | None:
    """Build the process's LangSmith client and make it the default of every trace.

    Returns None, and changes nothing, when langsmith's tracing switch is off. With the
    switch on and no API key, it turns tracing off (``langsmith.configure(enabled=False)``),
    logs ``tracing.disabled_no_key`` and returns None. Otherwise the client sends what
    ``settings.trace_content`` allows, samples at ``settings.trace_sampling_rate`` and
    logs its errors, redacted. When no project variable is set, the project is
    ``langgraph-and-observability-<env>``. Call it once, at startup; ``close_tracing``
    flushes and stops the client.
    """
    if ls_utils.tracing_is_enabled() is not True:
        return None
    if _first_set(_API_KEYS) is None:
        # langsmith would still trace, through its default client: no anonymizer.
        langsmith.configure(enabled=False)
        _log.warning(
            "tracing.disabled_no_key",
            hint="tracing is on but LANGSMITH_API_KEY is not set; tracing is now off",
        )
        return None
    client = langsmith.Client(
        tracing_sampling_rate=settings.trace_sampling_rate,
        tracing_error_callback=_log_tracing_error,
        **_client_content_arguments(settings.trace_content),
    )
    project = _first_set(_PROJECTS)
    if project is None:
        project = f"{PROJECT_PREFIX}-{settings.env}"
        langsmith.configure(client=client, project_name=project)
    else:
        # Set in code, a project would outrank the variable.
        langsmith.configure(client=client)
    _log.info(
        "tracing.configured",
        content=settings.trace_content,
        sampling_rate=settings.trace_sampling_rate,
        project=project,
    )
    return client


async def close_tracing(client: TracingClient) -> None:
    """Send the pending traces, for at most FLUSH_TIMEOUT_S, then stop ``client``.

    Spec §8.4 step 5. The flush blocks, so it runs in a thread. A failure is logged,
    never raised, so shutdown goes on.
    """
    try:
        await asyncio.to_thread(client.close, timeout=FLUSH_TIMEOUT_S)
    except Exception as exc:
        _log.error("tracing.flush_failed", error=_error(exc))


def content_hooks(mode: TraceContent) -> tuple[ContentHook, ContentHook]:
    """Return ``(hide_inputs, hide_outputs)``: the content that ``mode`` lets into a trace.

    ``full`` returns the payload as it is, ``redacted`` a copy with each value passed
    through ``redact_obj``, and ``none`` an empty dict. The two hooks are the same.
    configure_tracing gives the installed client other arguments with the same effect
    (see ``_client_content_arguments``).
    """
    if mode == "full":
        return _keep, _keep
    if mode == "redacted":
        return _redact_values, _redact_values
    if mode == "none":
        return _drop, _drop
    raise ValueError(f"unknown trace content mode {mode!r}; use full, redacted or none")


def trace_llm[F: Callable[..., Any]](name: str) -> Callable[[F], F]:
    """Trace each call of the decorated function as a LangSmith LLM run named ``name``.

    The function makes one model call and returns its response (an ``LlmResponse``). It
    may be sync or async. The response is read by attribute, and a missing attribute is
    skipped. ``model_id`` goes into the run's metadata as ``model_id``. ``usage``
    (``input_tokens``, ``output_tokens``, ``cache_read_input_tokens``) goes in as
    ``usage``, and in LangSmith's token form as ``usage_metadata`` (see
    ``_langsmith_usage``). Without tracing, the function runs unchanged.
    """

    def decorate(fn: F) -> F:
        traced = langsmith.traceable(run_type="llm", name=name)
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def call_async(*args: Any, **kwargs: Any) -> Any:
                response = await fn(*args, **kwargs)
                _record_response(response)
                return response

            return cast(F, traced(call_async))

        @functools.wraps(fn)
        def call(*args: Any, **kwargs: Any) -> Any:
            response = fn(*args, **kwargs)
            _record_response(response)
            return response

        return cast(F, traced(call))

    return decorate


def log_eval_results(outputs: dict[str, Any], scores: Mapping[str, float | None]) -> str | None:
    """Record an eval's outputs and scores in its LangSmith experiment; return the link.

    For the eval test when langsmith's pytest plugin tracks it (``@pytest.mark.langsmith``,
    spec §12.3): ``outputs`` become the test run's outputs, and each score that is not
    None becomes feedback on that run, under its key. The link is that of the
    experiment, the test run's project (``TracerSession.url``); it is None, and the
    error is logged, when LangSmith does not return it. Outside any run, as without the
    plugin or with ``LANGSMITH_TEST_TRACKING=false``, it records nothing and returns
    None. Inside a run that the plugin does not track, langsmith raises ValueError.
    """
    # Imported here: langsmith.testing imports pytest when pytest is installed.
    from langsmith import testing

    run = get_current_run_tree()
    if run is None:
        return None
    testing.log_outputs(outputs)
    for key, score in scores.items():
        if score is not None:
            testing.log_feedback(key=key, score=score)
    try:
        url: str | None = run.client.read_project(project_name=run.session_name).url
    except Exception as exc:
        _log.warning("tracing.experiment_unavailable", error=_error(exc))
        return None
    return url


def _client_content_arguments(mode: TraceContent) -> dict[str, Any]:
    """The ``langsmith.Client`` arguments that make it send what ``mode`` allows.

    The client does not get the content_hooks callables. In langsmith 0.14.17, once an
    anonymizer is set, ``Client._hide_run_inputs`` and ``_hide_run_outputs``
    (langsmith/client.py, lines 2853-2873) return the anonymizer's result and never call
    a callable ``hide_inputs`` or ``hide_outputs``; only ``True`` is applied before the
    anonymizer. Errors are redacted only by the anonymizer (``_hide_run_error``, line
    2875). So ``redacted`` is the anonymizer, ``none`` adds ``hide_*=True``, and ``full``
    has neither. tests/unit/test_tracing.py pins this on a real client.
    """
    if mode == "redacted":
        return {"anonymizer": _anonymize}
    if mode == "none":
        return {"anonymizer": _anonymize, "hide_inputs": True, "hide_outputs": True}
    if mode == "full":
        return {}
    raise ValueError(f"unknown trace content mode {mode!r}; use full, redacted or none")


def _anonymize(payload: dict[str, Any]) -> dict[str, Any]:
    """The client's anonymizer: every string in ``payload`` through ``redact``.

    It covers dict keys and values at any depth. The client calls it on run inputs,
    outputs and metadata after a JSON round trip, and on errors as ``{"error": ...}``.
    """
    return cast(dict[str, Any], _redact_strings(payload))


def _redact_strings(value: object) -> object:
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, dict):
        return {_redact_strings(key): _redact_strings(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_redact_strings(item) for item in value]
    return value


def _keep(payload: dict[str, Any]) -> dict[str, Any]:
    return payload


def _redact_values(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: redact_obj(value) for key, value in payload.items()}


def _drop(payload: dict[str, Any]) -> dict[str, Any]:
    return {}


def _record_response(response: object) -> None:
    """Put the response's model ID and token usage on the current LLM run, if traced."""
    run = get_current_run_tree()
    if run is None:
        return
    model_id = getattr(response, "model_id", None)
    if isinstance(model_id, str):
        run.add_metadata({"model_id": model_id})
    usage = getattr(response, "usage", None)
    counts: dict[str, int] = {}
    for field in _USAGE_FIELDS:
        count = getattr(usage, field, None)
        if isinstance(count, int):
            counts[field] = count
    if counts:
        run.add_metadata({"usage": counts})
        run.set(usage_metadata=_langsmith_usage(counts))


def _langsmith_usage(counts: dict[str, int]) -> ExtractedUsageMetadata:
    """``counts`` in LangSmith's token form, the run's ``usage_metadata``.

    LangSmith's ``input_tokens`` counts all input tokens. Bedrock's (Anthropic's)
    ``input_tokens`` leaves out cache reads, so they are added, as langchain-anthropic
    does, and also given as ``input_token_details.cache_read``.
    """
    usage: ExtractedUsageMetadata = {}
    cache_read = counts.get("cache_read_input_tokens")
    if "input_tokens" in counts:
        usage["input_tokens"] = counts["input_tokens"] + (cache_read or 0)
    if "output_tokens" in counts:
        usage["output_tokens"] = counts["output_tokens"]
    if "input_tokens" in usage and "output_tokens" in usage:
        usage["total_tokens"] = usage["input_tokens"] + usage["output_tokens"]
    if cache_read is not None:
        usage["input_token_details"] = {"cache_read": cache_read}
    return usage


def _log_tracing_error(error: Exception) -> None:
    """The client's tracing_error_callback: log the error's type and redacted message.

    The client calls it from its background thread when sending traces fails. The trace
    payload is never logged.
    """
    _log.warning("tracing.error", error=_error(error))


def _first_set(names: tuple[str, ...]) -> str | None:
    """The value of the first variable in ``names`` that is set and not blank."""
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


def _error(exc: BaseException) -> dict[str, str]:
    """An exception as a log field: its type, and its message with secrets redacted."""
    return {"type": type(exc).__name__, "message": redact(str(exc))[:_MAX_ERROR_MESSAGE]}
