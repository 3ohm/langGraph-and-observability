"""The publish step, the SQS sink and the triage_results table (spec §7.3, §9.8, §9.9; P13).

The tests marked ``integration`` publish to a fresh database that has the core and
``incident_triage`` migrations only: no worker tables (ruling R62). They register its
pool with ``set_pool``, as the worker does. The other tests need no database.
"""

import asyncio
import dataclasses
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, LiteralString

import boto3
import pytest
import structlog
from botocore.stub import Stubber
from langgraph.runtime import Runtime
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.agent_spec import RunInfo
from langgraph_postgres_starter.agents import incident_triage
from langgraph_postgres_starter.agents.incident_triage.adapters import build_adapters
from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.log import LogSink
from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.sqs import SqsSink
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext
from langgraph_postgres_starter.agents.incident_triage.evidence import make_item
from langgraph_postgres_starter.agents.incident_triage.nodes import publish as publish_module
from langgraph_postgres_starter.agents.incident_triage.nodes.publish import publish
from langgraph_postgres_starter.agents.incident_triage.schemas import (
    Assessment,
    CodeSnippet,
    EvidenceCitation,
    Target,
    TriageAnalysis,
    TriageResult,
    Usage,
    WorkloadRef,
)
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.ids import new_id
from langgraph_postgres_starter.persistence.migrate import run_migrate
from langgraph_postgres_starter.persistence.resources import current_pool, set_pool
from langgraph_postgres_starter.settings import load_settings

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]

MIGRATIONS = Path(incident_triage.__file__).parent / "migrations"
FIFO_QUEUE = "https://sqs.us-east-1.amazonaws.com/123456789012/triage-results.fifo"
STANDARD_QUEUE = "https://sqs.us-east-1.amazonaws.com/123456789012/triage-results"
MODEL_ID = "anthropic.claude-opus-5-5"

ANALYSIS = TriageAnalysis(
    category="application",
    summary="unit_price divides by a quantity of zero, so api crashes.",
    root_cause="An order with quantity 0 reaches unit_price, which divides by it.",
    confidence="high",
    evidence=[EvidenceCitation(id="log:previous:api", why="The ZeroDivisionError.")],
    suspected_code=[],
    recommended_actions=["Reject orders with quantity 0 before pricing them."],
    needs_human=False,
)

# The state that publish gets after analyze, with two k8s items, one Datadog event, one
# code snippet and one note.
STATE: TriageState = {
    "target": Target(
        cluster="local",
        namespace="shop",
        workload=WorkloadRef(kind="Deployment", name="api"),
        pod="api-6c9d8b7f5-x1k2p",
        container="api",
        node="node-1",
        images=[],
    ),
    "assessment": Assessment(
        collect_code=True,
        infra_signals=[],
        app_signals=["stack_trace"],
        reasons=["Application signal stack_trace: the logs hold 2 app frames."],
    ),
    "k8s_evidence": [
        make_item("k8s:status:api", "k8s_status", "Status of container api", "waiting"),
        make_item("log:previous:api", "log", "Previous log of api", "ZeroDivisionError"),
    ],
    "datadog_evidence": [
        make_item("dd:event:42", "datadog_event", "api is crash looping", "restarted 4 times"),
    ],
    "code_evidence": [
        CodeSnippet(
            id="code:snippet:1",
            repo="fixture://api",
            commit="fixture",
            path="api/pricing.py",
            start_line=10,
            end_line=11,
            symbol="unit_price",
            language="python",
            text="def unit_price(total, quantity):\n    return total // quantity",
        ),
    ],
    "notes": [make_item("note:code:unmapped_frames", "note", "Unmapped frames", "/app/x.py:7")],
    "analysis": ANALYSIS,
    "refusal": False,
    "usage": Usage(input_tokens=1200, output_tokens=90, cache_read_input_tokens=800),
    "model_id": MODEL_ID,
}
# The same run when the model refused: no analysis.
REFUSED: TriageState = STATE | {"analysis": None, "refusal": True}
# The same run when the model asks for a human.
ASKS_HUMAN: TriageState = STATE | {"analysis": ANALYSIS.model_copy(update={"needs_human": True})}


@dataclass
class RecordingSink:
    """A sink that keeps each result and key it gets."""

    name: str
    sent: list[tuple[TriageResult, str]] = field(default_factory=list)

    async def send(self, result: TriageResult, idempotency_key: str) -> dict[str, Any]:
        self.sent.append((result, idempotency_key))
        return {"sink": self.name, "count": len(self.sent)}


@dataclass
class FailingSink:
    """A sink whose sends raise ConnectionError."""

    name: str

    async def send(self, result: TriageResult, idempotency_key: str) -> dict[str, Any]:
        raise ConnectionError("the sink is down")


@dataclass
class SlowSink:
    """A sink whose sends take far longer than the test's send timeout."""

    name: str

    async def send(self, result: TriageResult, idempotency_key: str) -> dict[str, Any]:
        await asyncio.sleep(30)
        return {"sink": self.name}


def _runtime(*sinks: Any) -> Runtime[TriageContext]:
    """The runtime of one simulated run whose sinks are ``sinks``."""
    settings = load_settings()
    adapters = build_adapters(settings, fixture_id="python-keyerror")
    adapters = dataclasses.replace(adapters, sinks=sinks)
    run = RunInfo(run_id=new_id(), dedup_key="d" * 64, attempt=1)
    return Runtime(
        context=TriageContext(
            run=run, settings=settings, adapters=adapters, fixture_id="python-keyerror"
        )
    )


def _run_id(runtime: Runtime[TriageContext]):
    assert runtime.context is not None
    return runtime.context.run.run_id


@pytest.fixture
async def results_pool(pool, fresh_dsn):
    """The test pool with the core and incident_triage migrations, registered with set_pool."""
    await run_migrate(
        load_settings(database_url=fresh_dsn),
        components={"core", "incident_triage"},
        agent_sources=[("incident_triage", MIGRATIONS)],
    )
    set_pool(pool)
    try:
        yield pool
    finally:
        set_pool(None)


async def _rows(pool: _Pool, query: LiteralString) -> list[DictRow]:
    async with pool.connection() as conn:
        cur = await conn.execute(query)
        return await cur.fetchall()


@pytest.mark.integration
async def test_publish_twice_one_row_one_send(results_pool):
    # P13: a second publish of the run, as after a crash between the step's sends and
    # its checkpoint, stores no second row and sends nothing again.
    log, sqs = RecordingSink("log"), RecordingSink("sqs")
    runtime = _runtime(log, sqs)
    run_id = _run_id(runtime)

    first = await publish(STATE, runtime)
    [row] = await _rows(results_pool, "SELECT run_id, result FROM triage_results")
    second = await publish(STATE, runtime)

    assert first == second == {"published": ["log", "sqs"]}
    assert await _rows(results_pool, "SELECT run_id, result FROM triage_results") == [row]
    assert [key for _, key in log.sent] == [f"publish:{run_id}:log"]
    assert [key for _, key in sqs.sent] == [f"publish:{run_id}:sqs"]
    assert await _rows(results_pool, "SELECT key, action FROM action_ledger ORDER BY key") == [
        {"key": f"publish:{run_id}:log", "action": "publish"},
        {"key": f"publish:{run_id}:sqs", "action": "publish"},
    ]
    # The sinks got the stored result.
    assert log.sent[0][0] == sqs.sent[0][0] == TriageResult.model_validate(row["result"])


@pytest.mark.integration
@pytest.mark.parametrize(
    ("failing", "error"),
    [(FailingSink("sqs"), ConnectionError), (SlowSink("sqs"), TimeoutError)],
    ids=["error", "timeout"],
)
async def test_publish_retry_sends_the_stored_result(results_pool, monkeypatch, failing, error):
    # The first attempt stores the result, but its send fails or runs out of time (each
    # send has its own timeout, N27). The ledger records nothing. The retry inserts
    # nothing and sends the stored result, with the first attempt's created_at.
    monkeypatch.setattr(publish_module, "SEND_TIMEOUT_S", 0.05)
    runtime = _runtime(failing)

    with pytest.raises(error):
        await publish(STATE, runtime)
    [row] = await _rows(results_pool, "SELECT result FROM triage_results")
    assert await _rows(results_pool, "SELECT key FROM action_ledger") == []

    sqs = RecordingSink("sqs")
    retry = dataclasses.replace(
        runtime.context, adapters=dataclasses.replace(runtime.context.adapters, sinks=(sqs,))
    )
    assert await publish(STATE, Runtime(context=retry)) == {"published": ["sqs"]}

    assert [result for result, _ in sqs.sent] == [TriageResult.model_validate(row["result"])]
    assert await _rows(results_pool, "SELECT count(*) AS n FROM triage_results") == [{"n": 1}]


@pytest.mark.integration
@pytest.mark.parametrize(
    ("state", "category", "needs_human"),
    [
        (STATE, "application", False),
        (ASKS_HUMAN, "application", True),
        # Spec §9.7: a refusal is category unknown, needs_human and refusal.
        (REFUSED, "unknown", True),
    ],
    ids=["analysis", "analysis-needs-human", "refusal"],
)
async def test_result_row_validates_as_triage_result(results_pool, state, category, needs_human):
    runtime = _runtime(RecordingSink("log"))
    run_id = _run_id(runtime)

    await publish(state, runtime)

    [row] = await _rows(results_pool, "SELECT * FROM triage_results")
    result = TriageResult.model_validate(row["result"])
    assert result.run_id == row["run_id"] == run_id
    assert result.dedup_key == row["dedup_key"] == "d" * 64
    assert result.target == state["target"]
    assert row["target"] == state["target"].model_dump(mode="json")
    assert result.assessment == state["assessment"]
    assert result.analysis == state["analysis"]
    assert result.refusal is state["refusal"]
    assert row["category"] == category
    assert result.needs_human is needs_human
    assert row["result"]["needs_human"] is needs_human
    assert result.model_id == row["model_id"] == MODEL_ID
    assert result.usage == state["usage"]
    assert row["usage"] == state["usage"].model_dump(mode="json")
    assert result.evidence_counts == {"k8s": 2, "datadog": 1, "code": 1, "notes": 1}
    assert result.created_at == row["created_at"]


async def test_publish_without_pool_logs_only(monkeypatch):
    # Studio has no database: no row, no ledger, and only the log sinks get the result.
    async def no_ledger(*args, **kwargs):
        raise AssertionError("publish must not use the ledger without a pool")

    monkeypatch.setattr(publish_module, "run_once", no_ledger)
    sqs = RecordingSink("sqs")
    runtime = _runtime(LogSink(), sqs)
    assert current_pool() is None

    with structlog.testing.capture_logs() as logs:
        update = await publish(STATE, runtime)

    assert update == {"published": ["log"]}
    assert sqs.sent == []
    [published] = [event for event in logs if event["event"] == "triage.published"]
    assert TriageResult.model_validate(published["result"]).run_id == _run_id(runtime)
    [skipped] = [event for event in logs if event["event"] == "publish.skipped"]
    assert skipped["sinks"] == ["sqs"]


def _stubbed_client() -> SQSClient:
    """A boto3 SQS client with dummy credentials: building it touches no network."""
    session = boto3.session.Session(
        aws_access_key_id="test",
        aws_secret_access_key="test",  # noqa: S106 - a fake key
        region_name="us-east-1",
    )
    return session.client("sqs")


async def test_sqs_sink_fifo_sets_dedup_id():
    client = _stubbed_client()
    result = TriageResult(
        run_id=new_id(),
        dedup_key="d" * 64,
        target=STATE["target"],
        assessment=STATE["assessment"],
        analysis=ANALYSIS,
        refusal=False,
        needs_human=False,
        model_id=MODEL_ID,
        usage=None,
        evidence_counts={"k8s": 2},
        created_at=datetime(2026, 11, 2, 10, 32, tzinfo=UTC),
    )
    key = f"publish:{result.run_id}:sqs"
    attributes = {"idempotency_key": {"DataType": "String", "StringValue": key}}

    with Stubber(client) as stub:
        stub.add_response(
            "send_message",
            {"MessageId": "m-1"},
            {
                "QueueUrl": FIFO_QUEUE,
                "MessageBody": result.model_dump_json(),
                "MessageAttributes": attributes,
                "MessageDeduplicationId": key,
                "MessageGroupId": "shop",
            },
        )
        # A standard queue takes neither a deduplication ID nor a group ID.
        stub.add_response(
            "send_message",
            {"MessageId": "m-2"},
            {
                "QueueUrl": STANDARD_QUEUE,
                "MessageBody": result.model_dump_json(),
                "MessageAttributes": attributes,
            },
        )

        fifo = SqsSink(client, FIFO_QUEUE)
        assert fifo.name == "sqs"
        assert await fifo.send(result, key) == {"sink": "sqs", "message_id": "m-1"}
        # The factory form builds the client on the first send.
        standard = SqsSink(lambda: client, STANDARD_QUEUE)
        assert await standard.send(result, key) == {"sink": "sqs", "message_id": "m-2"}
        stub.assert_no_pending_responses()
