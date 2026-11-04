"""The ``publish`` step: store the run's result, then send it to each sink once (spec §9.9; P13).

1. The step builds the ``TriageResult`` from the state, which holds redacted evidence
   only: no settings, adapters or secrets go into it. With no analysis (a refusal, spec
   §9.7) its ``analysis`` is None, ``needs_human`` is True and its row's ``category`` is
   ``unknown``; otherwise ``needs_human`` is the analysis's.
2. With a pool (``current_pool()``, which the worker registers), it inserts the result
   into ``triage_results`` with ``ON CONFLICT (run_id) DO NOTHING``. When the row is
   there already, from an earlier attempt of the step, the sinks get the stored result,
   so they get what the table holds.
3. It sends the result to each sink, in the order of ``APP_RESULT_SINKS``, through the
   action ledger: ``run_once`` with the key ``publish:<run_id>:<sink>``, which the sink
   also gets as its idempotency key. A key that the ledger holds is not sent again.
   ``run_once`` holds a pooled connection and the key's advisory lock while the send
   runs, so each send has its own timeout, ``SEND_TIMEOUT_S``, well under the step's.
4. Without a pool (Studio, or a graph test without a database) the step uses no
   database and no ledger. Only the ``log`` sinks get the result; it logs
   ``publish.skipped`` with the others, which without the ledger could send twice.

It returns ``{"published": [<the sinks that got the result>]}``.

P13: a crash after the sends and before the step's checkpoint makes the step run again.
The insert then does nothing and the ledger replays every recorded send, so each sink
gets the result once. A crash between a send and its ledger row makes that one send
again (at-least-once, ``persistence/ledger.py``); a FIFO queue drops the repeat by its
deduplication ID.
"""

import asyncio
import functools
from datetime import UTC, datetime
from typing import Any

from langgraph.runtime import Runtime
from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from langgraph_postgres_starter.agents.incident_triage.adapters.sinks.interface import ResultSink
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext, ctx
from langgraph_postgres_starter.agents.incident_triage.schemas import (
    TriageResult,
    result_needs_human,
)
from langgraph_postgres_starter.agents.incident_triage.state import TriageState
from langgraph_postgres_starter.logging import get_logger
from langgraph_postgres_starter.persistence.ledger import run_once
from langgraph_postgres_starter.persistence.resources import current_pool

_log = get_logger(__name__)

type _Pool = AsyncConnectionPool[AsyncConnection[DictRow]]

# The longest one sink's send may take, in seconds: well under the step's 30 s.
SEND_TIMEOUT_S = 10.0
# The sink that the step uses without a pool: it only writes a log line.
_LOG_SINK = "log"
# The category column of a result without an analysis.
_NO_CATEGORY = "unknown"

_INSERT = """
INSERT INTO triage_results
    (run_id, dedup_key, target, category, result, model_id, usage, created_at)
VALUES
    (%(run_id)s, %(dedup_key)s, %(target)s, %(category)s, %(result)s, %(model_id)s,
     %(usage)s, %(created_at)s)
ON CONFLICT (run_id) DO NOTHING
"""
_SELECT = "SELECT result FROM triage_results WHERE run_id = %s"


async def publish(state: TriageState, runtime: Runtime[TriageContext]) -> TriageState:
    """The graph's ``publish`` step. Returns ``published``, the names of the sinks sent to.

    The module docstring gives the rules. A sink's error, or ``TimeoutError`` after
    ``SEND_TIMEOUT_S``, is raised as it is; the ledger then holds no row for that send,
    so the next attempt of the step sends it.
    """
    context = ctx(runtime)
    result = _build_result(state, context)
    sinks = context.adapters.sinks
    pool = current_pool()
    if pool is None:
        return {"published": await _log_only(result, sinks)}
    result = await _store(pool, result)
    run_id = str(result.run_id)
    for sink in sinks:
        key = f"publish:{run_id}:{sink.name}"
        outcome = await run_once(
            pool,
            key=key,
            action="publish",
            args={"run_id": run_id},
            fn=functools.partial(_send, sink, result, key),
        )
        _log.info("publish.sent", sink=sink.name, replayed=outcome.replayed)
    return {"published": [sink.name for sink in sinks]}


def _build_result(state: TriageState, context: TriageContext) -> TriageResult:
    """The run's result, from the state after ``analyze``, created now."""
    analysis = state.get("analysis")
    return TriageResult(
        run_id=context.run.run_id,
        dedup_key=context.run.dedup_key,
        target=state["target"],
        assessment=state["assessment"],
        analysis=analysis,
        refusal=state.get("refusal", False),
        needs_human=result_needs_human(analysis),
        model_id=state.get("model_id"),
        usage=state.get("usage"),
        evidence_counts={
            "k8s": len(state.get("k8s_evidence", [])),
            "datadog": len(state.get("datadog_evidence", [])),
            "code": len(state.get("code_evidence", [])),
            "notes": len(state.get("notes", [])),
        },
        created_at=datetime.now(UTC),
    )


async def _store(pool: _Pool, result: TriageResult) -> TriageResult:
    """Insert ``result`` unless its run has a row; return the stored result."""
    params = {
        "run_id": result.run_id,
        "dedup_key": result.dedup_key,
        "target": Jsonb(result.target.model_dump(mode="json")),
        "category": result.analysis.category if result.analysis else _NO_CATEGORY,
        "result": Jsonb(result.model_dump(mode="json")),
        "model_id": result.model_id,
        "usage": None if result.usage is None else Jsonb(result.usage.model_dump(mode="json")),
        "created_at": result.created_at,
    }
    async with pool.connection() as conn:
        cur = await conn.execute(_INSERT, params)
        if cur.rowcount == 1:
            return result
        cur = await conn.execute(_SELECT, [result.run_id])
        row = await cur.fetchone()
    if row is None:
        raise RuntimeError(f"triage_results has no row for run {result.run_id} after the insert")
    _log.info("publish.already_stored", run_id=str(result.run_id))
    return TriageResult.model_validate(row["result"])


async def _send(sink: ResultSink, result: TriageResult, key: str) -> dict[str, Any]:
    """One sink's send, cut off after ``SEND_TIMEOUT_S``."""
    async with asyncio.timeout(SEND_TIMEOUT_S):
        return await sink.send(result, key)


async def _log_only(result: TriageResult, sinks: tuple[ResultSink, ...]) -> list[str]:
    """Send ``result`` to the ``log`` sinks only; return their names."""
    logged = [sink for sink in sinks if sink.name == _LOG_SINK]
    skipped = [sink.name for sink in sinks if sink.name != _LOG_SINK]
    if skipped:
        _log.warning(
            "publish.skipped",
            sinks=skipped,
            reason="no database pool, so no ledger to make the sends idempotent",
        )
    for sink in logged:
        await _send(sink, result, f"publish:{result.run_id}:{sink.name}")
    return [sink.name for sink in logged]
