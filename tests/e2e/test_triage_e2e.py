"""The Incident Triage agent end to end, in simulated mode (spec §10.2, §15; P11, P13).

Each test starts ``python -m langgraph_postgres_starter worker`` subprocesses
(``helpers.start_worker``) on a fresh, migrated database and a new ElasticMQ queue,
with the simulated adapters and the recorded model, as ``docker compose up`` runs the
worker. The tests send incidents with ``scripts/send_incident.py``, as ``make incident``
does.

- Every incident fixture becomes one ``TriageResult`` in ``triage_results``, with the
  category of the fixture's ``expected.json`` (spec §15, "No-credential run").
- SIGKILL stops a worker while ``collect_datadog`` waits in the fake Datadog adapter
  (``APP_FAKE_DATADOG_DELAY_S``). Another worker resumes the run at that step. The fake
  adapters' call log (``APP_FAKE_CALL_LOG_DIR``) shows that the Kubernetes calls ran
  as often as in a clean run, that the Datadog search started twice, and that the
  result was published once (spec §15, "Resume"; P11, P13).

The agent's modules are imported with ``pytest.importorskip``: a fork that deletes the
example agent still collects the suite, and pytest skips this module.
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Literal, LiteralString
from uuid import UUID

import psycopg
import pytest
from psycopg.rows import dict_row

from langgraph_postgres_starter.persistence.migrate import run_migrate
from langgraph_postgres_starter.settings import load_settings
from tests.e2e.helpers import REPO_ROOT, free_port, start_worker, wait_for

triage = pytest.importorskip("langgraph_postgres_starter.agents.incident_triage.spec")
loader = pytest.importorskip("langgraph_postgres_starter.agents.incident_triage.fixtures.loader")
schemas = pytest.importorskip("langgraph_postgres_starter.agents.incident_triage.schemas")

pytestmark = pytest.mark.e2e

SEND_INCIDENT = REPO_ROOT / "scripts" / "send_incident.py"

# How long the script may take to send one message.
SEND_TIMEOUT_S = 60
# How long a worker may take to start and take a message from the queue.
START_TIMEOUT_S = 60
# How long a run may take to succeed, also after a second worker takes it over.
FINISH_TIMEOUT_S = 60
# How long one worker may take to store the results of every fixture.
RESULTS_TIMEOUT_S = 120
# Short leases, heartbeats and recovery, so another worker takes over within seconds.
LEASE_S = 3
HEARTBEAT_S = 1
RECOVERY_INTERVAL_S = 1
# Short long polls, so that a stopping worker's last receive ends soon.
SQS_WAIT_S = 1
# How long the fake Datadog search waits, so that a test can kill the worker meanwhile.
DATADOG_DELAY_S = 5

# The incident of the kill -9 test, and the alert ID of its clean run.
RESUME_FIXTURE = "python-keyerror"
CLEAN_ALERT_ID = "e2e-clean-run"
DATADOG_START = "datadog.search_events:start"

type StartWorker = Callable[..., subprocess.Popen[bytes]]
type Send = Callable[..., str]


async def _migrate(dsn: str) -> None:
    """Apply every migration: core, langgraph, worker and the incident_triage agent's."""
    spec = triage.spec
    await run_migrate(
        load_settings(database_url=dsn), agent_sources=[(spec.name, spec.migrations_dir)]
    )


@pytest.fixture
def db(fresh_dsn: str) -> str:
    """fresh_dsn with every migration applied."""
    asyncio.run(_migrate(fresh_dsn))
    return fresh_dsn


@pytest.fixture
def queue_url(sqs: Any) -> str:
    url: str = sqs.create_queue(QueueName=f"e2e-triage-{uuid.uuid4().hex}")["QueueUrl"]
    return url


@pytest.fixture
def calls_dir(tmp_path: Path) -> Path:
    """The fake adapters' call log folder: one ``<run_id>.log`` per run."""
    return tmp_path / "calls"


@pytest.fixture
def workers(
    db: str, sqs_endpoint: str, queue_url: str, calls_dir: Path, tmp_path: Path
) -> Iterator[StartWorker]:
    """Start triage workers for ``db`` and ``queue_url``: ``workers(name, datadog_delay_s=...)``.

    Each worker runs the simulated adapters and the recorded model, writes the fake
    call log to ``calls_dir``, gets its own health port and ``POD_NAME``
    (``e2e-triage-<name>``), and writes its output to ``tmp_path / "<name>.log"``.
    Afterwards, the fixture kills every worker that still runs and prints each log,
    which pytest shows when a test fails.
    """
    started: list[tuple[str, subprocess.Popen[bytes], Path]] = []

    def start(name: str, *, datadog_delay_s: float = 0.0) -> subprocess.Popen[bytes]:
        env = {
            "APP_ENV": "test",
            "APP_DATABASE_URL": db,
            "APP_ADAPTERS": "simulated",
            "APP_LLM_MODE": "recorded",
            "APP_SQS_ENDPOINT_URL": sqs_endpoint,
            "APP_SQS_QUEUE_URL": queue_url,
            "APP_SQS_WAIT_SECONDS": str(SQS_WAIT_S),
            "APP_LEASE_SECONDS": str(LEASE_S),
            "APP_HEARTBEAT_SECONDS": str(HEARTBEAT_S),
            "APP_RECOVERY_INTERVAL_SECONDS": str(RECOVERY_INTERVAL_S),
            "APP_HEALTH_PORT": str(free_port()),
            "POD_NAME": f"e2e-triage-{name}",
            "APP_FAKE_CALL_LOG_DIR": str(calls_dir),
            "APP_FAKE_DATADOG_DELAY_S": str(datadog_delay_s),
            # ElasticMQ accepts any credentials; botocore needs some to sign requests.
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            # As in the image (Dockerfile).
            "LANGGRAPH_STRICT_MSGPACK": "true",
        }
        log_path = tmp_path / f"{name}.log"
        proc = start_worker(env, log_path=log_path)
        started.append((name, proc, log_path))
        return proc

    yield start
    for name, proc, log_path in started:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        print(f"--- worker {name}: exit code {proc.returncode}, log {log_path}")
        print(log_path.read_text(errors="replace"))


@pytest.fixture
def send(sqs_endpoint: str, queue_url: str) -> Send:
    """Run ``scripts/send_incident.py`` for ``queue_url``: ``send("--fixture", id)``.

    Returns the SQS message ID that the script prints. The script gets the test
    process's environment without its AWS variables, and dummy credentials.
    """

    def run(*args: str) -> str:
        env = {
            name: value for name, value in os.environ.items() if not name.upper().startswith("AWS_")
        }
        env |= {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}
        # The command is fixed: this interpreter and the repository's own script.
        proc = subprocess.run(  # noqa: S603
            [
                sys.executable,
                str(SEND_INCIDENT),
                *args,
                "--queue-url",
                queue_url,
                "--endpoint-url",
                sqs_endpoint,
            ],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=SEND_TIMEOUT_S,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        message_id = proc.stdout.strip()
        assert message_id and len(message_id.split()) == 1, proc.stdout
        return message_id

    return run


def _alive(proc: subprocess.Popen[bytes]) -> Literal[True]:
    """True while ``proc`` runs. A worker that has exited fails the test at once."""
    if (code := proc.poll()) is not None:
        raise AssertionError(f"the worker exited with code {code}; its log is printed below")
    return True


def _query(dsn: str, sql: LiteralString, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        return conn.execute(sql, params).fetchall()


def _run_id(dsn: str, alert_id: str) -> UUID | None:
    """The run that intake recorded for the message with ``alert_id``, or None."""
    rows = _query(dsn, "SELECT run_id FROM runs WHERE input->>'alert_id' = %s", (alert_id,))
    return rows[0]["run_id"] if rows else None


def _succeeded(dsn: str, run_id: UUID) -> bool:
    rows = _query(dsn, "SELECT status FROM runs WHERE run_id = %s", (run_id,))
    return bool(rows) and rows[0]["status"] == "success"


def _wait_for_success(worker: subprocess.Popen[bytes], dsn: str, run_id: UUID) -> None:
    wait_for(lambda: _alive(worker) and _succeeded(dsn, run_id), FINISH_TIMEOUT_S)


def _results(dsn: str, count: int) -> list[dict[str, Any]] | None:
    """Every stored result with its run's fixture ID, once there are ``count``; else None."""
    rows = _query(
        dsn,
        "SELECT r.input->'raw'->>'fixture_id' AS fixture_id, t.result"
        " FROM triage_results t JOIN runs r USING (run_id)",
    )
    return rows if len(rows) >= count else None


def _calls(calls_dir: Path, run_id: UUID) -> Counter[str]:
    """The run's fake adapter calls, counted by line (``adapters/call_log.py``)."""
    path = calls_dir / f"{run_id}.log"
    return Counter(path.read_text(encoding="utf-8").splitlines()) if path.exists() else Counter()


def _k8s(calls: Counter[str]) -> dict[str, int]:
    return {line: count for line, count in calls.items() if line.startswith("k8s.")}


def test_all_fixtures_produce_results(workers, send, db):
    fixture_ids = loader.list_fixtures()
    worker = workers("all")
    for fixture_id in fixture_ids:
        send("--fixture", fixture_id)

    rows = wait_for(lambda: _alive(worker) and _results(db, len(fixture_ids)), RESULTS_TIMEOUT_S)

    # Each stored result is a valid TriageResult. A result without an analysis (a
    # refusal) has no category, so it fails the comparison below.
    categories: dict[str, str | None] = {}
    for row in rows:
        result = schemas.TriageResult.model_validate(row["result"])
        categories[row["fixture_id"]] = result.analysis.category if result.analysis else None
    expected = {fid: loader.load_fixture(fid).expected["category"] for fid in fixture_ids}
    assert categories == expected
    assert len(rows) == len(fixture_ids)


def test_kill_9_during_datadog_resumes_at_datadog(workers, send, db, calls_dir, tmp_path):
    incident = loader.load_fixture(RESUME_FIXTURE).incident
    worker_a = workers("a", datadog_delay_s=DATADOG_DELAY_S)

    # A clean run of the same incident, for its call counts. Another alert ID gives it
    # a dedup key, and so a run, of its own.
    clean_message = tmp_path / "clean.json"
    clean_message.write_text(json.dumps({**incident, "alert_id": CLEAN_ALERT_ID}))
    send("--message-file", str(clean_message))
    clean_id = wait_for(lambda: _alive(worker_a) and _run_id(db, CLEAN_ALERT_ID), START_TIMEOUT_S)
    _wait_for_success(worker_a, db, clean_id)
    clean = _calls(calls_dir, clean_id)

    # The run to kill: worker A dies while the fake Datadog search waits.
    send("--fixture", RESUME_FIXTURE)
    run_id = wait_for(
        lambda: _alive(worker_a) and _run_id(db, incident["alert_id"]), START_TIMEOUT_S
    )
    wait_for(
        lambda: _alive(worker_a) and DATADOG_START in _calls(calls_dir, run_id), START_TIMEOUT_S
    )
    worker_a.send_signal(signal.SIGKILL)
    assert worker_a.wait(timeout=10) == -signal.SIGKILL
    worker_b = workers("b", datadog_delay_s=DATADOG_DELAY_S)
    _wait_for_success(worker_b, db, run_id)

    calls = _calls(calls_dir, run_id)
    # The Kubernetes evidence was not collected again: each k8s call ran as often as in
    # the clean run.
    assert _k8s(clean)
    assert _k8s(calls) == _k8s(clean)
    # The Datadog step started in each worker, and the result was published once.
    assert calls[DATADOG_START] == 2
    assert calls["sink.log.send"] == 1
    # Every other call ran as in the clean run.
    assert calls == clean + Counter({DATADOG_START: 1})
