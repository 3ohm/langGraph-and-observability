"""The operator commands of ``python -m langgraph_and_observability`` (spec §7.3, §8.6).

- ``worker [ARGS]``: run the queue worker. ``ARGS`` go to ``worker.main.main``.
- ``migrate [--components a,b]``: bring the schema up to date (``run_migrate``). It
  prints one line per SQL migration that it applied, and one line for LangGraph's
  ``setup()``, which runs whenever the ``langgraph`` component is selected.
- ``runs create AGENT --input JSON [--dedup-key KEY]``: add a pending run, which a
  worker's recovery loop claims. It validates the input with the agent's input model and
  stores what the model dumps, as SQS intake does. Without ``--dedup-key``, the agent's
  ``dedup_key`` gives the key, from the input and the current time. It prints
  ``run_id``, ``created`` (false when a run already had the key) and ``dedup_key``.
- ``runs list [--status S] [--limit N]``, ``runs show RUN_ID``, ``runs retry RUN_ID``:
  read runs, and return a failed run to pending.

Results go to stdout as JSON lines, with UUIDs and datetimes as ISO 8601 strings, and
the exit code is 0. An error goes to stderr as one JSON line with an ``error`` key:

- exit 1: the run does not exist (``not found``), ``retry`` names a run that is not
  failed (``not retryable``), ``create`` names an agent that is not registered
  (``unknown agent``), or its input fails validation (``invalid input``, with
  ``details``: where each failure is and why, never the value that failed);
- exit 2: the settings are wrong, for example no database URL, or Postgres can't be
  reached. The message never holds the DSN. Usage errors also exit 2, for example an
  ``--input`` that is not JSON; that message never quotes the input.

``migrate`` and ``runs`` leave logging unconfigured, so stdout holds their results only.
"""

import argparse
import asyncio
import dataclasses
import json
import sys
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from langgraph_and_observability.persistence.migrate import LANGGRAPH, Migration, run_migrate
from langgraph_and_observability.persistence.pool import (
    DatabaseUnavailable,
    create_pool,
    open_pool,
)
from langgraph_and_observability.redaction import redact
from langgraph_and_observability.settings import load_settings
from langgraph_and_observability.worker.executor import validation_details
from langgraph_and_observability.worker.registry import load_agents
from langgraph_and_observability.worker.runs import RunRepo, RunRow, RunStatus

PROG = "python -m langgraph_and_observability"

EXIT_RUN_ERROR = 1
EXIT_SETUP_ERROR = 2

type _RunsAction = Callable[[RunRepo, argparse.Namespace], Awaitable[int]]


def build_parser() -> argparse.ArgumentParser:
    """Return the parser of every command except ``worker``, which parses its own ``ARGS``."""
    parser = argparse.ArgumentParser(prog=PROG, description="langgraph-and-observability commands.")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")
    commands.add_parser("worker", add_help=False, help="run the queue worker")

    migrate = commands.add_parser("migrate", help="apply the database migrations")
    migrate.add_argument(
        "--components",
        type=_components,
        metavar="A,B",
        help="the components to migrate, comma-separated (default: all): core, langgraph, "
        "worker and each agent's name",
    )
    migrate.set_defaults(handler=_migrate)

    runs = commands.add_parser("runs", help="create, list, show or retry runs")
    actions = runs.add_subparsers(dest="action", required=True, metavar="ACTION")
    create = actions.add_parser("create", help="add a pending run, which a worker claims")
    create.add_argument("agent", metavar="AGENT")
    create.add_argument(
        "--input",
        type=_json_value,
        required=True,
        metavar="JSON",
        help="the run's input, which the agent's input model validates",
    )
    create.add_argument(
        "--dedup-key", metavar="KEY", help="(default: the agent's dedup_key of the input)"
    )
    create.set_defaults(handler=_runs, run=_create)
    list_ = actions.add_parser("list", help="print runs, newest first")
    list_.add_argument("--status", choices=[s.value for s in RunStatus])
    list_.add_argument("--limit", type=_positive_int, default=50, help="(default: 50)")
    list_.set_defaults(handler=_runs, run=_list)
    show = actions.add_parser("show", help="print one run")
    show.add_argument("run_id", type=UUID, metavar="RUN_ID")
    show.set_defaults(handler=_runs, run=_show)
    retry = actions.add_parser("retry", help="return a failed run to pending")
    retry.add_argument("run_id", type=UUID, metavar="RUN_ID")
    retry.set_defaults(handler=_runs, run=_retry)
    return parser


def main(argv: Sequence[str]) -> int:
    """Run one command, for example ``["runs", "show", RUN_ID]``. Returns the exit code."""
    if argv and argv[0] == "worker":
        return _worker(argv[1:])
    args = build_parser().parse_args(argv)
    handler: Callable[[argparse.Namespace], int] = args.handler
    return handler(args)


def _worker(argv: Sequence[str]) -> int:
    # Imported here: only this command loads the worker's runtime.
    from langgraph_and_observability.worker.main import main as worker_main  # pylint: disable=import-outside-toplevel

    return worker_main(list(argv))


def _migrate(args: argparse.Namespace) -> int:
    components: set[str] | None = args.components
    try:
        settings = load_settings()
        agent_sources = [
            (s.name, s.migrations_dir) for s in load_agents(settings).values() if s.migrations_dir
        ]
        applied = asyncio.run(
            run_migrate(settings, components=components, agent_sources=agent_sources)
        )
    except (ValueError, DatabaseUnavailable) as exc:
        return _setup_error(exc)
    # The order in which run_migrate works: core, then setup(), then the rest.
    for migration in applied:
        if migration.component == "core":
            _print(_migration_line(migration))
    if components is None or LANGGRAPH in components:
        _print({"component": LANGGRAPH, "step": "setup"})
    for migration in applied:
        if migration.component != "core":
            _print(_migration_line(migration))
    return 0


def _runs(args: argparse.Namespace) -> int:
    return asyncio.run(_with_repo(args.run, args))


async def _with_repo(run: _RunsAction, args: argparse.Namespace) -> int:
    try:
        settings = load_settings()
        pool = create_pool(settings)
        await open_pool(pool, timeout=settings.db_connect_timeout_s)
    except (ValueError, DatabaseUnavailable) as exc:
        return _setup_error(exc)
    try:
        return await run(RunRepo(pool), args)
    finally:
        await pool.close()


async def _create(repo: RunRepo, args: argparse.Namespace) -> int:
    try:
        agents = load_agents(load_settings())
    except ValueError as exc:
        return _setup_error(exc)
    spec = agents.get(args.agent)
    if spec is None:
        return _error(EXIT_RUN_ERROR, {"error": "unknown agent", "agent": args.agent})
    try:
        model = spec.input_model.model_validate(args.input)
    except ValidationError as exc:
        details = validation_details(exc)
        return _error(EXIT_RUN_ERROR, {"error": "invalid input", "details": details})
    dedup_key: str | None = args.dedup_key
    if dedup_key is None:
        dedup_key = spec.dedup_key(model, datetime.now(UTC))
    run_id, created = await repo.create(spec.name, dedup_key, model.model_dump(mode="json"))
    _print({"run_id": run_id, "created": created, "dedup_key": dedup_key})
    return 0


async def _list(repo: RunRepo, args: argparse.Namespace) -> int:
    status = None if args.status is None else RunStatus(args.status)
    for run in await repo.list(status=status, limit=args.limit):
        _print(_run_line(run))
    return 0


async def _show(repo: RunRepo, args: argparse.Namespace) -> int:
    run = await repo.get(args.run_id)
    if run is None:
        return _error(EXIT_RUN_ERROR, {"error": "not found", "run_id": str(args.run_id)})
    _print(_run_line(run))
    return 0


async def _retry(repo: RunRepo, args: argparse.Namespace) -> int:
    run_id: UUID = args.run_id
    if await repo.get(run_id) is None:
        return _error(EXIT_RUN_ERROR, {"error": "not found", "run_id": str(run_id)})
    if not await repo.retry(run_id):
        return _error(EXIT_RUN_ERROR, {"error": "not retryable", "run_id": str(run_id)})
    _print({"run_id": str(run_id), "status": RunStatus.PENDING.value})
    return 0


def _components(value: str) -> set[str]:
    names = {name.strip() for name in value.split(",")} - {""}
    if not names:
        raise argparse.ArgumentTypeError("name at least one component")
    return names


def _json_value(value: str) -> Any:
    try:
        return json.loads(value)
    # json.loads raises RecursionError for a value that is nested too deeply. Neither
    # message quotes the text, which can hold secrets.
    except (json.JSONDecodeError, RecursionError) as exc:
        raise argparse.ArgumentTypeError(f"not valid JSON: {exc}") from None


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a whole number: {value!r}") from None
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be 1 or more, not {number}")
    return number


def _migration_line(migration: Migration) -> dict[str, Any]:
    return {"component": migration.component, "version": migration.version, "name": migration.name}


def _run_line(run: RunRow) -> dict[str, Any]:
    return dataclasses.asdict(run)


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def _print(line: dict[str, Any]) -> None:
    print(json.dumps(line, default=_json_default))


def _error(code: int, line: dict[str, Any]) -> int:
    print(json.dumps(line, default=_json_default), file=sys.stderr)
    return code


def _setup_error(exc: Exception) -> int:
    # The messages of create_pool, open_pool and run_migrate do not quote the DSN; redact()
    # removes a password in case a settings error quotes a URL.
    return _error(EXIT_SETUP_ERROR, {"error": redact(str(exc))})
