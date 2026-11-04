"""Command-line entry point: ``python -m langgraph_postgres_starter``.

``--version`` prints the version. The commands (``worker``, ``migrate``, ``runs``) live
in ``worker/cli.py``. That module is imported by name, only when a command runs, so
``--version`` and the type check work without the worker package, which a fork may
delete (spec §13.9); a command then exits with a usage error that says so.
"""

import argparse
import importlib
from collections.abc import Sequence
from typing import Protocol, cast

from langgraph_postgres_starter import __version__

PROG = "python -m langgraph_postgres_starter"
COMMANDS = ("worker", "migrate", "runs")
_WORKER = f"{__package__}.worker"


class _Cli(Protocol):
    """``worker/cli.py``, as this module uses it."""

    def main(self, argv: Sequence[str]) -> int: ...


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=PROG,
        description="langgraph-postgres-starter operator CLI.",
        epilog="Run '%(prog)s COMMAND --help' for the options of one command.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument(
        "command", nargs="?", choices=COMMANDS, metavar="COMMAND", help=", ".join(COMMANDS)
    )
    # Everything after the command goes to that command's own parser.
    parser.add_argument("args", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.error(f"a command is required: {', '.join(COMMANDS)}")
    try:
        cli = cast(_Cli, importlib.import_module(f"{_WORKER}.cli"))
    except ModuleNotFoundError as exc:
        # Only a missing worker package; a module that the CLI imports raises as it is.
        if exc.name not in (_WORKER, f"{_WORKER}.cli"):
            raise
        parser.error(f"the commands need the worker package ({_WORKER}), which is not installed")
    return cli.main([args.command, *args.args])


if __name__ == "__main__":
    raise SystemExit(main())
