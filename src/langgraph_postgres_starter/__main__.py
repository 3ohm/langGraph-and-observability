"""Command-line entry point: ``python -m langgraph_postgres_starter``.

``--version`` prints the version. The commands (``worker``, ``migrate``, ``runs``) live
in ``worker/cli.py``. That module loads only when a command runs, so ``--version``
works without the worker package, which a fork may delete (spec §13.9).
"""

import argparse
from collections.abc import Sequence

from langgraph_postgres_starter import __version__

PROG = "python -m langgraph_postgres_starter"
COMMANDS = ("worker", "migrate", "runs")


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
    # Imported here, so --version never imports the worker package.
    from langgraph_postgres_starter.worker import cli

    return cli.main([args.command, *args.args])


if __name__ == "__main__":
    raise SystemExit(main())
