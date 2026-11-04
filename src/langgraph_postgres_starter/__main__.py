"""Command-line entry point: ``python -m langgraph_postgres_starter``."""

import argparse
from collections.abc import Sequence

from langgraph_postgres_starter import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="langgraph-postgres-starter operator CLI.")
    parser.add_argument("--version", action="version", version=__version__)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    build_parser().parse_args(argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
