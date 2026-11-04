import subprocess
import sys
from importlib.metadata import version

import langgraph_postgres_starter as pkg


def test_version_matches_metadata():
    assert pkg.__version__ == version("langgraph-postgres-starter") == "0.1.0.dev0"


def test_cli_prints_version():
    out = subprocess.run(
        [sys.executable, "-m", "langgraph_postgres_starter", "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == pkg.__version__
