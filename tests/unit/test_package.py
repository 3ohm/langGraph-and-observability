import ast
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path

import pytest

import langgraph_postgres_starter as pkg
from langgraph_postgres_starter import __main__


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


def _imported_modules(path: Path) -> set[str]:
    """The modules that ``path`` imports, by absolute name (relative imports are unused here)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.update([node.module, *(f"{node.module}.{alias.name}" for alias in node.names)])
    return names


def test_only_the_worker_imports_the_worker():
    # Spec 13.9: a fork may delete worker/, so nothing else in the package imports it.
    package_dir = Path(pkg.__file__).parent
    worker = f"{pkg.__name__}.worker"
    importers = [
        path.relative_to(package_dir).as_posix()
        for path in sorted(package_dir.rglob("*.py"))
        if path.relative_to(package_dir).parts[0] != "worker"
        and any(name == worker or name.startswith(f"{worker}.") for name in _imported_modules(path))
    ]
    assert importers == []


def test_commands_without_the_worker_package_exit_with_a_usage_error(monkeypatch, capsys):
    # Spec 13.9: in a fork without worker/, --version still works and a command says why
    # it cannot run. None in sys.modules makes an import fail as for a deleted package.
    worker = f"{pkg.__name__}.worker"
    monkeypatch.setitem(sys.modules, worker, None)
    monkeypatch.delitem(sys.modules, f"{worker}.cli", raising=False)

    with pytest.raises(SystemExit) as exit_info:
        __main__.main(["migrate"])

    assert exit_info.value.code == 2
    assert "the worker package" in capsys.readouterr().err
