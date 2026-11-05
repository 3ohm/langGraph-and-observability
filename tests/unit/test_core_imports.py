"""The core modules import none of the example agent's dependencies (F4).

``scripts/remove_example.py`` prints a ``uv remove`` command for these dependencies. After
it, every module outside ``agents/incident_triage/`` must still import, so none of them
may import one.
"""

import ast
from pathlib import Path

import langgraph_and_observability

PACKAGE_DIR = Path(langgraph_and_observability.__file__).parent
EXAMPLE_DIR = PACKAGE_DIR / "agents" / "incident_triage"
# The import names of the example's dependencies, and of aiosonic, which the Datadog
# client's async extra brings.
EXAMPLE_PACKAGES = {
    "kubernetes_asyncio",
    "datadog_api_client",
    "aiosonic",
    "aiohttp",
    "tree_sitter",
    "tree_sitter_language_pack",
}


def _imported_packages(path: Path) -> set[str]:
    """Return the top-level packages that the module at ``path`` imports (absolute imports)."""
    packages: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            packages.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            packages.add(node.module.split(".")[0])
    return packages


def test_core_imports_no_example_dependency():
    core = [path for path in sorted(PACKAGE_DIR.rglob("*.py")) if EXAMPLE_DIR not in path.parents]
    assert PACKAGE_DIR / "settings.py" in core  # the walk finds the core modules
    found = {
        path.relative_to(PACKAGE_DIR).as_posix(): sorted(imported & EXAMPLE_PACKAGES)
        for path in core
        if (imported := _imported_packages(path)) & EXAMPLE_PACKAGES
    }
    assert found == {}
