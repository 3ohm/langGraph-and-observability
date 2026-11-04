"""The real-mode guide names every setting that the real adapters read (spec §15).

The settings come from the code, not from a list kept by hand. An AST scan reads the
adapter builders (``adapters/__init__.py``) along the path that real mode takes: a
branch whose test compares a setting with ``"real"``, ``"bedrock"`` or ``"sqs"`` counts,
its ``else`` does not, and what follows a branch that returns runs only outside real
mode. The modules of the adapters that those branches build are scanned whole. A read
is ``settings.<field>`` or ``self._settings.<field>`` of a ``Settings`` field.

A fork that deletes the example agent (``scripts/remove_example.py``) deletes its
adapters and the guide, so these tests are skipped then (as in ruling R29).
"""

import ast
import importlib.util
import re
from pathlib import Path

import pytest

import langgraph_postgres_starter
from langgraph_postgres_starter.settings import Settings

ROOT = Path(__file__).resolve().parents[2]
GUIDE = ROOT / "docs" / "guides" / "real-mode.md"
AGENT_DIR = Path(langgraph_postgres_starter.__file__).parent / "agents" / "incident_triage"
BUILDERS = AGENT_DIR / "adapters" / "__init__.py"

# The setting values that select a real adapter: APP_ADAPTERS=real, APP_LLM_MODE=bedrock
# and the sqs result sink in APP_RESULT_SINKS.
REAL_MODE_VALUES = frozenset({"real", "bedrock", "sqs"})
# The names under which code holds the settings.
SETTINGS_NAMES = frozenset({"settings", "_settings"})

pytestmark = pytest.mark.skipif(
    not BUILDERS.is_file(), reason="the example agent, and its real adapters, were removed"
)


def _settings_reads(node: ast.AST) -> set[str]:
    """Return the ``Settings`` fields that ``node`` reads, as ``<settings>.<field>``."""
    reads = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and sub.attr in Settings.model_fields:
            owner = sub.value
            name = getattr(owner, "id", None) or getattr(owner, "attr", None)
            if name in SETTINGS_NAMES:
                reads.add(sub.attr)
    return reads


def _is_real_mode_test(test: ast.expr) -> bool:
    return any(
        isinstance(node, ast.Constant) and node.value in REAL_MODE_VALUES for node in ast.walk(test)
    )


def _real_path(body: list[ast.stmt]) -> list[ast.AST]:
    """Return the parts of ``body`` that run when the settings select real adapters."""
    path: list[ast.AST] = []
    for stmt in body:
        if isinstance(stmt, ast.If) and _is_real_mode_test(stmt.test):
            path += [stmt.test, *_real_path(stmt.body)]
            if isinstance(stmt.body[-1], ast.Return | ast.Raise):
                break
        else:
            path.append(stmt)
    return path


def _module_file(module: str) -> Path:
    spec = importlib.util.find_spec(module)
    assert spec is not None and spec.origin is not None, module
    return Path(spec.origin)


def real_mode_fields() -> set[str]:
    """Return the ``Settings`` fields that real mode reads in the adapters."""
    tree = ast.parse(BUILDERS.read_text(encoding="utf-8"))
    functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
    fields = {
        field for fn in functions for part in _real_path(fn.body) for field in _settings_reads(part)
    }
    # The adapters that the real-mode branches build, by the module they come from.
    sources = {
        alias.asname or alias.name: node.module
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module
        for alias in node.names
    }
    branches = [
        node
        for fn in functions
        for node in ast.walk(fn)
        if isinstance(node, ast.If) and _is_real_mode_test(node.test)
    ]
    modules = {
        sources[name.id]
        for branch in branches
        for stmt in branch.body
        for name in ast.walk(stmt)
        if isinstance(name, ast.Name) and name.id in sources
    }
    for module in modules:
        fields |= _settings_reads(ast.parse(_module_file(module).read_text(encoding="utf-8")))
    return fields


def env_name(field: str) -> str:
    """Return the environment variable of a ``Settings`` field: its alias, or APP_<FIELD>."""
    alias = Settings.model_fields[field].validation_alias
    if isinstance(alias, str):
        return alias
    return f"{Settings.model_config.get('env_prefix', '')}{field}".upper()


def test_real_mode_scan_skips_the_simulated_branches():
    fields = real_mode_fields()
    # The mode switches, a setting of each real adapter, and the sqs sink's queue.
    assert {"adapters", "llm_mode", "dd_api_key", "git_token", "bedrock_model_id"} <= fields
    assert "result_queue_url" in fields
    # Only the simulated adapters and the recorded model read these.
    assert fields.isdisjoint({"fixtures_dir", "fake_datadog_delay_s", "fake_call_log_dir"})


def test_guide_lists_every_real_mode_setting():
    assert GUIDE.is_file(), f"{GUIDE.relative_to(ROOT)} is missing"
    guide = GUIDE.read_text(encoding="utf-8")
    names = sorted(env_name(field) for field in real_mode_fields())
    missing = [name for name in names if not re.search(rf"\b{re.escape(name)}\b", guide)]
    assert not missing, f"the real-mode guide does not name {missing}"
