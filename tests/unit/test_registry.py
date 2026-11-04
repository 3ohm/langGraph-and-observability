"""The agent registry: built-in agents, then included specs, then APP_EXTRA_AGENTS (spec 13.9)."""

import dataclasses
import importlib.util

import pytest

from langgraph_postgres_starter.settings import load_settings
from langgraph_postgres_starter.worker.registry import builtin_agents, load_agents
from tests.graphs.steps import STEPS_SPEC

STEPS_ENTRY = "tests.graphs.steps:STEPS_SPEC"
# The example agent's spec module; scripts/remove_example.py deletes it.
EXAMPLE_SPEC = "langgraph_postgres_starter.agents.incident_triage.spec"


@pytest.mark.skipif(
    importlib.util.find_spec(EXAMPLE_SPEC.rpartition(".")[0]) is None,
    reason="the example agent was removed",
)
def test_registry_includes_the_example_agent():
    # Imported by name: the module is gone, and mypy cannot see it, after the removal.
    example = importlib.import_module(EXAMPLE_SPEC).spec

    assert example in builtin_agents()
    assert load_agents(load_settings())["incident_triage"] is example


def test_load_agents_reads_extra_agents(monkeypatch):
    monkeypatch.setenv("APP_EXTRA_AGENTS", f'["{STEPS_ENTRY}"]')

    agents = load_agents(load_settings())

    assert agents["test_steps"] is STEPS_SPEC


def test_load_agents_order_is_builtins_then_include_then_extra():
    other = dataclasses.replace(STEPS_SPEC, name="other")

    agents = load_agents(load_settings(extra_agents=[STEPS_ENTRY]), include=[other])

    assert list(agents) == [*(spec.name for spec in builtin_agents()), "other", "test_steps"]


@pytest.mark.parametrize(
    "entry",
    [
        "tests.graphs.steps",
        "tests.graphs.steps:",
        ":STEPS_SPEC",
        "tests.graphs.steps:STEPS_SPEC:extra",
        "tests.graphs.steps:STEPS_SPEC.name",
        "tests graphs:STEPS_SPEC",
        "tests..graphs:STEPS_SPEC",
    ],
)
def test_malformed_entry_is_rejected(entry):
    with pytest.raises(ValueError, match="'module:attr'"):
        load_agents(load_settings(extra_agents=[entry]))


def test_missing_attribute_is_rejected():
    with pytest.raises(ValueError, match="has no attribute 'NO_SUCH_SPEC'"):
        load_agents(load_settings(extra_agents=["tests.graphs.steps:NO_SUCH_SPEC"]))


def test_attribute_that_is_not_an_agent_spec_is_rejected():
    with pytest.raises(ValueError, match="FAULTS is a dict, not an AgentSpec"):
        load_agents(load_settings(extra_agents=["tests.graphs.steps:FAULTS"]))


def test_missing_module_raises_its_import_error():
    # The import error names the module, and a failing import inside the module keeps
    # its own traceback.
    with pytest.raises(ModuleNotFoundError, match="no_such_module"):
        load_agents(load_settings(extra_agents=["no_such_module:SPEC"]))


def test_duplicate_agent_name_is_rejected():
    with pytest.raises(ValueError, match="duplicate agent name 'test_steps'"):
        load_agents(load_settings(extra_agents=[STEPS_ENTRY]), include=[STEPS_SPEC])
