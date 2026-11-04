"""Checks on the CI workflow and the Dependabot config (spec 13.3 and 13.4)."""

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WF = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())


def test_actions_pinned_to_sha():
    uses = [s["uses"] for j in WF["jobs"].values() for s in j["steps"] if "uses" in s]
    assert uses and all(re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", u) for u in uses)


def test_minimal_permissions():
    assert WF["permissions"] == {"contents": "read"}


def test_canary_does_not_block():
    j = WF["jobs"]["canary-3-15"]
    assert j["continue-on-error"] is True and "3.15" in json.dumps(j)


def test_dependabot_weekly_grouped_with_cooldown():
    d = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    eco = {u["package-ecosystem"]: u for u in d["updates"]}
    assert {"uv", "github-actions"} <= set(eco)
    assert eco["uv"]["schedule"]["interval"] == "weekly"
    assert eco["uv"]["cooldown"]["default-days"] == 7
    assert "langgraph*" in eco["uv"]["groups"]["langchain"]["patterns"]
