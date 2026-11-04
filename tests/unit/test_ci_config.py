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


def test_test_jobs_run_their_make_targets():
    runs = {name: [s.get("run") for s in job["steps"]] for name, job in WF["jobs"].items()}
    assert "make test-int" in runs["integration"]
    assert "make test-e2e" in runs["e2e"]
    assert {"postgres", "elasticmq"} <= set(WF["jobs"]["e2e"]["services"])


def test_test_jobs_fetch_grammars_after_sync():
    # Ruling R51: tests never download a grammar, so each job that runs tests fetches them first.
    for name in ("unit", "integration", "e2e", "canary-3-15"):
        runs = [s.get("run") for s in WF["jobs"][name]["steps"]]
        sync = runs.index("uv sync --all-groups --frozen")
        assert runs[sync + 1] == "make grammars", name


def test_docker_job_smoke_tests_migrations():
    # Spec 13.3 and P4: the built image migrates an empty Compose Postgres.
    script = "\n".join(s.get("run", "") for s in WF["jobs"]["docker"]["steps"])
    assert "docker compose run --rm --no-deps migrate" in script
