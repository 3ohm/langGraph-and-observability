"""Checks on the CI and Evals workflows and the Dependabot config (spec 12.3, 13.3, 13.4)."""

import json
import re
from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).resolve().parents[2]
WF = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())


def test_actions_pinned_to_sha():
    uses = [s["uses"] for j in WF["jobs"].values() for s in j["steps"] if "uses" in s]
    assert uses and all(re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", u) for u in uses)


def test_minimal_permissions():
    assert WF["permissions"] == {"contents": "read"}


def test_every_python_job_uses_python_3_14_through_setup_uv():
    # The owner's decision (2026-11-05): Python 3.14 only, installed by astral-sh/setup-uv.
    setups = [
        s
        for j in WF["jobs"].values()
        for s in j["steps"]
        if s.get("uses", "").startswith("astral-sh/setup-uv@")
    ]
    assert setups and all(s["with"]["python-version"] == "3.14" for s in setups)
    assert "actions/setup-python" not in json.dumps(WF)


def test_dependabot_weekly_grouped_with_cooldown():
    d = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    eco = {u["package-ecosystem"]: u for u in d["updates"]}
    # Python packages, actions, the Dockerfile's images and the Compose images.
    assert set(eco) == {"uv", "github-actions", "docker", "docker-compose"}
    for name, update in eco.items():
        assert update["directory"] == "/", name
        assert update["schedule"]["interval"] == "weekly", name
        assert update["cooldown"]["default-days"] == 7, name
    assert "langgraph*" in eco["uv"]["groups"]["langchain"]["patterns"]


def test_ci_services_use_the_compose_images():
    # Dependabot updates compose.yaml's images, not a workflow's service containers, so
    # these must match by hand: a Dependabot pull request for compose.yaml fails here.
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    for job in ("integration", "e2e"):
        services = WF["jobs"][job]["services"]
        for name in ("postgres", "elasticmq"):
            assert services[name]["image"] == compose[name]["image"], (job, name)


def _runs(workflow) -> list[str]:
    """The ``run`` of every step of a workflow (no typing import: the removal deletes it)."""
    return [s["run"] for j in workflow["jobs"].values() for s in j["steps"] if "run" in s]


def test_uv_commands_need_an_up_to_date_lock():
    # UV_LOCKED: every uv command in CI, also the `uv run` inside make targets, fails when
    # uv.lock does not match pyproject.toml. uv refuses --frozen alongside it.
    assert WF["env"] == {"UV_LOCKED": "1"}
    assert [run for run in _runs(WF) if "--frozen" in run] == []


def test_test_jobs_run_their_make_targets():
    runs = {name: [s.get("run") for s in job["steps"]] for name, job in WF["jobs"].items()}
    assert "make test-int" in runs["integration"]
    assert "make test-e2e" in runs["e2e"]
    assert {"postgres", "elasticmq"} <= set(WF["jobs"]["e2e"]["services"])


# example-agent:begin (scripts/remove_example.py deletes this block)
def test_test_jobs_fetch_grammars_after_sync():
    # Ruling R51: tests never download a grammar, so each job that runs tests fetches them first.
    for name in ("unit", "integration", "e2e"):
        runs = [s.get("run") for s in WF["jobs"][name]["steps"]]
        sync = runs.index("uv sync --all-groups")
        assert runs[sync + 1] == "make grammars", name


# example-agent:end


def test_test_jobs_install_ripgrep_before_tests():
    # Ruling R67: the agent's code step runs rg, and the runner image has no ripgrep.
    targets = {
        "unit": "make test",
        "integration": "make test-int",
        "e2e": "make test-e2e",
    }
    install = "sudo apt-get update && sudo apt-get install -y --no-install-recommends ripgrep"
    for name, target in targets.items():
        runs = [s.get("run") for s in WF["jobs"][name]["steps"]]
        assert install in runs, name
        assert runs.index(install) < runs.index(target), name


def test_docker_job_smoke_tests_migrations():
    # Spec 13.3 and P4: the built image migrates an empty Compose Postgres.
    script = "\n".join(s.get("run", "") for s in WF["jobs"]["docker"]["steps"])
    assert "docker compose run --rm --no-deps migrate" in script


# --- the removal job (spec 13.3, 13.9; P22) ------------------------------------------

REMOVAL = WF["jobs"]["removal"]
REMOVAL_COPY = "${{ runner.temp }}/removal"


def test_removal_job_installs_like_the_lint_job():
    # Checkout without persisted credentials, uv with its cache, then the locked sync.
    assert REMOVAL["steps"][:3] == WF["jobs"]["lint"]["steps"][:3]


def test_removal_job_removes_the_example_from_a_copy_then_checks_the_rest():
    steps = REMOVAL["steps"]
    [copy] = [i for i, s in enumerate(steps) if "git archive HEAD" in s.get("run", "")]
    assert '"$RUNNER_TEMP/removal"' in steps[copy]["run"]
    after = steps[copy + 1 :]
    assert all(s["working-directory"] == REMOVAL_COPY for s in after)
    commands = [line.strip() for s in after for line in s["run"].splitlines() if line.strip()]
    assert commands == [
        "uv sync --all-groups",
        "uv run scripts/remove_example.py",
        "! grep -r incident_triage src",
        'uv run pytest -m "not integration and not e2e"',
        "make lint typecheck",
    ]


def test_removal_job_greps_in_a_step_of_its_own():
    # GitHub runs a step with bash -e, which does not stop at a failed `!` command, so the
    # inverted grep is the only command of its step: the step's status is the grep's.
    [grep] = [s for s in REMOVAL["steps"] if "grep" in s.get("run", "")]
    assert grep["run"].strip() == "! grep -r incident_triage src"


# example-agent:begin (scripts/remove_example.py deletes from here to the end of the file)
# --- the Evals workflow (spec 12.3; P21) ---------------------------------------------

EVALS_PATH = ROOT / ".github/workflows/evals.yml"
EVALS = yaml.safe_load(EVALS_PATH.read_text())
EVALS_JOB = EVALS["jobs"]["evals"]
EVALS_STEPS = EVALS_JOB["steps"]
EVALS_TEST = "src/langgraph_and_observability/agents/incident_triage/evals"
AWS_ACTION = "aws-actions/configure-aws-credentials"
RIPGREP = "sudo apt-get update && sudo apt-get install -y --no-install-recommends ripgrep"


def _uses(workflow: dict[str, Any]) -> list[str]:
    return [s["uses"] for j in workflow["jobs"].values() for s in j["steps"] if "uses" in s]


def _eval_step() -> dict[str, Any]:
    [step] = [s for s in EVALS_STEPS if EVALS_TEST in s.get("run", "")]
    return cast(dict[str, Any], step)


def test_evals_actions_pinned_to_the_ci_shas():
    uses = _uses(EVALS)
    assert uses and all(re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", u) for u in uses)
    # The actions that ci.yml uses too are pinned to the same commits.
    ci = {u.split("@")[0]: u for u in _uses(WF)}
    shared = [u for u in uses if u.split("@")[0] in ci]
    assert {u.split("@")[0] for u in shared} == {"actions/checkout", "astral-sh/setup-uv"}
    assert all(u == ci[u.split("@")[0]] for u in shared)
    assert [u.split("@")[0] for u in uses].count(AWS_ACTION) == 1


def test_evals_triggers():
    # PyYAML reads the key `on` as the boolean True (YAML 1.1).
    triggers = EVALS.get("on", EVALS.get(True))
    assert set(triggers) == {"pull_request", "workflow_dispatch"}


def test_evals_uv_commands_need_an_up_to_date_lock():
    assert EVALS["env"] == {"UV_LOCKED": "1"}
    assert [run for run in _runs(EVALS) if "--frozen" in run] == []


def test_evals_minimal_permissions():
    assert EVALS["permissions"] == {"contents": "read"}
    # OIDC: the job asks GitHub for an ID token, which AWS exchanges for credentials.
    assert EVALS_JOB["permissions"] == {"contents": "read", "id-token": "write"}


def test_evals_run_only_when_configured_and_never_for_forks_or_dependabot():
    # The role variable gates the job; fork and Dependabot pull requests get no secrets.
    assert " ".join(EVALS_JOB["if"].split()) == (
        "vars.AWS_EVALS_ROLE_ARN != '' && "
        "github.actor != 'dependabot[bot]' && "
        "(github.event_name == 'workflow_dispatch' || "
        "github.event.pull_request.head.repo.full_name == github.repository)"
    )


def test_evals_install_like_the_test_jobs():
    [checkout] = [s for s in EVALS_STEPS if s.get("uses", "").startswith("actions/checkout@")]
    assert checkout["with"]["persist-credentials"] is False
    runs = [s.get("run") for s in EVALS_STEPS]
    sync = runs.index("uv sync --all-groups")
    assert runs[sync + 1] == "make grammars"
    assert runs.index(RIPGREP) < EVALS_STEPS.index(_eval_step())


def test_evals_get_aws_credentials_through_oidc():
    [aws] = [s for s in EVALS_STEPS if s.get("uses", "").startswith(f"{AWS_ACTION}@")]
    # A role to assume and a region: no access keys.
    assert set(aws["with"]) == {"role-to-assume", "aws-region"}
    assert re.fullmatch(r"\$\{\{ (secrets|vars)\.[A-Z_]+ \}\}", aws["with"]["role-to-assume"])
    assert "arn:aws" not in EVALS_PATH.read_text()
    # The credentials reach only the steps after the install steps.
    assert EVALS_STEPS.index(aws) == EVALS_STEPS.index(_eval_step()) - 1


def test_evals_run_against_bedrock_with_simulated_adapters():
    step = _eval_step()
    assert step["env"]["APP_LLM_MODE"] == "bedrock"
    assert step["env"]["APP_ADAPTERS"] == "simulated"
    assert step["env"]["LANGSMITH_API_KEY"] == "${{ secrets.LANGSMITH_API_KEY }}"
    # langsmith's pytest plugin posts the tracked test run only with tracing on.
    assert step["env"]["LANGSMITH_TRACING"] == "true"
    # The test appends its scorecard to the job summary.
    assert 'APP_EVAL_SCORECARD="$GITHUB_STEP_SUMMARY"' in step["run"]
    assert "uv run pytest" in step["run"]


# example-agent:end
