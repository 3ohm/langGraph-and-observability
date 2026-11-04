"""Checks on the example EKS manifests and the IAM policy (spec §7.1, §8.4, §8.6, §9.3,
§9.10, §10.1; P19).

The files are examples with placeholders, so these tests parse them as YAML and JSON and
check what the worker relies on: read-only RBAC, the shutdown and probe settings, a
locked-down container, secrets only from Secret references, two database roles, and an
IAM policy with exactly the actions of spec §9.10.
"""

import functools
import json
import re
from pathlib import Path
from typing import Any, get_args

import yaml
from pydantic import SecretStr

from langgraph_postgres_starter.settings import Settings

ROOT = Path(__file__).resolve().parents[2]
K8S = ROOT / "deploy" / "k8s"
POLICY = ROOT / "deploy" / "iam" / "policy.json"

# One object of each kind: no Secret, no HPA or KEDA object (spec §10.1).
KINDS = ["ClusterRole", "ClusterRoleBinding", "ConfigMap", "Deployment", "Job", "ServiceAccount"]

# What the real Kubernetes adapter reads (spec §9.3), as (API group, resource).
K8S_READS = {
    ("", "pods"),
    ("", "pods/log"),
    ("", "events"),
    ("", "nodes"),
    ("apps", "replicasets"),
    ("apps", "deployments"),
    ("apps", "statefulsets"),
    ("apps", "daemonsets"),
    ("batch", "jobs"),
}

IAM_ACTIONS = {
    "bedrock:InvokeModel",
    "sqs:ReceiveMessage",
    "sqs:DeleteMessage",
    "sqs:ChangeMessageVisibility",
    "sqs:SendMessage",
    "ecr:BatchGetImage",
    "ecr:GetDownloadUrlForLayer",
}

# The image runs as 10001:10001 (Dockerfile).
CONTAINER_SECURITY = {
    "runAsNonRoot": True,
    "runAsUser": 10001,
    "runAsGroup": 10001,
    "readOnlyRootFilesystem": True,
    "allowPrivilegeEscalation": False,
    "capabilities": {"drop": ["ALL"]},
    "seccompProfile": {"type": "RuntimeDefault"},
}
POD_SECURITY = {
    "runAsNonRoot": True,
    "runAsUser": 10001,
    "runAsGroup": 10001,
    "seccompProfile": {"type": "RuntimeDefault"},
}
TMP = "/tmp"  # noqa: S108 - a mount path in the manifests, not a temporary file

_QUANTITY = re.compile(r"(\d+)(Ki|Mi|Gi)")
_UNITS = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30}


@functools.cache
def _kustomization() -> dict[str, Any]:
    kustomization: dict[str, Any] = yaml.safe_load((K8S / "kustomization.yaml").read_text())
    return kustomization


@functools.cache
def _objects() -> list[dict[str, Any]]:
    """Every object in every manifest that the kustomization lists."""
    return [
        doc
        for name in _kustomization()["resources"]
        for doc in yaml.safe_load_all((K8S / name).read_text())
        if doc
    ]


def _kind(kind: str) -> dict[str, Any]:
    found = [o for o in _objects() if o["kind"] == kind]
    assert len(found) == 1, f"expected one {kind}, found {len(found)}"
    return found[0]


def _pod(kind: str) -> dict[str, Any]:
    pod: dict[str, Any] = _kind(kind)["spec"]["template"]["spec"]
    return pod


def _container(kind: str) -> dict[str, Any]:
    containers = _pod(kind)["containers"]
    assert len(containers) == 1
    container: dict[str, Any] = containers[0]
    return container


def _env(kind: str) -> dict[str, dict[str, Any]]:
    return {e["name"]: e for e in _container(kind).get("env", [])}


def _volumes(kind: str) -> dict[str, dict[str, Any]]:
    return {v["name"]: v for v in _pod(kind).get("volumes", [])}


def _bytes(quantity: str) -> int:
    match = _QUANTITY.fullmatch(quantity)
    assert match, quantity
    return int(match[1]) * _UNITS[match[2]]


def _as_list(value: str | list[str]) -> list[str]:
    return [value] if isinstance(value, str) else value


def _env_name(name: str, alias: object) -> str:
    return alias if isinstance(alias, str) else f"APP_{name.upper()}"


def _setting_env_names() -> set[str]:
    return {_env_name(n, f.validation_alias) for n, f in Settings.model_fields.items()}


def _secret_env_names() -> set[str]:
    """The secrets: every SecretStr setting, both database URLs and the LangSmith key."""
    names = {"APP_DATABASE_URL", "APP_MIGRATION_DATABASE_URL", "LANGSMITH_API_KEY"}
    for name, field in Settings.model_fields.items():
        if SecretStr in get_args(field.annotation):
            names.add(_env_name(name, field.validation_alias))
    return names


def _sqs_arn(queue_url: str) -> str:
    """``https://sqs.<region>.amazonaws.com/<account>/<name>`` as the queue's ARN."""
    match = re.fullmatch(r"https://sqs\.([^.]+)\.amazonaws\.com/([^/]+)/([^/]+)", queue_url)
    assert match, queue_url
    region, account, name = match.groups()
    return f"arn:aws:sqs:{region}:{account}:{name}"


def _policy() -> dict[str, Any]:
    policy: dict[str, Any] = json.loads(POLICY.read_text())
    return policy


def _statements_with(action: str) -> list[dict[str, Any]]:
    return [s for s in _policy()["Statement"] if action in _as_list(s["Action"])]


def test_kustomization_lists_every_manifest():
    manifests = {p.name for p in K8S.glob("*.yaml")} - {"kustomization.yaml"}
    assert sorted(_kustomization()["resources"]) == sorted(manifests)
    # The Deployment and the Job run the one image that the kustomization names.
    (image,) = _kustomization()["images"]
    assert _container("Deployment")["image"] == _container("Job")["image"] == image["name"]


def test_only_the_example_objects_and_no_secret():
    assert sorted(o["kind"] for o in _objects()) == KINDS
    assert "secretGenerator" not in _kustomization()


def test_rbac_read_only():
    role = _kind("ClusterRole")
    rules = role["rules"]
    assert "aggregationRule" not in role
    assert all(sorted(rule["verbs"]) == ["get", "list"] for rule in rules)
    assert not any("resourceNames" in rule or "nonResourceURLs" in rule for rule in rules)
    granted = [(g, r) for rule in rules for g in rule["apiGroups"] for r in rule["resources"]]
    assert sorted(granted) == sorted(K8S_READS)


def test_rbac_binds_the_worker_service_account():
    account = _kind("ServiceAccount")["metadata"]["name"]
    binding = _kind("ClusterRoleBinding")
    assert binding["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "ClusterRole",
        "name": _kind("ClusterRole")["metadata"]["name"],
    }
    namespace = _kustomization()["namespace"]
    assert binding["subjects"] == [
        {"kind": "ServiceAccount", "name": account, "namespace": namespace}
    ]
    assert _pod("Deployment")["serviceAccountName"] == account


def test_service_account_has_the_iam_role_placeholder():
    annotations = _kind("ServiceAccount")["metadata"]["annotations"]
    role_arn = annotations["eks.amazonaws.com/role-arn"]
    assert role_arn == "arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>"


def test_deployment_shutdown_and_probes():
    pod = _pod("Deployment")
    worker = _container("Deployment")
    env = _env("Deployment")
    assert pod["terminationGracePeriodSeconds"] == 60
    # The drain (spec §8.4) ends before the grace period does.
    assert int(env["APP_DRAIN_TIMEOUT_SECONDS"]["value"]) == 45
    assert worker["livenessProbe"]["httpGet"] == {"path": "/ok", "port": 8080}
    assert worker["readinessProbe"]["httpGet"] == {"path": "/ready", "port": 8080}
    assert env["APP_HEALTH_PORT"]["value"] == "8080"
    assert [p["containerPort"] for p in worker["ports"]] == [8080]
    assert env["POD_NAME"]["valueFrom"] == {"fieldRef": {"fieldPath": "metadata.name"}}


def test_security_context():
    for kind in ("Deployment", "Job"):
        pod = _pod(kind)
        assert pod["securityContext"].items() >= POD_SECURITY.items(), kind
        for container in pod["containers"] + pod.get("initContainers", []):
            assert container["securityContext"] == CONTAINER_SECURITY, kind


def test_writable_paths_are_empty_dirs():
    """The root file system is read-only: /tmp and the source cache are emptyDir volumes."""
    for kind in ("Deployment", "Job"):
        mounts = {m["mountPath"]: m["name"] for m in _container(kind)["volumeMounts"]}
        volumes = _volumes(kind)
        assert all("emptyDir" in volumes[name] for name in mounts.values()), kind
        assert TMP in mounts, kind
    env = _env("Deployment")
    # git writes its config under HOME.
    assert env["HOME"]["value"] == TMP
    mounts = {m["mountPath"]: m["name"] for m in _container("Deployment")["volumeMounts"]}
    cache = _volumes("Deployment")[mounts[env["APP_SOURCE_CACHE_DIR"]["value"]]]
    # The cache can run over its budget while runs still read their checkouts, and the
    # kubelet evicts the pod when an emptyDir passes its sizeLimit.
    max_bytes = int(env["APP_SOURCE_CACHE_MAX_MB"]["value"]) * 2**20
    assert _bytes(cache["emptyDir"]["sizeLimit"]) > max_bytes


def test_secrets_come_from_secret_references():
    secrets = _secret_env_names()
    assert secrets >= {"DD_API_KEY", "DD_APP_KEY", "APP_GIT_TOKEN", "LANGGRAPH_AES_KEY"}
    # The worker gets every secret but the migration role's URL (test_two_database_roles).
    assert secrets & set(_env("Deployment")) == secrets - {"APP_MIGRATION_DATABASE_URL"}
    for kind in ("Deployment", "Job"):
        for name, var in _env(kind).items():
            if name in secrets:
                assert set(var) == {"name", "valueFrom"}, name
                assert set(var["valueFrom"]) == {"secretKeyRef"}, name


def test_configmap_is_real_mode_without_secrets():
    data = _kind("ConfigMap")["data"]
    secret_like = re.compile(r"TOKEN|PASSWORD|SECRET|_API_KEY$|_APP_KEY$|_AES_KEY$|DATABASE_URL")
    assert not set(data) & _secret_env_names()
    assert not [key for key in data if secret_like.search(key)]
    # Every key is a setting or a LangSmith variable, so a misspelt key cannot hide.
    unknown = {k for k in data if k not in _setting_env_names() and not k.startswith("LANGSMITH_")}
    assert not unknown
    assert data["APP_ENV"] == "prod"
    assert data["APP_ADAPTERS"] == "real"
    assert data["APP_LLM_MODE"] == "bedrock"
    # Both load the ConfigMap, and nothing else wholesale: no secretRef.
    config = {"configMapRef": {"name": _kind("ConfigMap")["metadata"]["name"]}}
    assert _container("Deployment")["envFrom"] == _container("Job")["envFrom"] == [config]


def test_two_database_roles():
    """The worker gets the app role's URL; the migrate Job gets the migration role's."""
    worker, migrate = _env("Deployment"), _env("Job")
    assert "APP_DATABASE_URL" in worker and "APP_MIGRATION_DATABASE_URL" not in worker
    assert "APP_MIGRATION_DATABASE_URL" in migrate and "APP_DATABASE_URL" not in migrate


def test_migrate_job_runs_once():
    job = _kind("Job")["spec"]
    assert _container("Job")["args"] == ["migrate"]
    assert job["template"]["spec"]["restartPolicy"] == "Never"
    assert 0 <= job["backoffLimit"] <= 3
    assert job["ttlSecondsAfterFinished"] > 0
    # It reaches neither the Kubernetes API nor AWS.
    assert job["template"]["spec"]["automountServiceAccountToken"] is False


def test_iam_actions_exact():
    policy = _policy()
    statements = policy["Statement"]
    assert policy["Version"] == "2012-10-17"
    assert all(s["Effect"] == "Allow" for s in statements)
    assert not any({"NotAction", "NotResource", "Principal"} & set(s) for s in statements)
    actions = [a for s in statements for a in _as_list(s["Action"])]
    assert set(actions) == IAM_ACTIONS
    resources = [r for s in statements for r in _as_list(s["Resource"])]
    assert resources and not any("*" in r for r in resources)


def test_iam_statements_scoped_to_their_resources():
    data = _kind("ConfigMap")["data"]
    intake, results = _sqs_arn(data["APP_SQS_QUEUE_URL"]), _sqs_arn(data["APP_RESULT_QUEUE_URL"])
    for action in ("sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"):
        assert [_as_list(s["Resource"]) for s in _statements_with(action)] == [[intake]]
    assert [_as_list(s["Resource"]) for s in _statements_with("sqs:SendMessage")] == [[results]]
    for action in ("ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"):
        (statement,) = _statements_with(action)
        assert all(":repository/" in r for r in _as_list(statement["Resource"]))


def test_iam_bedrock_through_the_inference_profile():
    """A global profile needs its ARN and the model's in the source Region and globally."""
    data = _kind("ConfigMap")["data"]
    profile_id = data["APP_BEDROCK_MODEL_ID"]
    assert profile_id.startswith("global.")
    model = profile_id.removeprefix("global.")
    profile = f"arn:aws:bedrock:<REGION>:<ACCOUNT_ID>:inference-profile/{profile_id}"
    assert data["APP_AWS_REGION"] == "<REGION>"
    statements = _statements_with("bedrock:InvokeModel")
    by_resource = {r: s for s in statements for r in _as_list(s["Resource"])}
    assert sorted(by_resource) == sorted(
        [
            profile,
            f"arn:aws:bedrock:<REGION>::foundation-model/{model}",
            f"arn:aws:bedrock:::foundation-model/{model}",
        ]
    )
    # The models only through the profile.
    for resource, statement in by_resource.items():
        if ":foundation-model/" in resource:
            condition = statement["Condition"]["StringEquals"]
            assert condition["bedrock:InferenceProfileArn"] == profile, resource
