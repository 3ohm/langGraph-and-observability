"""Finding a target's source: annotations, OCI image labels, then the config map (spec §9.6).

ECR answers through botocore's ``Stubber``, and ``urllib.request.urlopen`` is
patched to serve the image config, so no test touches the network.
"""

import email.message
import hashlib
import io
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, cast

import boto3
import pytest
from botocore.stub import Stubber
from structlog.testing import capture_logs

from langgraph_and_observability.agents.incident_triage.adapters.source.ecr import (
    ACCEPTED_MEDIA_TYPES,
    MAX_CONFIG_BYTES,
    EcrLabelReader,
    ImageLabelError,
    NoRedirects,
    make_ecr_client,
)
from langgraph_and_observability.agents.incident_triage.adapters.source.interface import SourceRef
from langgraph_and_observability.agents.incident_triage.adapters.source.locator import ChainLocator
from langgraph_and_observability.agents.incident_triage.schemas import (
    ImageRef,
    Target,
    WorkloadRef,
)

if TYPE_CHECKING:
    from mypy_boto3_ecr import ECRClient

REPO_KEY = "lps.dev/source-repo"
COMMIT_KEY = "lps.dev/source-commit"
KEYS = (REPO_KEY, COMMIT_KEY)
HOSTS = ["github.com"]
SHA_A = "a" * 40
SHA_B = "b" * 40
ACCOUNT = "123456789012"
REPOSITORY = "shop/orders"
ECR_IMAGE = f"{ACCOUNT}.dkr.ecr.us-east-1.amazonaws.com/{REPOSITORY}:1.4.2"
DIGEST = "sha256:" + "d" * 64
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
DOWNLOAD_URL = "https://prod-us-east-1-starport-layer-bucket.s3.us-east-1.amazonaws.com/config"
LABELS = {
    "org.opencontainers.image.source": "https://github.com/example/orders",
    "org.opencontainers.image.revision": SHA_B,
    "maintainer": "shop-team",
}
ANNOTATIONS = {REPO_KEY: "https://github.com/example/orders-annotated", COMMIT_KEY: SHA_A}
CONFIG_MAP = """\
shop/orders:
  repo: https://github.com/example/orders-mapped
  ref: release/2026.11
shop/cart:
  repo: https://github.com/example/cart
  ref: main
"""


def _target(image: str = ECR_IMAGE, digest: str | None = DIGEST, workload: str = "orders"):
    return Target(
        cluster="local",
        namespace="shop",
        workload=WorkloadRef(kind="Deployment", name=workload),
        pod=f"{workload}-7d9f-x2",
        container=workload,
        node="node-a",
        images=[
            ImageRef(container="istio-proxy", image="istio/proxyv2:1.27.0", digest=None),
            ImageRef(container=workload, image=image, digest=digest),
        ],
    )


def _config_blob(labels: dict[str, str] | None, *, with_config: bool = True) -> bytes:
    config: dict[str, Any] = {"Env": ["PATH=/usr/bin"]}
    if labels is not None:
        config["Labels"] = labels
    image: dict[str, Any] = {"architecture": "amd64", "os": "linux"}
    if with_config:
        image["config"] = config
    return json.dumps(image).encode()


def _blob_digest(blob: bytes) -> str:
    return "sha256:" + hashlib.sha256(blob).hexdigest()


def _manifest(config_digest: str) -> str:
    return json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": config_digest,
                "size": 1234,
            },
            "layers": [],
        }
    )


def _ecr_client() -> ECRClient:
    # Explicit keys: building the client must not look for credentials.
    return boto3.session.Session().client(
        "ecr",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",  # noqa: S106 - a fake key
    )


def _stub_image(stubber: Stubber, image_id: dict[str, str], manifest: str, media_type: str):
    stubber.add_response(
        "batch_get_image",
        {
            "images": [
                {
                    "registryId": ACCOUNT,
                    "repositoryName": REPOSITORY,
                    "imageId": image_id,
                    "imageManifest": manifest,
                    "imageManifestMediaType": media_type,
                }
            ],
            "failures": [],
        },
        {
            "registryId": ACCOUNT,
            "repositoryName": REPOSITORY,
            "imageIds": [image_id],
            "acceptedMediaTypes": list(ACCEPTED_MEDIA_TYPES),
        },
    )


def _stub_config_url(stubber: Stubber, config_digest: str) -> None:
    stubber.add_response(
        "get_download_url_for_layer",
        {"downloadUrl": DOWNLOAD_URL, "layerDigest": config_digest},
        {"registryId": ACCOUNT, "repositoryName": REPOSITORY, "layerDigest": config_digest},
    )


@pytest.fixture
def serve_config(monkeypatch):
    """Patch urllib's opener to serve one blob; returns the list of requests it got.

    ``urlopen`` and the reader's own opener both open URLs with ``OpenerDirector.open``.
    """
    requests: list[tuple[str, float]] = []
    blob: dict[str, bytes] = {}

    def fake_open(self, url, data=None, timeout=None):
        requests.append((url, timeout))
        return io.BytesIO(blob["body"])

    def serve(body: bytes) -> list[tuple[str, float]]:
        blob["body"] = body
        return requests

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", fake_open)
    return serve


def _no_ecr() -> EcrLabelReader:
    """An ECR client factory that fails the test if the reader ever asks for a client."""

    def make_client() -> NoReturn:
        pytest.fail("the locator must not call ECR")

    return EcrLabelReader(make_client)


def _map_file(tmp_path: Path, text: str = CONFIG_MAP) -> Path:
    path = tmp_path / "source-map.yaml"
    path.write_text(text, encoding="utf-8")
    return path


async def test_locator_prefers_annotations(tmp_path):
    locator = ChainLocator(
        annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=_no_ecr(), config_map=_map_file(tmp_path)
    )

    with capture_logs() as logs:
        ref = await locator.locate(_target(), ANNOTATIONS | {"other": "x"})

    assert ref == SourceRef(
        repo_url="https://github.com/example/orders-annotated", commit=SHA_A, origin="annotation"
    )
    assert [log["origin"] for log in logs if log["event"] == "source.locate.found"] == [
        "annotation"
    ]


async def test_locator_reads_oci_labels_from_ecr(tmp_path, serve_config):
    blob = _config_blob(LABELS)
    requests = serve_config(blob)
    client = _ecr_client()
    with Stubber(client) as stubber:
        # The pod's digest, not the tag, names the image.
        _stub_image(stubber, {"imageDigest": DIGEST}, _manifest(_blob_digest(blob)), OCI_MANIFEST)
        _stub_config_url(stubber, _blob_digest(blob))
        locator = ChainLocator(
            annotation_keys=KEYS,
            allowed_hosts=HOSTS,
            ecr=EcrLabelReader(client),
            config_map=_map_file(tmp_path),
        )

        ref = await locator.locate(_target(), {REPO_KEY: "https://github.com/example/x"})

        stubber.assert_no_pending_responses()
    assert ref == SourceRef(
        repo_url="https://github.com/example/orders", commit=SHA_B, origin="image_label"
    )
    assert [url for url, _ in requests] == [DOWNLOAD_URL]
    assert all(timeout > 0 for _, timeout in requests)


async def test_locator_config_map_fallback(tmp_path):
    # A Docker Hub image has no ECR labels: the reader returns {} without calling AWS.
    locator = ChainLocator(
        annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=_no_ecr(), config_map=_map_file(tmp_path)
    )

    with capture_logs() as logs:
        ref = await locator.locate(_target(image="docker.io/example/orders:1.4.2"), {})

    assert ref == SourceRef(
        repo_url="https://github.com/example/orders-mapped",
        commit="release/2026.11",
        origin="config",
    )
    assert [log["origin"] for log in logs if log["event"] == "source.locate.found"] == ["config"]


async def test_locator_none_when_unknown(tmp_path):
    no_map = ChainLocator(annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=None, config_map=None)
    other_workload = ChainLocator(
        annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=_no_ecr(), config_map=_map_file(tmp_path)
    )
    no_pod = _target().model_copy(update={"images": [], "container": None, "workload": None})

    with capture_logs() as logs:
        assert await no_map.locate(_target(), {}) is None
        assert (
            await other_workload.locate(_target(image="nginx:1.29", workload="search"), {}) is None
        )
        assert await other_workload.locate(no_pod, {}) is None

    assert [log["event"] for log in logs] == ["source.locate.none"] * 3


@pytest.mark.parametrize(
    ("annotations", "reason"),
    [
        ({REPO_KEY: "https://github.com/example/orders"}, "missing"),
        ({COMMIT_KEY: SHA_A}, "missing"),
        ({REPO_KEY: "-uhttps://github.com/example/orders", COMMIT_KEY: SHA_A}, "https"),
        ({REPO_KEY: "http://github.com/example/orders", COMMIT_KEY: SHA_A}, "https"),
        ({REPO_KEY: "git@github.com:example/orders.git", COMMIT_KEY: SHA_A}, "https"),
        ({REPO_KEY: "file:///srv/git/orders.git", COMMIT_KEY: SHA_A}, "https"),
        ({REPO_KEY: "https://bot:s3cr3t@github.com/example/orders", COMMIT_KEY: SHA_A}, "cred"),
        ({REPO_KEY: "https://attacker.example/x", COMMIT_KEY: SHA_A}, "'attacker.example' is not"),
        ({REPO_KEY: "https://github.com:8443/example/orders", COMMIT_KEY: SHA_A}, "port"),
        ({REPO_KEY: "https://github.com/example/orders", COMMIT_KEY: "--upload-pack=x"}, "ref"),
        ({REPO_KEY: "https://github.com/example/orders", COMMIT_KEY: "main:refs/x"}, "ref"),
        ({REPO_KEY: "https://github.com/example/orders", COMMIT_KEY: "../../HEAD"}, "ref"),
    ],
    ids=[
        "no-commit",
        "no-repo",
        "dash",
        "http",
        "ssh",
        "file",
        "credentials",
        "other-host",
        "port",
        "option-ref",
        "refspec",
        "dotdot",
    ],
)
async def test_locator_skips_invalid_annotations(tmp_path, annotations, reason):
    locator = ChainLocator(
        annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=_no_ecr(), config_map=_map_file(tmp_path)
    )

    with capture_logs() as logs:
        ref = await locator.locate(_target(image="nginx:1.29"), annotations)

    assert ref is not None and ref.origin == "config"
    skipped = [log for log in logs if log["event"] == "source.locate.skipped"]
    if reason == "missing":
        assert skipped == []
    else:
        assert [log["origin"] for log in skipped] == ["annotation"]
        assert reason in skipped[0]["reason"]
    assert "s3cr3t" not in repr(logs)


async def test_ecr_reader_follows_an_image_index(serve_config):
    blob = _config_blob(LABELS)
    serve_config(blob)
    child = "sha256:" + "c" * 64
    index = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": OCI_INDEX,
            "manifests": [
                {
                    "mediaType": OCI_MANIFEST,
                    "digest": "sha256:" + "e" * 64,
                    "size": 500,
                    "platform": {"architecture": "unknown", "os": "unknown"},
                },
                {
                    "mediaType": OCI_MANIFEST,
                    "digest": child,
                    "size": 500,
                    "platform": {"architecture": "amd64", "os": "linux"},
                },
            ],
        }
    )
    client = _ecr_client()
    with Stubber(client) as stubber:
        # No digest from the pod: the tag in the image reference names the image.
        _stub_image(stubber, {"imageTag": "1.4.2"}, index, OCI_INDEX)
        _stub_image(stubber, {"imageDigest": child}, _manifest(_blob_digest(blob)), OCI_MANIFEST)
        _stub_config_url(stubber, _blob_digest(blob))

        labels = await EcrLabelReader(client).labels(ECR_IMAGE, None)

        stubber.assert_no_pending_responses()
    assert labels == LABELS


@pytest.mark.parametrize(
    "image",
    [
        "docker.io/example/orders:1.4.2",
        "nginx",
        "ghcr.io/example/orders@sha256:" + "d" * 64,
        f"{ACCOUNT}.dkr.ecr.us-east-1.amazonaws.com.evil.example/{REPOSITORY}:1.4.2",
        # Another region than the client's.
        f"{ACCOUNT}.dkr.ecr.eu-west-1.amazonaws.com/{REPOSITORY}:1.4.2",
    ],
)
async def test_ecr_reader_skips_other_images_without_calling_aws(image):
    client = _ecr_client()
    with Stubber(client) as stubber:
        assert await EcrLabelReader(client).labels(image, DIGEST) == {}
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize("with_config", [True, False], ids=["no-labels", "no-config"])
async def test_ecr_reader_builds_its_client_on_first_use(serve_config, with_config):
    # An image config without labels, or without its optional "config" object.
    blob = _config_blob(None, with_config=with_config)
    serve_config(blob)
    client = _ecr_client()
    made: list[str] = []

    def make_client():
        made.append("client")
        return client

    reader = EcrLabelReader(make_client)
    assert made == []
    assert await reader.labels("nginx:1.29", None) == {}
    assert made == []
    with Stubber(client) as stubber:
        for _ in range(2):
            _stub_image(
                stubber, {"imageDigest": DIGEST}, _manifest(_blob_digest(blob)), OCI_MANIFEST
            )
            _stub_config_url(stubber, _blob_digest(blob))
        assert await reader.labels(ECR_IMAGE, DIGEST) == {}
        assert await reader.labels(ECR_IMAGE, DIGEST) == {}
    assert made == ["client"]


async def test_ecr_reader_errors(serve_config):
    client = _ecr_client()
    reader = EcrLabelReader(client)
    blob = _config_blob(LABELS)
    with Stubber(client) as stubber:
        stubber.add_client_error(
            "batch_get_image", "RepositoryNotFoundException", http_status_code=400
        )
        with pytest.raises(ImageLabelError, match="RepositoryNotFoundException"):
            await reader.labels(ECR_IMAGE, DIGEST)

        stubber.add_response(
            "batch_get_image",
            {
                "images": [],
                "failures": [
                    {
                        "imageId": {"imageDigest": DIGEST},
                        "failureCode": "ImageNotFound",
                        "failureReason": "Requested image not found",
                    }
                ],
            },
        )
        with pytest.raises(ImageLabelError, match="ImageNotFound"):
            await reader.labels(ECR_IMAGE, DIGEST)

        # The config blob must have the digest that the manifest names.
        serve_config(blob)
        other = _blob_digest(b"other")
        _stub_image(stubber, {"imageDigest": DIGEST}, _manifest(other), OCI_MANIFEST)
        _stub_config_url(stubber, other)
        with pytest.raises(ImageLabelError, match="digest"):
            await reader.labels(ECR_IMAGE, DIGEST)

        # An index whose only manifest is an attestation.
        attestation = {
            "mediaType": OCI_MANIFEST,
            "digest": "sha256:" + "e" * 64,
            "size": 500,
            "platform": {"architecture": "unknown", "os": "unknown"},
        }
        only_attestation = json.dumps({"mediaType": OCI_INDEX, "manifests": [attestation]})
        _stub_image(stubber, {"imageDigest": DIGEST}, only_attestation, OCI_INDEX)
        with pytest.raises(ImageLabelError, match="no platform manifest"):
            await reader.labels(ECR_IMAGE, DIGEST)

        # The blob comes only from an https URL on an amazonaws.com host.
        serve_config(blob)
        for url in (
            "https://evil.example/config",
            "https://bucket.s3.amazonaws.com.evil.example/config",
            "https://amazonaws.com/config",
            "http://bucket.s3.us-east-1.amazonaws.com/config",
        ):
            _stub_image(
                stubber, {"imageDigest": DIGEST}, _manifest(_blob_digest(blob)), OCI_MANIFEST
            )
            stubber.add_response(
                "get_download_url_for_layer",
                {"downloadUrl": url, "layerDigest": _blob_digest(blob)},
            )
            with pytest.raises(ImageLabelError, match=r"amazonaws\.com"):
                await reader.labels(ECR_IMAGE, DIGEST)

        # A config blob over the size cap is not read to its end.
        big = b"x" * (MAX_CONFIG_BYTES + 1)
        serve_config(big)
        _stub_image(stubber, {"imageDigest": DIGEST}, _manifest(_blob_digest(big)), OCI_MANIFEST)
        _stub_config_url(stubber, _blob_digest(big))
        with pytest.raises(ImageLabelError, match="bytes"):
            await reader.labels(ECR_IMAGE, DIGEST)

        stubber.assert_no_pending_responses()


async def test_locator_falls_back_when_ecr_fails(tmp_path):
    client = _ecr_client()
    with Stubber(client) as stubber:
        stubber.add_client_error("batch_get_image", "AccessDeniedException", http_status_code=403)
        locator = ChainLocator(
            annotation_keys=KEYS,
            allowed_hosts=HOSTS,
            ecr=EcrLabelReader(client),
            config_map=_map_file(tmp_path),
        )

        with capture_logs() as logs:
            ref = await locator.locate(_target(), {})

    assert ref is not None and ref.origin == "config"
    failed = [log for log in logs if log["event"] == "source.locate.labels_failed"]
    assert len(failed) == 1 and "AccessDeniedException" in failed[0]["error"]


async def test_locator_skips_labels_without_both_keys(tmp_path, serve_config):
    blob = _config_blob({"org.opencontainers.image.source": "https://github.com/example/orders"})
    serve_config(blob)
    client = _ecr_client()
    with Stubber(client) as stubber:
        _stub_image(stubber, {"imageDigest": DIGEST}, _manifest(_blob_digest(blob)), OCI_MANIFEST)
        _stub_config_url(stubber, _blob_digest(blob))
        locator = ChainLocator(
            annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=EcrLabelReader(client), config_map=None
        )

        assert await locator.locate(_target(), {}) is None


@pytest.mark.parametrize(
    ("text", "error"),
    [
        ("shop/orders: [", "YAMLError"),
        ("- shop/orders\n", "ValidationError"),
        ("shop/orders:\n  repo: https://github.com/example/orders\n", "ValidationError"),
        ("shop/orders:\n  repo: https://x.example/o\n  ref: main\n  token: t\n", "ValidationError"),
    ],
    ids=["yaml", "not-a-mapping", "no-ref", "unknown-key"],
)
async def test_locator_logs_an_invalid_config_map(tmp_path, text, error):
    locator = ChainLocator(
        annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=None, config_map=_map_file(tmp_path, text)
    )

    with capture_logs() as logs:
        assert await locator.locate(_target(), {}) is None

    invalid = [log for log in logs if log["event"] == "source.locate.config_map_invalid"]
    assert [log["error"] for log in invalid] == [error]


async def test_locator_config_map_missing_empty_or_not_utf8(tmp_path):
    missing = ChainLocator(
        annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=None, config_map=tmp_path / "missing.yaml"
    )
    empty = ChainLocator(
        annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=None, config_map=_map_file(tmp_path, "")
    )
    latin1 = tmp_path / "latin1.yaml"
    latin1.write_bytes("shop/caf\xe9:\n".encode("latin-1"))
    not_utf8 = ChainLocator(annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=None, config_map=latin1)

    with capture_logs() as logs:
        assert await missing.locate(_target(), {}) is None
        assert await empty.locate(_target(), {}) is None
        assert await not_utf8.locate(_target(), {}) is None

    invalid = [log["error"] for log in logs if log["event"] == "source.locate.config_map_invalid"]
    assert invalid == ["FileNotFoundError", "UnicodeDecodeError"]


async def test_locator_validates_config_map_values(tmp_path):
    text = "shop/orders:\n  repo: https://github.com/example/orders\n  ref: '-n'\n"
    locator = ChainLocator(
        annotation_keys=KEYS, allowed_hosts=HOSTS, ecr=None, config_map=_map_file(tmp_path, text)
    )

    with capture_logs() as logs:
        assert await locator.locate(_target(), {}) is None

    assert [log["origin"] for log in logs if log["event"] == "source.locate.skipped"] == ["config"]


def test_make_ecr_client_uses_standard_retries(monkeypatch):
    # Keys in the environment: boto3 finds them first, without a network call.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(name, raising=False)

    client = make_ecr_client("eu-west-1")

    assert client.meta.region_name == "eu-west-1"
    # botocore stores max_attempts=3 (retries) as 4 attempts in all.
    # The stubs leave Config.retries out.
    retries = cast(Any, client.meta.config).retries
    assert retries == {"mode": "standard", "total_max_attempts": 4}


def test_ecr_config_download_follows_no_redirect():
    request = urllib.request.Request("https://bucket.s3.us-east-1.amazonaws.com/config")
    headers = email.message.Message()

    for target in ("https://evil.example/config", "https://bucket.s3.us-east-1.amazonaws.com/x"):
        response = io.BytesIO(b"moved")
        with pytest.raises(urllib.error.URLError, match="refused a redirect"):
            NoRedirects().redirect_request(request, response, 302, "Found", headers, target)
        assert response.closed
