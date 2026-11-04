"""The OCI labels of an ECR image, read with boto3 (spec §9.6 item 1, §9.10).

``EcrLabelReader.labels`` names the image by the digest that the pod reports, else by
the digest or tag in its reference (``latest`` when it has neither). It reads the
image's manifest with ``batch_get_image``, takes the config blob's digest from it,
asks ``get_download_url_for_layer`` for the blob's URL, fetches the blob (the image
config JSON) and returns its ``config.Labels``. The IAM permissions are
``ecr:BatchGetImage`` and ``ecr:GetDownloadUrlForLayer``.

A multi-platform image (an OCI index or a Docker manifest list; buildx writes an index
by default) has one manifest per platform. The reader follows the first one that is
not an attestation (platform ``unknown``): the platforms of one build carry the same
labels.

An image that is not in ECR, or that is in another region than the client's, has no
labels here: ``labels`` returns ``{}`` without calling AWS. Any other failure raises
ImageLabelError.

boto3 is synchronous, so the calls run in a thread (``asyncio.to_thread``). botocore
retries them in standard mode, the only retry layer (``make_ecr_client``).

The blob download (ruling R59) goes only to an ``https`` URL whose host ends with
``.amazonaws.com``, the presigned S3 URL that ECR returns, and follows no redirect. It
has its own timeout and size cap, and the blob must have the digest that the manifest
names. The China partition (``amazonaws.com.cn``) is not supported, so its image
references count as not in ECR.
"""

import asyncio
import hashlib
import json
import re
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from email.message import Message
from typing import IO, TYPE_CHECKING, Any, NoReturn
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from langgraph_postgres_starter.logging import get_logger

if TYPE_CHECKING:
    # The stubs are a dev dependency: only annotations name these types.
    from mypy_boto3_ecr import ECRClient
    from mypy_boto3_ecr.type_defs import ImageIdentifierTypeDef

_log = get_logger(__name__)

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
DOCKER_MANIFEST_LIST = "application/vnd.docker.distribution.manifest.list.v2+json"
ACCEPTED_MEDIA_TYPES = (OCI_MANIFEST, DOCKER_MANIFEST, OCI_INDEX, DOCKER_MANIFEST_LIST)
_INDEX_TYPES = (OCI_INDEX, DOCKER_MANIFEST_LIST)

# An image config is a few kilobytes; a long build history makes it larger.
MAX_CONFIG_BYTES = 4 * 1024 * 1024
CONFIG_TIMEOUT_S = 10.0

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
# The hosts that the config blob may come from.
_DOWNLOAD_HOST_SUFFIX = ".amazonaws.com"
# <account>.dkr.ecr[-fips].<region>.amazonaws.com/<repository>[:<tag>][@<digest>]
_ECR_IMAGE = re.compile(
    r"(?P<registry>\d{12})\.dkr\.ecr(?:-fips)?\.(?P<region>[a-z0-9-]+)\.amazonaws\.com"
    r"/(?P<repository>[a-z0-9]+(?:[._/-][a-z0-9]+)*)"
    r"(?::(?P<tag>[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}))?"
    r"(?:@(?P<digest>sha256:[0-9a-f]{64}))?"
)


class ImageLabelError(RuntimeError):
    """The labels of an ECR image could not be read."""


class NoRedirects(urllib.request.HTTPRedirectHandler):
    """Refuses every redirect: the blob URL is the one ECR returned, or nothing."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes] | None,
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> NoReturn:
        # The redirect response is not read; close it here, as urllib would.
        if fp is not None:
            fp.close()
        raise urllib.error.URLError(f"refused a redirect ({code}) of the image config download")


# build_opener leaves out the default redirect handler when it is given a subclass.
_OPENER = urllib.request.build_opener(NoRedirects())


@dataclass(frozen=True)
class EcrImage:
    """An image reference in ECR."""

    registry_id: str
    region: str
    repository: str
    tag: str | None
    digest: str | None


def parse_ecr_image(image: str) -> EcrImage | None:
    """The parts of an ECR image reference, or None for an image that is not in ECR."""
    match = _ECR_IMAGE.fullmatch(image)
    if match is None:
        return None
    return EcrImage(
        registry_id=match["registry"],
        region=match["region"],
        repository=match["repository"],
        tag=match["tag"],
        digest=match["digest"],
    )


def make_ecr_client(region: str) -> ECRClient:
    """A boto3 ECR client for ``region``, with its own session.

    As for SQS (``worker.intake.make_sqs_client``), credentials come from boto3's own
    chain, and botocore retries in standard mode, at most 3 times. boto3 looks for
    credentials when it builds the client, which can mean a network call, so
    ``build_adapters`` passes this function, not a client, to ``EcrLabelReader``.
    """
    return boto3.session.Session().client(
        "ecr",
        region_name=region,
        config=Config(retries={"mode": "standard", "max_attempts": 3}),
    )


class EcrLabelReader:
    """Reads the OCI labels of ECR images.

    ``client`` is a boto3 ECR client, or a function that returns one (such as
    ``functools.partial(make_ecr_client, region)``). The reader calls the function
    once, for the first image in ECR, so building the reader touches no network.
    """

    def __init__(self, client: ECRClient | Callable[[], ECRClient]) -> None:
        self._make_client: Callable[[], ECRClient] = (
            client if callable(client) else (lambda: client)
        )
        self._client: ECRClient | None = None
        self._client_lock = threading.Lock()

    async def labels(self, image: str, digest: str | None) -> dict[str, str]:
        """The labels of ``image``; ``digest`` is the pod's digest of it, if known.

        Returns ``{}`` for an image that is not in ECR, one in another region than the
        client's, or one whose config has no labels. Raises ImageLabelError when ECR
        or the blob download fails, or when the manifest or config is not as expected.
        """
        ref = parse_ecr_image(image)
        if ref is None:
            return {}
        if digest is not None and _DIGEST.fullmatch(digest):
            ref = EcrImage(ref.registry_id, ref.region, ref.repository, ref.tag, digest)
        try:
            return await asyncio.to_thread(self._read_labels, ref)
        # json.loads raises RecursionError for a document that is nested too deeply.
        except (
            ImageLabelError,
            BotoCoreError,
            ClientError,
            OSError,
            ValueError,
            RecursionError,
        ) as exc:
            raise ImageLabelError(f"could not read the labels of {image}: {exc}") from exc

    def _client_for_use(self) -> ECRClient:
        with self._client_lock:
            if self._client is None:
                self._client = self._make_client()
            return self._client

    def _read_labels(self, ref: EcrImage) -> dict[str, str]:
        """Read the labels (runs in a thread). Raises the client's and urllib's errors."""
        client = self._client_for_use()
        if client.meta.region_name != ref.region:
            _log.info(
                "source.ecr.other_region", image_region=ref.region, region=client.meta.region_name
            )
            return {}
        image_id: ImageIdentifierTypeDef = (
            {"imageDigest": ref.digest} if ref.digest else {"imageTag": ref.tag or "latest"}
        )
        manifest, media_type = _manifest(client, ref, image_id)
        if media_type in _INDEX_TYPES:
            manifest, media_type = _manifest(client, ref, {"imageDigest": _platform(manifest)})
        if media_type in _INDEX_TYPES:
            raise ImageLabelError("the image index names another index")
        config_digest = _str(_mapping(manifest.get("config")).get("digest"), "config.digest")
        url = client.get_download_url_for_layer(
            registryId=ref.registry_id, repositoryName=ref.repository, layerDigest=config_digest
        )["downloadUrl"]
        blob = _download(url)
        if "sha256:" + hashlib.sha256(blob).hexdigest() != config_digest:
            raise ImageLabelError(
                "the image config does not have the digest that its manifest names"
            )
        # OCI makes both "config" and its "Labels" optional.
        labels = _mapping(_mapping(json.loads(blob)).get("config") or {}).get("Labels") or {}
        return {k: v for k, v in _mapping(labels).items() if isinstance(v, str)}


def _manifest(
    client: ECRClient, ref: EcrImage, image_id: ImageIdentifierTypeDef
) -> tuple[Mapping[str, Any], str]:
    """The manifest that ``image_id`` names, and its media type."""
    response = client.batch_get_image(
        registryId=ref.registry_id,
        repositoryName=ref.repository,
        imageIds=[image_id],
        acceptedMediaTypes=list(ACCEPTED_MEDIA_TYPES),
    )
    images = response.get("images", [])
    if not images:
        failures = response.get("failures", [])
        code = failures[0].get("failureCode", "unknown") if failures else "no image"
        raise ImageLabelError(f"ECR returned no manifest: {code}")
    manifest = _mapping(json.loads(images[0].get("imageManifest", "")))
    media_type = images[0].get("imageManifestMediaType") or manifest.get("mediaType")
    return manifest, _str(media_type, "mediaType")


def _platform(index: Mapping[str, Any]) -> str:
    """The digest of the first manifest in ``index`` that is not an attestation."""
    manifests = index.get("manifests")
    for entry in manifests if isinstance(manifests, list) else []:
        entry = _mapping(entry)
        platform = _mapping(entry.get("platform") or {})
        if platform.get("os") != "unknown" and entry.get("mediaType") not in _INDEX_TYPES:
            digest = _str(entry.get("digest"), "manifests[].digest")
            if _DIGEST.fullmatch(digest):
                return digest
    raise ImageLabelError("the image index names no platform manifest")


def _download(url: str) -> bytes:
    """The body at ``url`` (the presigned S3 URL that ECR returned), at most the size cap."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if parts.scheme != "https" or not host.endswith(_DOWNLOAD_HOST_SUFFIX):
        raise ImageLabelError(
            f"ECR returned a download URL that is not https:// on a {_DOWNLOAD_HOST_SUFFIX} host"
        )
    with _OPENER.open(url, timeout=CONFIG_TIMEOUT_S) as response:
        body: bytes = response.read(MAX_CONFIG_BYTES + 1)
    if len(body) > MAX_CONFIG_BYTES:
        raise ImageLabelError(f"the image config is larger than {MAX_CONFIG_BYTES} bytes")
    return body


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ImageLabelError(f"expected a JSON object, not {type(value).__name__}")
    return value


def _str(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ImageLabelError(f"{name} is not a string")
    return value
