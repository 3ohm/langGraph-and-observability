"""The SQS client of ``scripts/send_incident.py`` (ruling R66, R67).

For a local endpoint (this machine, or Compose's ``elasticmq``) with no AWS credentials
configured, the client signs with dummy credentials, and boto3's credential chain does
not probe the EC2 metadata endpoint first: off EC2, that probe takes seconds. For AWS's
own endpoint, the chain stays as it is.

The script imports the example agent, so a fork without it skips this module.
"""

import os
from typing import Any

import botocore.httpsession
import pytest

send_incident = pytest.importorskip("scripts.send_incident")


@pytest.fixture
def no_aws_config(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """No AWS variables or files, and no HTTP: any request that botocore sends fails."""
    for name in list(os.environ):
        if name.upper().startswith(("AWS_", "BOTO_")):
            monkeypatch.delenv(name)
    # ~/.aws/config, ~/.aws/credentials and ~/.boto are then missing.
    monkeypatch.setenv("HOME", str(tmp_path))
    # make_client may set this variable: record that it is unset, so that monkeypatch
    # removes it again after the test.
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "unset")
    monkeypatch.delenv("AWS_EC2_METADATA_DISABLED")

    def no_network(self: object, request: object) -> None:
        raise AssertionError(f"unexpected HTTP request: {getattr(request, 'url', request)}")

    monkeypatch.setattr(botocore.httpsession.URLLib3Session, "send", no_network)


def _signing_credentials(client: Any) -> Any:
    # botocore keeps the client's credentials on its request signer.
    return client._request_signer._credentials


@pytest.mark.parametrize(
    "endpoint", ["http://localhost:9324", "http://127.0.0.1:9324", "http://elasticmq:9324"]
)
def test_local_endpoint_without_credentials_signs_with_dummy_ones(no_aws_config, endpoint):
    # no_network fails the call if the credential chain asks the EC2 metadata endpoint.
    credentials = _signing_credentials(send_incident.make_client(endpoint))
    assert (credentials.access_key, credentials.secret_key) == ("test", "test")
    assert os.environ["AWS_EC2_METADATA_DISABLED"] == "true"


def test_local_endpoint_uses_configured_credentials(no_aws_config, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "configured-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "configured-secret")
    credentials = _signing_credentials(send_incident.make_client("http://localhost:9324"))
    assert credentials.access_key == "configured-key-id"


def test_aws_endpoint_keeps_the_credential_chain(no_aws_config, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "configured-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "configured-secret")
    credentials = _signing_credentials(send_incident.make_client(None))
    assert credentials.access_key == "configured-key-id"
    assert "AWS_EC2_METADATA_DISABLED" not in os.environ
