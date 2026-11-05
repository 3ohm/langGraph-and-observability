"""The checkpoint serializer: an explicit allowlist, no pickle, optional AES (spec 7.2; P7).

Each test runs with LANGGRAPH_STRICT_MSGPACK off and on (the strict_msgpack fixture):
the explicit allowlist must give the same result either way.
"""

import dataclasses

import pydantic
import pytest

from langgraph_and_observability.persistence.serde import make_serde

# 32 ASCII characters, so 32 bytes: AES-256. A test value, not a secret.
AES_KEY = "test-key-0123456789abcdef-012345"


@dataclasses.dataclass
class Sample:
    a: int


@dataclasses.dataclass
class Other:
    b: int


class Report(pydantic.BaseModel):
    title: str


class Outer:
    @dataclasses.dataclass
    class Nested:
        c: int


pytestmark = pytest.mark.usefixtures("strict_msgpack")


@pytest.mark.parametrize(
    "value", [Sample(a=1), Report(title="disk full")], ids=["dataclass", "pydantic"]
)
def test_round_trip_allowed_type(value):
    serde = make_serde([Sample, Report])

    assert serde.loads_typed(serde.dumps_typed(value)) == value


@pytest.mark.parametrize("aes_key", [None, AES_KEY], ids=["plain", "encrypted"])
def test_unknown_type_is_not_revived(aes_key):
    serde = make_serde([Sample], aes_key=aes_key)

    loaded = serde.loads_typed(serde.dumps_typed(Other(b=1)))

    assert loaded == {"b": 1}
    assert not isinstance(loaded, Other)


def test_pickle_payload_is_refused():
    with pytest.raises(NotImplementedError):
        make_serde([Sample]).loads_typed(("pickle", b"any payload"))


def test_encrypted_round_trip_hides_plaintext():
    value = {"note": "card 12345", "sample": Sample(a=1)}
    # Without encryption the bytes hold the text, so the check below means something.
    assert b"12345" in make_serde([Sample]).dumps_typed(value)[1]
    serde = make_serde([Sample], aes_key=AES_KEY)

    type_, ciphertext = serde.dumps_typed(value)

    assert type_ == "msgpack+aes"
    assert b"12345" not in ciphertext
    assert serde.loads_typed((type_, ciphertext)) == value


def test_aes_key_of_wrong_length_is_rejected():
    with pytest.raises(ValueError, match="16, 24 or 32 bytes") as exc_info:
        make_serde([Sample], aes_key="short-key")
    assert "short-key" not in str(exc_info.value)


def test_nested_type_is_rejected():
    # The serializer stores a class by module and __name__ and looks it up at module
    # level, so a nested class could never be revived.
    with pytest.raises(ValueError, match="Outer.Nested"):
        make_serde([Outer.Nested])
