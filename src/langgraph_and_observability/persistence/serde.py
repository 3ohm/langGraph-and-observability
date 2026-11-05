"""The checkpoint serializer (spec §7.2).

Checkpoints are stored as msgpack, never as pickle. Only the types on an explicit
allowlist, plus LangGraph's built-in safe types, come back as themselves. Any other
class comes back as its plain data, for example a dict for a dataclass or a pydantic
model. Spec §7.2 names the advisories behind this: CVE-2025-64439 and CVE-2026-27794.

The allowlist is always passed explicitly, so ``LANGGRAPH_STRICT_MSGPACK`` does not
change it: that variable only picks the default when no allowlist is given. When the
variable is on, ``StateGraph.compile()`` also adds the graph's own state types.

With an AES key, the serialized bytes are encrypted at rest (AES, EAX mode).
"""

from collections.abc import Sequence

from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.serde.encrypted import EncryptedSerializer
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

_AES_KEY_LENGTHS = (16, 24, 32)


def make_serde(allowed_types: Sequence[type], *, aes_key: str | None = None) -> SerializerProtocol:
    """Return a serializer that revives only ``allowed_types`` and the built-in safe types.

    Each type must be defined at module level: the serializer stores a class by its
    module and name and looks it up there. ``aes_key`` is the AES key as text, as
    ``LANGGRAPH_AES_KEY`` holds it, 16, 24 or 32 bytes long in UTF-8. With a key, the
    bytes are encrypted; that needs pycryptodome (the ``encryption`` extra).

    Raises ValueError for a type that is not defined at module level, and for an AES
    key of another length.
    """
    allowlist: list[tuple[str, str]] = []
    for type_ in allowed_types:
        if type_.__qualname__ != type_.__name__:
            raise ValueError(
                f"{type_.__module__}.{type_.__qualname__} is not defined at module level, "
                "so the checkpoint serializer cannot revive it"
            )
        allowlist.append((type_.__module__, type_.__qualname__))
    serde = JsonPlusSerializer(pickle_fallback=False, allowed_msgpack_modules=allowlist)
    if aes_key is None:
        return serde
    key = aes_key.encode()
    if len(key) not in _AES_KEY_LENGTHS:
        raise ValueError("the AES key must be 16, 24 or 32 bytes long")
    return EncryptedSerializer.from_pycryptodome_aes(serde, key=key)
