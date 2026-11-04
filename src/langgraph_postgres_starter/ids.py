"""Time-ordered identifiers."""

import uuid


def new_id() -> uuid.UUID:
    """Return a new UUIDv7. IDs sort in creation order, also within one millisecond."""
    return uuid.uuid7()
