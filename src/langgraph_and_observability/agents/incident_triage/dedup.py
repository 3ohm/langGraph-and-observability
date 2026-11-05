"""The dedup key of an incident (spec §9.11).

Repeated alerts for the same workload in the same 15-minute window, or with the same
alert ID, get the same key, and the worker maps messages with one key to one run.
"""

import hashlib
from datetime import UTC, datetime

from langgraph_and_observability.agents.incident_triage.schemas import IncidentMessage

# The length of a dedup window, in minutes. It divides an hour.
BUCKET_MINUTES = 15


def dedup_key(msg: IncidentMessage, received_at: datetime) -> str:
    """Return the SHA-256 hex digest of four fields of ``msg``, joined by ``|``.

    The fields, in order:

    1. ``cluster``, or the empty string when the message names none;
    2. ``namespace``;
    3. the workload as ``<kind>/<name>``, or else the ``pod``;
    4. ``alert_id``, or else the 15-minute bucket of ``fired_at``, or of ``received_at``
       when the message has no ``fired_at``. The bucket is that time in UTC, rounded
       down to a multiple of 15 minutes, in ISO 8601.

    For example, a message for Deployment ``api`` in namespace ``payments`` of cluster
    ``prod-eu``, fired at 10:14 UTC on 2 November 2026 with no alert ID, hashes
    ``prod-eu|payments|Deployment/api|2026-11-02T10:00:00+00:00``. A message for pod
    ``api-7d9f-x2`` in ``payments``, with no cluster and alert ID ``dd-123``, hashes
    ``|payments|api-7d9f-x2|dd-123``.

    A naive ``received_at`` is taken to be UTC.
    """
    # IncidentMessage requires a pod when it has no workload.
    subject = f"{msg.workload.kind}/{msg.workload.name}" if msg.workload else msg.pod or ""
    window = msg.alert_id or _bucket(msg.fired_at or received_at).isoformat()
    key = "|".join([msg.cluster or "", msg.namespace, subject, window])
    return hashlib.sha256(key.encode()).hexdigest()


def _bucket(moment: datetime) -> datetime:
    """Return ``moment`` in UTC, rounded down to a multiple of ``BUCKET_MINUTES``."""
    utc = moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)
    minute = utc.minute - utc.minute % BUCKET_MINUTES
    return utc.replace(minute=minute, second=0, microsecond=0)
