"""The ``collect_datadog`` step: Datadog events about the target (spec §9.2, §9.4).

It searches the events tagged with the target's namespace and its pod or workload
(``build_query``) in the incident's window (``window``): from ``dd_lookback_minutes``
(``APP_DD_LOOKBACK_MINUTES``, 30) before the incident to 5 minutes after it. The
incident's time is the message's ``fired_at``, else the time in the run's UUIDv7, which
is when the message was received (``incident_time``); a step that runs again, as after
a resume, so searches the same window.

It keeps the newest ``dd_max_events`` (``APP_DD_MAX_EVENTS``, 50) events, newest first,
each as a ``datadog_event`` item (``evidence.make_item`` redacts it and caps its text):

- ID ``dd:event:<id>``, the event's Datadog ID with each character that an ID may not
  hold replaced by ``_``. When two events get the same ID that way, the newer keeps it
  and the older gets the first free suffix ``_2``, ``_3``, and so on; an event whose ID
  is free keeps it. The same events give the same IDs.
- Title: the event's title, or ``Datadog event without a title``.
- Text: the event's text, then its time and source, then its tags.
- Meta: ``source`` (``""`` if not known), ``timestamp`` (ISO 8601, UTC) and ``tags``
  (the first ``MAX_META_TAGS`` tags, joined by ``,``).
"""

import re
from datetime import UTC, datetime, timedelta
from uuid import UUID

from langgraph.runtime import Runtime

from langgraph_postgres_starter.agents.incident_triage.adapters.datadog.interface import DdEvent
from langgraph_postgres_starter.agents.incident_triage.context import TriageContext, ctx
from langgraph_postgres_starter.agents.incident_triage.evidence import make_id, make_item
from langgraph_postgres_starter.agents.incident_triage.schemas import EvidenceItem, Target
from langgraph_postgres_starter.agents.incident_triage.state import TriageState

# The window's end, after the incident (spec §9.4).
LOOKAHEAD = timedelta(minutes=5)

# The most tags that an item's meta lists.
MAX_META_TAGS = 10

# The Datadog Agent's Kubernetes tag for each kind of workload.
_WORKLOAD_TAGS = {
    "Deployment": "kube_deployment",
    "StatefulSet": "kube_stateful_set",
    "DaemonSet": "kube_daemon_set",
    "Job": "kube_job",
}

# The characters that the search syntax escapes with "\" (see build_query). "-" is
# special only where it starts a term, as the exclusion operator.
_SPECIAL = re.compile(r'[\\!&|<>(){}\[\]"*?:#\s]')

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


async def collect_datadog(state: TriageState, runtime: Runtime[TriageContext]) -> TriageState:
    """Collect the Datadog events about the target. Returns ``{"datadog_evidence": [...]}``."""
    context = ctx(runtime)
    settings = context.settings
    start, end = window(
        incident_time(state["message"].fired_at, context.run.run_id),
        settings.dd_lookback_minutes,
    )
    events = await context.adapters.datadog.search_events(
        build_query(state["target"]), start, end, settings.dd_max_events
    )
    return {"datadog_evidence": _event_items(events, settings.dd_max_events)}


def build_query(target: Target) -> str:
    """The event search query for the target's namespace and its pod or workload.

    For example ``kube_namespace:shop (pod_name:orders-7d9f-x2 OR kube_deployment:orders)``,
    or ``kube_namespace:shop pod_name:orders-7d9f-x2`` when only the pod is known. The
    workload's tag follows its kind: ``kube_deployment``, ``kube_stateful_set``,
    ``kube_daemon_set`` or ``kube_job``.

    Events search uses the logs search syntax
    (https://docs.datadoghq.com/events/explorer/searching/ and
    https://docs.datadoghq.com/logs/explorer/search_syntax/): ``key:value`` terms, ``AND``
    between terms by default, ``OR``, and parentheses. A value escapes the characters
    that the syntax treats as special with ``\\``: ``\\ ! & | < > ( ) { } [ ] " * ? : #``
    and spaces, and a leading ``-``. A ``-`` inside a value stays as it is, as in the
    syntax page's own ``host:i-0ade23e6``, so a Kubernetes name (``[a-z0-9.-]``) needs
    no escaping.
    """
    terms = []
    if target.pod is not None:
        terms.append(_term("pod_name", target.pod))
    if target.workload is not None:
        terms.append(_term(_WORKLOAD_TAGS[target.workload.kind], target.workload.name))
    namespace = _term("kube_namespace", target.namespace)
    if not terms:
        return namespace
    scope = terms[0] if len(terms) == 1 else f"({' OR '.join(terms)})"
    return f"{namespace} {scope}"


def window(fired_at: datetime, lookback_min: int) -> tuple[datetime, datetime]:
    """The search window: ``lookback_min`` minutes before ``fired_at`` to 5 minutes after."""
    return fired_at - timedelta(minutes=lookback_min), fired_at + LOOKAHEAD


def incident_time(fired_at: datetime | None, run_id: UUID) -> datetime:
    """The incident's time: ``fired_at``, else the time in ``run_id`` (spec §9.1).

    The run's UUIDv7 holds the time the message was received, to the millisecond, and
    stays the same when the run resumes. Raises ValueError if ``run_id`` is not a UUIDv7.
    """
    if fired_at is not None:
        return fired_at
    if run_id.version != 7:
        raise ValueError(f"the run ID {run_id} is not a UUIDv7, so it holds no time")
    return _EPOCH + timedelta(milliseconds=run_id.time)


def _term(tag: str, value: str) -> str:
    """The term ``tag:value``, with the value escaped (see ``build_query``)."""
    escaped = _SPECIAL.sub(lambda match: "\\" + match.group(), value)
    if escaped.startswith("-"):
        escaped = "\\" + escaped
    return f"{tag}:{escaped}"


def _utc(value: datetime) -> datetime:
    """``value`` in UTC. A time without a zone is taken to be UTC."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _event_items(events: list[DdEvent], max_events: int) -> list[EvidenceItem]:
    """The newest ``max_events`` events as items, newest first."""
    newest = sorted(events, key=lambda event: (_utc(event.timestamp), event.id), reverse=True)
    kept = newest[: max(max_events, 0)]
    return [_item(item_id, event) for item_id, event in zip(_ids(kept), kept, strict=True)]


def _ids(events: list[DdEvent]) -> list[str]:
    """Unique IDs for ``events``, in order: the sanitized ID, or the first free suffix.

    An ID that some event has as its own sanitized ID is never given as a suffixed one.
    """
    plain = [make_id("dd", "event", event.id) for event in events]
    claimed = set(plain)
    used: set[str] = set()
    ids = []
    for base in plain:
        item_id, suffix = base, 1
        while item_id in used or (item_id != base and item_id in claimed):
            suffix += 1
            item_id = f"{base}_{suffix}"
        used.add(item_id)
        ids.append(item_id)
    return ids


def _item(item_id: str, event: DdEvent) -> EvidenceItem:
    when = _utc(event.timestamp).isoformat()
    lines = [event.text or "(no text)", f"at {when}, source {event.source or 'unknown'}"]
    if event.tags:
        lines.append(f"tags: {', '.join(event.tags)}")
    return make_item(
        item_id,
        "datadog_event",
        event.title or "Datadog event without a title",
        "\n".join(lines),
        {
            "source": event.source or "",
            "timestamp": when,
            "tags": ",".join(event.tags[:MAX_META_TAGS]),
        },
    )
