# Fixture format

The simulated adapters (`APP_ADAPTERS=simulated`) and the recorded model
(`APP_LLM_MODE=recorded`) read their data from this directory. `loader.py` loads an
incident, and `build_adapters` (`adapters/__init__.py`) builds the fakes from it.

```text
fixtures/
├── incidents/<id>/                         incident.json k8s.json datadog.json expected.json [source.json]
├── apps/<app>/                             the source code of a sample app
└── recorded/<id>/<prompt_version>/attempt-<n>.json
```

A fixture ID is the path of the incident's folder below `incidents/`. It may contain
`/`, as in `_test/invalid-json`. `list_fixtures` lists the folders directly in
`incidents/`, except those whose name starts with `_`: they hold test-only fixtures.

Each file holds one JSON object. `build_adapters` checks `k8s.json`, `datadog.json` and
`source.json` when it builds the fakes, and `RecordedLlm` checks each reply that it
reads: a missing key, or a key that is not listed here, is an error.

## `incidents/<id>/`

- `incident.json` is an `IncidentMessage` without `fired_at`, with `raw.fixture_id = <id>`.
- `k8s.json` holds `{"pods": [PodInfo…], "events": [K8sEvent…], "logs": {"<pod>/<container>/current": str|null, "<pod>/<container>/previous": str|null}, "nodes": {"<node>": [NodeCondition…]}, "annotations": {}}`.
- `datadog.json` holds `{"events": [DdEvent…]}`.
- `expected.json` holds `{"category": str, "code": {"path", "start_line", "end_line", "symbol"} | null, "facts": [str]}`.
- `source.json` holds `{"app": "<apps dir>", "commit": "fixture"}`.

`source.json` is optional: an incident without it has no app, for example an
infrastructure failure. The other four files are required.

In `k8s.json`, `annotations` holds the annotations of the incident's workload.

### The objects in k8s.json and datadog.json

Their keys are the fields of the dataclasses in `adapters/k8s/interface.py` and
`adapters/datadog/interface.py`. A time is an ISO 8601 string with a zone, such as
`"2026-11-02T10:01:00Z"`.

| Object | Keys |
|---|---|
| `PodInfo` | `name`, `namespace`, `node` (or null), `phase` (`Pending`, `Running`, `Succeeded`, `Failed` or `Unknown`), `containers` (`[ContainerStatus…]`), `annotations` (`{str: str}`), `owner` (`{"kind", "name"}` with the kind `Deployment`, `StatefulSet`, `DaemonSet` or `Job`, or null) |
| `ContainerStatus` | `name`, `image`, `image_id` (the image with its digest, or null), `restart_count` (int), `state` (`waiting`, `running` or `terminated`), `waiting_reason` (or null), `last_terminated` (`Terminated` or null), `ready` (bool) |
| `Terminated` | `reason` (or null), `exit_code` (int), `signal` (int or null) |
| `K8sEvent` | `reason`, `message`, `type` (`Normal` or `Warning`), `count` (int), `last_seen` (a time, or null), `involved_kind` (such as `Pod` or `Deployment`), `involved_name` |
| `NodeCondition` | `type` (such as `Ready` or `MemoryPressure`), `status` (`True`, `False` or `Unknown`), `reason` (or null) |
| `DdEvent` | `id`, `title`, `text`, `timestamp` (a time), `tags` (`[str]`), `source` (or null) |

### What the fakes return

- `FakeK8s` (`k8s.json`):
  - `get_pod(ns, name)`: the pod with that namespace and name, or null.
  - `list_pods_for_workload(ns, wl)`: the pods in `ns` whose `owner` is `wl`.
  - `get_owner_workload(ns, pod)`: the `owner` of that pod.
  - `list_events(ns, kind, name)`: the events whose `involved_kind` and `involved_name`
    match.
  - `read_logs(ns, pod, container, previous=…, tail_lines=…)`: the last `tail_lines`
    lines of `logs["<pod>/<container>/previous"]` (or `…/current`). Null when the key
    is missing or null, or when the pod is not in `ns`.
  - `get_node_conditions(node)`: `nodes[node]`, or `[]`.
  - `get_workload_annotations(ns, wl)`: `annotations`, for every workload.
- `FakeDatadog` (`datadog.json`): `search_events` returns the first `limit` events,
  whatever the query and time window.
- `FixtureLocator` (`source.json`): `SourceRef(repo_url="fixture://<app>",
  commit=<commit>, origin="fixture")`, or null when there is no `source.json`.
- `LocalSourceProvider`: `fixture://<app>` is the folder `apps/<app>`.

## `recorded/<id>/<prompt_version>/attempt-<n>.json`

The model's reply (`LlmResponse`) that `RecordedLlm` returns for the request with that
fixture ID, prompt version and attempt. Attempts count from 0: `attempt-0.json` is the
first call of an analysis, and `attempt-1.json` is its repair retry.

```json
{
  "text": "<the model's answer>",
  "stop_reason": "end_turn",
  "model_id": "anthropic.claude-opus-5-5",
  "usage": {"input_tokens": 0, "output_tokens": 0, "cache_read_input_tokens": 0}
}
```

`text` is the answer as the model sends it: usually the JSON of a `TriageAnalysis`.
`stop_reason` is the model's value, such as `end_turn`, `max_tokens` or `refusal`.
`cache_read_input_tokens` may be left out (it is then 0). A request with no recorded
reply fails with an error that names the expected file.

## The fake call log

With `APP_FAKE_CALL_LOG_DIR` set, the adapters of a run append one line per call to
`<APP_FAKE_CALL_LOG_DIR>/<run_id>.log`. Tests read it to count calls, also across
worker processes. A line is `<adapter>.<method>`:

- `k8s.get_pod`, `k8s.list_pods_for_workload`, `k8s.get_owner_workload`,
  `k8s.list_events`, `k8s.read_logs`, `k8s.get_node_conditions`,
  `k8s.get_workload_annotations`;
- `datadog.search_events:start` when a search starts, then `datadog.search_events`
  after its delay of `APP_FAKE_DATADOG_DELAY_S` seconds, when it returns;
- `locator.locate`, `source.checkout` and `llm.analyze`;
- `sink.<name>.send`, such as `sink.log.send`.
