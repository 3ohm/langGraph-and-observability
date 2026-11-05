# Quickstart

Run the example agent, Incident Triage, on your machine with no credentials: no AWS account, no
cluster, no Datadog and no LangSmith. Everything runs in Docker Compose in simulated mode.

## What you need

- A clone of the repository: `git clone https://github.com/3ohm/langGraph-and-observability.git`.
  Run the commands below in it.
- Docker with Compose v2 (Docker Desktop, OrbStack or colima).
- [uv](https://docs.astral.sh/uv/). `make incident` runs a script with uv, and uv installs
  Python 3.14 when it is missing.
- Free local ports 5432 (Postgres), 9324 and 9325 (ElasticMQ). Compose binds them to
  127.0.0.1 only.

## Three commands

```bash
make up                    # build the image; start Postgres, ElasticMQ, migrate, then the worker
make incident LANG=python  # send the Python sample app's crash to the queue
docker compose logs worker # the result is the triage.published line
```

`make up` starts four services in order:

1. `postgres` (`pgvector/pgvector:0.8.4-pg18`), with a healthcheck;
2. `elasticmq`, a local SQS with the `incidents` queue, its dead-letter queue and a result queue;
3. `migrate`, which applies every migration once and exits (P4);
4. `worker`, in simulated mode: fake Kubernetes and Datadog adapters that serve the incident
   fixtures, the sample app's source from a local folder, and recorded model replies.

`make incident` validates the fixture's `incident.json` as an `IncidentMessage` and sends it to
the queue. The worker takes it, collects the evidence, decides whether to read code, asks the
recorded model, and publishes a typed `TriageResult` to the log and to Postgres.

## Other incidents

`make incident LANG=<language>` sends the incident of that language's sample app:
`javascript`, `typescript`, `python`, `java`, `go`, `php`, `ruby`, `kotlin`, `scala`, `c` and
`cpp`. `make incident FIXTURE=<id>` sends any fixture by its folder name in
`src/langgraph_and_observability/agents/incident_triage/fixtures/incidents/`, and wins over
`LANG`.

- `make incident LANG=java` sends `java-spring-npe`. The plain-Java sample is
  `make incident FIXTURE=java-plain-arithmetic`.
- The infrastructure cases have no sample app: `FIXTURE=infra-oomkilled`,
  `FIXTURE=infra-imagepull`, `FIXTURE=infra-probe-failure` and `FIXTURE=infra-config-error`.
  For these the agent does not read code.

Sending the same fixture again is a duplicate: each fixture has an `alert_id`, which gives the
same dedup key, so the worker logs `intake.duplicate` and keeps the first run (P8).
`docker compose down -v` starts over with an empty database.

## Look at the results

```bash
docker compose exec worker python -m langgraph_and_observability runs list
docker compose exec worker python -m langgraph_and_observability runs show <run_id>
docker compose exec postgres psql -U postgres -c "select run_id, category from triage_results"
```

`runs retry <run_id>` returns a failed run to pending. To see what a running worker is waiting
on, `docker compose exec worker python -m asyncio ps 1` lists its asyncio tasks (Python 3.14;
see [run-on-eks.md](run-on-eks.md#debug-a-stuck-run)).

## LangGraph Studio

```bash
make setup                 # install every dependency group and fetch the tree-sitter grammars
cp .env.example .env       # langgraph.json loads .env
make studio                # langgraph dev on http://127.0.0.1:2024, simulated adapters
```

Open Studio from the URL that `langgraph dev` prints. Any fixture incident is a valid input, and
an empty context runs `APP_STUDIO_FIXTURE` (`python-keyerror`).

## Develop

```bash
make lint typecheck        # ruff and mypy (strict)
make test                  # unit tests: no Docker, no network, no credentials
make test-int              # integration tests against Postgres and ElasticMQ (Docker)
make test-e2e              # worker subprocesses, kill -9 and SIGTERM (Docker)
```

## Stop

`make down` stops the stack and keeps its database volume. `docker compose down -v` deletes the
volume too.

## Next

- [run-on-eks.md](run-on-eks.md): the worker on EKS with real Kubernetes, Datadog, Bedrock and SQS.
- [real-mode.md](real-mode.md): each real adapter and its settings, and a first real triage.
- [customize.md](customize.md): rename the package, remove the example, add your own agent.
