# Run on EKS

This guide runs the worker on Amazon EKS in real mode: real Kubernetes, Datadog, Bedrock and SQS.
The example manifests are in `deploy/k8s/` (kustomize), the IAM policy in `deploy/iam/policy.json`
and the database roles in `deploy/sql/roles.sql`. Each adapter's settings, and the steps to a
first real triage, are in [real-mode.md](real-mode.md); this guide is about the deployment.

## What you need

- An EKS cluster, and a namespace for the worker (the examples use `langgraph-postgres-starter`).
- Postgres, reachable from the pods: RDS, Aurora or your own (Compose and CI use Postgres 18).
- An SQS input queue with a dead-letter queue (a redrive policy with `maxReceiveCount`), and a
  result queue if you use the `sqs` sink.
- An ECR repository for the image.
- Access to Claude Opus 5.5 on Bedrock in your Region (see "The model ID" below).
- Datadog API and application keys. Optional: a Git token, a LangSmith API key, an AES key.

## 1. Build and push the image

```bash
docker build -t <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/langgraph-postgres-starter:<TAG> .
docker push <ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/langgraph-postgres-starter:<TAG>
```

Set the image in `deploy/k8s/kustomization.yaml`; prefer a digest to a tag.

## 2. Create the database roles

Run `deploy/sql/roles.sql` once per database, before the first migration, as an admin:

```bash
psql "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1 \
  -v migrator_password="$LPS_MIGRATOR_PASSWORD" -v app_password="$LPS_APP_PASSWORD" \
  -f deploy/sql/roles.sql
```

It creates `lps_migrator`, which runs the migrations (DDL), and `lps_app`, which the worker uses
and which can only read and write rows (P4, P19). The script is safe to run again.

**Managed Postgres (RDS, Aurora; Postgres 16 and later).** The script assumes a superuser. The RDS
admin is not one, and `ALTER DEFAULT PRIVILEGES FOR ROLE lps_migrator` needs the admin to be a
member of `lps_migrator`. If the script stops there, grant the membership and run it again:

```sql
GRANT lps_migrator TO <admin>;
```

## 3. Create the Secrets and the ConfigMap

The manifests read secrets only from two Secrets that are not shipped; create each from an env
file that you keep out of git (the comments in `deployment.yaml` and `migrate-job.yaml` list the
keys):

- `langgraph-postgres-starter-secrets`: `APP_DATABASE_URL` (as `lps_app`), `DD_API_KEY`,
  `DD_APP_KEY`, and optionally `APP_GIT_TOKEN`, `LANGGRAPH_AES_KEY`, `LANGSMITH_API_KEY`;
- `langgraph-postgres-starter-migration`: `APP_MIGRATION_DATABASE_URL` (as `lps_migrator`).

Fill in the placeholders of `deploy/k8s/configmap.yaml`: cluster name, Region and queue URLs.
With `APP_ENV=prod`, a missing secret stops the worker at start.

## 4. IAM

Give the worker's ServiceAccount an IAM role through EKS Pod Identity or IRSA (the annotation in
`serviceaccount.yaml` is a placeholder). `deploy/iam/policy.json` grants exactly
`bedrock:InvokeModel`, the SQS receive, delete and visibility actions on the input queue,
`sqs:SendMessage` on the result queue, and `ecr:BatchGetImage` and `ecr:GetDownloadUrlForLayer`
on the repositories of the workloads you triage, to read their images' labels (P19). The migrate Job gets no IAM role
and no Kubernetes token.

### The model ID

The settings default, the bare model ID `anthropic.claude-opus-5-5`, runs in-Region on
`bedrock-runtime` only in `eu-west-2` (the AWS model card). From any other Region, such as
`us-east-1`, set `APP_BEDROCK_MODEL_ID` to an inference profile: `global.anthropic.claude-opus-5-5`
(the example ConfigMap) or a geographic one such as `us.anthropic.claude-opus-5-5`. Confirm the
ID in your account with `aws bedrock list-inference-profiles`, and keep the Bedrock ARNs of
`deploy/iam/policy.json` in step with it.

## 5. Apply: the migrate Job first

The worker needs the migrated schema, and the migrate Job needs the ConfigMap. Apply everything
except the worker, wait for the Job, then apply the rest:

```bash
kubectl create namespace langgraph-postgres-starter
kubectl apply -k deploy/k8s -l 'app.kubernetes.io/component!=worker'
kubectl -n langgraph-postgres-starter wait --for=condition=complete \
  job/langgraph-postgres-starter-migrate --timeout=10m
kubectl apply -k deploy/k8s
```

A Job's pod template cannot change. For a new image, delete the finished Job first (it also
deletes itself an hour after it finishes), then run the same three `kubectl` commands. Applied
migrations are skipped, so the Job only applies what is new.

The Deployment has `terminationGracePeriodSeconds: 60`, liveness on `/ok`, readiness on `/ready`
(P5), and `POD_NAME` from the downward API as the lease owner (P9). You set the replica count;
there is no HPA or KEDA object.

The shutdown fits in the 60-second grace period. On SIGTERM the worker drains its runs (P10)
for at most `APP_DRAIN_TIMEOUT_SECONDS` (45), returns the runs that are left to pending, then
stops the health server (at most 5 s for open connections), closes the pool (at most 5 s) and
flushes the traces (at most 5 s): 60 s in the worst case, and the last three steps usually take
well under a second. If you raise `APP_DRAIN_TIMEOUT_SECONDS`, raise
`terminationGracePeriodSeconds` with it, at least 15 s above it. A step that is still running
at SIGKILL starts again from its beginning on another pod.

## PgBouncer and direct connections

- **The worker through PgBouncer in transaction mode:** set `APP_PGBOUNCER_MODE=true`. It turns
  off prepared statements and the checkpointer's pipeline mode (P2).
- **Migrations go straight to Postgres.** Point `APP_MIGRATION_DATABASE_URL` at Postgres itself,
  not at PgBouncer: the runner holds a session advisory lock for the whole run, which
  transaction pooling does not keep.
- **The pruner's leader lock is best effort behind PgBouncer.** The lock and its unlock may run
  on different server sessions, so two pods may prune at once, and a lock left on a server
  session makes the other pods skip their passes until PgBouncer closes that session. Retention
  stays correct: each batch locks its runs `FOR UPDATE SKIP LOCKED`, and
  `aprune(strategy="delete")` is idempotent (P6).
- **Keep `APP_DB_MAX_LIFETIME_S` (1800) below the idle timeout** of PgBouncer, RDS Proxy or any
  load balancer in front of Postgres (P1).

## Code evidence: source, tokens and the cache

The worker finds the repository and commit of a crashed workload from, in order: the workload's
annotations (`lps.dev/source-repo`, `lps.dev/source-commit`), the image's OCI labels
(`org.opencontainers.image.source`, `org.opencontainers.image.revision`) read from ECR, or a
source map file (`APP_SOURCE_MAP_FILE`).

- **Allowed hosts.** Checkouts fetch only `https://` URLs on `APP_GIT_ALLOWED_HOSTS` (default
  `["github.com"]`), the only hosts that receive `APP_GIT_TOKEN`. The URLs come from the cluster,
  so anyone who can edit a workload's annotations chooses them: keep the list short.
- **Token scope.** Use a read-only token scoped to the repositories the agent needs, such as a
  GitHub fine-grained token with read access to contents, because every allowed host gets it.
- **GitHub and GitLab.** git gets the token as an `Authorization: Bearer` header. GitHub accepts
  that for git over HTTPS. GitLab's git HTTP endpoint ignores Bearer and needs Basic auth, so
  private GitLab repositories are not supported in this version.
- **Cache sizing.** Checkouts are cached under `APP_SOURCE_CACHE_DIR` with a budget of
  `APP_SOURCE_CACHE_MAX_MB` (2048). An entry used less than the run deadline ago
  (`APP_RUN_DEADLINE_SECONDS`, 900) is never evicted, nor is the one just checked out, so the
  cache can exceed its budget by the checkouts used in that window; each checkout that cannot
  evict enough logs `source.cache_over_budget`. Size the cache volume's `sizeLimit` for the
  budget plus those checkouts (the example sets 4Gi, twice the budget): the kubelet evicts the
  pod when an `emptyDir` passes its `sizeLimit`.

## Alerts and dedup

The dedup key is `sha256(cluster | namespace | workload or pod | alert_id, or else the 15-minute
bucket of fired_at)` (P8). With an `alert_id`, time is ignored: two messages with the same
`alert_id` are one run, however far apart. So a producer must send an `alert_id` that identifies
one occurrence of an alert, such as the alert event's ID, never a stable monitor ID; otherwise
every later incident of that monitor maps to the first run and is dropped as a duplicate. With
no `alert_id`, repeated alerts for one workload in one 15-minute window map to one run.

## Operate

```bash
kubectl -n langgraph-postgres-starter exec deploy/langgraph-postgres-starter-worker -- \
  python -m langgraph_postgres_starter runs list --status failed
kubectl -n langgraph-postgres-starter exec deploy/langgraph-postgres-starter-worker -- \
  python -m langgraph_postgres_starter runs retry <run_id>
```

`runs show <run_id>` prints one run, with its error. Logs are structlog JSON lines with run IDs.

A run counts its attempts (P9). A failed attempt is a graph error after the graph's own retries,
or a lost lease: the run's pod died without ending it (OOMKill, SIGKILL, node loss), and another
pod claimed it again after the lease expired. After `APP_MAX_ATTEMPTS` (3) attempts the run is
`failed`. A run failed by lost leases has the error type `LeaseExpired`: it most likely kills its
pod, so check the pod's memory limit and the run's input before `runs retry`. A drained run
(SIGTERM) counts no attempt.
With `LANGSMITH_TRACING=true` and `LANGSMITH_API_KEY`, each run is traced to the project
`langgraph-postgres-starter-<env>`, redacted (P15).

## Debug a stuck run

Python 3.14 can list the asyncio tasks of a running process, and what each one waits on, without
stopping it:

```bash
python -m asyncio ps <pid>      # one line per task: its name, coroutine stack and awaiter
python -m asyncio pstree <pid>  # the same tasks as a tree, with file and line
```

The worker is PID 1 in its container. Each run is a task named `run-<run_id>`, next to
`worker-intake`, `worker-recovery`, `worker-heartbeat`, `worker-prune` and `worker-health`. A
run that waits in the same coroutine across several calls is the one to look at; its run ID
leads to `runs show` and to its trace.

```bash
kubectl -n langgraph-postgres-starter exec <pod> -c worker -- python -m asyncio ps 1
```

The tool reads the target's memory, so the node must allow it. If `exec` fails with a permission
error, use an ephemeral debug container that shares the worker's processes and has the
`SYS_PTRACE` capability, with the same Python minor version as the image:

```bash
kubectl -n langgraph-postgres-starter debug -it <pod> --target=worker --profile=general \
  --image=python:3.14-slim -- python -m asyncio ps 1
```

A namespace that enforces the `baseline` or `restricted` Pod Security Standard rejects that
container. Locally,
`docker compose exec worker python -m asyncio ps 1` does the same.
