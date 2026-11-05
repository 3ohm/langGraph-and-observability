# Real mode

Real mode runs Incident Triage against real systems: the Kubernetes API of the cluster the
worker runs in, Datadog, Claude on Amazon Bedrock, your Git host and ECR, and SQS. This guide
takes you from the simulated stack to one real triage, in eight steps, and ends with a smoke test
on a real crashed pod (spec §15).

[run-on-eks.md](run-on-eks.md) covers the deployment itself: the image, the database roles, the
apply order, PgBouncer, the source cache and how to operate and debug the worker. This guide links
to it where the two meet and does not repeat it.

## Before you start

- Everything in run-on-eks.md's [What you need](run-on-eks.md#what-you-need): the cluster,
  Postgres, the input queue with its dead-letter queue, a result queue for the `sqs` sink,
  Bedrock access and the Datadog keys.
- The image built and pushed to ECR
  ([run-on-eks.md, step 1](run-on-eks.md#1-build-and-push-the-image)).
- The database roles created with `deploy/sql/roles.sql`
  ([run-on-eks.md, step 2](run-on-eks.md#2-create-the-database-roles)). On managed Postgres 16 or
  later (RDS, Aurora) the admin is not a superuser: if the script stops at
  `ALTER DEFAULT PRIVILEGES FOR ROLE lps_migrator`, run `GRANT lps_migrator TO <admin>;` and run
  the script again.
- A crashed pod to triage. An application crash takes every step: a workload in
  `CrashLoopBackOff` whose log has a stack trace in one of the supported languages, with its
  source on a Git host that you allow (step 4). An infrastructure failure, such as `OOMKilled`,
  works too, but skips the code step.
- One AWS Region for everything the worker calls: the queues, Bedrock and the ECR repositories of
  the triaged images. The worker uses `APP_AWS_REGION` for all three.

The manifests in `deploy/k8s/` and the policy in `deploy/iam/policy.json` are examples with
`<PLACEHOLDERS>`, and `tests/unit/test_manifests.py` checks them as they are. Fill them in a copy
that you keep with your deployment (or in a kustomize overlay), or update those tests when you
change the examples.

## The real adapters and their settings

`APP_ADAPTERS=real` switches the Kubernetes, Datadog and source adapters to the real ones;
`APP_LLM_MODE=bedrock` switches the model. `adapters/__init__.py` (`build_adapters`) builds them.
Settings not listed here keep their defaults; `.env.example` lists every setting.

| Adapter | Code | Settings | Access |
|---|---|---|---|
| Kubernetes | `k8s/real.py` | `APP_CLUSTER_NAME`, `APP_LOG_TAIL_LINES` (500) | `deploy/k8s/rbac.yaml` |
| Datadog | `datadog/real.py` | `DD_SITE`, `DD_API_KEY`, `DD_APP_KEY`, `APP_DD_LOOKBACK_MINUTES` (30), `APP_DD_MAX_EVENTS` (50) | the two keys |
| Source locator | `source/locator.py`, `source/ecr.py` | `APP_SOURCE_ANNOTATION_REPO_KEY`, `APP_SOURCE_ANNOTATION_COMMIT_KEY`, `APP_SOURCE_MAP_FILE`, `APP_GIT_ALLOWED_HOSTS`, `APP_AWS_REGION` | ECR statement of the policy |
| Git source | `source/git.py` | `APP_GIT_TOKEN`, `APP_GIT_ALLOWED_HOSTS`, `APP_SOURCE_CACHE_DIR`, `APP_SOURCE_CACHE_MAX_MB` (2048), `APP_RUN_DEADLINE_SECONDS` (900), `APP_CODE_BUDGET_CHARS` (60000) | the token |
| Model | `llm/bedrock.py` | `APP_BEDROCK_MODEL_ID`, `APP_AWS_REGION`, `APP_MODEL_EFFORT` (`medium`), `APP_MAX_OUTPUT_TOKENS` (16000) | Bedrock statements of the policy |
| Result sink `sqs` | `sinks/sqs.py` | `APP_RESULT_SINKS` (default `["log"]`; the ConfigMap sets `["log", "sqs"]`), `APP_RESULT_QUEUE_URL`, `APP_AWS_REGION`, `APP_SQS_ENDPOINT_URL` | `sqs:SendMessage` |
| Intake (the worker) | `worker/intake.py` | `APP_SQS_QUEUE_URL`, `APP_AWS_REGION`, `APP_SQS_ENDPOINT_URL` | the SQS receive statement |

The code paths are under `src/langgraph_and_observability/agents/incident_triage/adapters/`, except
the intake's. `APP_RUN_DEADLINE_SECONDS` matters to the Git source because a checkout used less
than one run deadline ago is never evicted from the cache.

## 1. Set up the IAM role with Pod Identity or IRSA

The worker's ServiceAccount, `langgraph-and-observability` in the namespace
`langgraph-and-observability`, gets one IAM role. Its permissions policy is
`deploy/iam/policy.json` with the placeholders filled in; the commands below call that copy
`policy.json`. The migrate Job gets no role.

**EKS Pod Identity** (simpler; needs the EKS Pod Identity Agent add-on). Save this trust policy
as `pod-identity-trust.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "pods.eks.amazonaws.com" },
      "Action": ["sts:AssumeRole", "sts:TagSession"]
    }
  ]
}
```

```bash
aws eks create-addon --cluster-name <CLUSTER_NAME> --addon-name eks-pod-identity-agent
aws iam create-role --role-name <ROLE_NAME> \
  --assume-role-policy-document file://pod-identity-trust.json
aws iam put-role-policy --role-name <ROLE_NAME> --policy-name langgraph-and-observability \
  --policy-document file://policy.json
aws eks create-pod-identity-association --cluster-name <CLUSTER_NAME> \
  --namespace langgraph-and-observability --service-account langgraph-and-observability \
  --role-arn arn:aws:iam::<ACCOUNT_ID>:role/<ROLE_NAME>
```

Then delete the `eks.amazonaws.com/role-arn` annotation from `deploy/k8s/serviceaccount.yaml`.

**IRSA.** The cluster needs an IAM OIDC provider. Its issuer, without `https://`, is the
`<OIDC_PROVIDER>` below:

```bash
aws eks describe-cluster --name <CLUSTER_NAME> --query cluster.identity.oidc.issuer --output text
```

Create the role with this trust policy, attach `policy.json` as above, and set the role's ARN in
the ServiceAccount's `eks.amazonaws.com/role-arn` annotation:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/<OIDC_PROVIDER>" },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "<OIDC_PROVIDER>:sub": "system:serviceaccount:langgraph-and-observability:langgraph-and-observability",
          "<OIDC_PROVIDER>:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
```

### What the policy grants

It grants only what the code calls (P19), each action on its own resource:

- **The input queue:** `sqs:ReceiveMessage`, `sqs:DeleteMessage` and `sqs:ChangeMessageVisibility`.
  The worker never writes to the dead-letter queue: SQS moves a message there after the queue's
  `maxReceiveCount`.
- **The result queue:** `sqs:SendMessage`, for the `sqs` sink. Without that sink, drop the
  statement.
- **KMS, for encrypted queues.** With SQS-managed encryption (SSE-SQS) nothing changes. A queue
  encrypted with a customer-managed KMS key also needs `kms:Decrypt` on that key to receive, and
  `kms:GenerateDataKey` and `kms:Decrypt` to send. The policy leaves them out: add a statement for
  your key's ARN, and allow the role in the key policy. Whoever sends incidents to the input
  queue (step 7) needs the sending pair too.
- **ECR:** `ecr:BatchGetImage` and `ecr:GetDownloadUrlForLayer`, to read the OCI labels of the
  triaged images (step 4). The example lists one repository ARN. List one ARN per repository that
  the triaged workloads' images come from,
  `arn:aws:ecr:<REGION>:<REGISTRY_ACCOUNT_ID>:repository/<REPOSITORY_NAME>`. For a registry in
  another account, its repository policy must also allow the role. Images outside ECR, or in
  another Region than `APP_AWS_REGION` (logged as `source.ecr.other_region`), have no labels for
  the agent: use annotations or the source map instead.
- **Bedrock:** `bedrock:InvokeModel` in three statements, for the global inference profile that
  the example ConfigMap sets (`global.anthropic.claude-opus-5-5`):
  1. `InvokeTheGlobalInferenceProfile`: the profile's ARN in your Region and account, for
     requests made in your Region.
  2. `InvokeTheModelInTheSourceRegionThroughTheProfile`: the foundation model in your Region,
     only when the request goes through that profile (`bedrock:InferenceProfileArn`).
  3. `InvokeTheModelInOtherRegionsThroughTheProfile`: the foundation model in any other Region.
     A global profile can route to any commercial Region, so this ARN has no Region and no
     account (`arn:aws:bedrock:::foundation-model/...`), and its requests carry
     `aws:RequestedRegion` `unspecified`. It too applies only through that profile.

  The role can therefore call the model only through that one profile.

### The model ID

The settings default, the bare model ID `anthropic.claude-opus-5-5`, runs in-Region on
`bedrock-runtime` only in `eu-west-2` (the AWS model card). From any other Region, such as
`us-east-1`, set `APP_BEDROCK_MODEL_ID` to an inference profile: `global.anthropic.claude-opus-5-5`
(the example ConfigMap) or a geographic one such as `us.anthropic.claude-opus-5-5`. Confirm the
profile in your account:

```bash
aws bedrock list-inference-profiles --region <REGION> \
  --query "inferenceProfileSummaries[?contains(inferenceProfileId, 'claude-opus-5-5')]"
```

The Bedrock statements must match the ID you set:

- **The global profile:** the three statements as shipped.
- **A geographic profile** (`us.`, `eu.` and so on): put its ID in the profile ARN. Replace
  statements 2 and 3 with one statement whose resources are the foundation model's ARN in each
  Region the profile routes to,
  `arn:aws:bedrock:<DESTINATION_REGION>::foundation-model/anthropic.claude-opus-5-5`, with the
  same `bedrock:InferenceProfileArn` condition and no `aws:RequestedRegion` condition.
  `aws bedrock get-inference-profile --inference-profile-identifier us.anthropic.claude-opus-5-5`
  lists those Regions' model ARNs.
- **The bare ID in `eu-west-2`:** one statement on
  `arn:aws:bedrock:eu-west-2::foundation-model/anthropic.claude-opus-5-5`, and no profile
  statements.

Before you deploy, one call with your own credentials and your `APP_BEDROCK_MODEL_ID` shows that
the account can use the model (any reply, even one cut off at `max_tokens`, is a pass):

```bash
aws bedrock-runtime invoke-model --region <REGION> --model-id global.anthropic.claude-opus-5-5 \
  --cli-binary-format raw-in-base64-out \
  --body '{"anthropic_version": "bedrock-2023-05-31", "max_tokens": 1024,
           "messages": [{"role": "user", "content": "Reply with OK."}]}' \
  /dev/stdout
```

## 2. Apply RBAC

`deploy/k8s/rbac.yaml` is a ClusterRole with `get` and `list` only, on pods, pod logs, events,
nodes, ReplicaSets, Deployments, StatefulSets, DaemonSets and Jobs, and a ClusterRoleBinding to
the worker's ServiceAccount. It is cluster-wide because nodes are cluster-scoped and an incident
can be in any namespace. There is no `watch`, no Secrets, no `exec` and no write (P19).

Step 5 applies it with the rest. To apply the ServiceAccount and RBAC now, and check them:

```bash
kubectl create namespace langgraph-and-observability
kubectl apply -n langgraph-and-observability \
  -f deploy/k8s/serviceaccount.yaml -f deploy/k8s/rbac.yaml

SA=system:serviceaccount:langgraph-and-observability:langgraph-and-observability
kubectl auth can-i list pods --all-namespaces --as="$SA"                 # yes
kubectl auth can-i get pods --subresource=log -n <APP_NAMESPACE> --as="$SA"  # yes
kubectl auth can-i list nodes --as="$SA"                                 # yes
kubectl auth can-i get secrets -n <APP_NAMESPACE> --as="$SA"             # no
kubectl auth can-i delete pods -n <APP_NAMESPACE> --as="$SA"             # no
```

## 3. Create the Secrets: DD_API_KEY, DD_APP_KEY and APP_GIT_TOKEN

The manifests read secrets from two Secrets that are not shipped (the comments at the top of
`deployment.yaml` and `migrate-job.yaml` list their keys):

- `langgraph-and-observability-secrets`, for the worker: `APP_DATABASE_URL` (as `lps_app`),
  `DD_API_KEY`, `DD_APP_KEY` and `APP_GIT_TOKEN`, and optionally `LANGGRAPH_AES_KEY` and
  `LANGSMITH_API_KEY`;
- `langgraph-and-observability-migration`, for the migrate Job: `APP_MIGRATION_DATABASE_URL` (as
  `lps_migrator`).

Write each as an env file, one `KEY=value` line per key, with an editor rather than `echo` (which
keeps the values in your shell history), outside the repository. Then:

```bash
kubectl -n langgraph-and-observability create secret generic langgraph-and-observability-secrets \
  --from-env-file=worker.env
kubectl -n langgraph-and-observability create secret generic langgraph-and-observability-migration \
  --from-env-file=migration.env
rm worker.env migration.env
```

- **Datadog.** `DD_API_KEY` is an API key and `DD_APP_KEY` an application key of the organization
  whose events you want. The agent only searches events (`POST /api/v2/events/search`). Create the
  application key for a service account with a read-only role; a scoped key needs the
  `events_read` scope.
- **The Git token.** `APP_GIT_TOKEN` is optional: public repositories need none. Use a read-only
  token scoped to the repositories that the agent reads, such as a GitHub fine-grained personal
  access token with read access to Contents on the selected repositories. git gets it as an
  `Authorization: Bearer <token>` header, only for the repository's own host, and only for hosts
  in `APP_GIT_ALLOWED_HOSTS` (step 4). There is one token for all those hosts, so any allowed host
  can receive it.
- **GitHub and GitLab.** GitHub accepts the token as a Bearer header for git over HTTPS. GitLab's
  git HTTP endpoint ignores Bearer and needs Basic auth, so private GitLab repositories are not
  supported in this version.
- **With `APP_ENV=prod`** (the example ConfigMap), a missing `APP_DATABASE_URL`,
  `APP_SQS_QUEUE_URL`, `DD_API_KEY` or `DD_APP_KEY` stops the worker at start, with a
  `worker.startup_failed` log line. The worker checks them in two steps, and the line names the
  settings fields that are missing in the first step that fails. First come the worker's two
  (`database_url`, and `sqs_queue_url` while `APP_INTAKE_AGENT` names an agent). Only when both
  are set does it check the two Datadog keys that the agent needs in real mode (`dd_api_key`,
  `dd_app_key`, from `AgentSpec.required_settings`). So a start with none of the four set names
  only the first two; the next start, with those two set, names the Datadog keys.

## 4. Set APP_ADAPTERS=real and APP_LLM_MODE=bedrock

`deploy/k8s/configmap.yaml` already sets `APP_ENV=prod`, `APP_ADAPTERS=real` and
`APP_LLM_MODE=bedrock`. Fill in its placeholders and check the rest:

| Key | Set it to |
|---|---|
| `APP_CLUSTER_NAME` | The cluster's name. It is part of the dedup key, and the run of a message that names another cluster fails. |
| `APP_AWS_REGION` | The Region of the queues, Bedrock and ECR, as in the policy. |
| `APP_SQS_QUEUE_URL` | The input queue's URL. |
| `APP_RESULT_SINKS` | `["log", "sqs"]`, or `["log"]` without a result queue. |
| `APP_RESULT_QUEUE_URL` | The result queue's URL, for the `sqs` sink. |
| `APP_BEDROCK_MODEL_ID` | The inference profile from step 1. |
| `APP_GIT_ALLOWED_HOSTS` | A JSON list of the Git hosts to fetch from, such as `["github.com"]`. |
| `DD_SITE` | Your Datadog site, such as `datadoghq.com` or `datadoghq.eu`. |

Leave `APP_SQS_ENDPOINT_URL` unset in AWS: it points boto3 at ElasticMQ in the local stack. Keep
`APP_MODEL_EFFORT` (`medium`) and `APP_MAX_OUTPUT_TOKENS` (16000) for the smoke test.
`deployment.yaml` sets `APP_SOURCE_CACHE_DIR` and `APP_SOURCE_CACHE_MAX_MB` to match its cache
volume ([run-on-eks.md](run-on-eks.md#code-evidence-source-tokens-and-the-cache) explains the
sizing).

### Where the agent finds the source

For an application crash, the agent reads the crashing code at the deployed commit (spec §9.6).
It tries, in order, and takes the first that names both a repository and a commit:

1. **The workload's annotations** (on the Deployment, StatefulSet, DaemonSet or Job itself), with
   the keys `APP_SOURCE_ANNOTATION_REPO_KEY` (`lps.dev/source-repo`) and
   `APP_SOURCE_ANNOTATION_COMMIT_KEY` (`lps.dev/source-commit`). Set them where you deploy:

   ```bash
   kubectl -n <APP_NAMESPACE> annotate deployment/<NAME> --overwrite \
     lps.dev/source-repo=https://github.com/<ORG>/<REPO> \
     lps.dev/source-commit=<FULL_40_HEX_SHA>
   ```

2. **The image's OCI labels** `org.opencontainers.image.source` and
   `org.opencontainers.image.revision`, read from ECR. Add them at build time, for example
   `docker build --label org.opencontainers.image.source=https://github.com/<ORG>/<REPO>
   --label org.opencontainers.image.revision=$(git rev-parse HEAD) ...`.
3. **The source map**, a YAML file at `APP_SOURCE_MAP_FILE` (mount it from a ConfigMap) that maps
   `<namespace>/<workload name>` to `repo` and `ref`. A branch as the ref is fetched again on each
   run and may not be the deployed commit, so prefer a full SHA.

A repository URL must be `https://` on a host in `APP_GIT_ALLOWED_HOSTS`, with no credentials, no
port other than 443, no query and no fragment; any other origin is skipped with a
`source.locate.skipped` warning, and no git process starts. Anyone who can edit a workload's
annotations chooses these URLs, so keep the list short. Each match is logged as
`source.locate.found`, with its origin, URL and commit.

### What the Datadog query covers

The agent searches events with `kube_namespace:<namespace> (pod_name:<pod> OR
kube_deployment:<name>)` (the workload's tag follows its kind), from `APP_DD_LOOKBACK_MINUTES`
before the incident to 5 minutes after, at most `APP_DD_MAX_EVENTS` events. The query has no
cluster tag. If one Datadog organization watches several clusters, events of a same-named
workload in the same namespace of another cluster can join the evidence; the result cites them
as `dd:event:<id>` items, so you can spot them in step 8.

## 5. Run the migrate Job

The migrate Job applies the SQL migrations and the LangGraph checkpointer setup as
`lps_migrator`, then exits. The worker never migrates. Apply everything except the worker, wait
for the Job, and read its log:

```bash
kubectl apply -k deploy/k8s -l 'app.kubernetes.io/component!=worker'
kubectl -n langgraph-and-observability wait --for=condition=complete \
  job/langgraph-and-observability-migrate --timeout=10m
kubectl -n langgraph-and-observability logs job/langgraph-and-observability-migrate
```

The log has one JSON line per migration it applied, such as
`{"component": "incident_triage", "version": 1, "name": "triage_results"}`, and
`{"component": "langgraph", "step": "setup"}`. A second run applies nothing new. If the Job
fails, its log names the error; on RDS or Aurora, check the `GRANT lps_migrator TO <admin>` step
in "Before you start". For a new image, delete the finished Job first
([run-on-eks.md, step 5](run-on-eks.md#5-apply-the-migrate-job-first)).

## 6. Deploy

```bash
kubectl apply -k deploy/k8s
kubectl -n langgraph-and-observability rollout status deploy/langgraph-and-observability-worker
kubectl -n langgraph-and-observability logs deploy/langgraph-and-observability-worker | head -20
```

Check that:

- each pod becomes Ready: `/ready` checks Postgres, the pool and the intake loop;
- the log has `worker.started`, and no `worker.startup_failed`;
- with `LANGSMITH_API_KEY` set, the log has `tracing.configured` (`tracing.disabled_no_key`
  means the key is missing);
- the pod runs as the IAM role from step 1:

  ```bash
  kubectl -n langgraph-and-observability exec deploy/langgraph-and-observability-worker -- \
    python -c "import boto3; print(boto3.client('sts', region_name='<REGION>').get_caller_identity()['Arn'])"
  ```

  The ARN is `arn:aws:sts::<ACCOUNT_ID>:assumed-role/<ROLE_NAME>/...`.

## 7. Send a real incident with scripts/send_incident.py --message-file

Find a crashed pod, then write the incident as a JSON `IncidentMessage` (spec §9.1) in
`incident.json`:

```bash
kubectl get pods -A | grep -E 'CrashLoopBackOff|Error|OOMKilled'
```

```json
{
  "version": 1,
  "source": "real-mode-smoke-test",
  "alert_id": "smoke-test-<DATE>-1",
  "cluster": "<CLUSTER_NAME>",
  "namespace": "<APP_NAMESPACE>",
  "pod": "<POD_NAME>",
  "workload": { "kind": "Deployment", "name": "<DEPLOYMENT_NAME>" },
  "container": "<CONTAINER_NAME>"
}
```

- `pod` or `workload` is required; `container` is optional (the agent picks the failing one).
- `cluster` must equal `APP_CLUSTER_NAME`, or be left out.
- `fired_at` is optional: the script sets the current time.
- **`alert_id` must identify one occurrence.** With an `alert_id`, dedup ignores time: two
  messages with the same `alert_id` are one run, however far apart, and the second is logged as
  `intake.duplicate` and dropped. So give each test send a new value. A real producer must send
  an ID that is new for each occurrence of an alert, such as the alert event's ID, never a stable
  monitor ID. With no `alert_id`, repeated alerts for one workload in one 15-minute window map to
  one run ([run-on-eks.md](run-on-eks.md#alerts-and-dedup)).

Send it from a machine with AWS credentials that may send to the input queue:

```bash
AWS_DEFAULT_REGION=<REGION> uv run scripts/send_incident.py --message-file incident.json \
  --queue-url https://sqs.<REGION>.amazonaws.com/<ACCOUNT_ID>/<INPUT_QUEUE_NAME> \
  --endpoint-url ''
```

`--endpoint-url ''` makes boto3 use the AWS endpoint (the default is the local ElasticMQ). The
script validates the message first, then prints the SQS message ID.

## 8. Check that the TriageResult looks right

Follow the run in the worker's log, then read its result:

```bash
kubectl -n langgraph-and-observability logs -f deploy/langgraph-and-observability-worker \
  | grep -E '"(intake|source|analyze|publish|triage)\.'
kubectl -n langgraph-and-observability exec deploy/langgraph-and-observability-worker -- \
  python -m langgraph_and_observability runs list --limit 5
kubectl -n langgraph-and-observability logs deploy/langgraph-and-observability-worker \
  | grep '"triage.published"' | jq '.result'
```

`runs show <run_id>` prints the run and, for a failed run, its error. The result is also a row in
`triage_results` (`select result from triage_results where run_id = '<run_id>'`), and a message on
the result queue with the attribute `idempotency_key` `publish:<run_id>:sqs`.

A good `TriageResult` has:

- **`target`**: your cluster, namespace and workload, the crashed pod (a replacement pod adds the
  note `note:target:pod_replaced`), its container and node, and its images with their digests.
- **`assessment`**: for a code bug, `collect_code: true` with an application signal (a stack trace
  or a non-zero exit code) in `app_signals`; for an infrastructure cause, such as `OOMKilled`,
  that signal in `infra_signals`. `reasons` says why.
- **`evidence_counts`**: `k8s` above 0 (pod status, events, logs and the node); `datadog` above 0
  if Datadog has events for that pod in the window; `code` above 0 for a code bug; `notes` counts
  the notes below.
- **`analysis`**:
  - `category` matches what you see (`application` for a code bug);
  - `summary` (at most 600 characters) and `root_cause` are concrete and agree with the log;
  - `confidence` is `low`, `medium` or `high`;
  - `evidence` cites items by ID (`k8s:...`, `log:...`, `dd:...`, `code:...`, `note:...`), each
    with a `why` that makes sense. The worker accepts only IDs that the run collected;
  - `suspected_code` points at the crashing code: open each `path` at lines `start_line` to
    `end_line` at the deployed commit, for example
    `https://github.com/<ORG>/<REPO>/blob/<COMMIT>/<PATH>#L<START>-L<END>`, and check `symbol`.
    The worker accepts only locations inside the snippets it collected;
  - `recommended_actions` has at most 5 actions that fit the cause;
  - `needs_human` is the model's call; expect `true` for a low-confidence or `unknown` answer.
- **`needs_human`** equals `analysis.needs_human`, and is `true` when there is no analysis.
- **`refusal`** is `false`. A refusal leaves `analysis` null, with `needs_human: true` and the
  category `unknown` in `triage_results`.
- **`model_id`** is your `APP_BEDROCK_MODEL_ID`, and **`usage`** has input and output tokens above
  0. A second run within a few minutes can show `cache_read_input_tokens` above 0: the system
  prompt is cached.

Notes tell you what the run could not collect:

- `note:code:source_unavailable`: no origin named a repository and commit, or each one was
  skipped. Look for `source.locate.skipped` and `source.locate.none` in the log, and check the
  annotations, the labels, the source map and `APP_GIT_ALLOWED_HOSTS`.
- `note:code:unmapped_frames`: stack frames that matched no file in the checkout, such as paths
  from a build folder.
- `note:datadog:unavailable` or `note:code:unavailable`: that step failed for good, and the run
  went on without it. The note, and the log's `step.unavailable` line, name only the error's
  type, never its message, which may hold a secret. For Datadog, check the keys and `DD_SITE`;
  for the code, the token and `APP_GIT_ALLOWED_HOSTS`.
- `note:logs:previous_unavailable`: the container has no previous log, so the current log was
  used.
- `note:target:not_found`: the pod and workload were not found in that namespace.

If the run failed, `runs show <run_id>` names the error. A Bedrock `AccessDeniedException`
usually means that the Bedrock statements do not match `APP_BEDROCK_MODEL_ID` (step 1), and a
`ValidationException` about the model ID that the ID does not run in `APP_AWS_REGION`.

The smoke test passes when one real crashed pod gives a valid `TriageResult` that passes these
checks. `docs/release-checklist.md` says where to record it.
