# Security policy

## Supported versions

The project is at 0.x. Only the latest release gets security fixes.

## Report a vulnerability

Do not open a public issue, pull request or discussion.

Report it privately through GitHub: open the repository's **Security** tab and select
**Report a vulnerability**. Include:

- what the problem is and what an attacker can do with it;
- the version or commit you tested;
- the steps to reproduce it.

We will confirm that we received the report, keep you informed, and credit you in the release
notes unless you ask us not to.

## What is in scope

This template handles credentials for AWS, Datadog, Git and LangSmith. Its design requires:

- no default secrets in the image, the settings or the example manifests;
- no secrets in graph state, in `configurable`, in logs, in traces or in prompts;
- redaction of evidence text before it enters graph state;
- read-only access to the cluster.

A way to break any of these is a vulnerability. Please report it.
