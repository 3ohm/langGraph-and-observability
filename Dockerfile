# Worker image (spec 13.5): a uv build stage and a slim runtime stage.
# Both stages use python:3.14-slim, so the virtual environment that the build stage
# makes (it links to /usr/local/bin/python3.14) also works in the runtime stage.

FROM python:3.14-slim AS builder

# uv comes from its official image, pinned by version and digest.
COPY --from=ghcr.io/astral-sh/uv:0.13.6@sha256:af22a52f5eafef3d78e551599d405c02df61919d18028f0cea8eda93bc9986d0 /uv /uvx /bin/

# Compile bytecode at build time. Copy files from the cache mount (hard links cannot
# cross file systems). Use the image's Python; never download one.
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies first: this layer stays cached until one of these three files changes.
# The runtime dependencies only (no dev or test groups), plus the `encryption` extra
# (pycryptodome), which encrypts checkpoints when LANGGRAPH_AES_KEY is set (spec 7.2).
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra encryption --no-install-project

# Then the project. hatchling needs README.md and LICENSE (pyproject.toml names them).
# --no-editable installs the package into .venv, so the runtime stage needs only .venv.
COPY README.md LICENSE ./
COPY src/ src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra encryption --no-editable

# The tree-sitter grammars of code evidence, fetched now: the worker never downloads one
# (ruling R51). The download's bundle archive is not needed afterwards. Everyone may read
# the grammars, and no one may change them.
RUN APP_GRAMMAR_DIR=/app/grammars /app/.venv/bin/python \
        -m langgraph_postgres_starter.agents.incident_triage.codeintel.languages --download \
    && rm -rf /app/grammars/tree-sitter-language-pack/*/bundles \
    && chmod -R a=rX /app/grammars


FROM python:3.14-slim AS runtime

# ripgrep and git are for the code-evidence step; ca-certificates is for TLS to AWS,
# Datadog and LangSmith.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ripgrep git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --create-home --shell /usr/sbin/nologin app

# The virtual environment and the grammars stay owned by root: the app user can read
# them, not change them.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/grammars /app/grammars

ENV PATH="/app/.venv/bin:$PATH" \
    LANGGRAPH_STRICT_MSGPACK=true \
    APP_GRAMMAR_DIR=/app/grammars

WORKDIR /app

# A numeric user lets Kubernetes runAsNonRoot verify that the user is not root.
USER 10001:10001

STOPSIGNAL SIGTERM

ENTRYPOINT ["python", "-m", "langgraph_postgres_starter"]
CMD ["worker"]
