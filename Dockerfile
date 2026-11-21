# One image for the control plane, the migration job and the worker simulator
# (same codebase, different command). A second target adds dev tools for tests.

ARG PYTHON_IMAGE=python:3.13-slim

# ---------------------------------------------------------------- base
FROM ${PYTHON_IMAGE} AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:${PATH}"
RUN pip install --no-cache-dir uv==0.11.32
WORKDIR /app

# ---------------------------------------------------------------- runtime
FROM base AS runtime
# Dependencies first, in their own layer: code changes don't reinstall them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
RUN uv sync --frozen --no-dev --no-editable \
 && useradd --system --uid 10001 --no-create-home app
# Never run as root in the container.
USER app
EXPOSE 8000
CMD ["control-plane"]

# ---------------------------------------------------------------- test
FROM base AS test
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project
COPY . .
RUN uv sync --frozen
CMD ["sh", "scripts/check.sh"]
