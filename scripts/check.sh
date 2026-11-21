#!/usr/bin/env sh
# All quality gates, in the order a CI pipeline would run them. Fails fast.
# Runs inside the `test` container (make check) or locally (uv run scripts/check.sh).
set -eu

echo "==> format (ruff format --check)"
ruff format --check src tests migrations scripts

echo "==> lint (ruff check)"
ruff check src tests migrations scripts

echo "==> types (mypy --strict)"
mypy

echo "==> security (bandit)"
bandit -q -c pyproject.toml -r src

echo "==> tests + coverage (pytest)"
pytest --cov --cov-report=term "$@"
