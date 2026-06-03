set -euo pipefail

cp test.env.sample test.env

docker compose build
docker compose up -d

# Install uv in the container user environment only when needed.
command -v uv >/dev/null 2>&1 || pip install uv

# Use a workspace-local virtualenv so package installs do not fail on user permissions.
[ -d .venv ] || uv venv
source .venv/bin/activate

uv sync --group dev --extra pyodbc
pre-commit install
