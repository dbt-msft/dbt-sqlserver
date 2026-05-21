set -euo pipefail

cp test.env.sample test.env

sudo apt-get update
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2

docker compose build
docker compose up -d

# Install uv in the container user environment.
pip install uv

# Use a workspace-local virtualenv so package installs do not fail on user permissions.
[ -d .venv ] || uv venv
source .venv/bin/activate

uv sync --group dev --extra pyodbc
pre-commit install
