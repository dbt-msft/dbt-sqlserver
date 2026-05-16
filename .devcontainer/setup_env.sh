set -euo pipefail

cp test.env.sample test.env

docker compose build
docker compose up -d

# Install uv in the container user environment.
pip install uv

# Use a workspace-local virtualenv so package installs do not fail on user permissions.
uv pip install -r dev_requirements.txt
source .venv/bin/activate
pre-commit install
