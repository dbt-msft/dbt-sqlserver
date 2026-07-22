set -euo pipefail

# Persist for all shell sessions (Zed uses devcontainer exec, no VS Code server for remoteEnv)
if ! grep -qF 'SQLSERVER_TEST_DRIVER' ~/.bashrc 2>/dev/null; then
  echo 'export SQLSERVER_TEST_DRIVER="ODBC Driver 18 for SQL Server"' >> ~/.bashrc
fi
export SQLSERVER_TEST_DRIVER="ODBC Driver 18 for SQL Server"

cp test.env.sample test.env

sudo apt-get update
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2

docker compose build
docker compose up -d

# Install uv in the container user environment only when needed.
command -v uv >/dev/null 2>&1 || pip install uv

# Use a workspace-local virtualenv so package installs do not fail on user permissions.
[ -d .venv ] || uv venv
source .venv/bin/activate

# Install both backend extras so the devcontainer can exercise either connection path,
# plus the dev dependency group (pre-commit, pytest, etc.). Groups are installed
# explicitly rather than relying on uv's default-group behaviour, which varies by version.
uv sync --all-extras --group dev
uv run pre-commit install
