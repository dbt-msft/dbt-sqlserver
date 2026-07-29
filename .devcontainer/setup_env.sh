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

# `--all-extras` above installs the adbc-driver-manager/pyarrow *client
# library* for the adbc extra, but not the driver binary that speaks the SQL
# Server wire protocol -- that ships separately via the `dbc` CLI and is not
# on PyPI. See docs/adbc_backend.md. `dbc` installs into $VIRTUAL_ENV when a
# venv is active (it is here), so point ADBC_DRIVER_PATH there.
pip install --quiet dbc
dbc install mssql
ADBC_DRIVER_PATH="$(pwd)/.venv/etc/adbc/drivers"
if ! grep -qF 'ADBC_DRIVER_PATH' ~/.bashrc 2>/dev/null; then
  echo "export ADBC_DRIVER_PATH=\"$ADBC_DRIVER_PATH\"" >> ~/.bashrc
fi
export ADBC_DRIVER_PATH

uv run pre-commit install
