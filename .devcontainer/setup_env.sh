set -eu

cp test.env.sample test.env

sudo apt-get update
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2

docker compose build
docker compose up -d

# Install uv in system Python
pip install uv

# Use uv to install dependencies in system Python
uv pip install --system -r dev_requirements.txt

# Install pre-commit hooks
uv pip install --system pre-commit
pre-commit install
