cp test.env.sample test.env

docker compose build
docker compose up -d

# Install uv in system Python
pip install uv

# Use uv to install dependencies in system Python
uv pip install --system -r dev_requirements.txt

# Install pre-commit hooks
uv pip install --system pre-commit
pre-commit install
