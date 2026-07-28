.DEFAULT_GOAL:=help
THREADS ?= auto

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@\
	uv sync --all-extras && uv run pre-commit install

.PHONY: ty
ty: ## Runs ty over the adapter package for static type checking.
	@\
	uv run pre-commit run --hook-stage manual ty

.PHONY: ruff
ruff: ## Runs ruff against staged changes to enforce style guide.
	@\
	uv run pre-commit run --hook-stage manual ruff-check-manual

.PHONY: format
format: ## Runs ruff format against staged changes to enforce style guide.
	@\
	uv run pre-commit run --hook-stage manual ruff-format-check -v

.PHONY: lint
lint: ## Runs ruff and ty code checks against staged changes.
	@status=0; \
	uv run pre-commit run ruff-check-manual --hook-stage manual || status=1; \
	uv run pre-commit run ty --hook-stage manual || status=1; \
	exit $$status

.PHONY: all
all: ## Runs all checks against staged changes.
	@\
	uv run pre-commit run -a

.PHONY: unit
unit: ## Runs unit tests.
	@\
	uv run pytest -n auto -ra -v tests/unit

.PHONY: functional
functional: ## Runs functional tests.
	@\
	uv run pytest -n $(THREADS) -ra -v tests/functional

.PHONY: test
test: ## Runs unit tests and code checks against staged changes.
	@status=0; \
	uv run pytest -n auto -ra -v tests/unit || status=1; \
	uv run pre-commit run ruff-format-check --hook-stage manual || status=1; \
	uv run pre-commit run ruff-check-manual --hook-stage manual || status=1; \
	uv run pre-commit run ty --hook-stage manual || status=1; \
	exit $$status

.PHONY: server
server: ## Spins up a local MS SQL Server instance for development. Docker-compose is required.
	@\
	docker compose up -d

.PHONY: clean
clean: ## Removes ignored files and build artifacts from the repo.
	@echo "cleaning repo"
	@git clean -f -X

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@grep -E '^[7+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
