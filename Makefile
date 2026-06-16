.DEFAULT_GOAL:=help
THREADS ?= auto

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@\
	uv pip install -e . --group dev && pre-commit install

.PHONY: mypy
mypy: ## Runs mypy against staged changes for static type checking.
	@\
	pre-commit run --hook-stage manual mypy-check | grep -v "INFO"

.PHONY: ruff
ruff: ## Runs ruff against staged changes to enforce style guide.
	@\
	pre-commit run --hook-stage manual ruff-check-manual | grep -v "INFO"

.PHONY: black
black: ## Runs black  against staged changes to enforce style guide.
	@\
	pre-commit run --hook-stage manual black-check -v | grep -v "INFO"

.PHONY: lint
lint: ## Runs ruff and mypy code checks against staged changes.
	@\
	pre-commit run ruff-check-manual --hook-stage manual | grep -v "INFO"; \
	pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: all
all: ## Runs all checks against staged changes.
	@\
	pre-commit run -a

.PHONY: unit
unit: ## Runs unit tests.
	@\
	pytest -n auto -ra -v tests/unit

.PHONY: functional
functional: ## Runs functional tests.
	@\
	pytest -n $(THREADS) -ra -v tests/functional

.PHONY: test
test: ## Runs unit tests and code checks against staged changes.
	@\
	pytest -n auto -ra -v tests/unit; \
	pre-commit run black-check --hook-stage manual | grep -v "INFO"; \
	pre-commit run ruff-check-manual --hook-stage manual | grep -v "INFO"; \
	pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

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
