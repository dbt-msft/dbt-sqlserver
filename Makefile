.DEFAULT_GOAL:=help
THREADS ?= auto
MSSQL_VERSION ?= 2022
PODMAN_IMAGE ?= dbt-sqlserver-mssql:$(MSSQL_VERSION)
PODMAN_CONTAINER ?= dbt-sqlserver-mssql

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

# Podman equivalents of `server`, for anyone who would rather not run Docker
# Desktop. They build the same devops/server.Dockerfile and pass the same
# environment docker-compose.yml does, so test.env works unchanged.
.PHONY: server-podman
server-podman: ## Spins up the same SQL Server instance under rootless podman.
	@\
	podman build -t $(PODMAN_IMAGE) --build-arg MSSQL_VERSION=$(MSSQL_VERSION) \
		-f devops/server.Dockerfile devops && \
	podman rm -f $(PODMAN_CONTAINER) >/dev/null 2>&1 || true; \
	podman run -d --name $(PODMAN_CONTAINER) \
		-e ACCEPT_EULA=Y \
		-e SA_PASSWORD='L0calTesting!' \
		-e COLLATION='SQL_Latin1_General_CP1_CS_AS' \
		--env-file test.env \
		-p 1433:1433 \
		$(PODMAN_IMAGE)

.PHONY: server-podman-stop
server-podman-stop: ## Removes the podman SQL Server instance.
	@\
	podman rm -f $(PODMAN_CONTAINER)

.PHONY: server-podman-logs
server-podman-logs: ## Tails the podman SQL Server logs (init completes on "user creation completed").
	@\
	podman logs -f $(PODMAN_CONTAINER)

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
