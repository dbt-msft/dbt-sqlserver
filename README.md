# dbt-sqlserver

[dbt](https://www.getdbt.com) adapter for Microsoft SQL Server and Azure SQL services.

The adapter supports dbt-core 0.14 or newer and follows the same versioning scheme.
E.g. version 1.1.x of the adapter will be compatible with dbt-core 1.1.x.

## Documentation

We've bundled all documentation on the dbt docs site:

* [Profile setup & authentication](https://docs.getdbt.com/reference/warehouse-profiles/mssql-profile)
* [Adapter documentation, usage and important notes](https://docs.getdbt.com/reference/resource-configs/mssql-configs)

Join us on the [dbt Slack](https://getdbt.slack.com/archives/CMRMDDQ9W) to ask questions, get help, or to discuss the project.

## Installation

Latest version: ![PyPI](https://img.shields.io/pypi/v/dbt-sqlserver?label=latest%20stable&logo=pypi)

```shell
pip install -U dbt-sqlserver
```

Latest pre-release: ![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/dbt-msft/dbt-sqlserver?include_prereleases&label=latest%20pre-release&logo=pypi)

```shell
pip install -U --pre dbt-sqlserver
```

## Profile Setup

The adapter now uses the native `mssql-python` driver by default, eliminating the need for ODBC driver installation.

### SQL Server (SQL Authentication)

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: "mssql-python"
      host: 127.0.0.1
      port: 1433
      user: my_user
      password: "my_password"
      database: my_database
      schema: dbo
      authentication: sql
      encrypt: true
      trust_cert: true
```

### Azure SQL (CLI Authentication)

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: "mssql-python"
      host: your-server.database.windows.net
      database: your-db
      schema: dbo
      authentication: cli
      encrypt: true
      trust_cert: true
```

### Azure SQL (Service Principal)

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: sqlserver
      driver: "mssql-python"
      host: your-server.database.windows.net
      database: your-db
      schema: dbo
      authentication: serviceprincipal
      tenant_id: "your-tenant-id"
      client_id: "your-client-id"
      client_secret: "your-client-secret"
      encrypt: true
      trust_cert: true
```

### Profile Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `type` | yes | — | Must be `sqlserver` |
| `driver` | yes | — | Set to `mssql-python` (recommended) or an ODBC driver name for legacy pyodbc |
| `host` | yes | — | SQL Server hostname or IP |
| `port` | no | `1433` | SQL Server port |
| `user` | yes* | — | Username (*not required for CLI/auto auth) |
| `password` | yes* | — | Password (*not required for CLI/auto auth) |
| `database` | yes | — | Target database |
| `schema` | yes | — | Target schema |
| `authentication` | no | `sql` | Auth method: `sql`, `cli`, `auto`, `environment`, `serviceprincipal`, `msi` |
| `encrypt` | no | `true` | Encrypt the connection |
| `trust_cert` | no | `false` | Trust the server certificate |
| `driver_type` | no | `mssql-python` | Driver backend: `mssql-python` (default) or `pyodbc` |

## High-Concurrency Mode (mssql-python)

When using `driver: "mssql-python"`, the adapter enables **The Sentinel** — a load-aware admission controller that prevents SQL Server CPU exhaustion during high-thread dbt runs.

Before each query, the Sentinel checks `sys.dm_os_schedulers` (a lightweight, in-memory DMV with `NOLOCK` — executes in <10 ms). If the number of runnable tasks exceeds a threshold (default: 4), new queries are held back with a fixed-interval back-off loop until the server recovers. A safety cap of 10 retries ensures dbt never deadlocks itself.

This is fully automatic — no configuration needed. It works transparently with `dbt run --threads 8` or higher, keeping SQL Server responsive under heavy concurrency.

## Changelog

See [the changelog](CHANGELOG.md)

## Contributing

[![Unit tests](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/unit-tests.yml)
[![Integration tests on SQL Server](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-sqlserver.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-sqlserver.yml)
[![Integration tests on Azure](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-azure.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-azure.yml)

This adapter is community-maintained.
You are welcome to contribute by creating issues, opening or reviewing pull requests or helping other users in Slack channel.
If you're unsure how to get started, check out our [contributing guide](CONTRIBUTING.md).

## License

[![PyPI - License](https://img.shields.io/pypi/l/dbt-sqlserver)](https://github.com/dbt-msft/dbt-sqlserver/blob/master/LICENSE)

## Code of Conduct

This project and everyone involved is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
