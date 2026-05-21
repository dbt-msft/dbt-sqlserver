# dbt-sqlserver

[dbt](https://www.getdbt.com) adapter for Microsoft SQL Server and Azure SQL services.

The adapter supports dbt-core 0.14 or newer and follows the same versioning scheme.
E.g. version 1.1.x of the adapter will be compatible with dbt-core 1.1.x.

## Documentation

We've bundled all documentation on the dbt docs site:

- [Profile setup & authentication](https://docs.getdbt.com/reference/warehouse-profiles/mssql-profile)
- [Adapter documentation, usage and important notes](https://docs.getdbt.com/reference/resource-configs/mssql-configs)

Join us on the [dbt Slack](https://getdbt.slack.com/archives/CMRMDDQ9W) to ask questions, get help, or to discuss the project.

## Installation

The base package does not bundle any connection driver. Install the adapter together with the backend extra that matches your setup.

Latest version: ![PyPI](https://img.shields.io/pypi/v/dbt-sqlserver?label=latest%20stable&logo=pypi)  
Latest pre-release: ![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/dbt-msft/dbt-sqlserver?include_prereleases&label=latest%20pre-release&logo=pypi)

### `pyodbc` backend

The legacy and currently default ODBC path uses `pyodbc` and the Microsoft ODBC driver.

```shell
pip install -U "dbt-sqlserver[pyodbc]"
```

You also need the Microsoft ODBC driver for SQL Server installed on your system:
[Windows](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#download-for-windows) |
[macOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16) |
[Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-sql-server?view=sql-server-ver16)

<details><summary>Debian/Ubuntu</summary>

Install the ODBC headers as well as the driver linked above:

```shell
sudo apt-get install -y unixodbc-dev
```

</details>

### `mssql-python` backend

An alternative backend that does not require the ODBC driver.

```shell
pip install -U "dbt-sqlserver[mssql]"
```

On Debian/Ubuntu-based systems, `mssql-python` requires these system libraries:

```shell
sudo apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2
```

Enable it per target in your `profiles.yml`:

```yaml
your_profile:
  target: dev
  outputs:
    dev:
      type: sqlserver
      host: your-server
      port: 1433
      database: your-database
      schema: dbo
      user: your-user
      password: your-password
      encrypt: true
      trust_cert: false
      use_mssql_python: true  # <-- enables this backend
```

## Changelog

See [the changelog](CHANGELOG.md)

## Configuration

### `use_mssql_python`

*(default: `false`)* Set to `true` in a profile target to use the `mssql-python` backend instead of `pyodbc`. The adapter fails if the required driver is not installed.

### `dbt_sqlserver_use_default_schema_concat`

*(default: `false`)* Controls schema name generation when a [custom schema](https://docs.getdbt.com/docs/build/custom-schemas) is set on a model.

| Value | `custom_schema_name` | Result |
|---|---|---|
| `false` (default) | *(none)* | `target.schema` |
| `false` (default) | `"reporting"` | `reporting` |
| `true` | *(none)* | `target.schema` |
| `true` | `"reporting"` | `target.schema_reporting` |

When `false`, `custom_schema_name` is used as-is without being prefixed by `target.schema`.  
When `true`, the adapter delegates to dbt-core's `default__generate_schema_name`.

```yaml
# dbt_project.yml
vars:
  dbt_sqlserver_use_default_schema_concat: true
```

> **Note:** To permanently customise schema generation without a flag dependency, override the `sqlserver__generate_schema_name` macro directly in your project.

## Contributing

[![Unit tests](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/unit-tests.yml)
[![Integration tests on SQL Server](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-sqlserver.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-sqlserver.yml)
[![Integration tests on Azure](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-azure.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-azure.yml)

This adapter is community-maintained.
You are welcome to contribute by creating issues, opening or reviewing pull requests, or helping other users in the Slack channel.
If you're unsure how to get started, check out our [contributing guide](CONTRIBUTING.md).

## License

[![PyPI - License](https://img.shields.io/pypi/l/dbt-sqlserver)](https://github.com/dbt-msft/dbt-sqlserver/blob/master/LICENSE)

## Code of Conduct

This project and everyone involved is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
