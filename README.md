# dbt-sqlserver

[dbt](https://www.getdbt.com) adapter for Microsoft SQL Server and Azure SQL services.

The adapter supports dbt-core 1.10 or newer and follows the same versioning scheme.
E.g. version 1.10.x of the adapter will be compatible with dbt-core 1.10.x.

## Supported SQL Server versions

The adapter is tested against the following SQL Server versions:

| SQL Server version | Supported |
|---|---|
| SQL Server 2017 | ✅ (minimum supported version) |
| SQL Server 2019 | ✅ |
| SQL Server 2022 | ✅ |
| SQL Server 2025 | ✅ |
| Azure SQL Database | ✅ |
| Azure SQL Managed Instance | ✅ |

The minimum supported SQL Server version is SQL Server 2017; older versions are not supported.
## Supported SQL Server versions

SQL Server 2017, 2019, 2022, and 2025 are covered by the integration test suite. Azure SQL Database and Azure SQL Managed Instance are not covered by the integration test suite, but are expected to be compatible.

## Documentation

We've bundled all documentation on the dbt docs site:

- [Profile setup & authentication](https://docs.getdbt.com/reference/warehouse-profiles/mssql-profile)
- [Adapter documentation, usage and important notes](https://docs.getdbt.com/reference/resource-configs/mssql-configs)

Join us on the [dbt Slack](https://getdbt.slack.com/archives/CMRMDDQ9W) to ask questions, get help, or to discuss the project.

## Installation

The default install uses the `pyodbc` backend and includes the `pyodbc` dependency. If you want the optional `mssql-python` backend instead, install the `mssql` extra.

Latest version: ![PyPI](https://img.shields.io/pypi/v/dbt-sqlserver?label=latest%20stable&logo=pypi)  
Latest pre-release: ![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/dbt-msft/dbt-sqlserver?include_prereleases&label=latest%20pre-release&logo=pypi)


### Backend requirements at a glance

| Backend | Python package | Debian/Ubuntu system packages |
|---|---|---|
| `pyodbc` | `dbt-sqlserver[pyodbc]` or `pyodbc` | `unixodbc-dev` plus the Microsoft ODBC Driver for SQL Server |
| `mssql-python` | `dbt-sqlserver[mssql]` or `mssql-python` | `libltdl7`, `libkrb5-3`, `libgssapi-krb5-2` |


### `pyodbc` backend

The legacy and currently default ODBC path uses `pyodbc` and the Microsoft ODBC driver.

```shell
pip install -U dbt-sqlserver
```

You should migrate to using an explicit extra in preparation for deprecation; the following is equivalent:

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
      backend: mssql-python  # <-- enables this backend
```

## Changelog

See [the changelog](CHANGELOG.md)

## Configuration

### `dbt_sqlserver_use_default_schema_concat`

*(default: `false`)* Controls schema name generation when a [custom schema](https://docs.getdbt.com/docs/build/custom-schemas) is set on a model.

| Flag value | `custom_schema_name` | Result |
|---|---|---|
| `false` (default, legacy) | *(none)* | `target.schema` |
| `false` (default, legacy) | `"reporting"` | `reporting` |
| `true` (dbt-core standard) | *(none)* | `target.schema` |
| `true` (dbt-core standard) | `"reporting"` | `target.schema_reporting` |

When `false` (the default), the adapter uses its legacy behaviour: `custom_schema_name` is used **as-is** without being prefixed by `target.schema`.  
When `true`, the adapter delegates to dbt-core's `default__generate_schema_name`, which concatenates `target.schema` + `_` + `custom_schema_name`.

**Example usage in `dbt_project.yml`:**

```yaml
flags:
  dbt_sqlserver_use_default_schema_concat: true  # Enable standard schema concatenation
```

The same setting is also honoured via `vars:` for backwards compatibility; the behavior flag under `flags:` takes precedence when both are set.

> **Note:** If you want to permanently customise schema generation and avoid any future changes, override the `sqlserver__generate_schema_name` macro directly in your project instead.

### `backend`

*(default: `pyodbc`)* Set to `mssql-python` in a profile target to use the `mssql-python` backend instead of `pyodbc`. The adapter fails if the required backend package (Python dependency), such as `pyodbc` or `mssql-python`, is not installed.

### `dbt_sqlserver_use_dbt_transactions`

_(default: `false`)_ When enabled, makes dbt's transaction hooks real at the SQL Server level by emitting `BEGIN TRANSACTION` / `COMMIT TRANSACTION` through the adapter's `add_begin_query` and `add_commit_query` methods. 

The default is `false`, preserving existing behavior where `begin`/`commit` hooks are logical no-ops and the ODBC driver auto-commits each statement. When `dbt_sqlserver_use_dbt_transactions: true`, the adapter emits real T-SQL transaction statements, and rollback uses `IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION`.

The driver connection remains in autocommit mode (`autocommit=true`) in both modes.

This mode is opt-in and should be tested carefully with project-specific materializations and hooks.

```yaml
# dbt_project.yml
flags:
    dbt_sqlserver_use_dbt_transactions: true # <-- opt-in; default is false
```

**Compatibility notes:** Enabling `dbt_sqlserver_use_dbt_transactions: true` may expose transaction-state assumptions hidden by autocommit-only mode. Explicit transaction macros may interact with dbt-managed transactions, and cleanup after failed DDL/DML may differ. Review pre/post hooks for in-transaction vs out-of-transaction semantics.

### `as_columnstore`

*(default: `true`)* When building a table, the adapter creates a [clustered columnstore index](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-overview) (CCI) on it. Set `as_columnstore: false` to build a plain rowstore table instead.

This matters for any table containing a `(n)varchar(max)` or other LOB column, because SQL Server does not allow those data types to participate in a columnstore index. The table build fails with:

> Column '...' has a data type that cannot participate in a columnstore index.

A common case is dbt's [test failure storage](https://docs.getdbt.com/reference/resource-configs/store_failures): the audit tables can contain `VARCHAR(MAX)` columns (dbt's `STRING` type maps to `VARCHAR(MAX)`), so disable the CCI on those resources:

```yaml
# dbt_project.yml
data_tests:
  +store_failures: true
  +as_columnstore: false  # avoids CCI on (n)varchar(max) audit columns
```

You can also set it per model:

```sql
{{ config(materialized="table", as_columnstore=false) }}
```

## Contributing

[![Unit tests](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/unit-tests.yml)
[![Integration tests on SQL Server](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-sqlserver.yml/badge.svg)](https://github.com/dbt-msft/dbt-sqlserver/actions/workflows/integration-tests-sqlserver.yml)

This adapter is community-maintained.
You are welcome to contribute by creating issues, opening or reviewing pull requests, or helping other users in the Slack channel.
If you're unsure how to get started, check out our [contributing guide](CONTRIBUTING.md).

## License

[![PyPI - License](https://img.shields.io/pypi/l/dbt-sqlserver)](https://github.com/dbt-msft/dbt-sqlserver/blob/master/LICENSE)

## Code of Conduct

This project and everyone involved is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
