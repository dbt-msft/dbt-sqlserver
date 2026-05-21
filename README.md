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

By default this adapter uses the Microsoft ODBC driver.

This adapter requires the Microsoft ODBC driver to be installed:
[Windows](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#download-for-windows) |
[macOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16) |
[Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-sql-server?view=sql-server-ver16)

<details><summary>Debian/Ubuntu</summary>
<p>

Make sure to install the ODBC headers as well as the driver linked above:

```shell
sudo apt-get install -y unixodbc-dev
```

</p>
</details>

Latest version: ![PyPI](https://img.shields.io/pypi/v/dbt-sqlserver?label=latest%20stable&logo=pypi)

```shell
pip install -U dbt-sqlserver
```

Latest pre-release: ![GitHub tag (latest SemVer pre-release)](https://img.shields.io/github/v/tag/dbt-msft/dbt-sqlserver?include_prereleases&label=latest%20pre-release&logo=pypi)

```shell
pip install -U --pre dbt-sqlserver
```

### Optional: `mssql-python` backend

This adapter can also use the `mssql-python` driver behind a feature flag.

Install it explicitly when you want to use that backend:

```shell
pip install -U mssql-python
```

When this backend is enabled, the adapter does not require the ODBC driver-based connection path for that profile.

## Changelog

See [the changelog](CHANGELOG.md)

## Configuration

### Flags

- `dbt_sqlserver_use_default_schema_concat`: *(default: `false`)* Controls schema name generation when a [custom schema](https://docs.getdbt.com/docs/build/custom-schemas) is set on a model.

- `use_mssql_python`: *(default: `false` in the profile)* Switches the connection backend from the legacy ODBC / `pyodbc` path to the `mssql-python` driver for that target profile.

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
  vars:
    dbt_sqlserver_use_default_schema_concat: true  # Enable standard schema concatenation
  ```

  > **Note:** If you want to permanently customise schema generation and avoid any future deprecation of this flag, override the `sqlserver__generate_schema_name` macro directly in your project.

### `mssql-python` feature flag usage

Enable the backend per target in your `profiles.yml`:

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
      use_mssql_python: true
```

#### Notes

- `use_mssql_python: true` is a profile-level feature flag.
- When enabled, the adapter uses `mssql-python` instead of the legacy `pyodbc` connection path.
- The legacy ODBC driver setting is only needed for profiles that continue to use the ODBC backend.
- If you enable `use_mssql_python`, make sure the `mssql-python` package is installed in the environment running dbt.
- On Debian/Ubuntu-based environments, `mssql-python` also requires `libltdl7`, `libkrb5-3`, and `libgssapi-krb5-2`.
- This path is intended to fail fast when required dependencies or unsupported settings are missing.

#### Testing

For local development and validation, use the documented adapter workflow from `CONTRIBUTING.md`:

```shell
make dev
make server
cp test.env.sample test.env
make unit
make functional
```

To exercise the `mssql-python` backend in tests, configure the profile or environment so that the target under test sets:

```yaml
use_mssql_python: true
```

If you are testing in the devcontainer, the backend prerequisites are installed automatically. Outside the devcontainer, install `mssql-python` and the system libraries above before running the unit or functional suite.



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
