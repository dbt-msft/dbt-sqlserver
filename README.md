# dbt-fabric

[dbt](https://www.getdbt.com) adapter for Microsoft Fabric Synapse Data Warehouse.

The adapter supports dbt-core 1.4 or newer and follows the same versioning scheme.
E.g. version 1.1.x of the adapter will be compatible with dbt-core 1.1.x.

## Documentation

We've bundled all documentation on the dbt docs site
* [Profile setup & authentication](https://docs.getdbt.com/docs/core/connect-data-platform/fabric-setup)
* [Adapter documentation, usage and important notes](https://docs.getdbt.com/reference/resource-configs/fabric-configs)

## Installation

This adapter requires the Microsoft ODBC driver to be installed:
[Windows](https://docs.microsoft.com/nl-be/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#download-for-windows) |
[macOS](https://docs.microsoft.com/nl-be/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16) |
[Linux](https://docs.microsoft.com/nl-be/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16)

<details><summary>Debian/Ubuntu</summary>
<p>

Make sure to install the ODBC headers as well as the driver linked above:

```shell
sudo apt-get install -y unixodbc-dev
```

</p>
</details>

Latest version: ![PyPI](https://img.shields.io/pypi/v/dbt-fabric?label=latest&logo=pypi)

```shell
pip install -U dbt-fabric
```

## Changelog

See [the changelog](CHANGELOG.md)

## Contributing

[![Unit tests](https://github.com/microsoft/dbt-fabric/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/microsoft/dbt-fabric/actions/workflows/unit-tests.yml)
[![Integration tests on Azure](https://github.com/microsoft/dbt-fabric/actions/workflows/integration-tests-azure.yml/badge.svg)](https://github.com/microsoft/dbt-fabric/actions/workflows/integration-tests-azure.yml)
[![Publish Docker images for CI/CD](https://github.com/microsoft/dbt-fabric/actions/workflows/publish-docker.yml/badge.svg)](https://github.com/microsoft/dbt-fabric/actions/workflows/publish-docker.yml)

This adapter is Microsoft-maintained.
You are welcome to contribute by creating issues, opening or reviewing pull requests.
If you're unsure how to get started, check out our [contributing guide](CONTRIBUTING.md).

## License

[![PyPI - License](https://img.shields.io/pypi/l/dbt-fabric)](https://github.com/microsoft/dbt-fabric/blob/main/LICENSE)

## Code of Conduct

This project and everyone involved is expected to follow the [Microsoft Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
