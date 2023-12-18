# Development of the adapter

## CI/CD

We use Docker images that have all the things we need to test the adapter in the CI/CD workflows.
The Dockerfile is located in the *devops* directory and pushed to GitHub Packages to this repo.
There is one tag per supported Python version.

All CI/CD pipelines are using GitHub Actions. The following pipelines are available:

* `publish-docker`: publishes the image we use in all other pipelines.
* `unit-tests`: runs the unit tests for each supported Python version.
* `integration-tests-azure`: runs the integration tests for Azure SQL Server.
* `integration-tests-fabric`: runs the integration tests for SQL Server.
* `release-version`: publishes the adapter to PyPI.

There is an additional [Pre-commit](https://pre-commit.ci/) pipeline that validates the code style.

### Azure integration tests

The following environment variables are available:

* `DBT_AZURESQL_SERVER`: full hostname of the server hosting the Azure SQL database
* `DBT_AZURESQL_DB`: name of the Azure SQL database
* `DBT_AZURESQL_UID`: username of the SQL admin on the server hosting the Azure SQL database
* `DBT_AZURESQL_PWD`: password of the SQL admin on the server hosting the Azure SQL database
* `DBT_AZURE_TENANT`: Azure tenant ID
* `DBT_AZURE_SUBSCRIPTION_ID`: Azure subscription ID
* `DBT_AZURE_RESOURCE_GROUP_NAME`: Azure resource group name
* `DBT_AZURE_SP_NAME`: Client/application ID of the service principal used to connect to Azure AD
* `DBT_AZURE_SP_SECRET`: Password of the service principal used to connect to Azure AD

## Releasing a new version

Make sure the version number is bumped in `__version__.py`. Then, create a git tag named `v<version>` and push it to GitHub.
A GitHub Actions workflow will be triggered to build the package and push it to PyPI.

If you're releasing support for a new version of `dbt-core`, also bump the `dbt_version` in `setup.py`.
