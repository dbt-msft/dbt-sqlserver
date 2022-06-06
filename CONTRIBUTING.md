# Development of the adapter

Python 3.10 is used for developing the adapter. To get started, bootstrap your environment as follows:

Create a virtual environment, [pyenv](https://github.com/pyenv/pyenv) is used in the example:

```shell
pyenv install 3.7.13
pyenv install 3.8.13
pyenv install 3.9.12
pyenv install 3.10.4
pyenv virtualenv 3.10.4 dbt-sqlserver
pyenv local dbt-sqlserver 3.9.12 3.8.13 3.7.13
```

Install the development dependencies and pre-commit and get information about possible make commands:

```shell
make dev
make help
```

[Pre-commit](https://pre-commit.com/) helps us to maintain a consistent style and code quality across the entire project.
After running `make dev`, pre-commit will automatically validate your commits and fix any formatting issues whenever possible.

## Testing

The functional tests require a running SQL Server instance. You can easily spin up a local instance with the following command:

```shell
make server
```

This will use Docker Compose to spin up a local instance of SQL Server. Docker Compose is now bundled with Docker, so make sure to [install the latest version of Docker](https://docs.docker.com/get-docker/).

Next, tell our tests how they should connect to the local instance by creating a file called `test.env` in the root of the project.
You can use the provided `test.env.sample` as a base and if you started the server with `make server`, then this matches the instance running on your local machine.

```shell
cp test.env.sample test.env
```

We use tox to isolate the environment for testing and for making it possible to test in multiple environments.
You can use the following commands to run the unit and the functional tests respectively:

```shell
make unit
make functional
```

This will start tox and test in all environments. If you only want to run the tests in a specific environment, you can use the following commands:

```shell
tox -e py310 -- -v tests/functional # will only functional run in Python 3.10
tox -e py39 -- -v tests/unit # will only run unit test in Python 3.9
```

## CI/CD

We use Docker images that have all the things we need to test the adapter in the CI/CD workflows.
The Dockerfile is located in the *.github* directory and pushed to GitHub Packages to this repo.
There is one tag per supported Python version.

All CI/CD pipelines are using GitHub Actions. The following pipelines are available:

* `publish-docker`: publishes the image we use in all other pipelines.
* `unit-tests`: runs the unit tests for each supported Python version.
* `integration-tests-azure`: runs the integration tests for Azure SQL Server.
* `integration-tests-sqlserver`: runs the integration tests for SQL Server.
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
* `DBT_AZURE_USERNAME`: Username of the user to connect to Azure AD
* `DBT_AZURE_PASSWORD`: Password of the user to connect to Azure AD

## Releasing a new version

Make sure the version number is bumped in `__version__.py`. Then, create a git tag named `v<version>` and push it to GitHub.
A GitHub Actions workflow will be triggered to build the package and push it to PyPI. 

If you're releasing support for a new version of `dbt-core`, also bump the `dbt_version` in `setup.py`.
