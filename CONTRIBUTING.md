# Development of the adapter

Python 3.9 is used for developing the adapter. To get started, setup your environment as follows:

Create a virtual environment, pyenv is used in the example:

```shell
pyenv install 3.9.12
pyenv virtualenv 3.9.12 dbt-sqlserver
pyenv activate dbt-sqlserver
```

Install the development dependencies:

```shell
pip install -r devrequirements.txt
pip install -e .
```

## Testing

### pytest-dbt-adapter

The package [pytest-dbt-adapter](https://github.com/dbt-labs/dbt-adapter-tests) is used for running tests against the adapter.
However, this is no longer the recommended way to test adapters and we should into replacing this with [the recommended way to test new adapter](https://docs.getdbt.com/docs/contributing/testing-a-new-adapter)

### Tox

Running the unit tests:

```shell
tox -- -v test/unit
```

## CI/CD

We use Docker image that has all the things we need to run the adapter. The Dockerfile is located in the *.github* directory and pushed to GitHub Packages to this repo.
To update the image, push a new tag that starts with `docker-` (e.g. `docker-2022052301`). The image is built and pushed using GitHub actions.

There is a Circle CI workflow with jobs that run the following tasks:

* Run the unit tests
* Use the adapter to connect to a SQL Server Docker container
* Run the pytest-dbt-adapter specs against a SQL Server Docker container
* Use the adapter to connect to an Azure SQL Database with various options
