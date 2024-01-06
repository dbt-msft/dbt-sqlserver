import os

import pytest
from _pytest.fixtures import FixtureRequest

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption(
        "--profile",
        action="store",
        default=os.getenv("PROFILE_NAME", "dev"),
        type=str,
    )


@pytest.fixture(scope="class")
def dbt_profile_target(request: FixtureRequest, dbt_profile_target_update):
    profile = request.config.getoption("--profile")

    target = {
        "type": "sqlserver",
        "driver": "ODBC Driver 18 for SQL Server",
        "port": 1433,
        "retries": 2,
        "host": "localhost",
        "user": "sa",
        "pass": "L0calTesting!",
        "database": "dbt",
        "encrypt": True,
        "trust_cert": True,
    }

    target.update(dbt_profile_target_update)
    return target


@pytest.fixture(scope="class")
def dbt_profile_target_update():
    return {}


@pytest.fixture(autouse=True)
def skip_by_profile_type(request: FixtureRequest):
    profile_type = request.config.getoption("--profile")

    if request.node.get_closest_marker("skip_profile"):
        if profile_type in request.node.get_closest_marker("skip_profile").args:
            pytest.skip(f"Skipped on '{profile_type}' profile")

    if request.node.get_closest_marker("only_with_profile"):
        if (
            profile_type
            not in request.node.get_closest_marker("only_with_profile").args
        ):
            pytest.skip(f"Skipped on '{profile_type}' profile")
