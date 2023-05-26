import os

import pytest
from _pytest.fixtures import FixtureRequest

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption(
        "--profile",
        action="store",
        default=os.getenv("PROFILE_NAME", "user_azure"),
        type=str,
    )


@pytest.fixture(scope="class")
def dbt_profile_target(request: FixtureRequest, dbt_profile_target_update):
    profile = request.config.getoption("--profile")

    if profile == "ci_azure_cli":
        target = _profile_ci_azure_cli()
    elif profile == "ci_azure_auto":
        target = _profile_ci_azure_auto()
    elif profile == "ci_azure_environment":
        target = _profile_ci_azure_environment()
    elif profile == "user_azure":
        target = _profile_user_azure()
    else:
        raise ValueError(f"Unknown profile: {profile}")

    target.update(dbt_profile_target_update)
    return target


@pytest.fixture(scope="class")
def dbt_profile_target_update():
    return {}


def _all_profiles_base():
    return {
        "type": "fabric",
        "driver": os.getenv("FABRIC_TEST_DRIVER", "ODBC Driver 18 for SQL Server"),
        "port": int(os.getenv("FABRIC_TEST_PORT", "1433")),
        "retries": 2,
    }


def _profile_ci_azure_base():
    return {
        **_all_profiles_base(),
        **{
            "host": os.getenv("DBT_AZURESQL_SERVER"),
            "database": os.getenv("DBT_AZURESQL_DB"),
            "encrypt": True,
            "trust_cert": True,
        },
    }


def _profile_ci_azure_cli():
    return {
        **_profile_ci_azure_base(),
        **{
            "authentication": "CLI",
        },
    }


def _profile_ci_azure_auto():
    return {
        **_profile_ci_azure_base(),
        **{
            "authentication": "auto",
        },
    }


def _profile_ci_azure_environment():
    return {
        **_profile_ci_azure_base(),
        **{
            "authentication": "environment",
        },
    }


def _profile_user_azure():
    profile = {
        **_all_profiles_base(),
        **{
            "host": os.getenv("FABRIC_TEST_HOST"),
            "authentication": os.getenv("FABRIC_TEST_AUTH", "auto"),
            "encrypt": True,
            "trust_cert": True,
            "database": os.getenv("FABRIC_TEST_DBNAME"),
            "client_id": os.getenv("FABRIC_TEST_CLIENT_ID"),
            "client_secret": os.getenv("FABRIC_TEST_CLIENT_SECRET"),
            "tenant_id": os.getenv("FABRIC_TEST_TENANT_ID"),
        },
    }
    return profile


@pytest.fixture(autouse=True)
def skip_by_profile_type(request: FixtureRequest):
    profile_type = request.config.getoption("--profile")

    if request.node.get_closest_marker("skip_profile"):
        if profile_type in request.node.get_closest_marker("skip_profile").args:
            pytest.skip(f"Skipped on '{profile_type}' profile")

    if request.node.get_closest_marker("only_with_profile"):
        if profile_type not in request.node.get_closest_marker("only_with_profile").args:
            pytest.skip(f"Skipped on '{profile_type}' profile")
