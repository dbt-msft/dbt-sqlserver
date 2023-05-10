import os

import pytest
from _pytest.fixtures import FixtureRequest

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="user", type=str)


@pytest.fixture(scope="class")
def dbt_profile_target(request: FixtureRequest):
    profile = request.config.getoption("--profile")

    if profile == "ci_sql_server":
        return _profile_ci_sql_server()
    if profile == "ci_azure_cli":
        return _profile_ci_azure_cli()
    if profile == "ci_azure_auto":
        return _profile_ci_azure_auto()
    if profile == "ci_azure_environment":
        return _profile_ci_azure_environment()
    if profile == "ci_azure_basic":
        return _profile_ci_azure_basic()
    if profile == "user":
        return _profile_user()
    if profile == "user_azure":
        return _profile_user_azure()

    raise ValueError(f"Unknown profile: {profile}")


def _all_profiles_base():
    return {
        "type": "sqlserver",
        "driver": os.getenv("SQLSERVER_TEST_DRIVER", "ODBC Driver 18 for SQL Server"),
        "port": int(os.getenv("SQLSERVER_TEST_PORT", "1433")),
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


def _profile_ci_azure_basic():
    return {
        **_profile_ci_azure_base(),
        **{
            "user": os.getenv("DBT_AZURESQL_UID"),
            "pass": os.getenv("DBT_AZURESQL_PWD"),
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


def _profile_ci_sql_server():
    return {
        **_all_profiles_base(),
        **{
            "host": "sqlserver",
            "user": "SA",
            "pass": "5atyaNadella",
            "database": "msdb",
            "encrypt": True,
            "trust_cert": True,
        },
    }


def _profile_user():
    return {
        **_all_profiles_base(),
        **{
            "host": os.getenv("SQLSERVER_TEST_HOST"),
            "user": os.getenv("SQLSERVER_TEST_USER"),
            "pass": os.getenv("SQLSERVER_TEST_PASS"),
            "database": os.getenv("SQLSERVER_TEST_DBNAME"),
            "encrypt": bool(os.getenv("SQLSERVER_TEST_ENCRYPT", "False")),
            "trust_cert": bool(os.getenv("SQLSERVER_TEST_TRUST_CERT", "False")),
        },
    }


def _profile_user_azure():
    return {
        **_all_profiles_base(),
        **{
            "host": os.getenv("SQLSERVER_TEST_HOST"),
            "authentication": "auto",
            "encrypt": True,
            "trust_cert": True,
            "database": os.getenv("SQLSERVER_TEST_DBNAME"),
        },
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request: FixtureRequest):
    profile_type = request.config.getoption("--profile")

    if request.node.get_closest_marker("skip_profile"):
        if profile_type in request.node.get_closest_marker("skip_profile").args:
            pytest.skip(f"Skipped on '{profile_type}' profile")

    if request.node.get_closest_marker("only_with_profile"):
        if profile_type not in request.node.get_closest_marker("only_with_profile").args:
            pytest.skip(f"Skipped on '{profile_type}' profile")
