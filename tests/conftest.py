import os

import pytest

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="user", type=str)


@pytest.fixture(scope="class")
def dbt_profile_target(request):
    profile = request.config.getoption("--profile")

    if profile == "ci_sql_server":
        return _profile_ci_sql_server()
    if profile == "ci_sql_server_encrypt":
        return _profile_ci_sql_server_encrypt()
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

    raise ValueError(f"Unknown profile: {profile}")


def _all_profiles_base():
    return {
        "type": "sqlserver",
        "threads": 1,
        "driver": "ODBC Driver 17 for SQL Server",
        "port": 1433,
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
        },
    }


def _profile_ci_sql_server_encrypt():
    return {
        **_profile_ci_sql_server(),
        **{
            "encrypt": True,
            "trust_cert": True,
        },
    }


def _profile_user():
    return {
        **_all_profiles_base(),
        **{
            "driver": os.getenv("SQLSERVER_TEST_DRIVER"),
            "host": os.getenv("SQLSERVER_TEST_HOST"),
            "port": int(os.getenv("SQLSERVER_TEST_PORT")),
            "user": os.getenv("SQLSERVER_TEST_USER"),
            "pass": os.getenv("SQLSERVER_TEST_PASS"),
            "database": os.getenv("SQLSERVER_TEST_DBNAME"),
        },
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip("Skipped on '{profile_type}' profile")
