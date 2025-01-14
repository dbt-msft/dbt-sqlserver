import pytest
from azure.identity import AzureCliCredential

from dbt.adapters.sqlserver.sqlserver_connections import (  # byte_array_to_datetime,
    bool_to_connection_string_arg,
    get_pyodbc_attrs_before_credentials,
)
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials

# See
# https://github.com/Azure/azure-sdk-for-python/blob/azure-identity_1.5.0/sdk/identity/azure-identity/tests/test_cli_credential.py
CHECK_OUTPUT = AzureCliCredential.__module__ + ".subprocess.check_output"


@pytest.fixture
def credentials() -> SQLServerCredentials:
    credentials = SQLServerCredentials(
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )
    return credentials


def test_get_pyodbc_attrs_before_empty_dict_when_service_principal(
    credentials: SQLServerCredentials,
) -> None:
    """
    When the authentication is set to sql we expect an empty attrs before.
    """
    attrs_before = get_pyodbc_attrs_before_credentials(credentials)
    assert attrs_before == {}


@pytest.mark.parametrize(
    "key, value, expected",
    [("somekey", False, "somekey=No"), ("somekey", True, "somekey=Yes")],
)
def test_bool_to_connection_string_arg(key: str, value: bool, expected: str) -> None:
    assert bool_to_connection_string_arg(key, value) == expected
