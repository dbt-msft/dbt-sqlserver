import datetime as dt
import json
from unittest import mock

import pytest
from azure.identity import AzureCliCredential

from dbt.adapters.sqlserver.sql_server_connection_manager import (
    bool_to_connection_string_arg,
    get_pyodbc_attrs_before,
)
from dbt.adapters.sqlserver.sql_server_credentials import SQLServerCredentials

# See
# https://github.com/Azure/azure-sdk-for-python/blob/azure-identity_1.5.0/sdk/identity/azure-identity/tests/test_cli_credential.py
CHECK_OUTPUT = AzureCliCredential.__module__ + ".subprocess.check_output"


@pytest.fixture
def credentials() -> SQLServerCredentials:
    credentials = SQLServerCredentials(
        driver="ODBC Driver 17 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )
    return credentials


@pytest.fixture
def mock_cli_access_token() -> str:
    access_token = "access token"
    expected_expires_on = 1602015811
    successful_output = json.dumps(
        {
            "expiresOn": dt.datetime.fromtimestamp(expected_expires_on).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
            "accessToken": access_token,
            "subscription": "some-guid",
            "tenant": "some-guid",
            "tokenType": "Bearer",
        }
    )
    return successful_output


def test_get_pyodbc_attrs_before_empty_dict_when_service_principal(
    credentials: SQLServerCredentials,
) -> None:
    """
    When the authentication is set to sql we expect an empty attrs before.
    """
    attrs_before = get_pyodbc_attrs_before(credentials)
    assert attrs_before == {}


@pytest.mark.parametrize("authentication", ["CLI", "cli", "cLi"])
def test_get_pyodbc_attrs_before_contains_access_token_key_for_cli_authentication(
    credentials: SQLServerCredentials,
    authentication: str,
    mock_cli_access_token: str,
) -> None:
    """
    When the cli authentication is used, the attrs before should contain an
    access token key.
    """
    credentials.authentication = authentication
    with mock.patch(CHECK_OUTPUT, mock.Mock(return_value=mock_cli_access_token)):
        attrs_before = get_pyodbc_attrs_before(credentials)
    assert 1256 in attrs_before.keys()


@pytest.mark.parametrize(
    "key, value, expected", [("somekey", False, "somekey=No"), ("somekey", True, "somekey=Yes")]
)
def test_bool_to_connection_string_arg(key: str, value: bool, expected: str) -> None:
    assert bool_to_connection_string_arg(key, value) == expected
