import datetime as dt
import struct
import json
from unittest import mock

import pytest
from azure.identity import AzureCliCredential

from dbt.adapters.sqlserver.sql_server_connection_manager import (
    bool_to_connection_string_arg,
    get_pyodbc_attrs_before,
    byte_array_to_datetime
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

@pytest.mark.parametrize(
    "value, expected_datetime, expected_str", [
        (
            bytes([
                0xE5, 0x07,             # year: 2022
                0x0C, 0x00,             # month: 12
                0x11, 0x00,             # day: 17
                0x16, 0x00,             # hour: 22
                0x16, 0x00,             # minute: 22
                0x12, 0x00,             # second: 18
                0x15, 0xCD, 0x5B, 0x07, # microsecond: 123456789
                0x02, 0x00, 0x1E, 0x00  # tzinfo: +02:30
            ]),
            dt.datetime(2022, 12, 17, 22, 22, 18, 123456, dt.timezone(dt.timedelta(hours=2, minutes=30))),
            "2021-12-17 22:22:18.123456+02:30"
        )
    ]
)
def test_byte_array_to_datetime(value: bytes, expected_datetime: dt.datetime, expected_str: str) -> None:
    """
    Assert SQL_SS_TIMESTAMPOFFSET_STRUCT bytes convert to string in an expected isoformat
    https://docs.python.org/3/library/datetime.html#datetime.datetime.__str__
    https://learn.microsoft.com/sql/relational-databases/native-client-odbc-date-time/data-type-support-for-odbc-date-and-time-improvements#sql_ss_timestampoffset_struct
    """
    assert byte_array_to_datetime(value) == expected_datetime
    assert str(byte_array_to_datetime(value)) == expected_str