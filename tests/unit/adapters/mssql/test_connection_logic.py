from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dbt.adapters.sqlserver import sqlserver_auth
from dbt.adapters.sqlserver.sqlserver_connections import (
    SQLServerConnectionManager,
)
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials
from dbt.adapters.sqlserver.sqlserver_runtime import (
    configure_runtime_state_for_test,
    reset_runtime_state_for_test,
)


def _fake_pyodbc_module(connect):
    return SimpleNamespace(
        connect=connect,
        pooling=False,
        InternalError=type("InternalError", (Exception,), {}),
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
    )


@pytest.fixture
def base_credentials():
    return SQLServerCredentials(
        driver="ODBC Driver 18 for SQL Server",
        host="servers.database.windows.net",
        database="db",
        schema="schema",
        encrypt=True,
        trust_cert=True,
        port=1444,
    )


def test_connection_string_windows_login_with_port(base_credentials):
    base_credentials.windows_login = True

    connection = MagicMock()
    connection.state = "closed"
    connection.credentials = base_credentials

    reset_runtime_state_for_test()
    fake_pyodbc = _fake_pyodbc_module(MagicMock(return_value=MagicMock()))
    configure_runtime_state_for_test(pyodbc_module=fake_pyodbc, pyodbc_import_error=None)

    SQLServerConnectionManager.open(connection)

    args, _kwargs = fake_pyodbc.connect.call_args
    connection_string = args[0]

    assert "SERVER=servers.database.windows.net,1444" in connection_string
    assert "Trusted_Connection=yes" in connection_string
    assert "UID=" not in connection_string
    assert "PWD=" not in connection_string
    assert "APP=dbt-sqlserver/" in connection_string


def test_connection_string_standard_login_with_port(base_credentials):
    base_credentials.windows_login = False
    base_credentials.authentication = "sql"
    base_credentials.UID = "user"
    base_credentials.PWD = "password"
    base_credentials.trace_flag = True

    connection = MagicMock()
    connection.state = "closed"
    connection.credentials = base_credentials

    reset_runtime_state_for_test()
    fake_pyodbc = _fake_pyodbc_module(MagicMock(return_value=MagicMock()))
    configure_runtime_state_for_test(pyodbc_module=fake_pyodbc, pyodbc_import_error=None)

    SQLServerConnectionManager.open(connection)

    args, _kwargs = fake_pyodbc.connect.call_args
    connection_string = args[0]

    assert "SERVER=servers.database.windows.net,1444" in connection_string
    assert "UID={user}" in connection_string
    assert "PWD={password}" in connection_string
    assert "Pooling=true" in connection_string
    assert "SQL_ATTR_TRACE=SQL_OPT_TRACE_ON" in connection_string
    assert "APP=dbt-sqlserver/" in connection_string
    assert "ConnectRetryCount=3" in connection_string
    assert "ConnectRetryInterval=10" in connection_string


def test_pyodbc_token_authentication_passes_attrs_before(base_credentials):
    base_credentials.authentication = "cli"
    base_credentials.windows_login = False

    fake_token = SimpleNamespace(token="fake-token", expires_on=9999999999)
    fake_credential = SimpleNamespace(get_token=lambda *args, **kwargs: fake_token)
    fake_identity = SimpleNamespace(AzureCliCredential=lambda *args, **kwargs: fake_credential)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        azure_identity_module=fake_identity, azure_identity_import_error=None
    )

    attrs_before = sqlserver_auth.get_pyodbc_attrs_before_credentials(base_credentials)

    assert 1256 in attrs_before
    assert isinstance(attrs_before[1256], bytes)


def test_connection_string_named_instance_no_port(base_credentials):
    base_credentials.host = "myhost\\instance"
    base_credentials.windows_login = True

    connection = MagicMock()
    connection.state = "closed"
    connection.credentials = base_credentials

    reset_runtime_state_for_test()
    fake_pyodbc = _fake_pyodbc_module(MagicMock(return_value=MagicMock()))
    configure_runtime_state_for_test(pyodbc_module=fake_pyodbc, pyodbc_import_error=None)

    SQLServerConnectionManager.open(connection)

    args, _kwargs = fake_pyodbc.connect.call_args
    connection_string = args[0]

    assert "SERVER=myhost\\instance" in connection_string
    assert ",1444" not in connection_string
