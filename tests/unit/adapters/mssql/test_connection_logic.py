from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.sqlserver import sqlserver_connections
from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials


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
    """Port is included in the SERVER token when windows_login is True."""
    base_credentials.windows_login = True

    connection = MagicMock()
    connection.state = "closed"
    connection.credentials = base_credentials

    fake_pyodbc = SimpleNamespace(
        connect=MagicMock(return_value=MagicMock()),
        pooling=False,
        InternalError=type("InternalError", (Exception,), {}),
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
    )

    with (
        patch.object(sqlserver_connections, "_PYODBC_MODULE", fake_pyodbc),
        patch.object(sqlserver_connections, "_PYODBC_IMPORT_ERROR", None),
    ):

        SQLServerConnectionManager.open(connection)

        args, _kwargs = fake_pyodbc.connect.call_args
        connection_string = args[0]

        assert "SERVER=servers.database.windows.net,1444" in connection_string
        assert "Trusted_Connection=yes" in connection_string
        assert "UID=" not in connection_string
        assert "PWD=" not in connection_string
        assert "APP=dbt-sqlserver/" in connection_string


def test_connection_string_standard_login_with_port(base_credentials):
    """Port is included in the SERVER token for sql authentication."""
    base_credentials.windows_login = False
    base_credentials.authentication = "sql"
    base_credentials.UID = "user"
    base_credentials.PWD = "password"
    base_credentials.trace_flag = True

    connection = MagicMock()
    connection.state = "closed"
    connection.credentials = base_credentials

    fake_pyodbc = SimpleNamespace(
        connect=MagicMock(return_value=MagicMock()),
        pooling=False,
        InternalError=type("InternalError", (Exception,), {}),
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
    )

    with (
        patch.object(sqlserver_connections, "_PYODBC_MODULE", fake_pyodbc),
        patch.object(sqlserver_connections, "_PYODBC_IMPORT_ERROR", None),
    ):

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


def test_pyodbc_token_authentication_passes_attrs_before(base_credentials, monkeypatch):
    base_credentials.authentication = "cli"
    base_credentials.windows_login = False

    fake_token = SimpleNamespace(token="fake-token", expires_on=9999999999)
    fake_credential = SimpleNamespace(get_token=lambda *args, **kwargs: fake_token)
    fake_identity = SimpleNamespace(AzureCliCredential=lambda *args, **kwargs: fake_credential)

    monkeypatch.setattr(
        sqlserver_connections, "_AZURE_IDENTITY_MODULE", fake_identity, raising=False
    )
    monkeypatch.setattr(sqlserver_connections, "_AZURE_IDENTITY_IMPORT_ERROR", None, raising=False)

    attrs_before = sqlserver_connections.get_pyodbc_attrs_before_credentials(base_credentials)

    assert 1256 in attrs_before
    assert isinstance(attrs_before[1256], bytes)


def test_connection_string_named_instance_no_port(base_credentials):
    """A named-instance host (containing `\\`) must not append a port to SERVER."""
    base_credentials.host = "myhost\\instance"
    base_credentials.windows_login = True

    connection = MagicMock()
    connection.state = "closed"
    connection.credentials = base_credentials

    fake_pyodbc = SimpleNamespace(
        connect=MagicMock(return_value=MagicMock()),
        pooling=False,
        InternalError=type("InternalError", (Exception,), {}),
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
    )

    with (
        patch.object(sqlserver_connections, "_PYODBC_MODULE", fake_pyodbc),
        patch.object(sqlserver_connections, "_PYODBC_IMPORT_ERROR", None),
    ):

        SQLServerConnectionManager.open(connection)

        args, _kwargs = fake_pyodbc.connect.call_args
        connection_string = args[0]

        assert "SERVER=myhost\\instance" in connection_string
        assert ",1444" not in connection_string
