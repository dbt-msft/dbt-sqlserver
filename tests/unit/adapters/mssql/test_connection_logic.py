from unittest.mock import MagicMock, patch

import pytest

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

    with patch("dbt.adapters.sqlserver.sqlserver_connections.pyodbc") as mock_pyodbc:
        mock_pyodbc.connect.return_value = MagicMock()

        SQLServerConnectionManager.open(connection)

        args, _kwargs = mock_pyodbc.connect.call_args
        connection_string = args[0]

        assert "SERVER=servers.database.windows.net,1444" in connection_string
        assert "trusted_connection=Yes" in connection_string


def test_connection_string_standard_login_with_port(base_credentials):
    """Port is included in the SERVER token for sql authentication."""
    base_credentials.windows_login = False
    base_credentials.authentication = "sql"
    base_credentials.UID = "user"
    base_credentials.PWD = "password"

    connection = MagicMock()
    connection.state = "closed"
    connection.credentials = base_credentials

    with patch("dbt.adapters.sqlserver.sqlserver_connections.pyodbc") as mock_pyodbc:
        mock_pyodbc.connect.return_value = MagicMock()

        SQLServerConnectionManager.open(connection)

        args, _kwargs = mock_pyodbc.connect.call_args
        connection_string = args[0]

        assert "SERVER=servers.database.windows.net,1444" in connection_string
        assert "UID={user}" in connection_string


def test_connection_string_named_instance_no_port(base_credentials):
    """A named-instance host (containing `\\`) must not append a port to SERVER."""
    base_credentials.host = "myhost\\instance"
    base_credentials.windows_login = True

    connection = MagicMock()
    connection.state = "closed"
    connection.credentials = base_credentials

    with patch("dbt.adapters.sqlserver.sqlserver_connections.pyodbc") as mock_pyodbc:
        mock_pyodbc.connect.return_value = MagicMock()

        SQLServerConnectionManager.open(connection)

        args, _kwargs = mock_pyodbc.connect.call_args
        connection_string = args[0]

        assert "SERVER=myhost\\instance" in connection_string
        assert ",1444" not in connection_string
