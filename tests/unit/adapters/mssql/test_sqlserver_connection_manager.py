import builtins
import importlib
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest
from azure.identity import AzureCliCredential
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.sqlserver import sqlserver_connections
from dbt.adapters.sqlserver.sqlserver_connections import (
    SQLServerConnectionManager,
    bool_to_connection_string_arg,
    get_pyodbc_attrs_before_credentials,
)
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials

# See
# https://github.com/Azure/azure-sdk-for-python/blob/azure-identity_1.5.0/sdk/identity/azure-identity/tests/test_cli_credential.py
CHECK_OUTPUT = AzureCliCredential.__module__ + ".subprocess.check_output"


@pytest.fixture
def credentials() -> SQLServerCredentials:
    return SQLServerCredentials(
        driver="ODBC Driver 17 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )


def test_get_pyodbc_attrs_before_empty_dict_when_service_principal(
    credentials: SQLServerCredentials,
) -> None:
    """
    When the authentication is set to sql we expect an empty attrs before.
    """
    attrs_before = get_pyodbc_attrs_before_credentials(credentials)
    assert attrs_before == {}


def test_get_pyodbc_attrs_before_sql_auth_without_azure_identity(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        sqlserver_connections, "_AZURE_IDENTITY_IMPORT_ERROR", ModuleNotFoundError()
    )

    attrs_before = get_pyodbc_attrs_before_credentials(credentials)

    assert attrs_before == {}


def test_get_pyodbc_attrs_before_cli_auth_requires_azure_identity(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.authentication = "cli"
    monkeypatch.setattr(
        sqlserver_connections, "_AZURE_IDENTITY_IMPORT_ERROR", ModuleNotFoundError()
    )

    with pytest.raises(DbtRuntimeError, match="requires the optional dependency 'azure-identity'"):
        get_pyodbc_attrs_before_credentials(credentials)


@pytest.mark.parametrize(
    "key, value, expected",
    [("somekey", False, "somekey=No"), ("somekey", True, "somekey=Yes")],
)
def test_bool_to_connection_string_arg(key: str, value: bool, expected: str) -> None:
    assert bool_to_connection_string_arg(key, value) == expected


def test_adapter_module_import_does_not_import_optional_backends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"pyodbc", "mssql_python"}:
            raise AssertionError(f"unexpected import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    importlib.reload(sqlserver_connections)

    assert sqlserver_connections._PYODBC_MODULE is None
    assert sqlserver_connections._MSSQL_PYTHON_MODULE is None


def test_get_pyodbc_returns_cached_module(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pyodbc = SimpleNamespace(name="cached-pyodbc")
    monkeypatch.setattr(sqlserver_connections, "_PYODBC_MODULE", fake_pyodbc, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_PYODBC_IMPORT_ERROR", None, raising=False)

    def fail_import(*args, **kwargs):
        raise AssertionError("pyodbc import should not run when cached")

    monkeypatch.setattr(builtins, "__import__", fail_import)

    assert sqlserver_connections._get_pyodbc() is fake_pyodbc
    assert sqlserver_connections._get_pyodbc() is fake_pyodbc


def test_get_mssql_python_returns_cached_module(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_mssql_python = SimpleNamespace(name="cached-mssql-python")
    monkeypatch.setattr(
        sqlserver_connections, "_MSSQL_PYTHON_MODULE", fake_mssql_python, raising=False
    )
    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_IMPORT_ERROR", None, raising=False)

    def fail_import(*args, **kwargs):
        raise AssertionError("mssql_python import should not run when cached")

    monkeypatch.setattr(builtins, "__import__", fail_import)

    assert sqlserver_connections._get_mssql_python() is fake_mssql_python
    assert sqlserver_connections._get_mssql_python() is fake_mssql_python


def test_get_pyodbc_raises_only_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sqlserver_connections, "_PYODBC_MODULE", None, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_PYODBC_IMPORT_ERROR", None, raising=False)
    original_import = builtins.__import__

    def missing_pyodbc(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "pyodbc":
            raise ModuleNotFoundError("No module named 'pyodbc'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", missing_pyodbc)

    with pytest.raises(DbtRuntimeError, match="pyodbc"):
        sqlserver_connections._get_pyodbc()


def test_get_mssql_python_raises_only_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_MODULE", None, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_IMPORT_ERROR", None, raising=False)
    original_import = builtins.__import__

    def missing_mssql_python(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "mssql_python":
            raise ModuleNotFoundError("No module named 'mssql_python'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", missing_mssql_python)

    with pytest.raises(DbtRuntimeError, match="mssql-python"):
        sqlserver_connections._get_mssql_python()


def test_open_with_mssql_python_feature_flag_requires_optional_dependency(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.driver = None
    credentials.use_mssql_python = True

    connection = Connection(type="sqlserver", name="feature-flag-test", credentials=credentials)

    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_MODULE", None, raising=False)
    monkeypatch.setattr(
        sqlserver_connections,
        "_MSSQL_PYTHON_IMPORT_ERROR",
        ModuleNotFoundError("No module named 'mssql_python'"),
        raising=False,
    )

    with pytest.raises(DbtRuntimeError, match="mssql-python"):
        SQLServerConnectionManager.open(connection)


def test_open_with_mssql_python_feature_flag_builds_connection_without_odbc_driver(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.driver = None
    credentials.UID = "dbt_user"
    credentials.PWD = "super-secret"
    credentials.encrypt = True
    credentials.trust_cert = True
    credentials.login_timeout = 17
    credentials.query_timeout = 23
    credentials.retries = 5
    credentials.use_mssql_python = True

    captured: Dict[str, Any] = {}
    pooling_calls: List[Dict[str, Any]] = []

    class FakeHandle:
        def __init__(self):
            self.timeout = None

    fake_handle = FakeHandle()

    def fake_connect(connection_string, autocommit, timeout):
        captured["connection_string"] = connection_string
        captured["autocommit"] = autocommit
        captured["timeout"] = timeout
        return fake_handle

    def fake_pooling(*, enabled):
        pooling_calls.append({"enabled": enabled})

    fake_module = SimpleNamespace(
        connect=fake_connect,
        pooling=fake_pooling,
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
        InternalError=type("InternalError", (Exception,), {}),
    )

    def fake_retry_connection(
        cls,
        connection,
        connect,
        logger,
        retry_limit,
        retryable_exceptions,
    ):
        captured["retry_limit"] = retry_limit
        captured["retryable_exceptions"] = retryable_exceptions
        handle = connect()
        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_MODULE", fake_module, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_IMPORT_ERROR", None, raising=False)
    monkeypatch.setattr(
        SQLServerConnectionManager,
        "retry_connection",
        classmethod(fake_retry_connection),
    )

    connection = Connection(type="sqlserver", name="feature-flag-test", credentials=credentials)
    opened = SQLServerConnectionManager.open(connection)

    assert opened is connection
    assert opened.handle is fake_handle
    assert opened.state == ConnectionState.OPEN

    assert captured["autocommit"] is True
    assert captured["timeout"] == 17
    assert captured["retry_limit"] == 5
    assert pooling_calls == [{"enabled": False}]

    con_str = captured["connection_string"]
    assert "DRIVER=" not in con_str
    assert "SERVER=fake.sql.sqlserver.net,1433" in con_str
    assert "Database=dbt" in con_str
    assert "UID={dbt_user}" in con_str
    assert "PWD={super-secret}" in con_str
    assert "encrypt=Yes" in con_str
    assert "TrustServerCertificate=Yes" in con_str
    assert "APP=dbt-sqlserver/" not in con_str

    assert fake_module.OperationalError in captured["retryable_exceptions"]
    assert fake_module.InternalError in captured["retryable_exceptions"]


def test_open_with_mssql_python_feature_flag_fails_fast_for_pyodbc_token_auth_aliases(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.driver = None
    credentials.use_mssql_python = True
    credentials.authentication = "cli"

    fake_module = SimpleNamespace(
        connect=lambda *args, **kwargs: None,
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
        InternalError=type("InternalError", (Exception,), {}),
    )

    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_MODULE", fake_module, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_IMPORT_ERROR", None, raising=False)

    connection = Connection(type="sqlserver", name="feature-flag-test", credentials=credentials)

    with pytest.raises(DbtRuntimeError, match="authentication"):
        SQLServerConnectionManager.open(connection)


def test_open_with_pyodbc_path_still_requires_driver_when_feature_flag_disabled(
    credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials.driver = None
    credentials.use_mssql_python = False

    fake_pyodbc = SimpleNamespace(
        connect=lambda *args, **kwargs: None,
        pooling=False,
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
        InternalError=type("InternalError", (Exception,), {}),
    )

    monkeypatch.setattr(sqlserver_connections, "_PYODBC_MODULE", fake_pyodbc, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_PYODBC_IMPORT_ERROR", None, raising=False)

    connection = Connection(type="sqlserver", name="pyodbc-test", credentials=credentials)

    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_MODULE", None, raising=False)
    with pytest.raises(DbtRuntimeError, match="driver"):
        SQLServerConnectionManager.open(connection)
