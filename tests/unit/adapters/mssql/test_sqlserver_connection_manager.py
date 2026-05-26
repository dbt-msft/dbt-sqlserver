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
    _build_mssql_python_connection_string,
    _normalize_mssql_python_authentication,
    _validate_mssql_python_requirements,
    bool_to_connection_string_arg,
    get_pyodbc_attrs_before_credentials,
)
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerBackend, SQLServerCredentials

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


@pytest.mark.parametrize(
    "input_auth, expected",
    [
        ("msi", "ActiveDirectoryMSI"),
        ("ActiveDirectoryMsi", "ActiveDirectoryMSI"),
        ("ActiveDirectoryMSI", "ActiveDirectoryMSI"),
        ("active_directory_msi", "ActiveDirectoryMSI"),
        ("ActiveDirectoryIntegrated", "ActiveDirectoryIntegrated"),
        ("active_directory_integrated", "ActiveDirectoryIntegrated"),
        ("adintegrated", "ActiveDirectoryIntegrated"),
        ("serviceprincipal", "ActiveDirectoryServicePrincipal"),
        ("ActiveDirectoryServicePrincipal", "ActiveDirectoryServicePrincipal"),
        ("auto", "ActiveDirectoryDefault"),
        ("ActiveDirectoryDefault", "ActiveDirectoryDefault"),
        ("default", "ActiveDirectoryDefault"),
        ("ActiveDirectoryPassword", "ActiveDirectoryPassword"),
        ("ActiveDirectoryInteractive", "ActiveDirectoryInteractive"),
        ("ActiveDirectoryDeviceCode", "ActiveDirectoryDeviceCode"),
    ],
)
def test_normalize_mssql_python_authentication(input_auth: str, expected: str) -> None:
    assert _normalize_mssql_python_authentication(input_auth) == expected


def test_escape_connection_string_value_quotes_only_when_needed() -> None:
    assert sqlserver_connections._escape_connection_string_value("plain") == "plain"
    assert (
        sqlserver_connections._escape_connection_string_value("contains;semicolon")
        == "{contains;semicolon}"
    )
    assert sqlserver_connections._escape_connection_string_value("brace}") == "{brace}}}"
    assert sqlserver_connections._escape_connection_string_value(" leading") == "{ leading}"
    assert sqlserver_connections._escape_connection_string_value("trailing ") == "{trailing }"


def test_mssql_python_active_directory_default_passes() -> None:
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        backend=SQLServerBackend.mssql_python,
        authentication="auto",
    )

    conn_str = _build_mssql_python_connection_string(credentials)

    assert "Authentication=ActiveDirectoryDefault" in conn_str


def test_mssql_python_device_code_authentication() -> None:
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        backend=SQLServerBackend.mssql_python,
        authentication="ActiveDirectoryDeviceCode",
    )

    conn_str = _build_mssql_python_connection_string(credentials)

    assert "Authentication=ActiveDirectoryDeviceCode" in conn_str


def test_mssql_python_service_principal_authentication() -> None:
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        backend=SQLServerBackend.mssql_python,
        authentication="serviceprincipal",
        client_id="client-id",
        client_secret="client-secret",
    )

    conn_str = _build_mssql_python_connection_string(credentials)

    assert "Authentication=ActiveDirectoryServicePrincipal" in conn_str
    assert "UID=client-id" in conn_str
    assert "PWD=client-secret" in conn_str


def test_mssql_python_password_authentication() -> None:
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        backend=SQLServerBackend.mssql_python,
        authentication="ActiveDirectoryPassword",
        UID="user",
        PWD="password",
    )

    conn_str = _build_mssql_python_connection_string(credentials)

    assert "Authentication=ActiveDirectoryPassword" in conn_str
    assert "UID=user" in conn_str
    assert "PWD=password" in conn_str


def test_mssql_python_default_does_not_append_app_when_installed() -> None:
    pytest.importorskip("mssql_python")
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        backend=SQLServerBackend.mssql_python,
        authentication="sql",
        UID="user",
        PWD="password",
    )

    conn_str = _build_mssql_python_connection_string(credentials)
    assert "APP=dbt-sqlserver/" not in conn_str


def test_mssql_python_windows_login_rejects_user_password(
    credentials: SQLServerCredentials,
) -> None:
    credentials.backend = SQLServerBackend.mssql_python
    credentials.windows_login = True
    credentials.UID = "dbt_user"
    credentials.PWD = "super-secret"
    credentials.encrypt = True
    credentials.trust_cert = True

    with pytest.raises(DbtRuntimeError, match="user/password are not valid"):
        _build_mssql_python_connection_string(credentials)


def test_mssql_python_system_assigned_msi() -> None:
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        backend=SQLServerBackend.mssql_python,
        authentication="ActiveDirectoryMsi",
    )

    conn_str = _build_mssql_python_connection_string(credentials)

    assert "Authentication=ActiveDirectoryMSI" in conn_str
    assert "UID=" not in conn_str
    assert "PWD=" not in conn_str


def test_mssql_python_user_assigned_msi() -> None:
    client_id = "00000000-0000-0000-0000-000000000000"
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        backend=SQLServerBackend.mssql_python,
        authentication="msi",
        UID=client_id,
    )

    conn_str = _build_mssql_python_connection_string(credentials)

    assert "Authentication=ActiveDirectoryMSI" in conn_str
    assert f"UID={client_id}" in conn_str
    assert "PWD=" not in conn_str


def test_mssql_python_active_directory_integrated() -> None:
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        backend=SQLServerBackend.mssql_python,
        authentication="ActiveDirectoryIntegrated",
    )

    conn_str = _build_mssql_python_connection_string(credentials)

    assert "Authentication=ActiveDirectoryIntegrated" in conn_str
    assert "PWD=" not in conn_str


def test_mssql_python_supported_authentication_modes() -> None:
    for authentication in [
        "msi",
        "ActiveDirectoryMSI",
        "active_directory_msi",
        "ActiveDirectoryIntegrated",
        "active_directory_integrated",
        "adintegrated",
        "serviceprincipal",
        "ActiveDirectoryServicePrincipal",
        "auto",
        "ActiveDirectoryDefault",
        "default",
    ]:
        credentials = SQLServerCredentials(
            driver=None,
            host="fake.sql.sqlserver.net",
            database="dbt",
            schema="sqlserver",
            encrypt=True,
            trust_cert=True,
            backend=SQLServerBackend.mssql_python,
            authentication=authentication,
        )

        _validate_mssql_python_requirements(credentials)


def test_open_with_mssql_python_system_assigned_msi_passes_connection_string(
    credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials.driver = None
    credentials.backend = SQLServerBackend.mssql_python
    credentials.authentication = "msi"
    credentials.encrypt = True
    credentials.trust_cert = True

    captured: Dict[str, Any] = {}

    class FakeHandle:
        def __init__(self):
            self.timeout = None

    def fake_connect(connection_string, autocommit, timeout):
        captured["connection_string"] = connection_string
        captured["autocommit"] = autocommit
        captured["timeout"] = timeout
        return FakeHandle()

    fake_module = SimpleNamespace(
        connect=fake_connect,
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
    assert opened.state == ConnectionState.OPEN
    assert "Authentication=ActiveDirectoryMSI" in captured["connection_string"]
    assert "UID=" not in captured["connection_string"]
    assert "PWD=" not in captured["connection_string"]


def test_adapter_module_import_does_not_import_optional_backends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"pyodbc", "mssql_python", "azure.identity", "azure.core.credentials"}:
            raise AssertionError(f"unexpected import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    importlib.reload(sqlserver_connections)

    assert sqlserver_connections._PYODBC_MODULE is None
    assert sqlserver_connections._MSSQL_PYTHON_MODULE is None


def test_get_pyodbc_imports_only_pyodbc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sqlserver_connections, "_PYODBC_MODULE", None, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_PYODBC_IMPORT_ERROR", None, raising=False)
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"mssql_python", "azure.identity", "azure.core.credentials"}:
            raise AssertionError(f"unexpected import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    module = sqlserver_connections._get_pyodbc()
    assert module is not None


def test_get_mssql_python_imports_only_mssql_python(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_MODULE", None, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_MSSQL_PYTHON_IMPORT_ERROR", None, raising=False)
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"pyodbc", "azure.identity", "azure.core.credentials"}:
            raise AssertionError(f"unexpected import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    module = sqlserver_connections._get_mssql_python()
    assert module is not None


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
    credentials.backend = SQLServerBackend.mssql_python

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
    credentials.backend = SQLServerBackend.mssql_python

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
    assert pooling_calls == []

    con_str = captured["connection_string"]
    assert "DRIVER=" not in con_str
    assert "SERVER=fake.sql.sqlserver.net,1433" in con_str
    assert "Database=dbt" in con_str
    assert "UID=dbt_user" in con_str
    assert "PWD=super-secret" in con_str
    assert "encrypt=Yes" in con_str
    assert "TrustServerCertificate=Yes" in con_str
    assert "APP=dbt-sqlserver/" not in con_str

    assert fake_module.OperationalError in captured["retryable_exceptions"]
    assert fake_module.InternalError in captured["retryable_exceptions"]

    assert pooling_calls == []
    assert "APP=dbt-sqlserver/" not in con_str


def test_open_with_mssql_python_feature_flag_fails_fast_for_pyodbc_token_auth_aliases(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.driver = None
    credentials.backend = SQLServerBackend.mssql_python
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


@pytest.mark.parametrize(
    "unsupported_auth",
    ["cli", "environment", "ActiveDirectoryAccessToken"],
)
def test_open_with_mssql_python_unsupported_authentications(
    credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
    unsupported_auth: str,
) -> None:
    credentials.driver = None
    credentials.backend = SQLServerBackend.mssql_python
    credentials.authentication = unsupported_auth
    credentials.UID = "dbt_user"
    credentials.PWD = "super-secret"

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


@pytest.mark.parametrize(
    "authentication",
    ["msi", "ActiveDirectoryMSI"],
)
def test_open_with_mssql_python_supported_managed_identity_auth(
    credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
    authentication: str,
) -> None:
    credentials.driver = None
    credentials.backend = SQLServerBackend.mssql_python
    credentials.authentication = authentication
    credentials.UID = None
    credentials.PWD = None
    credentials.encrypt = True
    credentials.trust_cert = True

    captured: Dict[str, Any] = {}

    class FakeHandle:
        def __init__(self):
            self.timeout = None

    def fake_connect(connection_string, autocommit, timeout):
        captured["connection_string"] = connection_string
        captured["autocommit"] = autocommit
        captured["timeout"] = timeout
        return FakeHandle()

    fake_module = SimpleNamespace(
        connect=fake_connect,
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
    assert opened.state == ConnectionState.OPEN
    assert "Authentication=ActiveDirectoryMSI" in captured["connection_string"]
    assert "UID=" not in captured["connection_string"]
    assert "PWD=" not in captured["connection_string"]


@pytest.mark.parametrize(
    "required_field, value, match_text",
    [
        ("host", None, "host"),
        ("database", None, "database"),
        ("schema", None, "schema"),
    ],
)
def test_open_requires_host_database_schema(
    credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
    required_field: str,
    value: object,
    match_text: str,
) -> None:
    setattr(credentials, required_field, value)
    credentials.UID = "dbt_user"
    credentials.PWD = "super-secret"

    fake_pyodbc = SimpleNamespace(
        connect=lambda *args, **kwargs: None,
        pooling=False,
        InternalError=type("InternalError", (Exception,), {}),
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
    )

    monkeypatch.setattr(sqlserver_connections, "_PYODBC_MODULE", fake_pyodbc, raising=False)
    monkeypatch.setattr(sqlserver_connections, "_PYODBC_IMPORT_ERROR", None, raising=False)

    connection = Connection(type="sqlserver", name="pyodbc-test", credentials=credentials)

    with pytest.raises(DbtRuntimeError, match=match_text):
        SQLServerConnectionManager.open(connection)


def test_open_with_pyodbc_path_still_requires_driver_when_feature_flag_disabled(
    credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials.driver = None
    credentials.backend = SQLServerBackend.pyodbc

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
