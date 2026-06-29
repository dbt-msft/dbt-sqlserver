import builtins
import importlib
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from azure.identity import AzureCliCredential
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

import dbt.adapters.sqlserver.sqlserver_auth
import dbt.adapters.sqlserver.sqlserver_backend as sqlserver_backend
from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.sqlserver import sqlserver_connections
from dbt.adapters.sqlserver.sqlserver_auth import (
    get_pyodbc_attrs_before_credentials,
    normalize_mssql_python_authentication,
    uses_aad_token_authentication,
)
from dbt.adapters.sqlserver.sqlserver_backend import (
    _finalize_connection_handle,
    _finalize_mssql_python_handle,
)
from dbt.adapters.sqlserver.sqlserver_backend import (
    build_mssql_python_connection_string as _build_mssql_python_connection_string,
)
from dbt.adapters.sqlserver.sqlserver_backend import (
    build_pyodbc_connection_string as _build_pyodbc_connection_string,
)
from dbt.adapters.sqlserver.sqlserver_backend import is_pyodbc_handle as _is_pyodbc_handle
from dbt.adapters.sqlserver.sqlserver_connections import (
    SQLServerConnectionManager,
)
from dbt.adapters.sqlserver.sqlserver_credentials import (
    SQLServerBackend,
    SQLServerCredentials,
)
from dbt.adapters.sqlserver.sqlserver_helpers import (
    bool_to_connection_string_arg,
    escape_connection_string_value,
    is_mssql_python_backend,
    sanitize_connection_string_for_logging,
    validate_connection_requirements,
    validate_mssql_python_requirements,
    validate_pyodbc_requirements,
)
from dbt.adapters.sqlserver.sqlserver_runtime import (
    configure_runtime_state_for_test,
    get_runtime_state_for_test,
    reset_runtime_state_for_test,
)

CHECK_OUTPUT = f"{AzureCliCredential.__module__}.subprocess.check_output"


@pytest.fixture
def credentials() -> SQLServerCredentials:
    return SQLServerCredentials(
        driver="ODBC Driver 17 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )


def test_get_pyodbc_attrs_before_sql_auth_returns_empty_dict(
    credentials: SQLServerCredentials,
) -> None:
    attrs_before = get_pyodbc_attrs_before_credentials(credentials)
    assert attrs_before == {}


def test_get_pyodbc_attrs_before_sql_auth_without_azure_identity(
    credentials: SQLServerCredentials,
) -> None:
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(azure_identity_import_error=ModuleNotFoundError())

    attrs_before = get_pyodbc_attrs_before_credentials(credentials)

    assert attrs_before == {}


def test_get_pyodbc_attrs_before_cli_auth_requires_azure_identity(
    credentials: SQLServerCredentials,
) -> None:
    credentials.authentication = "cli"
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(azure_identity_import_error=ModuleNotFoundError())

    with pytest.raises(DbtRuntimeError, match="requires the optional dependency 'azure-identity'"):
        get_pyodbc_attrs_before_credentials(credentials)


def test_get_pyodbc_attrs_before_active_directory_access_token_defaults_zero_expiry(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.authentication = "ActiveDirectoryAccessToken"
    credentials.access_token = "some-token"

    warnings: list[str] = []
    monkeypatch.setattr(
        dbt.adapters.sqlserver.sqlserver_auth.logger,
        "warning",
        lambda message, *args: warnings.append(message % args if args else message),
    )

    credentials.access_token_expires_on = 0
    attrs = get_pyodbc_attrs_before_credentials(credentials)
    assert 1256 in attrs
    assert any("defaulting expiry" in message for message in warnings)


def test_get_pyodbc_attrs_before_active_directory_access_token_requires_expiry(
    credentials: SQLServerCredentials,
) -> None:
    credentials.authentication = "ActiveDirectoryAccessToken"
    credentials.access_token = "some-token"

    credentials.access_token_expires_on = None
    with pytest.raises(DbtRuntimeError, match="access token expiry"):
        get_pyodbc_attrs_before_credentials(credentials)


def test_get_pyodbc_attrs_before_active_directory_access_token_honors_explicit_expiry(
    credentials: SQLServerCredentials,
) -> None:
    credentials.authentication = "ActiveDirectoryAccessToken"
    credentials.access_token = "some-token"
    credentials.access_token_expires_on = 123456789

    attrs = get_pyodbc_attrs_before_credentials(credentials)
    assert 1256 in attrs


@pytest.mark.parametrize(
    "driver",
    [None, "", "   "],
)
def test_validate_pyodbc_requirements_rejects_blank_driver(
    driver: str | None,
) -> None:
    credentials = SQLServerCredentials(
        driver=driver,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )

    with pytest.raises(
        DbtRuntimeError,
        match="The pyodbc backend requires a SQL Server ODBC driver name",
    ):
        validate_pyodbc_requirements(credentials)


def test_validate_pyodbc_requirements_accepts_valid_driver() -> None:
    credentials = SQLServerCredentials(
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )

    validate_pyodbc_requirements(credentials)


def test_validate_connection_requirements_allows_windows_login_without_auth() -> None:
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.mssql_python,
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        windows_login=True,
        authentication="",
        encrypt=True,
        trust_cert=True,
    )

    validate_connection_requirements(credentials)


def test_sqlserver_credentials_reject_negative_query_timeout() -> None:
    with pytest.raises(DbtRuntimeError, match="query_timeout"):
        SQLServerCredentials(
            driver="ODBC Driver 18 for SQL Server",
            host="fake.sql.sqlserver.net",
            database="dbt",
            schema="sqlserver",
            query_timeout=-1,
        )


def test_build_pyodbc_connection_string_formats_driver_name() -> None:
    credentials = SQLServerCredentials(
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        authentication="sql",
        UID="user",
        PWD="password",
    )

    conn_str = _build_pyodbc_connection_string(credentials)

    assert conn_str.startswith("DRIVER={ODBC Driver 18 for SQL Server};")
    assert "encrypt=Yes" in conn_str
    assert "TrustServerCertificate=Yes" in conn_str


def test_build_pyodbc_connection_string_preserves_prebraced_driver_name() -> None:
    credentials = SQLServerCredentials(
        driver="{ODBC Driver 18 for SQL Server}",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        authentication="sql",
        UID="user",
        PWD="password",
    )

    conn_str = _build_pyodbc_connection_string(credentials)

    assert conn_str.startswith("DRIVER={ODBC Driver 18 for SQL Server};")
    assert "DRIVER={{ODBC Driver 18 for SQL Server}}" not in conn_str


@pytest.mark.parametrize(
    "key, value, expected",
    [("somekey", False, "somekey=No"), ("somekey", True, "somekey=Yes")],
)
def test_bool_to_connection_string_arg(key: str, value: bool, expected: str) -> None:
    assert bool_to_connection_string_arg(key, value) == expected


def test_is_mssql_python_backend() -> None:
    assert is_mssql_python_backend(SQLServerBackend.mssql_python) is True
    assert is_mssql_python_backend(SQLServerBackend.pyodbc) is False


def test_connection_keys_do_not_mutate_authentication() -> None:
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.pyodbc,
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        authentication="serviceprincipal",
    )

    original_authentication = credentials.authentication

    credentials._connection_keys()

    assert credentials.authentication == original_authentication


def test_connection_keys_include_driver_only_for_pyodbc() -> None:
    pyodbc_credentials = SQLServerCredentials(
        backend=SQLServerBackend.pyodbc,
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )
    mssql_python_credentials = SQLServerCredentials(
        backend=SQLServerBackend.mssql_python,
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )

    assert "driver" in pyodbc_credentials._connection_keys()
    assert "driver" not in mssql_python_credentials._connection_keys()
    assert "windows_login" in pyodbc_credentials._connection_keys()
    assert "windows_login" in mssql_python_credentials._connection_keys()


def test_is_pyodbc_handle_false_for_mssql_python_handle() -> None:
    handle = type("Handle", (), {"driver_type": "mssql-python"})()
    assert _is_pyodbc_handle(handle) is False


@pytest.mark.parametrize(
    "authentication, expected",
    [
        ("cli", True),
        ("environment", True),
        ("auto", True),
        ("serviceprincipal", True),
        ("msi", True),
        ("ActiveDirectoryAccessToken", True),
        ("ActiveDirectoryServicePrincipal", False),
        ("ActiveDirectoryDefault", False),
    ],
)
def test_uses_aad_token_authentication_matches_pyodbc_token_aliases(
    authentication: str, expected: bool
) -> None:
    credentials = SQLServerCredentials(
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        authentication=authentication,
    )

    assert uses_aad_token_authentication(credentials) is expected


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
    assert normalize_mssql_python_authentication(input_auth) == expected


def test_escape_connection_string_value_quotes_only_when_needed() -> None:
    assert escape_connection_string_value("plain") == "plain"
    assert escape_connection_string_value("contains;semicolon") == "{contains;semicolon}"
    assert escape_connection_string_value("brace}") == "{brace}}}"
    assert escape_connection_string_value(" leading") == "{ leading}"
    assert escape_connection_string_value("trailing ") == "{trailing }"


def test_sanitize_connection_string_for_logging_redacts_common_secret_fields() -> None:
    sanitized = sanitize_connection_string_for_logging(
        "SERVER=fake;UID=user@example.com;User Id=another@example.com;"
        "PWD=password;Password=hello;ClientSecret=mysecret;ACCESS_TOKEN=token123"
    )

    assert "PWD=***" in sanitized
    assert "Password=***" in sanitized
    assert "ClientSecret=***" in sanitized
    assert "ACCESS_TOKEN=***" in sanitized
    assert "UID=***" in sanitized
    assert "User Id=***" in sanitized


def test_sanitize_connection_string_for_logging_handles_braced_values() -> None:
    sanitized = sanitize_connection_string_for_logging(
        "SERVER=fake;PWD={token;with=separators};ClientSecret={secret;value};UID=user"
    )

    assert "PWD=***" in sanitized
    assert "ClientSecret=***" in sanitized
    assert "UID=***" in sanitized


def test_sanitize_connection_string_for_logging_trims_whitespace_around_segments() -> None:
    sanitized = sanitize_connection_string_for_logging(
        "  SERVER=fake.sql.sqlserver.net ;  UID = user@example.com  ;  PWD = password  ;  "
    )

    assert sanitized == "SERVER=fake.sql.sqlserver.net;UID=***;PWD=***"


def test_sanitize_connection_string_for_logging_treats_unterminated_brace_as_literal() -> None:
    sanitized = sanitize_connection_string_for_logging("SERVER=fake;PWD={token;APP=foo")

    assert sanitized == "SERVER=fake;PWD=***;APP=foo"


def test_sanitize_connection_string_for_logging_preserves_non_secret_auth_metadata() -> None:
    sanitized = sanitize_connection_string_for_logging(
        "SERVER=fake;Authentication=sql;Auth=sql;NonToken=literal;PWD=password"
    )

    assert "Authentication=sql" in sanitized
    assert "Auth=sql" in sanitized
    assert "NonToken=literal" in sanitized
    assert "PWD=***" in sanitized


def test_finalize_connection_handle_warns_when_timeout_is_unsupported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = object()
    reset_runtime_state_for_test()
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        query_timeout=30,
    )
    warnings: list[str] = []

    monkeypatch.setattr(
        sqlserver_backend.logger,
        "warning",
        lambda message, *args: warnings.append(message % args if args else message),
    )

    result = _finalize_mssql_python_handle(handle, credentials)
    second_result = _finalize_mssql_python_handle(handle, credentials)

    assert result is handle
    assert second_result is handle
    assert len(warnings) == 1
    assert any("query_timeout=30" in message for message in warnings)


def test_finalize_connection_handle_ignores_missing_timeout_attribute() -> None:
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        query_timeout=30,
    )

    handle = object()

    assert _finalize_connection_handle(handle, credentials) is handle


def test_finalize_connection_handle_coerces_string_query_timeout() -> None:
    class Handle:
        def __init__(self) -> None:
            self.timeout = None

    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        query_timeout=0,
    )
    credentials.query_timeout = "23"

    handle = Handle()

    assert _finalize_connection_handle(handle, credentials) is handle
    assert handle.timeout == 23


def test_validate_connection_requirements_rejects_negative_query_timeout() -> None:
    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
    )
    credentials.query_timeout = -1

    with pytest.raises(DbtRuntimeError, match="query_timeout"):
        validate_connection_requirements(credentials)


def test_finalize_connection_handle_propagates_non_attribute_errors() -> None:
    class BrokenHandle:
        @property
        def timeout(self) -> None:
            return None

        @timeout.setter
        def timeout(self, value: object) -> None:
            raise TypeError("boom")

    credentials = SQLServerCredentials(
        driver=None,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        query_timeout=30,
    )

    with pytest.raises(TypeError, match="boom"):
        _finalize_connection_handle(BrokenHandle(), credentials)


def test_exception_handler_preserves_unknown_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_runtime_state_for_test()

    manager = object.__new__(SQLServerConnectionManager)
    credentials = SQLServerCredentials(
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
    )
    release_calls: list[int] = []

    monkeypatch.setattr(
        manager,
        "get_thread_connection",
        lambda: SimpleNamespace(credentials=credentials),
    )
    monkeypatch.setattr(manager, "release", lambda: release_calls.append(1))
    debug_messages: list[str] = []
    monkeypatch.setattr(
        sqlserver_connections.logger,
        "debug",
        lambda message, *args: debug_messages.append(message % args if args else message),
    )

    with pytest.raises(TypeError, match="boom"):
        with manager.exception_handler("select 1"):
            raise TypeError("boom")

    assert release_calls == [1]
    assert any("TypeError" in message for message in debug_messages)


def test_exception_handler_routes_backend_database_errors_without_falling_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BackendDatabaseError(Exception):
        pass

    reset_runtime_state_for_test()

    manager = object.__new__(SQLServerConnectionManager)
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.pyodbc,
        driver="ODBC Driver 18 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
    )
    release_calls: list[int] = []
    handler_calls: list[tuple[str, str]] = []
    debug_messages: list[str] = []

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        pyodbc_module=SimpleNamespace(DatabaseError=BackendDatabaseError)
    )

    try:
        monkeypatch.setattr(
            manager,
            "get_thread_connection",
            lambda: SimpleNamespace(credentials=credentials),
        )
        monkeypatch.setattr(manager, "release", lambda: release_calls.append(1))

        def fake_handle_backend_database_error(
            error: Exception,
            database_error: type[Exception] | None,
            release_connection: Any,
        ) -> None:
            handler_calls.append(
                (
                    type(error).__name__,
                    database_error.__name__ if database_error else "",
                )
            )
            release_connection()
            raise DbtDatabaseError(str(error).strip()) from error

        monkeypatch.setattr(
            sqlserver_connections,
            "handle_backend_database_error",
            fake_handle_backend_database_error,
        )
        monkeypatch.setattr(
            sqlserver_connections.logger,
            "debug",
            lambda message, *args: debug_messages.append(message % args if args else message),
        )

        with pytest.raises(DbtDatabaseError, match="boom"):
            with manager.exception_handler("select 1"):
                raise BackendDatabaseError("boom")

        assert handler_calls == [("BackendDatabaseError", "BackendDatabaseError")]
        assert release_calls == [1]
        assert all("Rolling back transaction." not in message for message in debug_messages)
        assert all("Error running SQL:" not in message for message in debug_messages)
    finally:
        reset_runtime_state_for_test()


def test_data_type_code_to_name_handles_repr_and_rejects_integer_codes() -> None:
    assert SQLServerConnectionManager.data_type_code_to_name("<class 'str'>") == "varchar"
    assert SQLServerConnectionManager.data_type_code_to_name("int") == "int"

    with pytest.raises(DbtRuntimeError, match="integer type codes are not mapped"):
        SQLServerConnectionManager.data_type_code_to_name(7)


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


def test_mssql_python_connection_string_does_not_append_pyodbc_retry_hints() -> None:
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

    assert "ConnectRetryCount=3" not in conn_str
    assert "ConnectRetryInterval=10" not in conn_str


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

        validate_mssql_python_requirements(credentials)


def test_open_with_mssql_python_backend_system_assigned_msi_passes_connection_string(
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

    fake_module = _fake_mssql_python_module(fake_connect)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        mssql_python_module=fake_module, mssql_python_import_error=None
    )
    monkeypatch.setattr(
        SQLServerConnectionManager,
        "retry_connection",
        classmethod(_fake_retry_connection_stub()),
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
        if name in {
            "pyodbc",
            "mssql_python",
            "azure.identity",
            "azure.core.credentials",
        }:
            raise AssertionError(f"unexpected import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    reset_runtime_state_for_test()
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    importlib.reload(sqlserver_connections)

    runtime_state = get_runtime_state_for_test()
    assert runtime_state.pyodbc_module is None
    assert runtime_state.mssql_python_module is None


def test_get_pyodbc_imports_only_pyodbc(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_runtime_state_for_test()
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"mssql_python", "azure.identity", "azure.core.credentials"}:
            raise AssertionError(f"unexpected import: {name}")
        if name == "pyodbc":
            return MagicMock()
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    module = dbt.adapters.sqlserver.sqlserver_runtime._get_pyodbc()
    assert module is not None


def test_get_mssql_python_imports_only_mssql_python(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_runtime_state_for_test()
    original_import = builtins.__import__

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"pyodbc", "azure.identity", "azure.core.credentials"}:
            raise AssertionError(f"unexpected import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    module = dbt.adapters.sqlserver.sqlserver_runtime._get_mssql_python()
    assert module is not None


def test_get_pyodbc_returns_cached_module(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pyodbc = SimpleNamespace(name="cached-pyodbc")
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(pyodbc_module=fake_pyodbc, pyodbc_import_error=None)

    def fail_import(*args, **kwargs):
        raise AssertionError("pyodbc import should not run when cached")

    monkeypatch.setattr(builtins, "__import__", fail_import)

    assert dbt.adapters.sqlserver.sqlserver_runtime._get_pyodbc() is fake_pyodbc
    assert dbt.adapters.sqlserver.sqlserver_runtime._get_pyodbc() is fake_pyodbc


def test_reset_runtime_state_for_test_clears_cached_modules() -> None:
    configure_runtime_state_for_test(
        pyodbc_module=SimpleNamespace(name="cached-pyodbc"),
        pyodbc_import_error=ModuleNotFoundError("No module named 'pyodbc'"),
        mssql_python_module=SimpleNamespace(name="cached-mssql-python"),
        mssql_python_import_error=ModuleNotFoundError("No module named 'mssql_python'"),
        azure_identity_module=SimpleNamespace(name="cached-azure-identity"),
        azure_identity_import_error=ModuleNotFoundError("No module named 'azure.identity'"),
        azure_credentials_module=SimpleNamespace(name="cached-azure-creds"),
        azure_credentials_import_error=ModuleNotFoundError(
            "No module named 'azure.core.credentials'"
        ),
        access_token_cache={
            ("cli", "scope", "profile"): SimpleNamespace(token="token", expires_on=0)
        },
        timeout_warning_logged=True,
    )

    reset_runtime_state_for_test()

    runtime_state = get_runtime_state_for_test()
    assert runtime_state.pyodbc_module is None
    assert runtime_state.pyodbc_import_error is None
    assert runtime_state.mssql_python_module is None
    assert runtime_state.mssql_python_import_error is None
    assert runtime_state.azure_identity_module is None
    assert runtime_state.azure_identity_import_error is None
    assert runtime_state.azure_credentials_module is None
    assert runtime_state.azure_credentials_import_error is None
    assert runtime_state.access_token_cache == {}
    assert runtime_state.timeout_warning_logged is False


def test_get_pyodbc_attrs_before_credentials_caches_tokens_per_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_runtime_state_for_test()
    calls: list[str | None] = []

    def fake_access_token(credentials: SQLServerCredentials, scope: str) -> SimpleNamespace:
        calls.append(credentials.client_id)
        return SimpleNamespace(token=f"token-{credentials.client_id}", expires_on=9999999999)

    monkeypatch.setitem(
        dbt.adapters.sqlserver.sqlserver_auth.AZURE_AUTH_FUNCTIONS,
        "cli",
        fake_access_token,
    )

    first = SQLServerCredentials(
        driver="ODBC Driver 17 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        authentication="cli",
        client_id="one",
    )
    second = SQLServerCredentials(
        driver="ODBC Driver 17 for SQL Server",
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        authentication="cli",
        client_id="two",
    )

    first_attrs = get_pyodbc_attrs_before_credentials(first)
    second_attrs = get_pyodbc_attrs_before_credentials(second)

    assert len(calls) == 2
    assert first_attrs != second_attrs


def test_get_pyodbc_attrs_before_credentials_ignores_high_cardinality_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_runtime_state_for_test()
    calls: list[str | None] = []

    def fake_access_token(credentials: SQLServerCredentials, scope: str) -> SimpleNamespace:
        calls.append(credentials.client_id)
        return SimpleNamespace(token=f"token-{credentials.client_id}", expires_on=9999999999)

    monkeypatch.setitem(
        dbt.adapters.sqlserver.sqlserver_auth.AZURE_AUTH_FUNCTIONS,
        "cli",
        fake_access_token,
    )

    first = SQLServerCredentials(
        driver="ODBC Driver 17 for SQL Server",
        host="first.sql.sqlserver.net",
        database="dbt_a",
        schema="schema_a",
        authentication="cli",
        client_id="shared-client-id",
        client_secret="secret-one",
    )
    second = SQLServerCredentials(
        driver="ODBC Driver 17 for SQL Server",
        host="second.sql.sqlserver.net",
        database="dbt_b",
        schema="schema_b",
        authentication="cli",
        client_id="shared-client-id",
        client_secret="secret-two",
    )

    first_attrs = get_pyodbc_attrs_before_credentials(first)
    second_attrs = get_pyodbc_attrs_before_credentials(second)

    assert len(calls) == 1
    assert first_attrs == second_attrs


def test_get_mssql_python_returns_cached_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_mssql_python = SimpleNamespace(name="cached-mssql-python")
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        mssql_python_module=fake_mssql_python,
        mssql_python_import_error=None,
    )

    def fail_import(*args, **kwargs):
        raise AssertionError("mssql_python import should not run when cached")

    monkeypatch.setattr(builtins, "__import__", fail_import)

    assert dbt.adapters.sqlserver.sqlserver_runtime._get_mssql_python() is fake_mssql_python
    assert dbt.adapters.sqlserver.sqlserver_runtime._get_mssql_python() is fake_mssql_python


def test_get_pyodbc_raises_only_when_requested(monkeypatch: pytest.MonkeyPatch) -> None:
    reset_runtime_state_for_test()
    original_import = builtins.__import__

    def missing_pyodbc(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "pyodbc":
            raise ModuleNotFoundError("No module named 'pyodbc'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", missing_pyodbc)

    with pytest.raises(DbtRuntimeError, match="pyodbc"):
        dbt.adapters.sqlserver.sqlserver_runtime._get_pyodbc()


def test_get_mssql_python_raises_only_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_runtime_state_for_test()
    original_import = builtins.__import__

    def missing_mssql_python(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "mssql_python":
            raise ModuleNotFoundError("No module named 'mssql_python'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", missing_mssql_python)

    with pytest.raises(DbtRuntimeError, match="mssql-python"):
        dbt.adapters.sqlserver.sqlserver_runtime._get_mssql_python()


def test_open_with_mssql_python_backend_requires_optional_dependency(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.driver = None
    credentials.backend = SQLServerBackend.mssql_python

    connection = Connection(type="sqlserver", name="feature-flag-test", credentials=credentials)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        mssql_python_module=None,
        mssql_python_import_error=ModuleNotFoundError("No module named 'mssql_python'"),
    )

    with pytest.raises(DbtRuntimeError, match="mssql-python"):
        SQLServerConnectionManager.open(connection)


def _fake_retry_connection_stub(
    captured: Dict[str, Any] | None = None,
):
    def fake_retry_connection(
        cls,
        connection,
        connect,
        logger,
        retry_limit,
        retryable_exceptions,
    ):
        if captured is not None:
            captured["retry_limit"] = retry_limit
            captured["retryable_exceptions"] = retryable_exceptions
        handle = connect()
        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    return fake_retry_connection


def _fake_mssql_python_module(
    connect,
    pooling=None,
):
    if pooling is None:

        def pooling(*args, **kwargs):
            return None

    module = {
        "connect": connect,
        "OperationalError": type("OperationalError", (Exception,), {}),
        "InterfaceError": type("InterfaceError", (Exception,), {}),
        "InternalError": type("InternalError", (Exception,), {}),
    }
    if pooling is not None:
        module["pooling"] = pooling
    return SimpleNamespace(**module)


def _fake_pyodbc_module(connect):
    return SimpleNamespace(
        connect=connect,
        pooling=False,
        OperationalError=type("OperationalError", (Exception,), {}),
        InterfaceError=type("InterfaceError", (Exception,), {}),
        InternalError=type("InternalError", (Exception,), {}),
    )


def test_open_with_mssql_python_backend_enables_pooling(
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

    def fake_pooling(max_size=100, idle_timeout=600, enabled=True):
        pooling_calls.append(
            {
                "max_size": max_size,
                "idle_timeout": idle_timeout,
                "enabled": enabled,
            }
        )

    fake_module = _fake_mssql_python_module(fake_connect, fake_pooling)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        mssql_python_module=fake_module, mssql_python_import_error=None
    )
    monkeypatch.setattr(
        SQLServerConnectionManager,
        "retry_connection",
        classmethod(_fake_retry_connection_stub(captured)),
    )

    connection = Connection(type="sqlserver", name="feature-flag-test", credentials=credentials)
    opened = SQLServerConnectionManager.open(connection)

    assert opened is connection
    assert opened.handle is fake_handle
    assert opened.state == ConnectionState.OPEN

    assert captured["autocommit"] is True
    assert captured["timeout"] == 17
    assert captured["retry_limit"] == 5
    assert pooling_calls == [
        {
            "max_size": 100,
            "idle_timeout": 600,
            "enabled": True,
        }
    ]
    assert fake_handle.timeout == 23

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


def test_open_with_mssql_python_backend_fails_fast_for_pyodbc_token_auth_aliases(
    credentials: SQLServerCredentials, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials.driver = None
    credentials.backend = SQLServerBackend.mssql_python
    credentials.authentication = "cli"

    fake_module = _fake_mssql_python_module(lambda *args, **kwargs: None)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        mssql_python_module=fake_module, mssql_python_import_error=None
    )

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

    fake_module = _fake_mssql_python_module(lambda *args, **kwargs: None)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        mssql_python_module=fake_module, mssql_python_import_error=None
    )

    connection = Connection(type="sqlserver", name="feature-flag-test", credentials=credentials)

    with pytest.raises(DbtRuntimeError, match="authentication"):
        SQLServerConnectionManager.open(connection)


@pytest.mark.parametrize(
    "authentication",
    ["msi", "ActiveDirectoryMSI"],
)
def test_open_with_mssql_python_backend_supported_managed_identity_auth(
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

    fake_module = _fake_mssql_python_module(fake_connect)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        mssql_python_module=fake_module, mssql_python_import_error=None
    )
    monkeypatch.setattr(
        SQLServerConnectionManager,
        "retry_connection",
        classmethod(_fake_retry_connection_stub()),
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

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(pyodbc_module=fake_pyodbc, pyodbc_import_error=None)

    connection = Connection(type="sqlserver", name="pyodbc-test", credentials=credentials)

    with pytest.raises(DbtRuntimeError, match=match_text):
        SQLServerConnectionManager.open(connection)


def test_open_with_pyodbc_backend_still_requires_driver(
    credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials.driver = None
    credentials.backend = SQLServerBackend.pyodbc

    fake_pyodbc = _fake_pyodbc_module(lambda *args, **kwargs: None)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(pyodbc_module=fake_pyodbc, pyodbc_import_error=None)

    connection = Connection(type="sqlserver", name="pyodbc-test", credentials=credentials)

    configure_runtime_state_for_test(mssql_python_module=None)
    with pytest.raises(DbtRuntimeError, match="driver"):
        SQLServerConnectionManager.open(connection)


def test_open_with_pyodbc_backend_enables_driver_pooling(
    credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credentials.backend = SQLServerBackend.pyodbc
    credentials.encrypt = True
    credentials.trust_cert = True
    credentials.UID = "dbt_user"
    credentials.PWD = "super-secret"

    captured: Dict[str, Any] = {}

    class FakeHandle:
        def __init__(self):
            self.timeout = None

    def fake_connect(connection_string, attrs_before, autocommit, timeout):
        captured["connection_string"] = connection_string
        captured["attrs_before"] = attrs_before
        captured["autocommit"] = autocommit
        captured["timeout"] = timeout
        return FakeHandle()

    fake_pyodbc = _fake_pyodbc_module(fake_connect)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(pyodbc_module=fake_pyodbc, pyodbc_import_error=None)
    monkeypatch.setattr(
        SQLServerConnectionManager,
        "retry_connection",
        classmethod(_fake_retry_connection_stub()),
    )

    connection = Connection(type="sqlserver", name="pyodbc-test", credentials=credentials)
    opened = SQLServerConnectionManager.open(connection)

    assert opened is connection
    assert opened.state == ConnectionState.OPEN
    assert fake_pyodbc.pooling is True
    assert captured["autocommit"] is True
    assert captured["timeout"] == credentials.login_timeout
    assert "Pooling=true" in captured["connection_string"]


@pytest.mark.parametrize("flag_value", [True, False])
def test_add_begin_query_respects_dbt_sqlserver_use_dbt_transactions(
    monkeypatch: pytest.MonkeyPatch,
    flag_value: bool,
) -> None:
    manager = object.__new__(SQLServerConnectionManager)
    manager._dbt_sqlserver_use_dbt_transactions = flag_value

    add_query_calls: list[tuple[str, bool]] = []

    def fake_add_query(sql, auto_begin=True):
        add_query_calls.append((sql, auto_begin))
        return None, None

    monkeypatch.setattr(manager, "add_query", fake_add_query)

    result = manager.add_begin_query()

    if flag_value:
        assert add_query_calls == [("BEGIN TRANSACTION", False)]
        assert result == (None, None)
    else:
        assert result is None
        assert add_query_calls == []


@pytest.mark.parametrize("flag_value", [True, False])
def test_add_commit_query_respects_dbt_sqlserver_use_dbt_transactions(
    monkeypatch: pytest.MonkeyPatch,
    flag_value: bool,
) -> None:
    manager = object.__new__(SQLServerConnectionManager)
    manager._dbt_sqlserver_use_dbt_transactions = flag_value

    add_query_calls: list[tuple[str, bool]] = []

    def fake_add_query(sql, auto_begin=True):
        add_query_calls.append((sql, auto_begin))
        return None, None

    monkeypatch.setattr(manager, "add_query", fake_add_query)

    result = manager.add_commit_query()

    if flag_value:
        assert add_query_calls == [("IF @@TRANCOUNT > 0 COMMIT TRANSACTION", False)]
        assert result == (None, None)
    else:
        assert result is None
        assert add_query_calls == []


def test_rollback_handle_enabled_executes_tsql_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    handle = MagicMock()
    connection = MagicMock(spec=Connection, handle=handle)
    connection.name = "test_conn"

    monkeypatch.setattr(
        SQLServerConnectionManager,
        "_dbt_sqlserver_use_dbt_transactions",
        True,
    )

    SQLServerConnectionManager._rollback_handle(connection)

    cursor = handle.cursor.return_value
    cursor.execute.assert_called_once_with("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
    cursor.close.assert_called_once()
    handle.rollback.assert_not_called()


def test_rollback_handle_disabled_calls_handle_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    handle = MagicMock()
    connection = MagicMock(spec=Connection, handle=handle)
    connection.name = "test_conn"

    monkeypatch.setattr(
        SQLServerConnectionManager,
        "_dbt_sqlserver_use_dbt_transactions",
        False,
    )

    SQLServerConnectionManager._rollback_handle(connection)

    handle.rollback.assert_called_once()
    handle.cursor.assert_not_called()


def test_rollback_handle_enabled_exception_fires_rollback_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = MagicMock()
    handle.cursor.side_effect = RuntimeError("connection lost")
    connection = MagicMock(spec=Connection, handle=handle)
    connection.name = "test_conn"

    monkeypatch.setattr(
        SQLServerConnectionManager,
        "_dbt_sqlserver_use_dbt_transactions",
        True,
    )

    with patch("dbt.adapters.sqlserver.sqlserver_connections.fire_event") as mock_fire_event:
        SQLServerConnectionManager._rollback_handle(connection)

    mock_fire_event.assert_called_once()
    args, _ = mock_fire_event.call_args
    fired_event = args[0]
    from dbt.adapters.events.types import RollbackFailed

    assert isinstance(fired_event, RollbackFailed)
    assert fired_event.conn_name == "test_conn"


def test_rollback_handle_disabled_exception_fires_rollback_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = MagicMock()
    handle.rollback.side_effect = RuntimeError("rollback failed")
    connection = MagicMock(spec=Connection, handle=handle)
    connection.name = "test_conn"

    monkeypatch.setattr(
        SQLServerConnectionManager,
        "_dbt_sqlserver_use_dbt_transactions",
        False,
    )

    with patch("dbt.adapters.sqlserver.sqlserver_connections.fire_event") as mock_fire_event:
        SQLServerConnectionManager._rollback_handle(connection)

    mock_fire_event.assert_called_once()


class _FakeRetryableError(Exception):
    """Stand-in for a backend retryable exception in add_query retry tests."""


def _make_add_query_manager(
    monkeypatch: pytest.MonkeyPatch,
    *,
    retries: int,
    execute_side_effect: Any,
):
    """Build a manager + thread connection wired for add_query retry tests.

    Uses ``object.__new__`` (the pattern used elsewhere in this module) so no
    real connection pool is constructed; only the collaborators add_query
    touches are stubbed.
    """

    manager = object.__new__(SQLServerConnectionManager)

    cursor = MagicMock()
    cursor.execute.side_effect = execute_side_effect
    cursor.rowcount = 0

    handle = MagicMock()
    handle.cursor.return_value = cursor

    credentials = MagicMock()
    credentials.retries = retries

    connection = MagicMock()
    connection.handle = handle
    connection.credentials = credentials
    connection.transaction_open = True
    connection.name = "retry-test"

    monkeypatch.setattr(manager, "get_thread_connection", lambda: connection)

    # Isolate the retry loop from error translation / connection release, which
    # have their own tests and otherwise depend on global runtime state.
    @contextmanager
    def _passthrough_exception_handler(_sql):
        yield

    monkeypatch.setattr(manager, "exception_handler", _passthrough_exception_handler)

    return manager, connection, cursor


def test_add_query_retries_retryable_errors_until_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _connection, cursor = _make_add_query_manager(
        monkeypatch,
        retries=3,
        execute_side_effect=[_FakeRetryableError(), _FakeRetryableError(), None],
    )

    with (
        patch("dbt.adapters.sqlserver.sqlserver_connections.fire_event"),
        patch("dbt.adapters.sqlserver.sqlserver_connections.time.sleep") as mock_sleep,
    ):
        _conn, result_cursor = manager.add_query(
            "select 1", auto_begin=False, retryable_exceptions=(_FakeRetryableError,)
        )

    assert cursor.execute.call_count == 3
    assert result_cursor is cursor
    assert mock_sleep.call_count == 2


def test_add_query_honors_configured_retries_over_method_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Regression guard: a `credentials.retries > 3` branch used to ignore
    # configured values of 1-3 and fall back to a hardcoded limit of 2, so the
    # default `retries: 3` attempted a query only twice. It must now attempt
    # the query three times before giving up.
    manager, _connection, cursor = _make_add_query_manager(
        monkeypatch,
        retries=3,
        execute_side_effect=_FakeRetryableError(),
    )

    with (
        patch("dbt.adapters.sqlserver.sqlserver_connections.fire_event"),
        patch("dbt.adapters.sqlserver.sqlserver_connections.time.sleep"),
    ):
        with pytest.raises(_FakeRetryableError):
            manager.add_query(
                "select 1", auto_begin=False, retryable_exceptions=(_FakeRetryableError,)
            )

    assert cursor.execute.call_count == 3


def test_add_query_does_not_retry_when_retries_is_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _connection, cursor = _make_add_query_manager(
        monkeypatch,
        retries=1,
        execute_side_effect=_FakeRetryableError(),
    )

    with (
        patch("dbt.adapters.sqlserver.sqlserver_connections.fire_event"),
        patch("dbt.adapters.sqlserver.sqlserver_connections.time.sleep"),
    ):
        with pytest.raises(_FakeRetryableError):
            manager.add_query(
                "select 1", auto_begin=False, retryable_exceptions=(_FakeRetryableError,)
            )

    assert cursor.execute.call_count == 1


def test_add_query_does_not_retry_non_retryable_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _connection, cursor = _make_add_query_manager(
        monkeypatch,
        retries=3,
        execute_side_effect=ValueError("not retryable"),
    )

    with (
        patch("dbt.adapters.sqlserver.sqlserver_connections.fire_event"),
        patch("dbt.adapters.sqlserver.sqlserver_connections.time.sleep"),
    ):
        with pytest.raises(ValueError):
            manager.add_query(
                "select 1", auto_begin=False, retryable_exceptions=(_FakeRetryableError,)
            )

    assert cursor.execute.call_count == 1
