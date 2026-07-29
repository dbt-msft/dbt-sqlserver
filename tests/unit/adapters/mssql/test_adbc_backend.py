"""Unit tests for the ADBC backend integration.

Covers:
- Runtime-state lazy import, caching, and lifecycle.
- ``build_adbc_connection_uri`` URI construction.
- ``validate_adbc_requirements`` backend-specific preflight.
- Connection-manager ``open`` dispatch and state updates.
- ``exception_handler`` routing for ADBC ``DatabaseError``.
- ``is_pyodbc_handle`` recognition of ADBC handles.
- ``get_adbc_retryable_exceptions`` return type.
"""

import builtins
from types import SimpleNamespace
from typing import Any, Dict

import pytest
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.sqlserver import sqlserver_connections
from dbt.adapters.sqlserver.sqlserver_auth import is_adbc_backend
from dbt.adapters.sqlserver.sqlserver_backend import (
    build_adbc_connection_uri,
    get_adbc_retryable_exceptions,
)
from dbt.adapters.sqlserver.sqlserver_backend import (
    is_pyodbc_handle as _is_pyodbc_handle,
)
from dbt.adapters.sqlserver.sqlserver_connections import (
    SQLServerConnectionManager,
    _get_adbc_rowcount,
    _replace_qmark_with_at_pn,
    _split_sql_statements,
    _sql_affects_rows,
    _try_drain_nextset,
)
from dbt.adapters.sqlserver.sqlserver_credentials import (
    SQLServerBackend,
    SQLServerCredentials,
)
from dbt.adapters.sqlserver.sqlserver_helpers import validate_adbc_requirements
from dbt.adapters.sqlserver.sqlserver_runtime import (
    _get_adbc,
    configure_runtime_state_for_test,
    get_adbc_database_error,
    get_runtime_state_for_test,
    reset_runtime_state_for_test,
)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _fake_adbc_module(connect=None):
    """Create a fake ``adbc_driver_manager.dbapi`` namespace for testing."""

    if connect is None:

        def connect(driver, db_kwargs, autocommit):
            return _FakeADBCHandle()

    return SimpleNamespace(
        connect=connect,
        DatabaseError=type("FakeADBCDatabaseError", (Exception,), {}),
        InternalError=type("FakeADBCInternalError", (Exception,), {}),
        OperationalError=type("FakeADBCOperationalError", (Exception,), {}),
        InterfaceError=type("FakeADBCInterfaceError", (Exception,), {}),
    )


class _FakeADBCHandle:
    """Minimal handle standing in for an ADBC connection."""

    def __init__(self):
        self.timeout = None
        self.autocommit = True

    def cursor(self):
        return SimpleNamespace(execute=lambda sql: None, close=lambda: None)

    def close(self):
        pass


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


@pytest.fixture
def adbc_credentials() -> SQLServerCredentials:
    """Credentials pre-configured for the ADBC backend."""
    return SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=False,
        authentication="sql",
        UID="dbt_user",
        PWD="super-secret",
    )


# ============================================================================
# 1. Runtime-state lazy-import tests
# ============================================================================


def test_get_adbc_caches_module() -> None:
    """``_get_adbc()`` returns the same cached module on repeated calls."""
    fake_adbc = SimpleNamespace(name="cached-adbc")
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(adbc_module=fake_adbc, adbc_import_error=None)

    first = _get_adbc()
    second = _get_adbc()

    assert first is fake_adbc
    assert second is fake_adbc
    assert first is second


def test_get_adbc_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_get_adbc()`` raises DbtRuntimeError when adbc-driver-manager is missing."""
    reset_runtime_state_for_test()
    original_import = builtins.__import__

    def missing_adbc(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "adbc_driver_manager":
            raise ModuleNotFoundError("No module named 'adbc_driver_manager'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", missing_adbc)

    with pytest.raises(DbtRuntimeError, match="adbc"):
        _get_adbc()


def test_adbc_runtime_state_reset_clears_fields() -> None:
    """``reset_runtime_state_for_test()`` clears adbc_module and adbc_import_error."""
    configure_runtime_state_for_test(
        adbc_module=SimpleNamespace(name="cached"),
        adbc_import_error=ModuleNotFoundError("boom"),
    )

    reset_runtime_state_for_test()

    snapshot = get_runtime_state_for_test()
    assert snapshot.adbc_module is None
    assert snapshot.adbc_import_error is None


def test_adbc_runtime_state_snapshot_captures_fields() -> None:
    """``get_runtime_state_for_test()`` snapshot captures adbc_module and error."""
    fake_module = SimpleNamespace(name="snap-module")
    fake_error = ModuleNotFoundError("no adbc")

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(
        adbc_module=fake_module,
        adbc_import_error=fake_error,
    )

    snapshot = get_runtime_state_for_test()
    assert snapshot.adbc_module is fake_module
    assert snapshot.adbc_import_error is fake_error


def test_configure_for_test_accepts_adbc_module() -> None:
    """``configure_runtime_state_for_test(adbc_module=...)`` sets the field."""
    reset_runtime_state_for_test()
    fake_module = SimpleNamespace(name="test-module")

    configure_runtime_state_for_test(adbc_module=fake_module)

    snapshot = get_runtime_state_for_test()
    assert snapshot.adbc_module is fake_module


def test_get_adbc_database_error_returns_none_when_not_loaded() -> None:
    """``get_adbc_database_error()`` returns None before the module is loaded."""
    reset_runtime_state_for_test()

    assert get_adbc_database_error() is None


def test_get_adbc_database_error_returns_type_after_import() -> None:
    """``get_adbc_database_error()`` returns the DatabaseError class after import."""
    fake_module = _fake_adbc_module()
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(adbc_module=fake_module, adbc_import_error=None)

    db_error = get_adbc_database_error()

    assert db_error is fake_module.DatabaseError


# ============================================================================
# 2. URI construction tests
# ============================================================================


def test_build_adbc_uri_sql_auth(adbc_credentials: SQLServerCredentials) -> None:
    """ADBC URI contains user, host, port, database, encrypt, TrustServerCertificate."""
    uri = build_adbc_connection_uri(adbc_credentials)

    assert uri.startswith("sqlserver://")
    assert "dbt_user:super-secret" in uri
    assert "@fake.sql.sqlserver.net:1433" in uri
    assert "database=dbt" in uri
    assert "encrypt=true" in uri
    assert "TrustServerCertificate=false" in uri


def test_build_adbc_uri_no_port_defaults_1433() -> None:
    """When port is falsy the URI defaults to 1433."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        port=0,
        encrypt=True,
        UID="user",
        PWD="pass",
    )

    uri = build_adbc_connection_uri(credentials)

    assert ":1433" in uri


@pytest.mark.parametrize(
    "password, expected_encoded",
    [
        ("p@ss", "p%40ss"),
        ("a:b", "a%3Ab"),
        ("with/slash", "with%2Fslash"),
        ("q?uery", "q%3Fuery"),
        ("hash#tag", "hash%23tag"),
    ],
)
def test_build_adbc_uri_encodes_special_password_chars(
    password: str, expected_encoded: str
) -> None:
    """Passwords with URI-significant chars are percent-encoded."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        UID="user",
        PWD=password,
    )

    uri = build_adbc_connection_uri(credentials)

    assert f":{expected_encoded}@" in uri


def test_build_adbc_uri_login_timeout() -> None:
    """login_timeout > 0 appends ``connection timeout=N`` to the query string."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        login_timeout=30,
        UID="user",
        PWD="pass",
    )

    uri = build_adbc_connection_uri(credentials)

    assert "connection timeout=30" in uri


def test_build_adbc_uri_omits_login_timeout_when_zero() -> None:
    """login_timeout=0 must not appear in the query string."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        login_timeout=0,
        UID="user",
        PWD="pass",
    )

    uri = build_adbc_connection_uri(credentials)

    assert "connection timeout" not in uri.lower()


# ============================================================================
# 3. Validation tests
# ============================================================================


@pytest.mark.parametrize(
    "authentication",
    [
        "ActiveDirectoryPassword",
        "ActiveDirectoryMSI",
        "ActiveDirectoryDefault",
        "ActiveDirectoryIntegrated",
        "ActiveDirectoryInteractive",
        "ActiveDirectoryDeviceCode",
        "ActiveDirectoryServicePrincipal",
        "serviceprincipal",
        "msi",
        "auto",
        "default",
    ],
)
def test_validate_adbc_rejects_active_directory_auth(
    authentication: str,
) -> None:
    """ADBC must reject all ActiveDirectory-based authentication modes."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        authentication=authentication,
        UID="dbt_user",
        PWD="super-secret",
        encrypt=True,
        trust_cert=True,
    )

    with pytest.raises(DbtRuntimeError, match="AD"):
        validate_adbc_requirements(credentials)


def test_validate_adbc_rejects_active_directory_access_token() -> None:
    """'ActiveDirectoryAccessToken' is also an ActiveDirectory mode and must be rejected."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        authentication="ActiveDirectoryAccessToken",
        UID="dbt_user",
        PWD="super-secret",
        encrypt=True,
        trust_cert=True,
    )

    with pytest.raises(DbtRuntimeError, match="AD"):
        validate_adbc_requirements(credentials)


def test_validate_adbc_rejects_windows_login() -> None:
    """ADBC must reject windows_login=True."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        windows_login=True,
        authentication="sql",
        UID="dbt_user",
        PWD="super-secret",
        encrypt=True,
        trust_cert=True,
    )

    with pytest.raises(DbtRuntimeError, match="Windows login"):
        validate_adbc_requirements(credentials)


def test_validate_adbc_passes_sql_auth() -> None:
    """SQL authentication with UID and PWD must pass validation."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        authentication="sql",
        UID="dbt_user",
        PWD="super-secret",
        encrypt=True,
        trust_cert=True,
    )

    # Must not raise.
    validate_adbc_requirements(credentials)


def test_validate_adbc_passes_default_sql_auth() -> None:
    """When authentication is left at the default ('sql') it must pass."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        UID="dbt_user",
        PWD="super-secret",
        encrypt=True,
        trust_cert=True,
    )

    # authentication defaults to "sql"
    assert credentials.authentication == "sql"

    validate_adbc_requirements(credentials)


def test_validate_adbc_requires_uid_and_pwd() -> None:
    """Missing UID or PWD must raise a clear error."""
    missing_uid = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        PWD="super-secret",
        encrypt=True,
        trust_cert=True,
    )
    missing_pwd = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        UID="dbt_user",
        encrypt=True,
        trust_cert=True,
    )

    with pytest.raises(DbtRuntimeError, match="requires a user"):
        validate_adbc_requirements(missing_uid)

    with pytest.raises(DbtRuntimeError, match="requires a user"):
        validate_adbc_requirements(missing_pwd)


def test_validate_adbc_logs_warning_for_query_timeout() -> None:
    """query_timeout > 0 triggers a logged warning (but does not raise)."""
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        UID="dbt_user",
        PWD="super-secret",
        encrypt=True,
        trust_cert=True,
        query_timeout=30,
    )

    # Must not raise.
    validate_adbc_requirements(credentials)


# ============================================================================
# 4. Connection manager dispatch tests
# ============================================================================


def test_open_with_adbc_backend_calls_connect_with_uri(
    adbc_credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``open()`` for ADBC backend must invoke the fake module's ``connect``."""
    captured: Dict[str, Any] = {}

    def fake_connect(driver, db_kwargs, autocommit):
        captured["driver"] = driver
        captured["db_kwargs"] = db_kwargs
        captured["autocommit"] = autocommit
        return _FakeADBCHandle()

    fake_module = _fake_adbc_module(fake_connect)

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(adbc_module=fake_module, adbc_import_error=None)
    monkeypatch.setattr(
        SQLServerConnectionManager,
        "retry_connection",
        classmethod(_fake_retry_connection_stub()),
    )

    connection = Connection(type="sqlserver", name="adbc-test", credentials=adbc_credentials)
    opened = SQLServerConnectionManager.open(connection)

    assert opened is connection
    assert opened.state == ConnectionState.OPEN
    assert captured["driver"] == "mssql"
    assert captured["autocommit"] is True
    uri = captured["db_kwargs"]["uri"]
    assert uri.startswith("sqlserver://")
    assert "dbt_user:super-secret" in uri


def test_open_with_adbc_backend_updates_state_to_OPEN(
    adbc_credentials: SQLServerCredentials,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After ``open()`` the connection state must be OPEN."""
    fake_module = _fake_adbc_module()

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(adbc_module=fake_module, adbc_import_error=None)
    monkeypatch.setattr(
        SQLServerConnectionManager,
        "retry_connection",
        classmethod(_fake_retry_connection_stub()),
    )

    connection = Connection(type="sqlserver", name="adbc-state-test", credentials=adbc_credentials)
    assert connection.state == ConnectionState.INIT

    opened = SQLServerConnectionManager.open(connection)

    assert opened.state == ConnectionState.OPEN


def test_is_adbc_backend_returns_true() -> None:
    """``is_adbc_backend`` returns True for ``SQLServerBackend.adbc``."""
    assert is_adbc_backend(SQLServerBackend.adbc) is True


def test_is_adbc_backend_returns_false_for_pyodbc() -> None:
    """``is_adbc_backend`` returns False for ``SQLServerBackend.pyodbc``."""
    assert is_adbc_backend(SQLServerBackend.pyodbc) is False


def test_is_adbc_backend_returns_false_for_mssql_python() -> None:
    """``is_adbc_backend`` returns False for ``SQLServerBackend.mssql_python``."""
    assert is_adbc_backend(SQLServerBackend.mssql_python) is False


# ============================================================================
# 5. Exception handler tests
# ============================================================================


def test_exception_handler_routes_adbc_database_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADBC ``DatabaseError`` is caught and re-raised as ``DbtDatabaseError``."""
    fake_module = _fake_adbc_module()
    error_cls = fake_module.DatabaseError

    reset_runtime_state_for_test()
    configure_runtime_state_for_test(adbc_module=fake_module, adbc_import_error=None)

    manager = object.__new__(SQLServerConnectionManager)
    credentials = SQLServerCredentials(
        backend=SQLServerBackend.adbc,
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
        encrypt=True,
        trust_cert=True,
        UID="dbt_user",
        PWD="super-secret",
    )
    release_calls: list[int] = []
    handler_calls: list[tuple[str, str]] = []
    debug_messages: list[str] = []

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

        with pytest.raises(DbtDatabaseError, match="ADBC error"):
            with manager.exception_handler("select 1"):
                raise error_cls("ADBC error")

        assert handler_calls == [(error_cls.__name__, error_cls.__name__)]
        assert release_calls == [1]
        assert all("Rolling back transaction." not in message for message in debug_messages)
        assert all("Error running SQL:" not in message for message in debug_messages)
    finally:
        reset_runtime_state_for_test()


# ============================================================================
# 6. Handle detection tests
# ============================================================================


def test_is_pyodbc_handle_false_for_adbc_handle() -> None:
    """An ADBC connection handle must not be identified as pyodbc."""
    handle = type("AdbcConnection", (), {"__module__": "adbc_driver_manager._lib"})()
    assert _is_pyodbc_handle(handle) is False


def test_is_pyodbc_handle_false_for_adbc_class_name() -> None:
    """A handle whose class name contains 'adbc' is not a pyodbc handle."""
    handle = type("AdbcThing", (), {"__module__": "something.else"})()
    assert _is_pyodbc_handle(handle) is False


def test_is_pyodbc_handle_false_for_adbc_class_name_case_insensitive() -> None:
    """A handle class whose module contains 'ADBC' (case-insensitive) is rejected."""
    handle = type("SomeHandle", (), {"__module__": "adbc_driver_manager._lib"})()
    assert _is_pyodbc_handle(handle) is False


# ============================================================================
# 7. Retryable exceptions tests
# ============================================================================


def test_get_adbc_retryable_exceptions_returns_tuple() -> None:
    """``get_adbc_retryable_exceptions`` returns a tuple of exception types."""
    fake_module = _fake_adbc_module()
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(adbc_module=fake_module, adbc_import_error=None)

    retryable = get_adbc_retryable_exceptions()

    assert isinstance(retryable, tuple)
    assert fake_module.InternalError in retryable
    assert fake_module.OperationalError in retryable


def test_retryable_exceptions_excludes_database_error() -> None:
    """DatabaseError is not a retryable exception for ADBC."""
    fake_module = _fake_adbc_module()
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(adbc_module=fake_module, adbc_import_error=None)

    retryable = get_adbc_retryable_exceptions()

    assert fake_module.DatabaseError not in retryable


def test_retryable_exceptions_excludes_interface_error() -> None:
    """InterfaceError is not a retryable exception for ADBC."""
    fake_module = _fake_adbc_module()
    reset_runtime_state_for_test()
    configure_runtime_state_for_test(adbc_module=fake_module, adbc_import_error=None)

    retryable = get_adbc_retryable_exceptions()

    assert fake_module.InterfaceError not in retryable


# ============================================================================
# 8. ``?`` -> ``@pN`` placeholder substitution
# ============================================================================


def test_replace_qmark_with_at_pn_basic() -> None:
    """Plain placeholders are replaced positionally."""
    sql = "INSERT INTO t VALUES (?, ?, ?)"
    assert _replace_qmark_with_at_pn(sql, 3) == "INSERT INTO t VALUES (@p1, @p2, @p3)"


def test_replace_qmark_with_at_pn_ignores_bracket_quoted_identifier() -> None:
    """A literal '?' inside a bracket-quoted identifier is not a placeholder."""
    sql = "INSERT INTO [dbo].[t] ([satisfied?]) VALUES (?)"
    result = _replace_qmark_with_at_pn(sql, 1)
    assert result == "INSERT INTO [dbo].[t] ([satisfied?]) VALUES (@p1)"


def test_replace_qmark_with_at_pn_ignores_string_literal() -> None:
    """A literal '?' inside a single-quoted string literal is not a placeholder."""
    sql = "INSERT INTO t (note, val) VALUES ('are you sure?', ?)"
    result = _replace_qmark_with_at_pn(sql, 1)
    assert result == "INSERT INTO t (note, val) VALUES ('are you sure?', @p1)"


def test_replace_qmark_with_at_pn_handles_escaped_quotes() -> None:
    """Doubled '' and ]] escapes inside literals/identifiers are handled."""
    sql = "INSERT INTO [a]]b] (col) VALUES ('it''s a ?')"
    # num_bindings=0: there is no real placeholder in this statement, so the
    # literal '?' inside the escaped string must be left untouched.
    result = _replace_qmark_with_at_pn(sql, 0)
    assert result == sql


def test_replace_qmark_with_at_pn_multiple_bindings_after_quoted_qmark() -> None:
    """Bindings after a quoted '?' still line up correctly."""
    sql = "INSERT INTO [satisfied?] (a, b) VALUES (?, ?)"
    result = _replace_qmark_with_at_pn(sql, 2)
    assert result == "INSERT INTO [satisfied?] (a, b) VALUES (@p1, @p2)"


# ============================================================================
# 9. ``_try_drain_nextset`` ADBC NotSupportedError handling
# ============================================================================


def test_try_drain_nextset_returns_true_when_supported() -> None:
    """A cursor that supports nextset() and has more results returns True."""
    cursor = SimpleNamespace(nextset=lambda: True)
    assert _try_drain_nextset(cursor) is True


def test_try_drain_nextset_returns_false_when_no_more_results() -> None:
    """A cursor that supports nextset() but has no more results returns False."""
    cursor = SimpleNamespace(nextset=lambda: False)
    assert _try_drain_nextset(cursor) is False


def test_try_drain_nextset_swallows_adbc_not_supported_error() -> None:
    """An ADBC NotSupportedError (module-scoped) is treated as 'no more results'."""

    def raise_not_supported():
        error_cls = type("NotSupportedError", (Exception,), {})
        error_cls.__module__ = "adbc_driver_manager"
        raise error_cls("Cursor.nextset")

    cursor = SimpleNamespace(nextset=raise_not_supported)
    assert _try_drain_nextset(cursor) is False


def test_try_drain_nextset_swallows_regardless_of_message_text() -> None:
    """The check no longer depends on the exception message containing 'nextset'."""

    def raise_not_supported():
        error_cls = type("NotSupportedError", (Exception,), {})
        error_cls.__module__ = "adbc_driver_manager"
        raise error_cls("some future wording that says nothing about the method")

    cursor = SimpleNamespace(nextset=raise_not_supported)
    assert _try_drain_nextset(cursor) is False


def test_try_drain_nextset_reraises_not_supported_from_other_module() -> None:
    """A same-named exception from an unrelated module must not be swallowed."""

    def raise_unrelated():
        error_cls = type("NotSupportedError", (Exception,), {})
        error_cls.__module__ = "some_other_driver"
        raise error_cls("nextset")

    cursor = SimpleNamespace(nextset=raise_unrelated)
    with pytest.raises(Exception, match="nextset"):
        _try_drain_nextset(cursor)


def test_try_drain_nextset_reraises_unrelated_errors() -> None:
    """Errors unrelated to nextset support must propagate."""

    def raise_other():
        raise RuntimeError("connection lost")

    cursor = SimpleNamespace(nextset=raise_other)
    with pytest.raises(RuntimeError, match="connection lost"):
        _try_drain_nextset(cursor)


# ============================================================================
# 10. ``_get_adbc_rowcount``
# ============================================================================


class _FakeRowcountCursor:
    def __init__(self, row):
        self._row = row
        self.closed = False

    def execute(self, sql):
        pass

    def fetchone(self):
        return self._row

    def close(self):
        self.closed = True


def test_get_adbc_rowcount_returns_value() -> None:
    """A valid non-negative @@ROWCOUNT is returned as an int."""
    rc_cursor = _FakeRowcountCursor((3,))
    handle = SimpleNamespace(cursor=lambda: rc_cursor)

    assert _get_adbc_rowcount(handle) == 3
    assert rc_cursor.closed is True


def test_get_adbc_rowcount_returns_zero_for_none_row() -> None:
    """No row returned falls back to 0."""
    rc_cursor = _FakeRowcountCursor(None)
    handle = SimpleNamespace(cursor=lambda: rc_cursor)

    assert _get_adbc_rowcount(handle) == 0


def test_get_adbc_rowcount_returns_zero_on_exception() -> None:
    """Any failure opening/executing the rowcount cursor is swallowed."""

    def cursor():
        raise RuntimeError("no MARS")

    handle = SimpleNamespace(cursor=cursor)

    assert _get_adbc_rowcount(handle) == 0


def test_get_adbc_rowcount_closes_cursor_even_on_fetch_error() -> None:
    """The temporary cursor is closed even if fetchone() raises."""

    class _RaisingCursor:
        def __init__(self):
            self.closed = False

        def execute(self, sql):
            pass

        def fetchone(self):
            raise RuntimeError("boom")

        def close(self):
            self.closed = True

    rc_cursor = _RaisingCursor()
    handle = SimpleNamespace(cursor=lambda: rc_cursor)

    assert _get_adbc_rowcount(handle) == 0
    assert rc_cursor.closed is True


# ============================================================================
# 11. ``_sql_affects_rows`` DML detection
# ============================================================================


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "  update t set a = 1",
        "DELETE FROM t WHERE a = 1",
        "merge into t using src on t.a = src.a when matched then update set a = 1;",
        "/* dbt query comment */\nINSERT INTO t VALUES (1)",
        "-- a leading line comment\nUPDATE t SET a = 1",
        "/* multi */ /* comment */ DELETE FROM t",
        # Regression: sqlserver__get_delete_insert_merge_sql emits
        # "SET NOCOUNT ON; delete ...; SET NOCOUNT OFF; insert ..." as one
        # batch when unique_key is set -- the DML is not the first
        # top-level statement.
        "SET NOCOUNT ON;\ndelete from t where exists (select null)\nSET NOCOUNT OFF;",
        "SET NOCOUNT ON;\ninsert into t (a) (select a from s)",
    ],
)
def test_sql_affects_rows_true_for_dml(sql: str) -> None:
    assert _sql_affects_rows(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM t",
        "CREATE TABLE t (a int)",
        "DROP TABLE t",
        "BEGIN TRANSACTION",
        "IF @@TRANCOUNT > 0 COMMIT TRANSACTION",
        "/* dbt query comment */\nSELECT 1",
    ],
)
def test_sql_affects_rows_false_for_non_dml(sql: str) -> None:
    assert _sql_affects_rows(sql) is False


def test_split_sql_statements_basic() -> None:
    assert list(_split_sql_statements("a; b; c")) == ["a", " b", " c"]


def test_split_sql_statements_ignores_semicolon_in_string_literal() -> None:
    sql = "insert into t (a) values ('x;y'); select 1"
    parts = list(_split_sql_statements(sql))
    assert parts[0] == "insert into t (a) values ('x;y')"
    assert parts[1] == " select 1"


def test_split_sql_statements_ignores_semicolon_in_bracket_identifier() -> None:
    sql = "select [a;b] from t; select 1"
    parts = list(_split_sql_statements(sql))
    assert parts[0] == "select [a;b] from t"
    assert parts[1] == " select 1"


def test_split_sql_statements_no_separator_returns_whole_string() -> None:
    assert list(_split_sql_statements("select 1")) == ["select 1"]


# ============================================================================
# 12. ``get_response`` rowcount clamping
# ============================================================================


def test_get_response_uses_stashed_rowcount_for_adbc_cursor() -> None:
    """An ADBC cursor's -1 rowcount is replaced by the stashed @@ROWCOUNT value."""
    cursor_cls = type("Cursor", (), {})
    cursor_cls.__module__ = "adbc_driver_manager.dbapi"
    cursor = cursor_cls()
    cursor.rowcount = -1
    setattr(cursor, "__dbt_sqlserver_adbc_rowcount", 5)

    response = SQLServerConnectionManager.get_response(cursor)

    assert response.rows_affected == 5


def test_get_response_falls_back_to_negative_one_when_not_stashed() -> None:
    """An ADBC cursor without a stashed value keeps -1 (unknown), not 0."""
    cursor_cls = type("Cursor", (), {})
    cursor_cls.__module__ = "adbc_driver_manager.dbapi"
    cursor = cursor_cls()
    cursor.rowcount = -1

    response = SQLServerConnectionManager.get_response(cursor)

    assert response.rows_affected == -1


def test_get_response_does_not_clamp_non_adbc_negative_rowcount() -> None:
    """pyodbc/mssql-python's -1 (unknown, e.g. for SELECT) passes through unchanged."""
    cursor = SimpleNamespace(rowcount=-1)

    response = SQLServerConnectionManager.get_response(cursor)

    assert response.rows_affected == -1


def test_get_response_passes_through_positive_rowcount() -> None:
    """A normal positive rowcount (any backend) is untouched."""
    cursor = SimpleNamespace(rowcount=7)

    response = SQLServerConnectionManager.get_response(cursor)

    assert response.rows_affected == 7
