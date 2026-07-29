"""Backend-policy helpers for the SQL Server adapter.

This module owns the backend-specific connection-string assembly and the
shared retry / error handling policy. Mutable lazy-import/runtime cache state
lives in ``sqlserver_runtime.py`` and is orchestrated by
``sqlserver_connections.py``.
"""

from __future__ import annotations

import urllib.parse
from contextlib import suppress
from typing import Any, Callable, Tuple

import dbt_common.exceptions

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sqlserver import __version__
from dbt.adapters.sqlserver.sqlserver_auth import (
    get_pyodbc_attrs_before_credentials,
    is_active_directory_authentication,
    normalize_connection_authentication,
    uses_aad_token_authentication,
)
from dbt.adapters.sqlserver.sqlserver_constants import (
    MSSQL_AUTH_ACTIVE_DIRECTORY_ACCESS_TOKEN,
    MSSQL_AUTH_ACTIVE_DIRECTORY_INTEGRATED,
    MSSQL_AUTH_ACTIVE_DIRECTORY_INTERACTIVE,
    MSSQL_AUTH_ACTIVE_DIRECTORY_MSI,
    MSSQL_AUTH_ACTIVE_DIRECTORY_PASSWORD,
    MSSQL_AUTH_ACTIVE_DIRECTORY_SERVICE_PRINCIPAL,
)
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials
from dbt.adapters.sqlserver.sqlserver_helpers import (
    _set_query_timeout_if_supported,
    bool_to_connection_string_arg,
    build_server_arg,
    format_connection_string_value,
    format_pyodbc_driver_value,
    sanitize_connection_string_for_logging,
)
from dbt.adapters.sqlserver.sqlserver_runtime import (
    _RUNTIME_STATE,
    MssqlPythonModuleProtocol,
    PyodbcModuleProtocol,
    _get_adbc,
)

logger = AdapterLogger("sqlserver")


def build_common_connection_string_parts(
    credentials: SQLServerCredentials,
    mssql_python_backend: bool,
) -> list[str]:
    """Build validated connection-string parts shared by both backends.

    Call this only after shared/backend-specific profile validation has run.
    `credentials.authentication` is canonicalized here so the backend branches
    below can compare one normalized auth label per mode.
    """

    con_str = [f"SERVER={build_server_arg(credentials)}"]
    con_str.append(f"Database={credentials.database}")

    authentication = normalize_connection_authentication(
        credentials.authentication,
        mssql_python_backend,
    )

    if is_active_directory_authentication(authentication) and (
        authentication != MSSQL_AUTH_ACTIVE_DIRECTORY_ACCESS_TOKEN
    ):
        con_str.append(f"Authentication={authentication}")

        if authentication == MSSQL_AUTH_ACTIVE_DIRECTORY_PASSWORD:
            con_str.append(
                f"UID={format_connection_string_value(credentials.UID, mssql_python_backend)}"
            )
            con_str.append(
                f"PWD={format_connection_string_value(credentials.PWD, mssql_python_backend)}"
            )
        elif authentication == MSSQL_AUTH_ACTIVE_DIRECTORY_SERVICE_PRINCIPAL:
            con_str.append(
                "UID="
                + format_connection_string_value(
                    credentials.client_id,
                    mssql_python_backend,
                )
            )
            con_str.append(
                "PWD="
                + format_connection_string_value(
                    credentials.client_secret,
                    mssql_python_backend,
                )
            )
        elif authentication == MSSQL_AUTH_ACTIVE_DIRECTORY_INTERACTIVE:
            con_str.append(
                "UID=%s"
                % format_connection_string_value(
                    credentials.UID,
                    mssql_python_backend,
                )
            )
        elif authentication == MSSQL_AUTH_ACTIVE_DIRECTORY_MSI:
            if credentials.PWD:
                raise dbt_common.exceptions.DbtRuntimeError(
                    "password is not valid with ActiveDirectoryMSI for the mssql-python backend."
                )
            if credentials.UID:
                con_str.append(
                    f"UID={format_connection_string_value(credentials.UID, mssql_python_backend)}"
                )
        elif authentication == MSSQL_AUTH_ACTIVE_DIRECTORY_INTEGRATED:
            if credentials.PWD:
                raise dbt_common.exceptions.DbtRuntimeError(
                    "password is not valid with ActiveDirectoryIntegrated"
                    " for the mssql-python backend."
                )

    elif credentials.windows_login:
        if mssql_python_backend and (credentials.UID or credentials.PWD):
            raise dbt_common.exceptions.DbtRuntimeError(
                "user/password are not valid with windows_login/trusted_connection "
                "for the mssql-python backend."
            )
        con_str.append("Trusted_Connection=yes")
    elif authentication == "sql":
        con_str.append(
            f"UID={format_connection_string_value(credentials.UID, mssql_python_backend)}"
        )
        con_str.append(
            f"PWD={format_connection_string_value(credentials.PWD, mssql_python_backend)}"
        )

    con_str.append(bool_to_connection_string_arg("encrypt", credentials.encrypt))
    con_str.append(bool_to_connection_string_arg("TrustServerCertificate", credentials.trust_cert))

    if not mssql_python_backend:
        application_name = f"dbt-{credentials.type}/{__version__.version}"
        con_str.append(f"APP={application_name}")

    return con_str


def build_pyodbc_connection_string(credentials: SQLServerCredentials) -> str:
    """Build the full pyodbc connection string used by the connection manager.

    Invariants:
        - `driver` must be specified and formatted properly (for example, enclosed
          in braces if not already).
        - Encrypted parameters and other connection attributes default to
          standard values suitable for pyodbc.

    Integration:
        Called by `SQLServerConnectionManager.open()` when the backend type is
        configured as `pyodbc`.
    """

    con_str = [f"DRIVER={format_pyodbc_driver_value(credentials.driver)}"]
    con_str.extend(build_common_connection_string_parts(credentials, mssql_python_backend=False))
    con_str.extend(
        [
            "Pooling=true",
            (
                "SQL_ATTR_TRACE=SQL_OPT_TRACE_ON"
                if credentials.trace_flag
                else "SQL_ATTR_TRACE=SQL_OPT_TRACE_OFF"
            ),
            "ConnectRetryCount=3",
            "ConnectRetryInterval=10",
        ]
    )

    return ";".join(con_str)


def build_mssql_python_connection_string(credentials: SQLServerCredentials) -> str:
    """Build the full mssql-python connection string used by the connection manager.

    Expected Inputs:
        credentials: An instance of SQLServerCredentials containing validated
        host, database, and auth details.

    Invariants:
        - Must not contain `DRIVER` or ODBC-specific tags.
        - Connection parameters are escaped specifically for the
          mssql-python parser backend.

    Integration:
        Called by `SQLServerConnectionManager.open()` when the backend type is
        configured as `mssql-python`.
    """

    con_str = build_common_connection_string_parts(credentials, mssql_python_backend=True)
    return ";".join(con_str)


def get_pyodbc_retryable_exceptions(
    credentials: SQLServerCredentials,
    pyodbc: PyodbcModuleProtocol,
) -> Tuple[type[Exception], ...]:
    """Return the pyodbc exception types that the connection manager may retry."""

    retryable_exceptions: list[type[Exception]] = [
        pyodbc.InternalError,
        pyodbc.OperationalError,
    ]

    if uses_aad_token_authentication(credentials):
        retryable_exceptions.append(pyodbc.InterfaceError)

    return tuple(retryable_exceptions)


def get_mssql_python_retryable_exceptions(
    credentials: SQLServerCredentials,
    mssql_python: MssqlPythonModuleProtocol,
) -> Tuple[type[Exception], ...]:
    """Return the mssql-python exception types that the connection manager may retry."""

    retryable_exceptions: list[type[Exception]] = [
        mssql_python.InternalError,
        mssql_python.OperationalError,
    ]

    if uses_aad_token_authentication(credentials):
        retryable_exceptions.append(mssql_python.InterfaceError)

    return tuple(retryable_exceptions)


def handle_backend_database_error(
    error: Exception,
    database_error: type[Exception] | None,
    release_connection: Callable[[], None],
) -> None:
    """Translate backend database exceptions into dbt runtime errors.

    Call this only after the caller has identified the backend-specific error
    type; non-database errors should bypass this helper.
    """

    if database_error is None or not isinstance(error, database_error):
        return

    logger.debug(f"Database error: {error}")

    with suppress(Exception):
        release_connection()

    raise dbt_common.exceptions.DbtDatabaseError(str(error).strip()) from error


def log_connection_string(connection_string: str) -> None:
    """Log a sanitized connection string for the current backend."""

    sanitized_connection_string = sanitize_connection_string_for_logging(connection_string)
    logger.debug(f"Using connection string: {sanitized_connection_string}")


def is_pyodbc_handle(handle: Any) -> bool:
    """Detect a pyodbc handle without importing pyodbc from the caller.

    ADBC handles (from ``adbc_driver_manager``) must always return False,
    even when the handle is a mock in test code.
    """

    handle_type = type(handle)
    module_name = getattr(handle_type, "__module__", "") or ""
    class_name = getattr(handle_type, "__name__", "") or ""

    if "adbc" in module_name.lower() or "adbc" in class_name.lower():
        return False

    if "pyodbc" in module_name or "pyodbc" in class_name:
        return True

    if "unittest.mock" in module_name or "mock" in class_name.lower():
        return hasattr(handle, "add_output_converter")

    return False


def _log_connected_database(credentials: SQLServerCredentials) -> None:
    logger.debug(f"Connected to db: {credentials.database}")


def _finalize_connection_handle(
    handle: Any,
    credentials: SQLServerCredentials,
) -> Any:
    """Apply conservative shared connection-handle configuration."""

    _set_query_timeout_if_supported(handle, credentials.query_timeout)
    _log_connected_database(credentials)
    return handle


def _finalize_mssql_python_handle(
    handle: Any,
    credentials: SQLServerCredentials,
) -> Any:
    """Apply mssql-python-specific post-connect policy."""

    timeout_supported = _set_query_timeout_if_supported(handle, credentials.query_timeout)
    if (
        not timeout_supported
        and credentials.query_timeout not in (None, 0)
        and _RUNTIME_STATE.take_timeout_warning()
    ):
        logger.warning(
            "Configured query_timeout=%r, but the mssql-python backend does not "
            "support per-connection query timeouts; the setting will be ignored.",
            credentials.query_timeout,
        )

    _log_connected_database(credentials)
    return handle


def _connect_mssql_python(
    mssql_python: MssqlPythonModuleProtocol,
    credentials: SQLServerCredentials,
    connection_string: str,
) -> Any:
    mssql_python.pooling(enabled=True)
    handle = mssql_python.connect(
        connection_string,
        autocommit=True,
        timeout=credentials.login_timeout,
    )
    return _finalize_mssql_python_handle(handle, credentials)


def _connect_pyodbc(
    pyodbc: PyodbcModuleProtocol,
    credentials: SQLServerCredentials,
    connection_string: str,
) -> Any:
    pyodbc.pooling = True
    attrs_before = get_pyodbc_attrs_before_credentials(credentials)

    handle = pyodbc.connect(
        connection_string,
        attrs_before=attrs_before,
        autocommit=True,
        timeout=credentials.login_timeout,
    )
    return _finalize_connection_handle(handle, credentials)


def _sanitize_adbc_uri_for_logging(uri: str) -> str:
    """Redact the password component of an ADBC URI for safe logging."""
    parsed = urllib.parse.urlparse(uri)
    if parsed.password:
        safe_netloc = parsed.netloc.replace(
            f":{parsed.password}@",
            ":***@",
        )
        sanitized = parsed._replace(netloc=safe_netloc).geturl()
        return sanitized
    return uri


def build_adbc_connection_uri(credentials: SQLServerCredentials) -> str:
    """Build a go-mssqldb URI for the ADBC backend.

    Constructs URIs in the format:
      ``sqlserver://[user:password@]host[:port]?query_params``

    - User and password are URL-encoded so special characters (``@``, ``:``,
      ``/``, etc.) are escaped per RFC 3986.
    - Named instances (host containing ``\\``) omit the port.
    - Query parameters: ``database``, ``encrypt``, ``TrustServerCertificate``,
      and optional ``connection timeout``.
    """

    host = (credentials.host or "").strip()
    port = credentials.port or 1433
    database = credentials.database or ""
    uid = credentials.UID or ""
    pwd = credentials.PWD or ""

    userinfo = f"{urllib.parse.quote(uid, safe='')}:{urllib.parse.quote(pwd, safe='')}"

    if "\\" in host:
        authority = f"{userinfo}@{host}"
    else:
        authority = f"{userinfo}@{host}:{port}"

    query_parts = [
        f"database={urllib.parse.quote(database, safe='')}",
        f"encrypt={str(credentials.encrypt).lower()}",
        f"TrustServerCertificate={str(credentials.trust_cert).lower()}",
    ]

    if credentials.login_timeout and credentials.login_timeout > 0:
        query_parts.append(f"connection timeout={credentials.login_timeout}")

    query_string = "&".join(query_parts)
    return f"sqlserver://{authority}?{query_string}"


def _connect_adbc(credentials: SQLServerCredentials, uri: str) -> Any:
    """Open an ADBC connection using the constructed go-mssqldb URI.

    The ADBC mssql driver (``"mssql"``) is the only supported driver.
    Connections are opened with ``autocommit=True`` so that DDL statements
    (``CREATE SCHEMA``, ``CREATE TABLE``, …) auto-commit without extra
    round-trips.  When ``dbt_sqlserver_use_dbt_transactions: true``, dbt
    emits explicit ``BEGIN TRANSACTION`` / ``COMMIT TRANSACTION`` SQL
    that correctly nests on top of the autocommit session -- exactly the
    same strategy used by the pyodbc backend.
    """

    sanitized = _sanitize_adbc_uri_for_logging(uri)
    logger.debug(f"Connecting to SQL Server with ADBC: {sanitized}")

    adbc_module = _get_adbc()
    return adbc_module.connect(
        driver="mssql",
        db_kwargs={"uri": uri},
        autocommit=True,
    )


def get_adbc_retryable_exceptions() -> Tuple[type[Exception], ...]:
    """Return the ADBC exception types that the connection manager may retry."""

    adbc_module = _get_adbc()
    return (
        adbc_module.InternalError,
        adbc_module.OperationalError,
    )
