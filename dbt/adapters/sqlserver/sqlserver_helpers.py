"""Shared backend and connection-string helpers for the SQL Server adapter.

Authentication constants and normalization live in ``sqlserver_constants``
and ``sqlserver_auth`` so this module can stay focused on connection-string
validation, formatting, and logging helpers.
"""

from __future__ import annotations

import datetime as dt
import numbers
import struct
from typing import TYPE_CHECKING, Any, Optional

import dbt_common.exceptions

from dbt.adapters.sqlserver.sqlserver_auth import (
    is_active_directory_authentication,
    is_mssql_python_backend,
    normalize_authentication_key,
    normalize_connection_authentication,
)
from dbt.adapters.sqlserver.sqlserver_constants import (
    MSSQL_PYTHON_UNSUPPORTED_AUTHENTICATIONS,
    SENSITIVE_CONNECTION_STRING_KEYS,
)

if TYPE_CHECKING:
    from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials


def validate_connection_requirements(credentials: SQLServerCredentials) -> None:
    """Connection-manager preflight for shared profile fields.

    Invariants:
        - `host`, `database`, and `schema` fields must not be empty or blank.
        - `encrypt` and `trust_cert` fields must not be None.
        - `authentication` is required unless `windows_login` is True.
        - `windows_login` and ActiveDirectory-based authentication are mutually exclusive.
        - `query_timeout` is normalized into a non-negative integer.

    Integration:
        This preflight validator runs immediately after credentials coercion and before
        backend-specific builders (`build_mssql_python_connection_string` or
        `build_pyodbc_connection_string`) or backend-specific requirement checks.
        It ensures consistent base states.
    """

    for name, value in (
        ("host", credentials.host),
        ("database", credentials.database),
        ("schema", credentials.schema),
    ):
        if value is None or not str(value).strip():
            raise dbt_common.exceptions.DbtRuntimeError(
                f"The `{name}` profile field is required for SQL Server connections."
            )

    normalized = normalize_connection_authentication(
        credentials.authentication,
        is_mssql_python_backend(credentials.backend),
    )
    credentials.query_timeout = normalize_query_timeout(credentials.query_timeout)
    if credentials.windows_login:
        if normalized and is_active_directory_authentication(normalized):
            raise dbt_common.exceptions.DbtRuntimeError(
                "windows_login/trusted_connection cannot be combined with ActiveDirectory "
                "authentication. Remove `authentication` or disable `windows_login`."
            )
    elif not normalized:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `authentication` profile field is required for SQL Server connections."
        )

    if credentials.encrypt is None:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `encrypt` profile field is required for SQL Server connections."
        )
    if credentials.trust_cert is None:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `trust_cert` profile field is required for SQL Server connections."
        )


def validate_pyodbc_requirements(credentials: SQLServerCredentials) -> None:
    """Backend-specific validation for the legacy pyodbc connection path."""

    driver = credentials.driver
    if driver is None or not driver.strip():
        raise dbt_common.exceptions.DbtRuntimeError(
            "The pyodbc backend requires a SQL Server ODBC driver name "
            "in the `driver` profile field."
        )


def validate_mssql_python_requirements(credentials: SQLServerCredentials) -> None:
    """Backend-specific validation for the mssql-python connection path."""

    authentication = normalize_connection_authentication(credentials.authentication, True)

    if authentication in MSSQL_PYTHON_UNSUPPORTED_AUTHENTICATIONS:
        raise dbt_common.exceptions.DbtRuntimeError(
            f"Authentication '{authentication}' is currently only supported by the pyodbc backend "
            "in this adapter. "
            "Use `backend: pyodbc` or use a connection-string-supported "
            "authentication mode such as "
            "`sql`, `ActiveDirectoryPassword`, `ActiveDirectoryInteractive`, "
            "`ActiveDirectoryIntegrated`, `ActiveDirectoryMSI`, "
            "`ActiveDirectoryDeviceCode`, or `ActiveDirectoryDefault`."
        )


def normalize_connection_string_key(key: str) -> str:
    """Normalize a connection-string key for secret-field lookups."""

    return normalize_authentication_key(key)


def split_connection_string_parts(connection_string: str) -> list[str]:
    """Split a SQL Server connection string into normalized segments."""
    parts: list[str] = []
    current: list[str] = []
    in_braces = False
    index = 0

    while index < len(connection_string):
        char = connection_string[index]

        if char == ";" and not in_braces:
            if segment := "".join(current).strip():
                parts.append(segment)
            current = []
        else:
            current.append(char)
            start = index + 1

            if char == "{" and not in_braces and "}" in connection_string[start:]:
                in_braces = True
            elif char == "}" and in_braces:
                if index + 1 < len(connection_string) and connection_string[index + 1] == "}":
                    current.append("}")
                    index += 1
                else:
                    in_braces = False

        index += 1

    if segment := "".join(current).strip():
        parts.append(segment)
    return parts


def escape_connection_string_value(value: Optional[str]) -> str:
    text = "" if value is None else str(value)
    if text.startswith(" ") or text.endswith(" ") or any(ch in text for ch in ";{}"):
        return "{" + text.replace("}", "}}") + "}"
    return text


def bool_to_connection_string_arg(key: str, value: Optional[bool]) -> str:
    return f"{key}={'Yes' if value else 'No'}"


def normalize_query_timeout(query_timeout: Any) -> int:
    """Normalize query timeouts and fail fast on invalid negative values.

    Accepts integers and integer-like strings so config parsing can hand this
    helper raw values without leaking type quirks into the connection layer.
    """

    if query_timeout is None:
        return 0
    if isinstance(query_timeout, bool):
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `query_timeout` profile field must be a non-negative integer."
        )

    if isinstance(query_timeout, numbers.Integral):
        normalized = int(query_timeout)
    elif isinstance(query_timeout, str):
        try:
            normalized = int(query_timeout)
        except ValueError as exc:
            raise dbt_common.exceptions.DbtRuntimeError(
                "The `query_timeout` profile field must be a non-negative integer."
            ) from exc
    else:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `query_timeout` profile field must be a non-negative integer."
        )

    if normalized < 0:
        raise dbt_common.exceptions.DbtRuntimeError(
            "The `query_timeout` profile field must be a non-negative integer."
        )

    return normalized


def build_server_arg(credentials: SQLServerCredentials) -> str:
    """Build the `SERVER` token, preserving named instances without a port."""

    host = (credentials.host or "").strip()
    port = credentials.port

    if "\\" in host:
        return host

    return f"{host},{port}" if port else host


def format_connection_string_value(value: Optional[str], mssql_python_backend: bool) -> str:
    """Format a connection-string value for the requested backend."""

    if mssql_python_backend:
        return escape_connection_string_value(value)
    return "{" + ("" if value is None else value) + "}"


def format_pyodbc_driver_value(value: Optional[str]) -> str:
    """Format a pyodbc driver value without double-wrapping explicit braces."""

    text = "" if value is None else str(value)
    if len(text) >= 2 and text.startswith("{") and text.endswith("}"):
        return text
    return "{" + text + "}"


def sanitize_connection_string_for_logging(connection_string: str) -> str:
    """Redact sensitive connection-string fields while preserving structure."""

    sanitized = []
    for part in split_connection_string_parts(connection_string):
        if "=" in part:
            key, _value = part.split("=", 1)
            normalized_key = normalize_connection_string_key(key.strip())
            if normalized_key in SENSITIVE_CONNECTION_STRING_KEYS:
                sanitized.append(f"{key.strip()}=***")
                continue
        sanitized.append(part)
    return ";".join(sanitized)


def byte_array_to_datetime(value: bytes) -> dt.datetime:
    """
    Converts a DATETIMEOFFSET byte array to a timezone-aware datetime object

    Parameters
    ----------
    value : buffer
        A binary value conforming to SQL_SS_TIMESTAMPOFFSET_STRUCT

    Returns
    -------
    out : datetime

    Source
    ------
    SQL_SS_TIMESTAMPOFFSET datatype and SQL_SS_TIMESTAMPOFFSET_STRUCT layout:
    https://learn.microsoft.com/sql/relational-databases/native-client-odbc-date-
    time/data-type-support-for-odbc-date-and-time-improvements
    """
    # unpack 20 bytes of data into a tuple of 9 values
    tup = struct.unpack("<6hI2h", value)

    # construct a datetime object
    return dt.datetime(
        year=tup[0],
        month=tup[1],
        day=tup[2],
        hour=tup[3],
        minute=tup[4],
        second=tup[5],
        microsecond=tup[6] // 1000,  # https://bugs.python.org/issue15443
        tzinfo=dt.timezone(dt.timedelta(hours=tup[7], minutes=tup[8])),
    )


def _set_query_timeout_if_supported(handle: Any, query_timeout: Any) -> bool:
    """Normalize and apply `query_timeout`; return False when the handle lacks support."""

    query_timeout = normalize_query_timeout(query_timeout)
    if query_timeout == 0:
        return True

    try:
        handle.timeout = query_timeout
    except AttributeError:
        return False

    return True
