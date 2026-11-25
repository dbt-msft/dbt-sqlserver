"""Only a raised warning is treated as masking an error; real errors are not."""

import pytest

from dbt.adapters.sqlserver.sqlserver_backend import (
    handle_backend_database_error,
    warning_sqlstate,
)


def test_pyodbc_real_error_is_not_a_warning() -> None:
    pyodbc = pytest.importorskip("pyodbc")
    assert warning_sqlstate(pyodbc.DataError("22012", "Divide by zero (8134)")) is None
    assert warning_sqlstate(pyodbc.Error("HY000", "general error")) is None


def test_non_driver_exception_shaped_like_a_warning_is_ignored() -> None:
    assert warning_sqlstate(RuntimeError("01003", "Warning: Null value is eliminated")) is None


def test_mssql_python_truncation_error_is_not_a_warning() -> None:
    """22001 maps to the same class as 01003, and to the same text as 01004."""
    pytest.importorskip("mssql_python")
    from mssql_python.exceptions import sqlstate_to_exception

    error = sqlstate_to_exception("22001", "String or binary data would be truncated.")
    assert warning_sqlstate(error) is None


def test_handler_leaves_a_non_database_error_to_the_caller() -> None:
    pyodbc = pytest.importorskip("pyodbc")
    release_calls: list[int] = []

    handle_backend_database_error(
        pyodbc.Error("HY000", "not a database error"),
        pyodbc.DatabaseError,
        lambda: release_calls.append(1),
    )

    assert release_calls == []
