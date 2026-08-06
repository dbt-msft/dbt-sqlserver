"""A cursor taken from ``add_select_query`` must be drained and closed.

Abandoning a cursor while the server is still streaming its result makes the
driver cancel the request. The cancel arrives as an *attention*, and every
connection this adapter opens runs under ``SET XACT_ABORT ON``
(``SQLServerConnectionManager._set_session_options``, #718), so SQL Server
answers an attention by rolling back the open transaction. An attention is not
an error, so nothing is raised and nothing is logged: the caller carries on and
fails later against relations the rollback removed.

These tests pin the invariant without a warehouse -- the cursor must be closed,
and it must have nothing left outstanding at the moment it is closed. The
end-to-end versions live with the callers that trip it: the snapshot staging
probe in tests/functional/adapter/dbt/test_transactions.py and the contract
probe in tests/functional/adapter/dbt/test_constraints.py.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.sqlserver.sqlserver_adapter import (
    SQLServerAdapter,
    _executed_name_for_system_type,
)
from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerBackend


class FakeCursor:
    """Records whether anything was still outstanding when it was closed."""

    def __init__(self, rows, description=None, extra_result_sets=0):
        self._rows = list(rows)
        self._extra_result_sets = extra_result_sets
        # PEP 249: (name, type_code, display_size, internal_size, precision,
        # scale, null_ok)
        self.description = description or [
            ("id", 4, None, None, None, None, None),
            ("payload", 12, None, None, None, None, None),
        ]
        self.closed = False
        self.rows_pending_at_close = None
        self.result_sets_pending_at_close = None
        self.fetchmany_calls = 0

    def fetchmany(self, size):
        self.fetchmany_calls += 1
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def nextset(self):
        if self._extra_result_sets:
            self._extra_result_sets -= 1
            self._rows = [("row",)] * 25
            return True
        return False

    def close(self):
        self.closed = True
        self.rows_pending_at_close = len(self._rows)
        self.result_sets_pending_at_close = self._extra_result_sets


@pytest.fixture
def adapter():
    config = MagicMock()
    config.flags = {}
    config.project_name = "test"
    config.credentials.type = "sqlserver"
    return SQLServerAdapter(config, MagicMock())


def attach(adapter, cursor):
    adapter.connections.add_select_query = MagicMock(return_value=(MagicMock(), cursor))
    adapter.connections.data_type_code_to_name = MagicMock(return_value="varchar")
    return cursor


class TestGetColumnSchemaFromQuery:
    def test_returns_the_column_schema(self, adapter):
        attach(adapter, FakeCursor(rows=[("a", "b")] * 100))

        columns = adapter.get_column_schema_from_query("select 1")

        assert [c.column for c in columns] == ["id", "payload"]

    def test_closes_the_cursor(self, adapter):
        cursor = attach(adapter, FakeCursor(rows=[("a", "b")] * 100))

        adapter.get_column_schema_from_query("select 1")

        assert cursor.closed, "cursor was abandoned instead of closed"

    def test_leaves_no_rows_outstanding_at_close(self, adapter):
        cursor = attach(adapter, FakeCursor(rows=[("a", "b")] * 25_000))

        adapter.get_column_schema_from_query("select 1")

        assert cursor.rows_pending_at_close == 0, (
            "closing with rows still pending makes the driver cancel the request, "
            "which rolls back the open transaction under XACT_ABORT ON"
        )

    def test_leaves_no_result_sets_outstanding_at_close(self, adapter):
        """Draining must re-fetch after each ``nextset()``, not just advance past it."""
        cursor = attach(adapter, FakeCursor(rows=[("a", "b")] * 50, extra_result_sets=2))

        adapter.get_column_schema_from_query("select 1")

        assert cursor.result_sets_pending_at_close == 0
        assert cursor.rows_pending_at_close == 0

    def test_fetches_in_batches_rather_than_one_row_at_a_time(self, adapter):
        cursor = attach(adapter, FakeCursor(rows=[("a", "b")] * 100_000))

        adapter.get_column_schema_from_query("select 1")

        # 100k rows in a bounded number of round trips, not 100k of them
        assert 0 < cursor.fetchmany_calls <= 50

    def test_closes_the_cursor_when_column_building_raises(self, adapter):
        cursor = attach(adapter, FakeCursor(rows=[("a", "b")] * 100))
        adapter.connections.data_type_code_to_name = MagicMock(side_effect=ValueError("boom"))

        with pytest.raises(ValueError):
            adapter.get_column_schema_from_query("select 1")

        assert cursor.closed, "cursor leaked when column building failed"

    def test_a_failure_while_discarding_is_not_raised_to_the_caller(self, adapter):
        """The caller already has its metadata; a discard problem must not become
        the error the user sees."""
        cursor = attach(adapter, FakeCursor(rows=[("a", "b")] * 100))
        cursor.fetchmany = MagicMock(side_effect=RuntimeError("driver went away"))

        columns = adapter.get_column_schema_from_query("select 1")

        assert [c.column for c in columns] == ["id", "payload"]
        assert cursor.closed


class TestDescribedTypeFollowsTheBackend:
    """``_describe_result_set`` reports the name the *executed* probe would
    have reported, and that is the driver's choice of Python class, not SQL
    Server's type. The two ODBC backends disagree on three types, so one
    fixed table cannot serve both -- a CTE-headed model with a
    uniqueidentifier column shifted its contract type when it did."""

    @pytest.mark.parametrize(
        "backend, system_type, expected",
        [
            # pyodbc: bytearray for sql_variant (uniqueidentifier is not here
            # -- it depends on a live flag, see the next test).
            (SQLServerBackend.pyodbc, "sql_variant", "varbinary"),
            # ... except datetimeoffset, whose class depends on whether the
            # -155 output converter was registered yet. No answer is better
            # than a guess: None sends the caller back to executing.
            (SQLServerBackend.pyodbc, "datetimeoffset", None),
            # mssql-python: uuid.UUID, str, datetime
            (SQLServerBackend.mssql_python, "uniqueidentifier", "uniqueidentifier"),
            (SQLServerBackend.mssql_python, "sql_variant", "varchar"),
            (SQLServerBackend.mssql_python, "datetimeoffset", "datetime2(6)"),
            # Types both drivers collapse the same way are backend-independent.
            (SQLServerBackend.pyodbc, "bigint", "int"),
            (SQLServerBackend.mssql_python, "bigint", "int"),
            (SQLServerBackend.pyodbc, "nvarchar", "varchar"),
            (SQLServerBackend.mssql_python, "nvarchar", "varchar"),
            # Unknown types fall back to executing on either backend.
            (SQLServerBackend.pyodbc, "some_future_type", None),
            (SQLServerBackend.mssql_python, "some_future_type", None),
        ],
    )
    def test_executed_name_for_system_type(self, backend, system_type, expected):
        assert _executed_name_for_system_type(system_type, backend) == expected

    def test_pyodbc_guid_follows_the_native_uuid_flag(self):
        """pyodbc hands back uuid.UUID instead of str when native_uuid is on.
        It is process-global and anything in the process can set it, so the
        describe branch reads it rather than assuming a default."""
        for native_uuid, expected in ((True, "uniqueidentifier"), (False, "varchar")):
            with patch(
                "dbt.adapters.sqlserver.sqlserver_adapter._get_pyodbc",
                return_value=SimpleNamespace(native_uuid=native_uuid),
            ):
                actual = _executed_name_for_system_type(
                    "uniqueidentifier", SQLServerBackend.pyodbc
                )
            assert actual == expected

    def test_an_unreadable_pyodbc_falls_back_to_executing(self):
        with patch(
            "dbt.adapters.sqlserver.sqlserver_adapter._get_pyodbc",
            side_effect=RuntimeError("pyodbc is not importable"),
        ):
            actual = _executed_name_for_system_type("uniqueidentifier", SQLServerBackend.pyodbc)

        assert actual is None
