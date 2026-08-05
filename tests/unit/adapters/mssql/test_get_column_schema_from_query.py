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

from unittest.mock import MagicMock

import pytest

from dbt.adapters.sqlserver.sqlserver_adapter import SQLServerAdapter


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
