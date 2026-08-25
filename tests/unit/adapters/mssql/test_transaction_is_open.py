"""``SQLServerAdapter.transaction_is_open`` reports dbt's transaction state.

A materialization deciding how to scope its build needs to know whether the
next statement will join an existing transaction or start on its own. That is
exactly the predicate ``SQLConnectionManager.add_query`` tests before honouring
``auto_begin``, and it cannot be inferred from config - see the method's
docstring for why the "does this model have an in-transaction pre-hook?" proxy
is wrong in both directions.
"""

from unittest.mock import MagicMock

import pytest

from dbt.adapters.sqlserver.sqlserver_adapter import SQLServerAdapter


def _adapter(connection):
    """An adapter whose connection manager yields ``connection``.

    ``object.__new__`` matches the pattern used in
    test_sqlserver_connection_manager: no real pool is constructed, and only
    the one collaborator under test is stubbed.
    """
    adapter = object.__new__(SQLServerAdapter)
    connections = MagicMock()
    connections.get_thread_connection.return_value = connection
    adapter.connections = connections
    return adapter


@pytest.mark.parametrize("transaction_open", [True, False])
def test_reports_the_connection_flag(transaction_open):
    connection = MagicMock()
    connection.transaction_open = transaction_open
    assert SQLServerAdapter.transaction_is_open(_adapter(connection)) is transaction_open


def test_no_connection_is_not_open():
    """No thread connection means nothing to join, so nothing is open."""
    assert SQLServerAdapter.transaction_is_open(_adapter(None)) is False


def test_returns_a_real_bool_not_a_truthy_mock():
    """The result is branched on in Jinja, so it must be a genuine bool.

    A MagicMock attribute is truthy, which would make the False case look
    open; the implementation coerces with bool() for this reason.
    """
    connection = MagicMock()  # transaction_open is an auto-created MagicMock
    result = SQLServerAdapter.transaction_is_open(_adapter(connection))
    assert isinstance(result, bool)


def test_is_exposed_to_jinja():
    """Macros call this, so it must carry dbt's @available marker."""
    assert getattr(SQLServerAdapter.transaction_is_open, "_is_available_", False)
