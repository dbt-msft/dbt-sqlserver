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


def test_missing_thread_connection_raises_rather_than_reporting_closed():
    """get_thread_connection raises; it never returns None.

    An earlier version guarded on `connection is not None`, which read as "no
    connection means nothing is open" but could never deliver that answer
    (dbt/adapters/base/connections.py raises InvalidConnectionError instead).
    Inside a materialization a connection is always acquired before rendering,
    so this path does not arise - but it must not be described as if it did.
    """
    adapter = object.__new__(SQLServerAdapter)
    connections = MagicMock()
    connections.get_thread_connection.side_effect = RuntimeError("no connection")
    adapter.connections = connections
    with pytest.raises(RuntimeError):
        SQLServerAdapter.transaction_is_open(adapter)


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


def test_pre_hook_schema_scope_flag_is_declared():
    """The flag supplies the default for pre_hook_transaction_scope.

    Declared False so the current (transaction-spanning) behaviour stays the
    default; dbt fires a one-off behaviour-change notice while it is off, which
    is the migration signal. Flipping it to True is a later, deliberate release.
    """
    adapter = object.__new__(SQLServerAdapter)
    flags = {flag["name"]: flag for flag in SQLServerAdapter._behavior_flags.fget(adapter)}
    flag = flags["dbt_sqlserver_pre_hook_schema_scope"]
    assert flag["default"] is False
    # dbt requires description or docs_url, and prints the description when off.
    assert "pre_hook_transaction_scope" in flag["description"]
