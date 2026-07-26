import pytest

from dbt.adapters.sqlserver.sqlserver_runtime import reset_runtime_state_for_test


@pytest.fixture(autouse=True)
def _reset_sqlserver_runtime_state():
    """_RUNTIME_STATE (dbt/adapters/sqlserver/sqlserver_runtime.py) is a
    process-global module cache. Tests here stub it with fake pyodbc /
    mssql-python modules via configure_runtime_state_for_test(); without a
    reset after each test, whichever test runs last leaks its fake module
    into every test that follows in the same process — including real
    functional tests, which then try to connect through the fake stub
    instead of the real driver."""
    reset_runtime_state_for_test()
    yield
    reset_runtime_state_for_test()
