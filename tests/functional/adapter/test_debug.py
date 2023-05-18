from dbt.tests.adapter.dbt_debug.test_dbt_debug import BaseDebugProfileVariable
from dbt.tests.adapter.dbt_debug.test_dbt_debug import (
    TestDebugInvalidProjectPostgres as BaseDebugInvalidProject,
)
from dbt.tests.adapter.dbt_debug.test_dbt_debug import TestDebugPostgres as BaseBaseDebug


class TestDebugProfileVariableSQLServer(BaseDebugProfileVariable):
    pass


class TestDebugInvalidProjectSQLServer(BaseDebugInvalidProject):
    pass


class TestDebugSQLServer(BaseBaseDebug):
    pass
