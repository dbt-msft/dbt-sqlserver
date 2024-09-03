from dbt.tests.adapter.dbt_debug.test_dbt_debug import (
    BaseDebugInvalidProjectPostgres,
    BaseDebugPostgres,
)


class TestDebugProfileVariable(BaseDebugPostgres):
    pass


class TestDebugInvalidProject(BaseDebugInvalidProjectPostgres):
    pass
