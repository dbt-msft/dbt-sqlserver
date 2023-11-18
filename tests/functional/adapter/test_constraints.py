from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsRollback,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)


class TestModelConstraintsRuntimeEnforcementSQLServer(
    BaseModelConstraintsRuntimeEnforcement
):
    pass


class TestTableConstraintsColumnsEqualSQLServer(BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqualSQLServer(BaseViewConstraintsColumnsEqual):
    pass


class TestIncrementalConstraintsColumnsEqualSQLServer(
    BaseIncrementalConstraintsColumnsEqual
):
    pass


class TestTableConstraintsRuntimeDdlEnforcementSQLServer(
    BaseConstraintsRuntimeDdlEnforcement
):
    pass


class TestTableConstraintsRollbackSQLServer(BaseConstraintsRollback):
    pass


class TestIncrementalConstraintsRuntimeDdlEnforcementSQLServer(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    pass


class TestIncrementalConstraintsRollbackSQLServer(BaseIncrementalConstraintsRollback):
    pass
