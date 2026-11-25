import pytest
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.sqlserver.sqlserver_auth import is_adbc_backend
from dbt.tests.util import get_connection

DIVIDE_BY_ZERO = "SELECT 1/0 AS z INTO #masked_warning_error;"
# 8153, the only warning SQL Server sends with a SQLSTATE (01003) the drivers
# rank ahead of the error; PRINT and other 01000 messages do not mask.
NULL_AGGREGATE_WARNING = (
    "DECLARE @m int; SELECT @m = MAX(x) FROM (VALUES (CAST(NULL AS int)), (1)) v(x);"
)


class TestWarningMasksError:
    def _nested_batch_error(self, project, batch):
        with get_connection(project.adapter):
            with pytest.raises(DbtDatabaseError) as excinfo:
                project.adapter.execute(f"SET NOCOUNT ON; EXEC('{batch}');")
        return str(excinfo.value)

    def test_nested_batch_error(self, project):
        if is_adbc_backend(project.adapter.config.credentials.backend):
            pytest.skip("only pyodbc and mssql-python raise the warning in place of the error")

        masked = self._nested_batch_error(project, f"{NULL_AGGREGATE_WARNING} {DIVIDE_BY_ZERO}")
        assert "reported only a warning (SQLSTATE 01003) and lost the real error" in masked
        assert "Null value is eliminated by an aggregate" in masked

        unmasked = self._nested_batch_error(project, DIVIDE_BY_ZERO)
        assert "Divide by zero" in unmasked
        assert "lost the real error" not in unmasked
