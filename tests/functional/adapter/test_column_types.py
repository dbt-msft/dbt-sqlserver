import pytest

from dbt.tests.adapter.column_types.test_column_types import (
    BaseColumnTypes,
    model_sql,
    schema_yml,
)


class TestColumnTypesSQLServer(BaseColumnTypes):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql, "schema.yml": schema_yml}

    def test_run_and_test(self, project):
        self.run_and_test()
