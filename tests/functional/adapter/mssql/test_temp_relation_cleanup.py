import pytest
from dbt.tests.util import get_connection, run_dbt

table_model = """
{{
config({
  "materialized": 'table'
})
}}

SELECT 1 as data
"""

model_yml = """
version: 2
models:
  - name: table_model
"""

validation_sql = """
SELECT
    *
FROM
    {database}.INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_SCHEMA = '{schema}'
    AND
    TABLE_NAME LIKE '%__dbt_tmp_vw'
"""


class TestTempRelationCleanup:
    """
    This tests to validate that the temporary relations,
    created by the `create_table` statement is cleaned up after a set of runs.
    """

    view_name = "__dbt_tmp_vw"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model,
            "schema.yml": model_yml,
        }

    def test_drops_temp_view_object(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        with get_connection(project.adapter):
            result, table = project.adapter.execute(
                validation_sql.format(
                    database=project.database, schema=project.created_schemas[0]
                ),
                fetch=True,
            )
        assert len(table.rows) == 0
