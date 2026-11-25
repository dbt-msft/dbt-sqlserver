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
    TABLE_NAME LIKE '%__dbt_tmp%'
"""

seed_schema_yml = """
version: 2
seeds:
  - name: raw_data
    config:
      column_types:
          id: integer
          value_col: nvarchar(20)
          date_col: datetime2(6)
"""

seed_csv = """id,data,date_col
1,1,2024-01-01
2,1,2024-01-01
3,1,2024-01-01"""

incremental_sql = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('raw_data') }} )

{% if is_incremental()  %}

SELECT id,
       data,
       date_col
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       data,
       date_col
FROM source_data where id <= 1

{% endif %}
"""


class BaseTempRelationCleanup:
    view_name = "__dbt_tmp_vw"

    def validate_temp_objects(self, project):
        with get_connection(project.adapter):
            result, table = project.adapter.execute(
                validation_sql.format(
                    database=project.database, schema=project.created_schemas[0]
                ),
                fetch=True,
            )
        assert len(table.rows) == 0


class TestTempRelationCleanup(BaseTempRelationCleanup):
    """
    This tests to validate that the temporary relations,
    created by the `create_table` statement is cleaned up after a set of runs.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model,
            "schema.yml": model_yml,
        }

    def test_drops_temp_view_object(self, project):
        run_dbt(["run"])

        self.validate_temp_objects(project)


unit_test_yml = """
unit_tests:
  - name: ut_pass
    model: table_model
    given: []
    expect:
      rows: [{data: 1}]
  - name: ut_fail
    model: table_model
    given: []
    expect:
      rows: [{data: 2}]
  - name: ut_error
    model: error_model
    given: []
    expect:
      rows: [{data: 1}]
  - name: ut_compile_error
    model: table_model
    given: []
    expect:
      rows: [{nope: 1}]
"""

# Divides by zero only when the rows are read, so the unit test's fixture table
# (built from the empty subquery) is created before the comparison raises.
error_model = """
SELECT 1 / (data - data) AS data FROM (SELECT 1 AS data) AS source
"""

unit_test_views_sql = """
SELECT TABLE_NAME
FROM {database}.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = '{schema}'
    AND (TABLE_NAME LIKE 'testview[_]%' OR TABLE_NAME LIKE 'expectedview[_]%')
"""


class BaseUnitTestTempCleanup(BaseTempRelationCleanup):
    """A unit test's fixture table and comparison views are dropped whether the
    test passes, fails, or errors."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model,
            "error_model.sql": error_model,
            "schema.yml": model_yml,
            "unit_tests.yml": unit_test_yml,
        }

    def test_drops_fixture_table(self, project):
        run_dbt(["run", "--select", "table_model"])
        results = run_dbt(["test", "--select", "test_type:unit"], expect_pass=False)
        assert sorted((r.node.name, str(r.status)) for r in results) == [
            ("ut_compile_error", "error"),
            ("ut_error", "error"),
            ("ut_fail", "fail"),
            ("ut_pass", "pass"),
        ]
        [error_result] = [r for r in results if r.node.name == "ut_error"]
        assert "Divide by zero" in error_result.message

        self.validate_temp_objects(project)
        with get_connection(project.adapter):
            _, table = project.adapter.execute(
                unit_test_views_sql.format(
                    database=project.database, schema=project.created_schemas[0]
                ),
                fetch=True,
            )
        assert [row[0] for row in table.rows] == []


class TestUnitTestTempCleanup(BaseUnitTestTempCleanup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": True}}


class TestUnitTestTempCleanupAutocommit(BaseUnitTestTempCleanup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": False}}


class TestIncrementalTempCleanup(BaseTempRelationCleanup):
    """Tests if the `dbt_tmp` views are properly cleaned up in an incremental model"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_data.csv": seed_csv,
            "schema.yml": seed_schema_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": incremental_sql,
            "schema.yml": model_yml,
        }

    def test_drops_temp_view_object(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["run"])

        self.validate_temp_objects(project)
