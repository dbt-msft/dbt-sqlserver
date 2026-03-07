import pytest
from dbt.tests.util import get_connection, run_dbt

model_sql = """
SELECT 1 AS data
"""

table_mat = """
{{
  config({
  "materialized": 'table'
  })
}}
SELECT 1 AS data
"""

view_mat = """
{{
  config({
  "materialized": 'view'
  })
}}
SELECT 1 AS data
"""

schema = """
version: 2
models:
  - name: mat_object
"""


class BaseTableView:
    def create_object(self, project, sql):
        with get_connection(project.adapter):
            project.adapter.execute(sql, fetch=True)


class TestTabletoView(BaseTableView):
    """Test if changing from a table object to a view object correctly replaces"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": view_mat, "schema.yml": schema}

    def test_passes(self, project):
        self.create_object(
            project, f"SELECT * INTO {project.test_schema}.mat_object FROM ({model_sql}) t"
        )
        run_dbt(["run"])


class TestViewtoTable(BaseTableView):
    """Test if changing from a view object to a table object correctly replaces"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": table_mat, "schema.yml": schema}

    def test_passes(self, project):
        self.create_object(project, f"CREATE VIEW {project.test_schema}.mat_object AS {model_sql}")
        run_dbt(["run"])
