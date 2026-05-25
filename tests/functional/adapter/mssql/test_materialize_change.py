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

invalid_view_mat = """
{{
    config({
    "materialized": 'view'
    })
}}
SELECT * FROM missing_relation
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


class TestTabletoViewRollback(BaseTableView):
    """Test that a failed table to view replacement leaves the original table intact."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": invalid_view_mat, "schema.yml": schema}

    def test_existing_table_is_preserved(self, project):
        self.create_object(
            project, f"SELECT * INTO {project.test_schema}.mat_object FROM ({model_sql}) t"
        )

        failing_results = run_dbt(["run"], expect_pass=False)
        assert len(failing_results) == 1

        rows = project.run_sql(f"select * from {project.test_schema}.mat_object", fetch="all")
        assert len(rows) == 1
        assert rows[0][0] == 1


class TestTabletoViewPreservesGrants(BaseTableView):
    """Test that grants on the existing table are preserved on the replaced view."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": view_mat, "schema.yml": schema}

    def test_public_select_grant_survives_swap(self, project):
        self.create_object(
            project, f"SELECT * INTO {project.test_schema}.mat_object FROM ({model_sql}) t"
        )
        project.run_sql(
            f"""grant select, insert, update, delete
                on object::{project.test_schema}.mat_object to public"""
        )

        run_dbt(["run"])

        grant_count = project.run_sql(
            f"""
                        select count(*)
                        from sys.database_permissions pe
                        join sys.objects o on pe.major_id = o.object_id
                        join sys.schemas s on o.schema_id = s.schema_id
                        join sys.database_principals pr
                            on pe.grantee_principal_id = pr.principal_id
                        where s.name = '{project.test_schema}'
                            and o.name = 'mat_object'
                            and pe.permission_name in ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
            """,
            fetch="one",
        )
        assert grant_count[0] == 4


class TestViewMaterializationNoOp(BaseTableView):
    """Test that rerunning an unchanged view avoids altering the view."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": view_mat, "schema.yml": schema}

    def test_unchanged_view_does_not_alter(self, project):
        self.create_object(project, f"CREATE VIEW {project.test_schema}.mat_object AS {model_sql}")

        before_modify_date = project.run_sql(
            f"""
            select modify_date
            from sys.objects o
            join sys.schemas s on o.schema_id = s.schema_id
            where upper(s.name) = upper('{project.test_schema}')
              and upper(o.name) = upper('mat_object')
            """,
            fetch="one",
        )[0]

        results = run_dbt(["run"])
        assert len(results) == 1

        after_modify_date = project.run_sql(
            f"""
            select modify_date
            from sys.objects o
            join sys.schemas s on o.schema_id = s.schema_id
            where upper(s.name) = upper('{project.test_schema}')
              and upper(o.name) = upper('mat_object')
            """,
            fetch="one",
        )[0]

        assert after_modify_date == before_modify_date


class TestViewtoTable(BaseTableView):
    """Test if changing from a view object to a table object correctly replaces"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": table_mat, "schema.yml": schema}

    def test_passes(self, project):
        self.create_object(project, f"CREATE VIEW {project.test_schema}.mat_object AS {model_sql}")
        run_dbt(["run"])
