import pytest

from dbt.tests.util import get_connection, run_dbt, run_dbt_and_capture, write_file

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

# Same body with and without a leading comment. Removing the comment leaves the
# new body as a *suffix* of the stored definition - the case the old endswith()
# skip test got wrong, skipping the rebuild so the change never reached the db.
view_with_leading_comment = """
{{ config(materialized='view') }}
-- leading_marker_comment
SELECT 1 AS data
"""

view_without_leading_comment = """
{{ config(materialized='view') }}
SELECT 1 AS data
"""

# Two bodies that differ only by the case of a string literal. Lowercasing before
# comparing (as the old code did) would treat these as identical and skip the
# rebuild - a correctness bug, not just a missed comment.
view_literal_upper = """
{{ config(materialized='view') }}
SELECT 'ABC' AS source
"""

view_literal_lower = """
{{ config(materialized='view') }}
SELECT 'abc' AS source
"""

# A view over a table created outside dbt, so the table survives between runs and
# can change shape underneath the view. `select *` is expanded and cached at
# CREATE/ALTER VIEW time, which is what the skip path has to keep in sync.
select_star_view = """
{{ config(materialized='view') }}
SELECT * FROM {{ this.schema }}.refresh_source
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
        project.run_sql(f"""grant select, insert, update, delete
                on object::{project.test_schema}.mat_object to public""")

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
    """Test that rerunning an unchanged view avoids rebuilding the view.

    ``modify_date`` used to be the signal here, but the skip path now runs
    ``sp_refreshview`` (see TestSkippedViewRefreshesSelectStarColumns), which bumps
    ``modify_date`` just as an ``ALTER`` would - so that column no longer separates a
    skip from a rebuild. The emitted SQL does: a skip issues ``sp_refreshview`` and
    no ``CREATE OR ALTER VIEW``, and leaves the stored definition byte-identical.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": view_mat, "schema.yml": schema}

    def test_unchanged_view_does_not_alter(self, project):
        self.create_object(project, f"CREATE VIEW {project.test_schema}.mat_object AS {model_sql}")

        before_definition = _stored_view_definition(project)

        results, log_output = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1

        emitted_sql = log_output.lower()
        assert "sp_refreshview" in emitted_sql
        assert "create or alter view" not in emitted_sql

        assert _stored_view_definition(project) == before_definition


class TestViewtoTable(BaseTableView):
    """Test if changing from a view object to a table object correctly replaces"""

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": table_mat, "schema.yml": schema}

    def test_passes(self, project):
        self.create_object(project, f"CREATE VIEW {project.test_schema}.mat_object AS {model_sql}")
        run_dbt(["run"])


def _stored_view_definition(project):
    """The whole stored CREATE ... VIEW ... AS <body> statement, as SQL Server keeps it."""
    return project.run_sql(
        f"select object_definition(object_id('{project.test_schema}.mat_object'))",
        fetch="one",
    )[0]


class TestViewLeadingTextRemovalReachesDatabase(BaseTableView):
    """Removing text from the *start* of a view body must rebuild the view.

    The old skip test compared with ``normalized_definition.endswith(normalized_sql)``.
    The stored definition is the whole statement while the model is only the body,
    so any edit whose new body is a tail of the old one (e.g. deleting a leading
    comment) satisfied endswith() and was silently skipped - PASS, but the change
    never reached the database, and --full-refresh did not fix it.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": view_with_leading_comment, "schema.yml": schema}

    def test_removal_of_leading_comment_lands(self, project):
        run_dbt(["run"])
        assert "leading_marker_comment" in _stored_view_definition(project)

        # Delete the leading comment - the new body is now a suffix of the old.
        write_file(view_without_leading_comment, "models", "mat_object.sql")
        results = run_dbt(["run"])
        assert len(results) == 1

        assert "leading_marker_comment" not in _stored_view_definition(project)


class TestViewLiteralCaseChangeRebuilds(BaseTableView):
    """A change confined to the case of a string literal must rebuild the view.

    The old skip test lowercased both sides before comparing, so ``'ABC'`` and
    ``'abc'`` looked identical and the rebuild was skipped - a correctness bug,
    since the two views return different data. The exact comparison rebuilds.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": view_literal_upper, "schema.yml": schema}

    def test_case_only_change_lands(self, project):
        run_dbt(["run"])
        assert (
            project.run_sql(f"select source from {project.test_schema}.mat_object", fetch="one")[0]
            == "ABC"
        )

        write_file(view_literal_lower, "models", "mat_object.sql")
        results = run_dbt(["run"])
        assert len(results) == 1

        assert (
            project.run_sql(f"select source from {project.test_schema}.mat_object", fetch="one")[0]
            == "abc"
        )


def _view_columns(project):
    """The view's cached column list, in order, as SQL Server currently reports it."""
    rows = project.run_sql(
        f"""
        select c.name
        from sys.columns c
        where c.object_id = object_id('{project.test_schema}.mat_object')
        order by c.column_id
        """,
        fetch="all",
    )
    return [row[0] for row in rows]


class TestSkippedViewRefreshesSelectStarColumns(BaseTableView):
    """A skipped rebuild must still re-derive a cached ``select *`` column list.

    SQL Server expands an unqualified ``select *`` at CREATE/ALTER VIEW time and caches
    the result. When the model SQL is unchanged the CREATE is skipped, so a column added
    to a referenced table never reached the view: it kept serving the old shape - old
    names in old positions - while every dbt run reported success. The skip path runs
    ``sp_refreshview``, which re-derives that metadata without any DDL on the view.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": select_star_view, "schema.yml": schema}

    def test_added_column_reaches_skipped_view(self, project):
        self.create_object(project, f"CREATE TABLE {project.test_schema}.refresh_source (a int)")

        run_dbt(["run"])
        assert _view_columns(project) == ["a"]

        project.run_sql(f"alter table {project.test_schema}.refresh_source add b int")

        # The model SQL is unchanged, so the CREATE/ALTER is skipped ...
        results, log_output = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "create or alter view" not in log_output.lower()

        # ... but the view still reports the table's current shape.
        assert _view_columns(project) == ["a", "b"]
