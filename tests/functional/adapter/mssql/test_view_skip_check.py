"""The unchanged-view check compiles the body only when a dependency changed."""

import pytest

from dbt.tests.util import get_connection, run_dbt, run_dbt_and_capture

DESCRIBE = "dm_exec_describe_first_result_set"

# Reports the gate's decision through the adapter macro, without a model build.
GATE_MACRO = """
{% macro view_needs_check(schema, identifier, body, database=none) -%}
    {% set relation = api.Relation.create(
        database=(target.database if database is none else database),
        schema=schema, identifier=identifier, type='view'
    ) %}
    {% set result = run_query(get_view_needs_check_sql(relation, body)) %}
    {% if result is not none and result.rows | length > 0 %}
        {% do log("needs_check: " ~ (result.rows[0][1] | int), info=True) %}
    {% else %}
        {% do log("needs_check: no_row", info=True) %}
    {% endif %}
{% endmacro %}
"""

select_star_view = """
{{ config(materialized='view') }}
SELECT * FROM {{ this.schema }}.gate_source
"""

synonym_view = """
{{ config(materialized='view') }}
SELECT * FROM {{ this.schema }}.gate_synonym
"""


def _gate(project, body, **kwargs):
    kwargs = {"schema": project.test_schema, "identifier": "mat_object", "body": body} | kwargs
    _, log = run_dbt_and_capture(["run-operation", "view_needs_check", "--args", str(kwargs)])
    marker = "needs_check: "
    assert marker in log, log
    return int(log.split(marker, 1)[1].split()[0])


def _modify_date(project, name="mat_object"):
    return project.run_sql(
        f"select modify_date from sys.objects "
        f"where object_id = object_id('{project.test_schema}.{name}')",
        fetch="one",
    )[0]


def _shape(project, name="mat_object"):
    rows = project.run_sql(
        f"""
        select c.name, t.name
        from sys.columns c
        join sys.types t on c.user_type_id = t.user_type_id
        where c.object_id = object_id('{project.test_schema}.{name}')
        order by c.column_id
        """,
        fetch="all",
    )
    return [(row[0], row[1]) for row in rows]


class TestQuietRunSkipsTheDescribe:
    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": select_star_view}

    def test_describe_follows_the_dependencies(self, project):
        project.run_sql(f"create table {project.test_schema}.gate_source (a varchar(10))")
        run_dbt(["run"])

        _, log = run_dbt_and_capture(["--debug", "run"])
        assert DESCRIBE not in log
        quiet = _modify_date(project)

        project.run_sql(f"alter table {project.test_schema}.gate_source add b int")
        _, log = run_dbt_and_capture(["--debug", "run"])
        assert DESCRIBE in log
        assert _shape(project) == [("a", "varchar"), ("b", "int")]

        # The refresh the check ran moved the view past its source, so the next
        # run is quiet again without anything being stored.
        _, log = run_dbt_and_capture(["--debug", "run"])
        assert DESCRIBE not in log
        assert _modify_date(project) != quiet


class TestNeedsCheckFollowsDependencies:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"view_needs_check.sql": GATE_MACRO}

    def test_gate_opens_when_a_dependency_changes(self, project):
        schema = project.test_schema
        project.run_sql(f"create table {schema}.gate_source (a varchar(10))")
        project.run_sql(f"create view {schema}.mat_object as select * from {schema}.gate_source")
        body = f"select * from {schema}.gate_source"

        assert _gate(project, body) == 0
        project.run_sql(f"alter table {schema}.gate_source add c int")
        assert _gate(project, body) == 1


class TestCannotVouchFallsBackToTheDescribe:
    """A synonym stops the walk: its target is not in the dependency closure."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {"view_needs_check.sql": GATE_MACRO}

    @pytest.fixture(scope="class")
    def models(self):
        return {"mat_object.sql": synonym_view}

    def test_view_over_a_synonym_still_checks(self, project):
        schema = project.test_schema
        project.run_sql(f"create table {schema}.gate_target (a int)")
        project.run_sql(f"create synonym {schema}.gate_synonym for {schema}.gate_target")
        run_dbt(["run"])

        assert _gate(project, f"select * from {schema}.gate_synonym") == 1
        _, log = run_dbt_and_capture(["--debug", "run"])
        assert DESCRIBE in log


class TestTextGuardOnACaseSensitiveDatabase:
    """A variable takes the database's collation, so a COLLATE on the assignment is
    lost. A case-sensitive database then misses an upper-case OPENQUERY unless each
    comparison pins its own collation."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {"view_needs_check.sql": GATE_MACRO}

    @pytest.fixture
    def case_sensitive_db(self, project):
        db = f"gate_cs_{project.test_schema}"
        with get_connection(project.adapter):
            project.adapter.execute(
                f"CREATE DATABASE [{db}] COLLATE SQL_Latin1_General_CP1_CS_AS",
                auto_begin=False,
            )
            project.adapter.execute(
                f"USE [{db}]; EXEC('CREATE VIEW dbo.gate_view AS SELECT 1 AS a')"
            )
        yield db
        with get_connection(project.adapter):
            project.adapter.execute(
                f"ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"DROP DATABASE [{db}]",
                auto_begin=False,
            )

    def test_uppercase_remote_function_is_still_refused(self, project, case_sensitive_db):
        assert (
            _gate(
                project,
                "select * from OPENQUERY([srv], 'select 1')",
                database=case_sensitive_db,
                schema="dbo",
                identifier="gate_view",
            )
            == 1
        )
