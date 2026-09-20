"""A query error must reach the user as that error.

A CTE-headed query cannot be wrapped as ``select * from (...) where 1 = 0``,
so its column shape is read with ``sp_describe_first_result_set`` instead.
That describe compiles the query, so a model naming a column that does not
exist fails there rather than at its build. Handling that failure closes the
connection, so falling back to executing the query reports "Attempt to use a
closed connection" and the Msg 207 survives only at debug level - which is not
where someone reading a scheduler's task log is looking, and retrying cannot
help because a missing column is not transient.
"""

import contextlib

import pytest

from dbt.tests.util import run_dbt_and_capture

upstream_sql = """
{{ config(materialized="table") }}
select 1 as id, 'a' as present_column
"""

# CTE-headed, so the shape is described rather than executed.
bad_column_model_sql = """
{{ config(materialized="table") }}
with source as (
    select * from {{ ref('upstream') }}
)
select id, absent_column from source
"""

bad_column_schema_yml = """
version: 2
models:
  - name: bad_column
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
      - name: absent_column
        data_type: varchar(10)
"""


class TestADescribeErrorNamesTheColumn:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": upstream_sql,
            "bad_column.sql": bad_column_model_sql,
            "schema.yml": bad_column_schema_yml,
        }

    def test_an_invalid_column_is_reported_as_an_invalid_column(self, project):
        _, output = run_dbt_and_capture(["run"], expect_pass=False)

        assert "absent_column" in output, (
            "the error did not name the column the model got wrong; it was "
            f"swallowed by the describe fallback. Output:\n{output}"
        )
        assert "Invalid column name" in output
        assert "closed connection" not in output


class TestADeclinedDescribeStillFallsBackToExecuting:
    """sp_describe_first_result_set declines some queries that execute
    perfectly well, and those must still get their columns by executing.

    SET STATISTICS XML ON is a dependable way to make it decline: every
    describe then fails with Msg 11541 while the query itself runs normally. A
    mocked describe cannot stand in for it, because what broke the fallback was
    the real error path -- handling a raised describe error closes the
    connection the fallback then executes on.
    """

    def test_the_fallback_returns_the_columns(self, project):
        sql = "with q as (select 1 as id, cast('x' as varchar(10)) as t) select * from q"

        with project.adapter.connection_named("_probe"):
            project.adapter.execute("set statistics xml on")
            try:
                assert project.adapter._describe_result_set(sql) is None, (
                    "the describe did not decline, so this no longer exercises the fallback"
                )
                columns = project.adapter.get_column_schema_from_query(sql)
            finally:
                with contextlib.suppress(Exception):
                    project.adapter.execute("set statistics xml off")

        assert [c.column for c in columns] == ["id", "t"]
