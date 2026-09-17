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
