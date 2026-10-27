"""A view build's catalog lookups share one cached plan across views.

Read from the plan cache, which needs VIEW SERVER STATE. The plans are shared by
every test on the server, so a class finds its own through its schema name: a
copy per view holds the name in its text, and the shared plan does not.
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file

view_sql = """
{{ config(materialized='view') }}
SELECT 1 AS data
"""

table_sql = """
{{ config(materialized='table') }}
SELECT 1 AS data
"""


def _cached_plan_types(project, *fragments):
    """objtype of each cached statement whose batch text holds every fragment."""
    matches = "".join(f" and t.text like '%{fragment}%'" for fragment in fragments)
    rows = project.run_sql(
        f"""
        select cp.objtype
        from sys.dm_exec_query_stats qs
        join sys.dm_exec_cached_plans cp on cp.plan_handle = qs.plan_handle
        cross apply sys.dm_exec_sql_text(qs.sql_handle) t
        where t.text not like '%dm[_]exec[_]query[_]stats%'{matches}
        """,
        fetch="all",
    )
    return sorted(row[0] for row in rows)


class TestViewCatalogLookupsShareOnePlan:
    @pytest.fixture(scope="class")
    def models(self):
        return {"first.sql": view_sql, "second.sql": view_sql}

    def test_lookups_share_one_plan(self, project, unique_schema):
        run_dbt(["run"])
        _, log = run_dbt_and_capture(["--debug", "run"])

        # A view is altered in place, so no build has a backup to drop.
        assert "__dbt_backup" not in log
        # Both unchanged views were checked through the shared plan, not a copy each.
        assert _cached_plan_types(project, "dbt_sqlserver_skip_check_body", unique_schema) == []
        assert "Prepared" in _cached_plan_types(project, "dbt_sqlserver_skip_check_body")

        # Replacing each view with a table drops the view through the shared lookup plan.
        write_file(table_sql, "models", "first.sql")
        write_file(table_sql, "models", "second.sql")
        run_dbt(["run"])
        assert _cached_plan_types(project, "sql_expression_dependencies", unique_schema) == []
        assert "Prepared" in _cached_plan_types(project, "sql_expression_dependencies")
