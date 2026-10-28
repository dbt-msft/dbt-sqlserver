"""A view build's catalog lookups share one cached plan across views.

Read from the plan cache, which needs VIEW SERVER STATE. Every test on the server
shares the cache, so a class finds a copy made for one of its views by its schema.
Each model's name and body hold the schema too, so a copy holds it whichever of the
view's schema, name or body went into its text. The shared plan holds none of them.
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file

view_sql = """
{{ config(materialized='view') }}
SELECT '{{ this.schema }}' AS data
"""

table_sql = """
{{ config(materialized='table') }}
SELECT 1 AS data
"""


def _cached_plan_types(project, *fragments):
    """objtype of each cached plan whose text holds every fragment.

    Reads whole batches as well as statements, so a batch cached once per view is
    found too.
    """
    matches = "".join(f" and t.text like '%{fragment}%'" for fragment in fragments)
    rows = project.run_sql(
        f"""
        select cp.objtype
        from sys.dm_exec_cached_plans cp
        cross apply sys.dm_exec_sql_text(cp.plan_handle) t
        where t.text not like '%dm[_]exec[_]cached[_]plans%'{matches}
        """,
        fetch="all",
    )
    return sorted(row[0] for row in rows)


class TestViewCatalogLookupsShareOnePlan:
    @pytest.fixture(scope="class")
    def models(self, unique_schema):
        return {f"{unique_schema}_first.sql": view_sql, f"{unique_schema}_second.sql": view_sql}

    def test_lookups_share_one_plan(self, project, unique_schema):
        run_dbt(["run"])
        _, log = run_dbt_and_capture(["--debug", "run"])

        # A view is altered in place, so no build has a backup to drop.
        assert "__dbt_backup" not in log
        # Both unchanged views were checked through the shared plan, not a copy each.
        assert _cached_plan_types(project, "dbt_sqlserver_skip_check_body", unique_schema) == []
        assert "Prepared" in _cached_plan_types(project, "dbt_sqlserver_skip_check_body")

        # Replacing each view with a table drops the view through the shared lookup plan.
        write_file(table_sql, "models", f"{unique_schema}_first.sql")
        write_file(table_sql, "models", f"{unique_schema}_second.sql")
        run_dbt(["run"])
        assert _cached_plan_types(project, "sql_expression_dependencies", unique_schema) == []
        assert "Prepared" in _cached_plan_types(project, "sql_expression_dependencies")
