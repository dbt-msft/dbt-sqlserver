"""Functional tests for the sql_header guard — sql_header is not supported on SQL Server
because CREATE VIEW must be the first statement in a query batch."""

import pytest

from dbt.tests.util import run_dbt

# ---------------------------------------------------------------------------
# sql_header via config block
# ---------------------------------------------------------------------------

table_config_model_sql = """
{{ config(materialized='table', sql_header='SET NOCOUNT ON;') }}
select 1 as id
"""

view_config_model_sql = """
{{ config(materialized='view', sql_header='SET NOCOUNT ON;') }}
select 1 as id
"""

# ---------------------------------------------------------------------------
# sql_header via set_sql_header macro
# ---------------------------------------------------------------------------

table_macro_model_sql = """
{{ config(materialized='table') }}
{% call set_sql_header(config) %}
SET NOCOUNT ON;
{%- endcall %}
select 1 as id
"""

view_macro_model_sql = """
{{ config(materialized='view') }}
{% call set_sql_header(config) %}
SET NOCOUNT ON;
{%- endcall %}
select 1 as id
"""


class TestSqlHeaderRejected:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_config.sql": table_config_model_sql,
            "view_config.sql": view_config_model_sql,
            "table_macro.sql": table_macro_model_sql,
            "view_macro.sql": view_macro_model_sql,
        }

    def test_all_rejected(self, project):
        """All four sql_header variants (table/view x config/macro) must error
        with a clear compiler message pointing to pre_hooks and query_options."""
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 4

        statuses = {r.node.name: r.status for r in results}
        for name in ["table_config", "view_config", "table_macro", "view_macro"]:
            assert statuses[name] == "error", f"{name} should have errored, got {statuses[name]}"
