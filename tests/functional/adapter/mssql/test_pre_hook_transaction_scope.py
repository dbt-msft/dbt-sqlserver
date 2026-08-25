"""pre_hook_transaction_scope decides whether a pre-hook rolls back with a failed load.

The config exists for one trade-off, and it is directly observable: does an
in-transaction pre-hook's write survive a load that fails afterwards?

  'build'  - the pre-hook's transaction spans the build, so a failed load rolls
             the pre-hook back. Costs the #819 fix: the new table's Sch-M is
             held for the length of the load.
  'schema' - the transaction covers schema resolution only and commits before
             the load, so the pre-hook's write is already durable when the load
             fails. The load then holds no Sch-M.

Everything else about the two paths (which locks are held, for how long) is not
observable from a dbt test without a second concurrent session, so this pins
the semantic difference that is.
"""

import pytest

from dbt.tests.util import run_dbt

audit_log_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select cast(0 as int) as marker where 1 = 0
"""

# Rows come from a table, not inline literals: the empty create is
# SELECT TOP 0, and constant folding could otherwise evaluate the failing CAST
# at create time rather than during the load.
source_rows_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select 1 as id, cast('not_a_number' as varchar(20)) as txt
"""


def _failing_model(scope):
    scope_config = f"'pre_hook_transaction_scope': '{scope}'," if scope else ""
    return f"""
{{{{ config({{
  'materialized': 'table',
  'as_columnstore': False,
  {scope_config}
  'pre_hook': [{{'sql': "insert into {{{{ ref('audit_log') }}}} (marker) values (1)",
                 'transaction': True}}],
}}) }}}}
select cast(txt as int) as val from {{{{ ref('source_rows') }}}}
"""


class _ScopeCase:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "audit_log.sql": audit_log_sql,
            "source_rows.sql": source_rows_sql,
            "failing_model.sql": _failing_model(self.scope),
        }

    def _audit_rows(self, project):
        return project.run_sql(
            f"select count(*) from {project.test_schema}.audit_log", fetch="one"
        )[0]


class TestBuildScopeRollsBackThePreHook(_ScopeCase):
    scope = "build"

    def test_pre_hook_write_is_rolled_back(self, project):
        run_dbt(["run"], expect_pass=False)
        assert self._audit_rows(project) == 0, (
            "pre_hook_transaction_scope='build' keeps the pre-hook in the "
            "build's transaction, so a failed load must roll its write back"
        )


class TestSchemaScopeCommitsThePreHook(_ScopeCase):
    scope = "schema"

    def test_pre_hook_write_survives_the_failed_load(self, project):
        run_dbt(["run"], expect_pass=False)
        assert self._audit_rows(project) == 1, (
            "pre_hook_transaction_scope='schema' commits before the load, so "
            "the pre-hook's write is durable when the load fails - the "
            "documented cost of releasing the create's Sch-M early"
        )


class TestInvalidScopeIsRejected:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_scope.sql": """
{{ config(materialized='table', pre_hook_transaction_scope='sideways') }}
select 1 as id
"""
        }

    def test_invalid_value_raises(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "pre_hook_transaction_scope" in str(results[0].message)
