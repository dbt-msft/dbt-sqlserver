# flake8: noqa: E501
"""Regression test for dbt-msft/dbt-sqlserver#601.

With ``--store-failures``, a test that *passes* must leave behind an empty
audit relation, not drop it. dbt's contract: "A test's results will always
replace previous failures for the same test, even if that test results in no
failures." The SQL Server adapter was reported to ``DROP`` the audit table on a
passing test instead of replacing it with an empty table (Postgres creates the
empty table).

This exercises the exact reported scenario: a passing test configured with
``store_failures`` materialized as a ``table``. It asserts the audit relation
exists, is a base table (not a view), is empty, and survives idempotent re-runs.
"""

import pytest

from dbt.tests.util import run_dbt

# the default audit schema (_dbt_test__audit) plus the test schema can exceed
# identifier limits; use a short suffix as the rest of the suite does.
TEST_AUDIT_SCHEMA_SUFFIX = "dbt_test__aud"

model__chipmunks = """
select 1 as id, 'alvin' as name
union all
select 2 as id, 'simon' as name
"""

# returns zero rows -> the test passes
test__passing_601 = """
{{ config(store_failures=true, store_failures_as='table') }}
select * from {{ ref('chipmunks') }}
where 1 = 2
"""


class TestStoreFailuresPassingKeepsEmptyTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {"chipmunks.sql": model__chipmunks}

    @pytest.fixture(scope="class")
    def tests(self):
        return {"passing_601.sql": test__passing_601}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {"dbt_sqlserver_use_default_schema_concat": True},
            "data_tests": {"+schema": TEST_AUDIT_SCHEMA_SUFFIX},
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        self.audit_schema = f"{project.test_schema}_{TEST_AUDIT_SCHEMA_SUFFIX}"
        run_dbt(["run"])
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=self.audit_schema
            )
            project.adapter.drop_schema(relation)

    def _assert_empty_audit_table(self, project):
        # type_desc proves the relation exists AND is a user table (not a view).
        # On the #601 bug the relation is dropped, so this returns no rows.
        # Queried via sys catalog (lowercase column names) so it is safe under a
        # case-sensitive database collation.
        rows = project.run_sql(
            f"""
            select o.type_desc
            from sys.objects o
            join sys.schemas s on o.schema_id = s.schema_id
            where s.name = '{self.audit_schema}'
              and o.name = 'passing_601'
            """,
            fetch="all",
        )
        assert len(rows) == 1 and rows[0][0] == "USER_TABLE", (
            f"audit relation [{self.audit_schema}].[passing_601] should be a user "
            f"table that persists after a passing store-failures run, got: "
            f"{[tuple(r) for r in rows]}"
        )
        # and it must be empty (the failures were replaced with nothing)
        count = project.run_sql(
            f"select count(*) from [{self.audit_schema}].[passing_601]",
            fetch="one",
        )
        assert count[0] == 0, f"audit table should be empty, has {count[0]} rows"

    def test_passing_test_keeps_empty_audit_table(self, project):
        results = run_dbt(["test", "--store-failures"], expect_pass=True)
        assert len(results) == 1
        assert results[0].status == "pass"
        assert results[0].failures == 0
        self._assert_empty_audit_table(project)

        # idempotency: a second run must still leave the empty table in place
        results = run_dbt(["test", "--store-failures"], expect_pass=True)
        assert results[0].status == "pass"
        self._assert_empty_audit_table(project)
