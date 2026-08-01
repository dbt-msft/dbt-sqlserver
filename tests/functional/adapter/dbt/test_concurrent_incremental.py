import pytest

from dbt.cli.main import dbtRunner

# Concurrency regression for the incremental materialization (plan
# 20260801-dbt-sqlserver-concurrent-incremental-deadlock).
#
# Root cause: with dbt_sqlserver_use_dbt_transactions on (the rc1 default),
# a read-only catalog lookup (find_references in sqlserver__get_drop_sql,
# the first statement of the temp build) auto-began the ambient transaction.
# The incremental temp build (create_table_as: CREATE OR ALTER VIEW +
# SELECT * INTO + DROP VIEW, all user-DB catalog DDL) then ran inside that
# transaction, holding transaction-scoped sysschobjs X keylocks until the
# final adapter.commit(). Two independent incremental models run in parallel
# (threads=2) then inverted X/S and deadlocked (Msg 1205).
#
# The fix stops the ambient transaction from being opened accidentally for
# catalog reads: find_references runs with auto_begin=false (relation.sql),
# and the fresh-create / full-refresh create_table_as batch runs with
# statement("main", auto_begin=False), so each statement autocommits and
# releases its catalog locks as it completes. The strategy DML keeps the
# default auto_begin and stays transactional until the final commit.
#
# Follows the BaseTransactionsEnabled pattern (test_transactions.py): the
# deadlock only forms when the flag wraps the build in the ambient
# transaction. Model SQL is values-derived (no sys.all_objects) to remove
# the catalog-scan confound flagged by the independent evaluation (A.1).
_concurrent_incremental_model_sql = """
{{ config(materialized='incremental', unique_key='id') }}
select
    row_number() over (order by (select null)) as id,
    current_timestamp as loaded_at
from (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) as a(n)
cross join (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) as b(n)
cross join (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) as c(n)
cross join (values (1),(2),(3),(4),(5)) as d(n)
"""


class BaseConcurrentIncremental:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": _concurrent_incremental_model_sql,
            "model_b.sql": _concurrent_incremental_model_sql,
        }

    def _build_concurrently(self, extra_args=None):
        runner = dbtRunner()
        result = runner.invoke(["build", "--threads", "2"] + (extra_args or []))
        assert result.success, (
            f"concurrent incremental build failed: {result.exception or result.result}"
        )

    def test_two_incremental_models_build_concurrently(self, project):
        # Run 1: fresh-create path (create_table_as into the final target) for
        # both models in parallel - covers the fresh-create deadlock class.
        self._build_concurrently()
        # Run 2: incremental temp-build + strategy-DML path - the boundary the
        # fix releases early.
        self._build_concurrently()
        # Run 3: full-refresh path (create into __dbt_tmp intermediate + swap)
        # for both models in parallel.
        self._build_concurrently(["--full-refresh"])

        for model in ("model_a", "model_b"):
            rows = project.run_sql(
                f"select count(*) as cnt from {project.test_schema}.{model}", fetch="one"
            )
            assert rows[0] == 5000


class TestConcurrentIncremental(BaseConcurrentIncremental):
    pass
