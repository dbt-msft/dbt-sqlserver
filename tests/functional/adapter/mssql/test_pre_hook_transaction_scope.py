"""pre_hook_transaction_scope: where schema resolution runs relative to in-transaction pre-hooks.

  'load'  (default) - the tmp view and the empty CREATE run before the in-tx
                      pre-hooks and autocommit; the load joins the hook's
                      transaction. No Sch-M spans the load, and the pre-hook
                      still rolls back with a failed load.
  'build'           - the create runs inside the hook's transaction, after
                      it, so its Sch-M is held for the whole load. Today's
                      behaviour, kept for a pre-hook that creates what the
                      model reads.

Three things are observable from a dbt test and pinned here:
  1. rollback - a transaction: true pre-hook's write is gone after a failed
     load under BOTH scopes (the two differ in locks, not in atomicity).
  2. locks - while the load runs, a second session's sys.tables scan is not
     blocked under 'load' and is blocked under 'build'.
  3. bindability - a transaction: true pre-hook that creates the model's
     source fails at the stage under 'load', works under 'build', and works
     under 'load' once declared transaction: false.
"""

import os
import threading
import time

import pyodbc
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
    return f"""
{{{{ config(
  materialized='table', as_columnstore=False,
  pre_hook_transaction_scope='{scope}',
  pre_hook=[{{'sql': "insert into {{{{ ref('audit_log') }}}} (marker) values (1)",
             'transaction': True}}]
) }}}}
select cast(txt as int) as val from {{{{ ref('source_rows') }}}}
"""


big_source_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select top 1500000
  row_number() over (order by (select null)) as id,
  replicate('x', 40) as payload
from sys.all_columns a cross join sys.all_columns b cross join sys.all_columns c
"""


def _slow_model(scope):
    # hashbytes over a widened payload keeps the load in the seconds range so
    # the poller below gets a meaningful number of samples
    return f"""
{{{{ config(
  materialized='table', as_columnstore=False,
  pre_hook_transaction_scope='{scope}',
  pre_hook=[{{'sql': "select 1 as noop", 'transaction': True}}]
) }}}}
select a.id, a.payload, v.n, hashbytes('SHA2_512', replicate(a.payload, 50)) as h
from {{{{ ref('big_source') }}}} a
cross join (values (1), (2), (3), (4)) v(n)
"""


def _staged_by_hook(scope, hook_tx):
    return f"""
{{{{ config(
  materialized='table', as_columnstore=False,
  pre_hook_transaction_scope='{scope}',
  pre_hook=[{{'sql': "drop table if exists {{{{ target.schema }}}}.hook_staged; "
                    "select 1 as id into {{{{ target.schema }}}}.hook_staged",
             'transaction': {hook_tx}}}]
) }}}}
select id from {{{{ target.schema }}}}.hook_staged
"""


def _second_session():
    return pyodbc.connect(
        "DRIVER={%s};SERVER=%s,%s;DATABASE=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes"
        % (
            os.environ["SQLSERVER_TEST_DRIVER"],
            os.environ["SQLSERVER_TEST_HOST"],
            os.environ["SQLSERVER_TEST_PORT"],
            os.environ["SQLSERVER_TEST_DBNAME"],
            os.environ["SQLSERVER_TEST_USER"],
            os.environ["SQLSERVER_TEST_PASS"],
        ),
        autocommit=True,
    )


# -- 1. rollback ------------------------------------------------------------


class _RollbackCase:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "audit_log.sql": audit_log_sql,
            "source_rows.sql": source_rows_sql,
            "failing_model.sql": _failing_model(self.scope),
        }

    def test_pre_hook_write_is_rolled_back(self, project):
        run_dbt(["run"], expect_pass=False)
        rows = project.run_sql(
            f"select count(*) from {project.test_schema}.audit_log", fetch="one"
        )[0]
        assert rows == 0, (
            f"pre_hook_transaction_scope='{self.scope}' keeps the pre-hook in the "
            "load's transaction, so a failed load must roll its write back"
        )
        target = project.run_sql(
            f"select object_id('{project.test_schema}.failing_model', 'U')", fetch="one"
        )[0]
        assert target is None, "a failed first build must leave no target behind"


class TestLoadScopeRollsBackThePreHook(_RollbackCase):
    scope = "load"

    def test_stage_committed_on_its_own(self, project):
        """Under 'load' the empty create is durable before the hook runs, so it
        survives the rollback; the next run's preexisting-intermediate drop
        clears it."""
        run_dbt(["run"], expect_pass=False)
        tmp = project.run_sql(
            f"select object_id('{project.test_schema}.failing_model__dbt_tmp', 'U')", fetch="one"
        )[0]
        assert tmp is not None


class TestBuildScopeRollsBackThePreHook(_RollbackCase):
    scope = "build"


# -- 2. locks ---------------------------------------------------------------


class _LockCase:
    @pytest.fixture(scope="class")
    def models(self):
        return {"big_source.sql": big_source_sql, "slow_model.sql": _slow_model(self.scope)}

    def _blocked_polls_during_run(self, project):
        run_dbt(["run", "--select", "big_source"])

        timeline = []
        stop = threading.Event()

        def poll():
            session = _second_session()
            session.execute("SET LOCK_TIMEOUT 400")
            while not stop.is_set():
                try:
                    session.execute("select count(*) from sys.tables").fetchall()
                    timeline.append("ok")
                except pyodbc.Error as e:
                    timeline.append("blocked" if "1222" in str(e) else "error")
                time.sleep(0.2)
            session.close()

        poller = threading.Thread(target=poll)
        poller.start()
        try:
            results = run_dbt(["run", "--select", "slow_model"])
        finally:
            stop.set()
            poller.join()
        assert results[0].status == "success"
        assert "error" not in timeline
        assert len(timeline) >= 8, "the load finished before the poller could sample it"
        return timeline.count("blocked"), len(timeline)


class TestLoadScopeDoesNotBlockCatalogReaders(_LockCase):
    scope = "load"

    def test_scan_proceeds_during_the_load(self, project):
        blocked, polls = self._blocked_polls_during_run(project)
        # at most the cutover's sp_rename, which is an instant
        assert blocked <= 1, f"{blocked} of {polls} catalog scans blocked during the load"


class TestBuildScopeBlocksCatalogReaders(_LockCase):
    scope = "build"

    def test_scan_blocks_during_the_load(self, project):
        blocked, polls = self._blocked_polls_during_run(project)
        # the create's Sch-M is held to commit, i.e. for the whole load
        assert blocked >= polls // 2, f"only {blocked} of {polls} catalog scans blocked"


# -- 3. bindability ---------------------------------------------------------


class _StagedByHook:
    @pytest.fixture(scope="class")
    def models(self):
        return {"staged.sql": _staged_by_hook(self.scope, self.hook_tx)}


class TestLoadScopeFailsWhenAnInTxHookStagesTheSource(_StagedByHook):
    scope, hook_tx = "load", "True"

    def test_invalid_object_at_the_stage(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "Invalid object name" in str(results[0].message)


class TestLoadScopeWorksWhenTheHookIsOutsideTheTransaction(_StagedByHook):
    scope, hook_tx = "load", "False"

    def test_passes(self, project):
        assert run_dbt(["run"])[0].status == "success"


class TestBuildScopeWorksWhenAnInTxHookStagesTheSource(_StagedByHook):
    scope, hook_tx = "build", "True"

    def test_passes(self, project):
        assert run_dbt(["run"])[0].status == "success"


class TestInvalidScopeIsRejected:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_scope.sql": """
{{ config(materialized='table', pre_hook_transaction_scope='schema') }}
select 1 as id
"""
        }

    def test_invalid_value_raises(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert "pre_hook_transaction_scope" in str(results[0].message)
