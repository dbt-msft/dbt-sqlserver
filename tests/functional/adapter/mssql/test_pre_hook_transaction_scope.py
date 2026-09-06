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
  2. locks - while the load runs, the building session holds no object-level
     Sch-M under 'load' and holds one under 'build'. Sch-M is the mode that
     blocks the Sch-S every metadata reader takes, so that is the whole of
     #819.
  3. bindability - a transaction: true pre-hook that creates the model's
     source fails at the stage under 'load', works under 'build', and works
     under 'load' once declared transaction: false.
  4. ordering - the snapshot materialization's own database work (the check
     strategy runs the snapshot SQL to compare column shapes) still happens
     after the pre-hooks each scope promises to have run by then.
"""

import threading
import time

import pytest

from dbt.adapters.contracts.connection import Connection
from dbt.adapters.sqlserver.sqlserver_connections import SQLServerConnectionManager
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


# hashbytes over a widened payload keeps the load in the seconds range, so the
# probe below samples it many times over
_slow_select = """
select a.id, a.payload, v.n, hashbytes('SHA2_512', replicate(a.payload, 50)) as h
from {{ ref('big_source') }} a
cross join (values (1), (2), (3), (4)) v(n)
"""


def _slow_model(scope, pre_hook=True, materialized="table"):
    hook = "pre_hook=[{'sql': \"select 1 as noop\", 'transaction': True}]," if pre_hook else ""
    return f"""
{{{{ config(
  materialized='{materialized}', as_columnstore=False,
  pre_hook_transaction_scope='{scope}',
  {hook}
) }}}}
{_slow_select}
"""


def _slow_snapshot(scope):
    """First build of a snapshot: the stage, then the load - the path
    sqlserver__snapshot_stage owns."""
    return f"""
{{% snapshot slow_snap %}}
{{{{ config(
  unique_key='id', strategy='check', check_cols=['n'],
  as_columnstore=False,
  pre_hook_transaction_scope='{scope}'
) }}}}
{_slow_select}
{{% endsnapshot %}}
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


# Sampled from a second connection while the model builds, once every 0.1s:
#
#   is the session running the load  request text names this schema + TABLOCK
#   holding a blocking lock          OBJECT / Sch-M / GRANT on that session
#
# DMVs take no lock on user objects, so this reads the building session
# directly instead of timing out a catalog scan from outside. That matters at
# `-n auto`: another worker's DDL blocks a sys.tables scan no matter what this
# model does, so a timing-out scan measures the suite, not the change.
#
# TABLOCK identifies the load half in both scopes (it is the only statement in
# the materialization that carries the hint) and under 'build' the fused batch
# carries the empty CREATE with it. `session_id <> @@spid` drops the probe's
# own request, whose text contains both literals.
_SAMPLE_SQL = """
with loading as (
  select r.session_id
  from sys.dm_exec_requests r
  cross apply sys.dm_exec_sql_text(r.sql_handle) t
  where r.session_id <> @@spid
    and t.text like '%{schema}%'
    and t.text like '%TABLOCK%'
)
select
  (select count(*) from loading),
  (select count(*) from loading
    where exists (select 1 from sys.dm_tran_locks l
                  where l.request_session_id = loading.session_id
                    and l.resource_type = 'OBJECT'
                    and l.request_mode = 'Sch-M'
                    and l.request_status = 'GRANT'))
"""


def _probe_connection(project):
    """A second session, opened through the adapter's own connect path so it
    speaks whichever backend the profile names, but owned by this test rather
    than by dbt's connection manager: run_dbt closes every connection that
    manager knows about, which would pull this one out from under the probe
    thread mid-query."""
    connection = Connection(
        type="sqlserver",
        name="sch_m_probe",
        state="init",
        transaction_open=False,
        handle=None,
        credentials=project.adapter.config.credentials,
    )
    SQLServerConnectionManager.open(connection)
    return connection.handle


def _probe(project, stop, samples, failures):
    """Append True/False - Sch-M held or not - once per sample taken while the
    load is running; ignore every sample taken when it is not."""
    try:
        handle = _probe_connection(project)
        sql = _SAMPLE_SQL.format(schema=project.test_schema)
        try:
            while not stop.is_set():
                cursor = handle.cursor()
                try:
                    cursor.execute(sql)
                    loading, holding_sch_m = cursor.fetchone()
                finally:
                    cursor.close()
                if loading:
                    samples.append(bool(holding_sch_m))
                time.sleep(0.1)
        finally:
            handle.close()
    except BaseException as e:  # noqa: BLE001 - a probe that dies silently lies
        failures.append(e)


class _LockCase:
    pre_hook = True
    materialized = "table"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "big_source.sql": big_source_sql,
            "slow_model.sql": _slow_model(self.scope, self.pre_hook, self.materialized),
        }

    def _build_the_model(self):
        return run_dbt(["run", "--select", "slow_model"])

    def _sch_m_during_the_load(self, project):
        # deliberately not a skip: this is the only functional guard on the
        # lock #819 is about, and a silent skip on a login without the
        # permission would retire it with no signal at all
        assert project.run_sql(
            "select has_perms_by_name(null, null, 'VIEW SERVER STATE')", fetch="one"
        )[0], "these tests read sys.dm_exec_requests; grant the test login VIEW SERVER STATE"

        run_dbt(["run", "--select", "big_source"])

        samples, failures = [], []
        stop = threading.Event()
        probe = threading.Thread(target=_probe, args=(project, stop, samples, failures))
        probe.start()
        try:
            results = self._build_the_model()
        finally:
            stop.set()
            probe.join()

        assert not failures, f"the lock probe failed: {failures[0]!r}"
        assert results[0].status == "success"
        assert len(samples) >= 5, "the load finished before the probe could sample it"
        return samples


class _NoSchM(_LockCase):
    def test_no_sch_m_during_the_load(self, project):
        samples = self._sch_m_during_the_load(project)
        held = samples.count(True)
        assert held == 0, f"Sch-M held during {held} of {len(samples)} samples: {samples}"


class TestLoadScopeDoesNotBlockCatalogReaders(_NoSchM):
    """load: the create committed before the load, and the INSERT takes an X
    table lock, which no metadata reader conflicts with."""

    scope = "load"


class TestBuildScopeWithoutAnInTxHookHoldsNothing(_NoSchM):
    """build with no transactional pre-hook: there is no transaction to join,
    so the create and the load autocommit exactly as under load. Pins what
    docs/transaction_scope.md promises - a folder-level +build must not take
    the lock for models underneath it that have no hooks."""

    scope, pre_hook = "build", False


class TestIncrementalBuildScopeWithoutAnInTxHookHoldsNothing(_NoSchM):
    """The same promise on incremental's fresh-build branch."""

    scope, pre_hook, materialized = "build", False, "incremental"


class TestSnapshotBuildScopeWithoutAnInTxHookHoldsNothing(_NoSchM):
    """And on a snapshot's first build, whose stage is sqlserver__snapshot_stage."""

    scope, pre_hook = "build", False

    @pytest.fixture(scope="class")
    def models(self):
        return {"big_source.sql": big_source_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"slow_snap.sql": _slow_snapshot(self.scope)}

    def _build_the_model(self):
        return run_dbt(["snapshot"])


class TestBuildScopeBlocksCatalogReaders(_LockCase):
    scope = "build"

    def test_sch_m_spans_the_load(self, project):
        samples = self._sch_m_during_the_load(project)
        # The create shares the pre-hook's transaction, so its Sch-M is held to
        # commit - held CONTINUOUSLY from the create to the end of the load,
        # which is what this asserts, rather than merely "held in most
        # samples". The request is matched by its batch text, so it is visible
        # for a moment at each end while no Sch-M is held yet - and the two
        # DMV reads in one sample are not atomic, so the sample that lands on
        # the commit can see the request still running with its locks already
        # gone. One sample at each end is allowed to miss; none in between.
        assert True in samples, f"Sch-M never held during the load: {samples}"
        first = samples.index(True)
        last = len(samples) - 1 - samples[::-1].index(True)
        held = samples[first : last + 1]
        assert all(held), f"Sch-M released mid-load: {samples}"
        assert len(samples) - len(held) <= 2, f"Sch-M held for only part of the load: {samples}"
        assert len(held) >= 5, f"too little of the load sampled: {samples}"


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


# -- 4. ordering ------------------------------------------------------------


def _hook_sourced_snapshot(scope, hook_tx):
    """A check-strategy snapshot whose source a pre-hook creates.

    The check strategy runs the snapshot's own SQL to compare column shapes
    (snapshot_check_all_get_existing_columns), but only once the target
    exists - so this binds trivially on the first run and only reaches the
    strategy probe on the second. Both escape hatches from 'load' have to
    survive that: transaction: false, and scope 'build'.
    """
    return f"""
{{% snapshot hook_sourced_snap %}}
{{{{ config(
  unique_key='id', strategy='check', check_cols='all',
  pre_hook_transaction_scope='{scope}',
  pre_hook=[{{'sql': "drop table if exists {{{{ target.schema }}}}.hook_sourced; "
                    "select 1 as id, cast('a' as varchar(10)) as txt "
                    "into {{{{ target.schema }}}}.hook_sourced",
             'transaction': {hook_tx}}}]
) }}}}
select id, txt from {{{{ target.schema }}}}.hook_sourced
{{% endsnapshot %}}
"""


class _HookSourcedSnapshot:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"hook_sourced_snap.sql": _hook_sourced_snapshot(self.scope, self.hook_tx)}

    def test_second_run_still_binds(self, project):
        assert run_dbt(["snapshot"])[0].status == "success"
        # the source belongs to the hook, so take it away again: otherwise the
        # first run's copy is still there and the strategy probe binds against
        # it whether the hook has run or not
        project.run_sql(f"drop table if exists {project.test_schema}.hook_sourced")
        # the run that reaches the check strategy's own query
        assert run_dbt(["snapshot"])[0].status == "success"


class TestLoadScopeRunsOutsideTxHooksBeforeTheStrategy(_HookSourcedSnapshot):
    scope, hook_tx = "load", "False"


class TestBuildScopeRunsInTxHooksBeforeTheStrategy(_HookSourcedSnapshot):
    scope, hook_tx = "build", "True"
