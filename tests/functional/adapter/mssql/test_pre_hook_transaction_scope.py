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
  5. the exception to (1) - the two paths that commit a full-refresh marker
     before the load commit the pre-hook with it, under either scope.
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


gate_source_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select id, cast('x' as varchar(10)) as payload from (values (1), (2)) v(id)
"""


# READCOMMITTEDLOCK makes the load wait on the test's row lock even where
# READ_COMMITTED_SNAPSHOT is on. The empty create reads no rows, so it does not.
_gated_select = """
select id, payload from {{ ref('gate_source') }} with (readcommittedlock)
"""


def _gated_model(scope, pre_hook=True, materialized="table"):
    hook = "pre_hook=[{'sql': \"select 1 as noop\", 'transaction': True}]," if pre_hook else ""
    return f"""
{{{{ config(
  materialized='{materialized}', as_columnstore=False,
  pre_hook_transaction_scope='{scope}',
  {hook}
) }}}}
{_gated_select}
"""


def _gated_snapshot(scope):
    """First build of a snapshot: the stage, then the load - the path
    sqlserver__snapshot_stage owns."""
    return f"""
{{% snapshot gated_snap %}}
{{{{ config(
  unique_key='id', strategy='check', check_cols=['payload'],
  as_columnstore=False,
  pre_hook_transaction_scope='{scope}'
) }}}}
{_gated_select}
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


# The test holds an X lock on one row of gate_source in an open transaction, so
# the model's load - the only statement that reads rows from it - stops inside
# its INSERT and waits. While it waits, the building session's locks are read
# once, then the gate commits and the load finishes. No timing is involved:
# the INSERT is known to be running when the locks are read.
#
# DMVs take no lock on user objects, so reading them cannot block behind the
# Sch-M being looked for. TABLOCK identifies the load in both scopes (it is
# the only statement in the materialization that carries the hint).
# `session_id <> @@spid` drops the probe's own request, whose text contains
# both literals.
_PAUSED_LOAD_SQL = """
select count(sch_m.held)
from sys.dm_exec_requests r
cross apply sys.dm_exec_sql_text(r.sql_handle) t
outer apply (select top 1 1 as held from sys.dm_tran_locks l
             where l.request_session_id = r.session_id
               and l.resource_type = 'OBJECT'
               and l.request_mode = 'Sch-M'
               and l.request_status = 'GRANT') sch_m
where r.session_id <> @@spid
  and r.wait_type like 'LCK_M_%'
  and t.text like '%{schema}%'
  and substring(t.text, r.statement_start_offset / 2 + 1,
                (case r.statement_end_offset when -1 then datalength(t.text)
                 else r.statement_end_offset end - r.statement_start_offset) / 2 + 1)
      like '%TABLOCK%'
having count(*) > 0
"""


def _gate_connection(project):
    """A second session, opened through the adapter's own connect path so it
    speaks whichever backend the profile names, but owned by this test rather
    than by dbt's connection manager: run_dbt closes every connection that
    manager knows about, which would pull this one out from under the gate
    thread mid-query."""
    connection = Connection(
        type="sqlserver",
        name="sch_m_gate",
        state="init",
        transaction_open=False,
        handle=None,
        credentials=project.adapter.config.credentials,
    )
    SQLServerConnectionManager.open(connection)
    return connection.handle


def _hold_the_load(project, gate_closed, result):
    """Hold the gate until the load waits on it, record whether the building
    session holds Sch-M at that point, then release the load."""
    try:
        handle = _gate_connection(project)
        cursor = handle.cursor()
        try:
            cursor.execute(
                "begin transaction; "
                f"update {project.test_schema}.gate_source set payload = payload where id = 1"
            )
            gate_closed.set()
            sql = _PAUSED_LOAD_SQL.format(schema=project.test_schema)
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    result["sch_m"] = bool(row[0])
                    return
                time.sleep(0.05)
        finally:
            # explicitly: mssql-python pools connections, so close() alone can
            # leave the gate's transaction open and the load waiting forever
            cursor.execute("if @@trancount > 0 rollback")
            cursor.close()
            handle.close()
    except BaseException as e:  # noqa: BLE001 - a probe that dies silently lies
        result["error"] = e
    finally:
        gate_closed.set()


class _LockCase:
    pre_hook = True
    materialized = "table"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "gate_source.sql": gate_source_sql,
            "gated_model.sql": _gated_model(self.scope, self.pre_hook, self.materialized),
        }

    def _build_the_model(self):
        return run_dbt(["run", "--select", "gated_model"])

    def _sch_m_while_loading(self, project):
        # deliberately not a skip: this is the only functional guard on the
        # lock #819 is about, and a silent skip on a login without the
        # permission would retire it with no signal at all
        assert project.run_sql(
            "select has_perms_by_name(null, null, 'VIEW SERVER STATE')", fetch="one"
        )[0], "these tests read sys.dm_exec_requests; grant the test login VIEW SERVER STATE"

        run_dbt(["run", "--select", "gate_source"])

        result, gate_closed = {}, threading.Event()
        gate = threading.Thread(target=_hold_the_load, args=(project, gate_closed, result))
        gate.start()
        gate_closed.wait()
        try:
            results = self._build_the_model()
        finally:
            gate.join()

        assert "error" not in result, f"the lock probe failed: {result['error']!r}"
        assert "sch_m" in result, "the load never waited on the gate"
        assert results[0].status == "success"
        return result["sch_m"]


class _NoSchM(_LockCase):
    def test_no_sch_m_during_the_load(self, project):
        assert not self._sch_m_while_loading(project), "Sch-M held during the load"


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
        return {"gate_source.sql": gate_source_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"gated_snap.sql": _gated_snapshot(self.scope)}

    def _build_the_model(self):
        return run_dbt(["snapshot"])


class TestBuildScopeBlocksCatalogReaders(_LockCase):
    scope = "build"

    def test_sch_m_spans_the_load(self, project):
        # The create shares the pre-hook's transaction, so its Sch-M is still
        # held while the load runs.
        assert self._sch_m_while_loading(project), "Sch-M not held during the load"


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


# -- 5. where neither scope can roll the pre-hook back -----------------------

# `bad: false` gives the source a castable value, so the model can be built
# once successfully before the run that fails.
switchable_source_rows_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select 1 as id,
       cast({{ "'not_a_number'" if var('bad', true) else "'20'" }} as varchar(20)) as txt
"""


def _marker_model(scope, materialized, full_refresh_build):
    return f"""
{{{{ config(
  materialized='{materialized}', as_columnstore=False,
  full_refresh_build='{full_refresh_build}',
  pre_hook_transaction_scope='{scope}',
  pre_hook=[{{'sql': "insert into {{{{ ref('audit_log') }}}} (marker) values (1)",
             'transaction': True}}]
) }}}}
select id, cast(txt as int) as val from {{{{ ref('source_rows') }}}}
"""


class _MarkerCase:
    """Both of these paths mark the target `dbt_full_refresh_incomplete` and
    commit that marker before the load - the marker's whole purpose is to
    outlive a failed rebuild, so it cannot share the load's transaction, and
    committing it commits any transaction: true pre-hook with it.

    So the rollback the other tests in this file assert is NOT available here,
    under either scope. That is documented (README, docs/transaction_scope.md,
    the macro comments); this pins it, so a change that quietly starts or stops
    delivering rollback here has to come past a test either way.
    """

    def _audit_rows(self, project):
        return project.run_sql(
            f"select count(*) from {project.test_schema}.audit_log", fetch="one"
        )[0]

    def _marked_incomplete(self, project, model):
        return project.run_sql(
            "select count(*) from sys.extended_properties where major_id = "
            f"object_id('{project.test_schema}.{model}') "
            "and name = 'dbt_full_refresh_incomplete'",
            fetch="one",
        )[0]


class _PrebuiltMarker(_MarkerCase):
    """full_refresh_build: prebuilt, on a first build."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "audit_log.sql": audit_log_sql,
            "source_rows.sql": source_rows_sql,
            "marker_model.sql": _marker_model(self.scope, "table", "prebuilt"),
        }

    def test_pre_hook_write_survives_the_failed_load(self, project):
        run_dbt(["run"], expect_pass=False)
        assert self._audit_rows(project) == 1, (
            "prebuilt commits its in-progress marker before the load, taking the "
            "pre-hook's write with it, so the write must still be there"
        )
        assert self._marked_incomplete(project, "marker_model") == 1, (
            "the marker is what forces that commit; if it is gone, this test is "
            "no longer measuring the path it claims to"
        )


class TestPrebuiltCommitsThePreHookUnderLoad(_PrebuiltMarker):
    scope = "load"


class TestPrebuiltCommitsThePreHookUnderBuild(_PrebuiltMarker):
    scope = "build"


class _IncrementalFullRefreshMarker(_MarkerCase):
    """An incremental --full-refresh of an existing table, on the default
    full_refresh_build - sqlserver__mark_full_refresh_incomplete commits."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "audit_log.sql": audit_log_sql,
            "source_rows.sql": switchable_source_rows_sql,
            "marker_model.sql": _marker_model(self.scope, "incremental", "heap_then_index"),
        }

    def test_pre_hook_write_survives_the_failed_load(self, project):
        # a clean build first: the marker path only applies to a table that
        # already exists
        run_dbt(["run", "--vars", "bad: false"])
        assert self._audit_rows(project) == 1

        # audit_log is rebuilt by this run too, so it starts empty again: one
        # row afterwards means this run's hook write survived
        run_dbt(["run", "--full-refresh"], expect_pass=False)
        assert self._audit_rows(project) == 1, (
            "the full-refresh marker commits before the load, taking the "
            "pre-hook's write with it, so the write must still be there"
        )
        assert self._marked_incomplete(project, "marker_model") == 1


class TestIncrementalFullRefreshCommitsThePreHookUnderLoad(_IncrementalFullRefreshMarker):
    scope = "load"


class TestIncrementalFullRefreshCommitsThePreHookUnderBuild(_IncrementalFullRefreshMarker):
    scope = "build"
