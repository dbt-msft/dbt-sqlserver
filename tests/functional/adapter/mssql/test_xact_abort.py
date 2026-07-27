import pytest

from dbt.tests.util import get_connection, run_dbt

# See dbt-msft/dbt-sqlserver#718: the DML refresh swap (DELETE + INSERT,
# possibly wrapped in an explicit BEGIN/COMMIT batch) can commit a partial
# result if a run-time error aborts only the failing statement instead of
# the whole batch. These tests confirm the session-level `SET XACT_ABORT ON`
# default (dbt/adapters/sqlserver/sqlserver_connections.py) prevents that.

# -- Model fixtures --

dml_check_model_seed_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False,
    "post_hook": "ALTER TABLE {{ this }} ADD CONSTRAINT ck_val_not_null CHECK (val IS NOT NULL)"
  })
}}
select 1 as id, cast('hello' as varchar(20)) as val
union all
select 2 as id, cast('world' as varchar(20)) as val
"""

dml_check_model_null_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
select 3 as id, cast(null as varchar(20)) as val
"""


def query_table(project, table_name):
    """Query all rows from a table, return as list of tuples."""
    sql = f"SELECT * FROM {project.test_schema}.{table_name} ORDER BY id"
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return table.rows


def write_model(project, filename, contents):
    import os

    path = os.path.join(project.project_root, "models", filename)
    with open(path, "w") as f:
        f.write(contents)


# -- Data-loss regression + interaction matrix --
#
# Crossed with dbt_sqlserver_use_dbt_transactions per #718's own reasoning:
# XACT_ABORT must protect the swap regardless of who owns the transaction
# boundary, since the bug reproduces even with no explicit transaction at
# all. xact_abort is left at its True default (via the shared profile) in
# every class below — these are the combinations the fix is meant to
# guarantee. See the module docstring-style comment near the bottom of this
# file for why the xact_abort=False x dbt_transactions=False combination is
# deliberately NOT asserted here.


class BaseDmlRefreshConstraintViolationPreservesRows:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_check_model.sql": dml_check_model_seed_sql}

    def test_constraint_violation_does_not_lose_committed_rows(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        seeded_rows = query_table(project, "dml_check_model")
        assert len(seeded_rows) == 2

        # Swap in a model that yields a NULL in the CHECK-constrained column.
        # Same (id int, val varchar(20)) schema as the seed model, so the DML
        # refresh path (DELETE + INSERT) is taken rather than the
        # rename-swap fallback.
        write_model(project, "dml_check_model.sql", dml_check_model_null_sql)

        run_dbt(["run"], expect_pass=False)

        rows_after_failure = query_table(project, "dml_check_model")
        assert len(rows_after_failure) == len(seeded_rows)


class TestDmlRefreshConstraintViolationPreservesRowsDbtTransactionsOff(
    BaseDmlRefreshConstraintViolationPreservesRows
):
    """xact_abort: true (default), dbt_sqlserver_use_dbt_transactions: false (default).

    This is the exact scenario in #718's background: the macro emits its own
    BEGIN TRANSACTION / COMMIT TRANSACTION around DELETE + INSERT as one
    autocommit batch. Without the session-level XACT_ABORT default, the
    DELETE would commit before the INSERT's constraint violation is
    noticed. With it, the whole batch aborts and rolls back.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": False}}


class TestDmlRefreshConstraintViolationPreservesRowsDbtTransactionsOn(
    BaseDmlRefreshConstraintViolationPreservesRows
):
    """xact_abort: true (default), dbt_sqlserver_use_dbt_transactions: true.

    Here dbt owns the transaction boundary (its own BEGIN/COMMIT statements,
    separate from the swap batch), so this also confirms XACT_ABORT doesn't
    interfere with dbt-managed transactions.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": True}}


class TestDmlRefreshConstraintViolationPreservesRowsXactAbortOffDbtTransactionsOn(
    BaseDmlRefreshConstraintViolationPreservesRows
):
    """xact_abort: false, dbt_sqlserver_use_dbt_transactions: true.

    With xact_abort off, protection now depends entirely on dbt owning the
    transaction: the macro sends no BEGIN/COMMIT text of its own here (dbt
    already opened one), so a statement-abort-only error leaves @@TRANCOUNT
    > 0 and dbt's own rollback path (SQLServerConnectionManager._rollback_handle)
    issues `IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION` — never a commit. Rows
    are preserved here, but via dbt's transaction ownership, not XACT_ABORT.
    """

    @pytest.fixture(scope="class")
    def dbt_profile_target_update(self):
        return {"xact_abort": False}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_dbt_transactions": True}}


# Deliberately not covered above: xact_abort=False with
# dbt_sqlserver_use_dbt_transactions=False (both disabled). In that specific
# combination the macro's own BEGIN/COMMIT text wraps an autocommit batch
# with no XACT_ABORT protection and no dbt-managed rollback to fall back on,
# so a statement-abort error can commit the DELETE and lose the row. That is
# the documented, intentional consequence of disabling xact_abort (see the
# warning in SQLServerConnectionManager._warn_xact_abort_disabled_once) —
# not a regression to assert against here. Confirming it empirically
# requires a live SQL Server instance; see the PR description.


# -- Session-level XACT_ABORT is actually set on the connection --


class TestXactAbortSessionOptionEnabledByDefault:
    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_xact_abort_option_bit_is_set(self, project):
        with get_connection(project.adapter):
            _, table = project.adapter.execute(
                "SELECT CASE WHEN (@@OPTIONS & 16384) > 0 THEN 1 ELSE 0 END AS xact_abort_on",
                fetch=True,
            )
        assert table.rows[0][0] == 1


class TestXactAbortSessionOptionUnsetWhenDisabled:
    @pytest.fixture(scope="class")
    def dbt_profile_target_update(self):
        return {"xact_abort": False}

    @pytest.fixture(scope="class")
    def models(self):
        return {}

    def test_xact_abort_option_bit_is_unset(self, project):
        with get_connection(project.adapter):
            _, table = project.adapter.execute(
                "SELECT CASE WHEN (@@OPTIONS & 16384) > 0 THEN 1 ELSE 0 END AS xact_abort_on",
                fetch=True,
            )
        assert table.rows[0][0] == 0
