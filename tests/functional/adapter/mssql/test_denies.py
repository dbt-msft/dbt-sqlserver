"""Functional tests for object-level ``DENY`` permissions (the ``denies`` config).

A SQL Server object-level ``DENY`` is stored against ``object_id``, so dbt
discards it on every drop-and-recreate — the regression this config closes. These
tests exercise that a declared DENY is present in ``sys.database_permissions``
after a build and, crucially, **still present after a second build**, across the
table / view / incremental / snapshot materializations; that removing a principal
revokes it; that a converged model emits no DDL; that an absent principal warns
and skips without failing; and that the DENY is actually enforced against a
principal that holds a schema-level GRANT.
"""

import pytest

from dbt.tests.util import get_connection, run_dbt, run_dbt_and_capture

# A login-less database user used as the deny target across the suite.
DENY_PRINCIPAL = "dbt_deny_reader"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def denied_permissions(project, object_name, schema=None):
    """Return {(PRIVILEGE, principal)} of object-level DENYs on the relation."""
    schema = schema or project.test_schema
    sql = f"""
        select dp.permission_name, pr.name
        from sys.database_permissions dp
        join sys.database_principals pr
            on pr.principal_id = dp.grantee_principal_id
        where dp.class = 1
          and dp.major_id = OBJECT_ID('"{schema}"."{object_name}"')
          and dp.minor_id = 0
          and dp.state_desc = 'DENY'
    """
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return {(row[0].upper(), row[1]) for row in table.rows}


def exec_sql(project, sql):
    with get_connection(project.adapter):
        project.adapter.execute(sql)


@pytest.fixture(scope="class", autouse=True)
def deny_principal(project):
    """Create the login-less user the models deny, and drop it afterwards."""
    exec_sql(
        project,
        f"if database_principal_id('{DENY_PRINCIPAL}') is null "
        f"create user {DENY_PRINCIPAL} without login;",
    )
    yield DENY_PRINCIPAL
    exec_sql(
        project,
        f"if database_principal_id('{DENY_PRINCIPAL}') is not null drop user {DENY_PRINCIPAL};",
    )


# ---------------------------------------------------------------------------
# table materialization
# ---------------------------------------------------------------------------

table_deny_sql = """
{{ config(materialized="table", denies={"select": ["dbt_deny_reader"]}) }}
select 1 as id, cast('secret' as varchar(50)) as ssn
"""


class TestTableDenies:
    @pytest.fixture(scope="class")
    def models(self):
        return {"denied_table.sql": table_deny_sql}

    def test_deny_applied_and_survives_rebuild(self, project):
        run_dbt(["run"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_table")

        # the regression: a full refresh drops & recreates the table (new
        # object_id), so the DENY must be re-applied or it silently vanishes
        run_dbt(["run", "--full-refresh"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_table")

    def test_deny_is_enforced_against_a_schema_grant(self, project):
        """The pattern this exists for: a broad schema GRANT, carved out by an
        object DENY. The denied principal must not be able to read the table."""
        run_dbt(["run"])
        exec_sql(project, f"grant select on schema::{project.test_schema} to {DENY_PRINCIPAL};")

        blocked = False
        try:
            with get_connection(project.adapter):
                project.adapter.execute(
                    f"execute as user = '{DENY_PRINCIPAL}';"
                    f"select id from {project.test_schema}.denied_table;"
                    f"revert;"
                )
        except Exception:
            blocked = True
        finally:
            # make sure the impersonation context is not left open on the conn
            try:
                exec_sql(project, "revert;")
            except Exception:
                pass
        assert blocked, "the denied principal was able to SELECT despite the DENY"


# ---------------------------------------------------------------------------
# view materialization — the case that matters most: a view is recreated on
# every run, so an object-level DENY is lost most often here.
# ---------------------------------------------------------------------------

view_deny_sql = """
{{ config(materialized="view", denies={"select": ["dbt_deny_reader"]}) }}
select 1 as id, cast('secret' as varchar(50)) as ssn
"""


class TestViewDenies:
    @pytest.fixture(scope="class")
    def models(self):
        return {"denied_view.sql": view_deny_sql}

    def test_deny_survives_ordinary_view_rebuild(self, project):
        run_dbt(["run"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_view")

        # an ordinary view->view rebuild (no --full-refresh) still recreates the
        # object; the DENY must survive it
        run_dbt(["run"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_view")


# ---------------------------------------------------------------------------
# incremental materialization — append and full-refresh paths
# ---------------------------------------------------------------------------

incremental_deny_sql = """
{{ config(materialized="incremental", denies={"select": ["dbt_deny_reader"]}) }}
select 1 as id
{% if is_incremental() %}where 1 = 0{% endif %}
"""


class TestIncrementalDenies:
    @pytest.fixture(scope="class")
    def models(self):
        return {"denied_incremental.sql": incremental_deny_sql}

    def test_deny_survives_append_and_full_refresh(self, project):
        run_dbt(["run"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_incremental")

        # append path (existing table kept in place). The object_id is stable, so
        # the DENY is already present and the run converges: no DDL, no-change
        # message (logged at debug, as apply_masks does).
        _, out = run_dbt_and_capture(["--debug", "run"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_incremental")
        assert "all denies are in place, no changes needed" in out
        assert "deny change(s) on" not in out

        # full-refresh path (drop & recreate — DENY must be re-applied)
        run_dbt(["run", "--full-refresh"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_incremental")


# ---------------------------------------------------------------------------
# snapshot materialization
# ---------------------------------------------------------------------------

snapshot_source_sql = """
{{ config(materialized="table") }}
select 1 as id, cast('Smith' as varchar(50)) as surname
"""

denied_snapshot_sql = """
{% snapshot denied_snapshot %}
{{ config(
    unique_key='id',
    strategy='check',
    check_cols=['surname'],
    denies={'select': ['dbt_deny_reader']}
) }}
select * from {{ ref('snap_source') }}
{% endsnapshot %}
"""


class TestSnapshotDenies:
    @pytest.fixture(scope="class")
    def models(self):
        return {"snap_source.sql": snapshot_source_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"denied_snapshot.sql": denied_snapshot_sql}

    def test_deny_applied_and_survives_second_snapshot(self, project):
        run_dbt(["run"])
        run_dbt(["snapshot"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_snapshot")

        run_dbt(["snapshot"])
        assert ("SELECT", DENY_PRINCIPAL) in denied_permissions(project, "denied_snapshot")


# ---------------------------------------------------------------------------
# revocation: a DENY present but no longer configured is revoked
# ---------------------------------------------------------------------------


class TestDenyRevocation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"denied_table.sql": table_deny_sql}

    def test_unconfigured_deny_is_revoked(self, project):
        run_dbt(["run"])
        # a DENY the config does not declare (INSERT), added out of band
        exec_sql(
            project,
            f"deny insert on {project.test_schema}.denied_table to {DENY_PRINCIPAL};",
        )
        assert ("INSERT", DENY_PRINCIPAL) in denied_permissions(project, "denied_table")

        # next run reconciles: the configured SELECT stays, the stray INSERT goes
        run_dbt(["run"])
        denies = denied_permissions(project, "denied_table")
        assert ("SELECT", DENY_PRINCIPAL) in denies
        assert ("INSERT", DENY_PRINCIPAL) not in denies


# ---------------------------------------------------------------------------
# absent principal: warn and skip, run still succeeds
# ---------------------------------------------------------------------------

absent_principal_deny_sql = """
{{ config(materialized="table", denies={"select": ["nonexistent_principal_xyz"]}) }}
select 1 as id
"""


class TestAbsentPrincipal:
    @pytest.fixture(scope="class")
    def models(self):
        return {"absent_deny.sql": absent_principal_deny_sql}

    def test_absent_principal_warns_and_run_succeeds(self, project):
        results, out = run_dbt_and_capture(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        assert "nonexistent_principal_xyz" in out
        assert "does not exist" in out
        # nothing was denied, but the build did not fail
        assert denied_permissions(project, "absent_deny") == set()


# ---------------------------------------------------------------------------
# unsupported privilege: warn and skip rather than taking down the run
# ---------------------------------------------------------------------------

unsupported_privilege_sql = """
{{ config(
    materialized="table",
    denies={"execute": ["dbt_deny_reader"], "select": ["dbt_deny_reader"]}
) }}
select 1 as id
"""


class TestUnsupportedPrivilege:
    @pytest.fixture(scope="class")
    def models(self):
        return {"bad_priv.sql": unsupported_privilege_sql}

    def test_unsupported_privilege_warns_and_run_succeeds(self, project):
        results, out = run_dbt_and_capture(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        assert "execute" in out.lower()
        # the unsupported privilege is skipped; the supported one is still applied
        denies = denied_permissions(project, "bad_priv")
        assert ("SELECT", DENY_PRINCIPAL) in denies
        assert ("EXECUTE", DENY_PRINCIPAL) not in denies
