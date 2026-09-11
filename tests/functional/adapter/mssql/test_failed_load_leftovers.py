"""A failed load leaves the staged objects behind; the next run must clear them.

Under pre_hook_transaction_scope='load' (the default) schema resolution
commits before the load, so a load that fails - or anything after it that
rolls the transaction back - leaves the empty intermediate and its tmp view in
the database. Three mechanisms clean that up on the next run, and these tests
prove each path actually recovers rather than tripping over its own leftovers:

  - the materialization drops the cached preexisting intermediate up front,
  - the stage batch carries an OBJECT_ID guard for adapter-generated
    throwaways, so a stale relation cache cannot make it hit Msg 2714, and
  - the tmp view is dropped by name and re-created with CREATE OR ALTER.

The dml path drops both of its scratch objects by name before staging.
"""

import pytest

from dbt.tests.util import run_dbt

# `bad: true` puts an unparseable row in the source; the models cast it, so
# the load fails after the empty create has committed.
source_rows_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select 1 as id, cast('10' as varchar(20)) as txt
union all
select 2 as id, cast({{ "'oops'" if var('bad', true) else "'20'" }} as varchar(20)) as txt
"""

rename_model_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select id, cast(txt as int) as val from {{ ref('source_rows') }}
"""

dml_model_sql = """
{{ config(materialized='table', as_columnstore=False, table_refresh_method='dml') }}
select id, cast(txt as int) as val from {{ ref('source_rows') }}
"""

snapshot_sql = """
{% snapshot snap %}
{{ config(unique_key='id', strategy='check', check_cols='all') }}
select id, cast(txt as int) as val from {{ ref('source_rows') }}
{% endsnapshot %}
"""


def _object_exists(project, name):
    return (
        project.run_sql(f"select object_id('{project.test_schema}.{name}')", fetch="one")[0]
        is not None
    )


class TestTableRenamePathRecovers:
    @pytest.fixture(scope="class")
    def models(self):
        return {"source_rows.sql": source_rows_sql, "rename_model.sql": rename_model_sql}

    def test_rerun_clears_leftovers(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert {r.node.name: r.status for r in results}["rename_model"] == "error"
        assert _object_exists(project, "rename_model__dbt_tmp")
        assert _object_exists(project, "rename_model__dbt_tmp__dbt_tmp_vw")
        assert not _object_exists(project, "rename_model")

        results = run_dbt(["run", "--vars", "bad: false"])
        assert all(r.status == "success" for r in results)
        assert not _object_exists(project, "rename_model__dbt_tmp")
        assert not _object_exists(project, "rename_model__dbt_tmp__dbt_tmp_vw")
        assert not _object_exists(project, "rename_model__dbt_backup")
        rows = project.run_sql(
            f"select count(*) from {project.test_schema}.rename_model", fetch="one"
        )[0]
        assert rows == 2


class TestDmlRefreshPathRecovers:
    @pytest.fixture(scope="class")
    def models(self):
        return {"source_rows.sql": source_rows_sql, "dml_model.sql": dml_model_sql}

    def test_rerun_clears_leftovers(self, project):
        # the dml path only applies once the target exists
        run_dbt(["run", "--vars", "bad: false"])
        assert _object_exists(project, "dml_model")

        results = run_dbt(["run"], expect_pass=False)
        assert {r.node.name: r.status for r in results}["dml_model"] == "error"
        assert _object_exists(project, "dml_model__dbt_refresh")
        assert _object_exists(project, "dml_model__dbt_refresh__dbt_tmp_vw")
        # the target was never touched: the failure was in the scratch load
        rows = project.run_sql(
            f"select count(*) from {project.test_schema}.dml_model", fetch="one"
        )[0]
        assert rows == 2

        results = run_dbt(["run", "--vars", "bad: false"])
        assert all(r.status == "success" for r in results)
        assert not _object_exists(project, "dml_model__dbt_refresh")
        assert not _object_exists(project, "dml_model__dbt_refresh__dbt_tmp_vw")


class TestSnapshotFirstBuildRecovers:
    @pytest.fixture(scope="class")
    def models(self):
        return {"source_rows.sql": source_rows_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": snapshot_sql}

    def test_rerun_clears_leftovers(self, project):
        run_dbt(["run"])
        results = run_dbt(["snapshot"], expect_pass=False)
        assert results[0].status == "error"
        assert _object_exists(project, "snap__dbt_tmp")
        assert _object_exists(project, "snap__dbt_tmp__dbt_tmp_vw")
        assert _object_exists(project, "snap_snapshot_staging_temp_view")
        assert not _object_exists(project, "snap")

        run_dbt(["run", "--vars", "bad: false"])
        results = run_dbt(["snapshot"])
        assert results[0].status == "success"
        assert _object_exists(project, "snap")
        assert not _object_exists(project, "snap__dbt_tmp")
        assert not _object_exists(project, "snap__dbt_tmp__dbt_tmp_vw")
        assert not _object_exists(project, "snap_snapshot_staging_temp_view")

        # and a second snapshot run (the merge path) leaves no staging objects
        results = run_dbt(["snapshot"])
        assert results[0].status == "success"
        assert not _object_exists(project, "snap__dbt_temp")
        assert not _object_exists(project, "snap__dbt_temp__dbt_tmp_vw")
        assert not _object_exists(project, "snap_snapshot_staging_temp_view")
