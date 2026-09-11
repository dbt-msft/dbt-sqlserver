"""A failed first build of an incremental model must not leave a target behind.

The fresh-create branch of the incremental materialization builds straight
into ``target_relation`` — no ``__dbt_tmp`` intermediate, so no rename swap and
no ``OBJECT_ID`` drop guard (that guard only covers adapter-generated
throwaways). Since #819 split the build into an empty ``CREATE`` followed by a
separate ``INSERT ... WITH (TABLOCK)``, and the build batch declines to open
the ambient transaction, the two statements autocommit independently: a load
that fails leaves the empty table committed under the model's real name.

That is silently destructive rather than merely untidy. dbt's next run sees a
relation that exists and is not a view, so it takes the append/merge branch and
merges that run's window into an empty table. Nothing errors, and every row the
first build should have loaded is gone for good.

The invariant under test is therefore: after a failed first build, the target
does not exist — leaving dbt to do a fresh create next time, which is correct.
"""

import os

import pytest

from dbt.tests.util import run_dbt

# Rows come from a table rather than inline VALUES on purpose: the empty create
# is `SELECT TOP 0 * INTO ... FROM <view>`, and constant-folded literals could
# let the failing CAST evaluate at create time. Reading from storage guarantees
# TOP 0 touches no rows, so the create succeeds and only the INSERT fails.
source_rows_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select 1 as id, cast('10' as varchar(20)) as txt
union all
select 2 as id, cast('not_a_number' as varchar(20)) as txt
"""

# CAST fails on row 2 during the load, never during schema resolution.
failing_incremental_sql = """
{{ config(materialized='incremental', unique_key='id', as_columnstore=False) }}
select id, cast(txt as int) as val
from {{ ref('source_rows') }}
"""


class TestFailedFirstIncrementalBuildLeavesNoTarget:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_rows.sql": source_rows_sql,
            "failing_incremental.sql": failing_incremental_sql,
        }

    def test_failed_first_build_leaves_no_target(self, project):
        results = run_dbt(["run"], expect_pass=False)

        statuses = {r.node.name: r.status for r in results}
        assert statuses["source_rows"] == "success"
        assert statuses["failing_incremental"] == "error"

        object_id = project.run_sql(
            f"select OBJECT_ID('{project.test_schema}.failing_incremental')",
            fetch="one",
        )[0]
        assert object_id is None, (
            "the failed load committed an empty table under the model's real "
            "name; dbt's next run will take the append branch and merge into "
            "it, silently losing every row the first build should have loaded"
        )


# The corrected model: same shape, no unconvertible row.
fixed_incremental_sql = """
{{ config(materialized='incremental', unique_key='id', as_columnstore=False) }}
select id, cast(txt as int) as val
from {{ ref('source_rows') }}
where txt <> 'not_a_number'
"""

clean_incremental_sql = """
{{ config(materialized='incremental', unique_key='id', as_columnstore=False) }}
select 1 as id, 10 as val
"""


class TestRecoveryAfterFailedFirstBuild:
    """The run after a failed first build must load everything, not a window."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_rows.sql": source_rows_sql,
            "failing_incremental.sql": failing_incremental_sql,
        }

    def test_rerun_after_failure_loads_all_rows(self, project):
        run_dbt(["run"], expect_pass=False)

        # Repair the model and run again. If the failed build had left an empty
        # target behind, this run would take the append branch and merge into
        # it; the row it should have loaded from the first build would be lost.
        path = os.path.join(project.project_root, "models", "failing_incremental.sql")
        with open(path, "w") as handle:
            handle.write(fixed_incremental_sql)

        run_dbt(["run"])

        rows = project.run_sql(
            f"select id, val from {project.test_schema}.failing_incremental order by id",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == [(1, 10)]


class TestFirstIncrementalBuildStillSucceeds:
    """Guards the swap's rename guard.

    The fresh-create branch now swaps, and the swap's target->backup rename is
    only correct when a target exists. Without the guard this is sp_rename
    against a missing name (Msg 15225) on the first build of every incremental
    model - so a plain happy-path build is the regression test.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"clean_incremental.sql": clean_incremental_sql}

    def test_first_build_and_rerun(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        rows = project.run_sql(
            f"select id, val from {project.test_schema}.clean_incremental", fetch="all"
        )
        assert [tuple(row) for row in rows] == [(1, 10)]

        # Second run takes the append branch against the swapped-in target.
        run_dbt(["run"])
        rows = project.run_sql(
            f"select count(*) from {project.test_schema}.clean_incremental", fetch="one"
        )
        assert rows[0] == 1
