"""A failed column widening must not wedge the model (dbt-msft/dbt-sqlserver#836).

sqlserver__alter_column_type's four-step path autocommits each step: add
<col>__dbt_alter, copy, drop the original, rename. A run that failed part way
left <col>__dbt_alter behind, and every later run failed on the ADD with
Msg 2705 until someone dropped it by hand. prefer_single_alter_column was also
ignored on this path, because the macro runs from Python without model config;
unset, a same-family widening now uses a single ALTER COLUMN.
"""

import pytest

from dbt.tests.util import run_dbt, write_file

MODEL = """
{{{{ config(materialized='incremental', as_columnstore=false{extra}) }}}}
select cast('keep me' as varchar({size})) as c, 1 as id
"""


def model(size, extra=""):
    return MODEL.format(size=size, extra=extra)


class AlterColumnBase:
    extra = ""

    @pytest.fixture(scope="class")
    def models(self):
        return {"m.sql": model(35, self.extra)}

    def col_length(self, project, column):
        return project.run_sql(
            f"select col_length('{project.test_schema}.m', '{column}')", fetch="one"
        )[0]

    def values(self, project):
        return [
            row[0]
            for row in project.run_sql(f"select c from {project.test_schema}.m", fetch="all")
        ]

    def widen(self, project):
        write_file(model(63, self.extra), project.project_root, "models", "m.sql")


class TestLeftoverPartialCopy(AlterColumnBase):
    extra = ", prefer_single_alter_column=false"

    def test_leftover_is_dropped_and_the_widening_runs(self, project):
        run_dbt(["run"])
        # What an interrupted run leaves: the original column plus a partial copy.
        project.run_sql(f"alter table {project.test_schema}.m add c__dbt_alter varchar(63)")

        self.widen(project)
        run_dbt(["run"])

        assert self.col_length(project, "c__dbt_alter") is None
        assert self.col_length(project, "c") == 63
        assert set(self.values(project)) == {"keep me"}


class TestFailedWideningRetries(AlterColumnBase):
    extra = ", prefer_single_alter_column=false"

    def test_retry_after_a_failed_widening(self, project):
        run_dbt(["run"])
        # An index on the column makes the DROP COLUMN step fail (Msg 5074).
        project.run_sql(f"create index ix_c on {project.test_schema}.m (c)")

        self.widen(project)
        run_dbt(["run"], expect_pass=False)

        project.run_sql(f"drop index ix_c on {project.test_schema}.m")
        run_dbt(["run"])

        assert self.col_length(project, "c__dbt_alter") is None
        assert self.col_length(project, "c") == 63


class TestWideningDefaultsToSingleAlterColumn(AlterColumnBase):
    def test_single_alter_column_is_used(self, project):
        run_dbt(["run"])
        # SQL Server lets ALTER COLUMN widen an indexed varchar, but the
        # four-step path fails dropping it (Msg 5074), so a pass here means the
        # single statement ran.
        project.run_sql(f"create index ix_c on {project.test_schema}.m (c)")

        self.widen(project)
        run_dbt(["run"])

        assert self.col_length(project, "c__dbt_alter") is None
        assert self.col_length(project, "c") == 63
        assert set(self.values(project)) == {"keep me"}
