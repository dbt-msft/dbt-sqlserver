"""Changing a column's type must keep its NOT NULL.

ALTER COLUMN without NULL or NOT NULL makes the column nullable, and the
four-step rewrite adds the new column as nullable.
"""

import pytest

from dbt.tests.util import run_dbt, write_file

MODEL = """
{{{{ config(materialized='incremental', as_columnstore=false{extra}) }}}}
select isnull(cast('keep me' as varchar({size})), '') as c, 1 as id
"""


def model(size, extra=""):
    return MODEL.format(size=size, extra=extra)


class NullabilityBase:
    extra = ""

    @pytest.fixture(scope="class")
    def models(self):
        return {"m.sql": model(35, self.extra)}

    def column(self, project):
        return tuple(
            project.run_sql(
                f"""
            select max_length, is_nullable from sys.columns
            where object_id = object_id('{project.test_schema}.m') and name = 'c'
            """,
                fetch="one",
            )
        )

    def test_widening_keeps_not_null(self, project):
        run_dbt(["run"])
        assert self.column(project) == (35, False)

        write_file(model(63, self.extra), project.project_root, "models", "m.sql")
        run_dbt(["run"])

        assert self.column(project) == (63, False)


class TestSingleAlterColumnKeepsNotNull(NullabilityBase):
    pass


class TestRewriteKeepsNotNull(NullabilityBase):
    extra = ", prefer_single_alter_column=false"
