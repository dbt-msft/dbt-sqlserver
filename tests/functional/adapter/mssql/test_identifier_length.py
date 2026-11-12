"""Relation names up to 128 characters, the SQL Server limit."""

import pytest

from dbt.tests.util import run_dbt


class TestMaxLengthRelationName:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": "select 1 as id"}

    def test_run_beside_a_table_with_a_128_character_name(self, project):
        # dbt lists every relation in a schema it builds into, not only its own models.
        project.run_sql(f"create table {project.test_schema}.{'t' * 128} (id int)")
        run_dbt(["run"])
