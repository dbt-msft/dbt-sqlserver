"""Seed column types fit values beyond int and varchar(8000)."""

import pytest

from dbt.tests.util import run_dbt

LONG_TEXT = "x" * 9000


class TestSeedColumnTypes:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "ints.csv": "id,n\n1,-2147483648\n2,2147483647\n",
            "bigints.csv": "id,n\n1,5000000000\n2,-3000000000\n",
            "huge_ints.csv": "id,n\n1,9223372036854775808\n",
            "long_text.csv": f"id,t\n1,{LONG_TEXT}\n",
        }

    def column(self, project, table, column):
        return tuple(
            project.run_sql(
                f"""
                select data_type, character_maximum_length, numeric_precision, numeric_scale
                from INFORMATION_SCHEMA.COLUMNS
                where table_schema = '{project.test_schema}'
                    and table_name = '{table}' and column_name = '{column}'
                """,
                fetch="one",
            )
        )

    def values(self, project, table, column):
        return [
            row[0]
            for row in project.run_sql(
                f"select {column} from {project.test_schema}.{table} order by id", fetch="all"
            )
        ]

    def test_seed_column_types(self, project):
        run_dbt(["seed"])

        assert self.column(project, "ints", "n") == ("int", None, 10, 0)
        assert self.column(project, "bigints", "n") == ("bigint", None, 19, 0)
        assert self.values(project, "bigints", "n") == [5000000000, -3000000000]
        assert self.column(project, "huge_ints", "n") == ("numeric", None, 38, 0)
        assert self.values(project, "huge_ints", "n") == [9223372036854775808]
        assert self.column(project, "long_text", "t") == ("varchar", -1, None, None)
        assert self.values(project, "long_text", "t") == [LONG_TEXT]
