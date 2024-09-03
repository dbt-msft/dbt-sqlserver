import pytest
from dbt.tests.util import run_dbt

seed_schema_yml = """
version: 2
seeds:
  - name: raw_data
"""


class TestLargeSeed:
    def build_large_seed_file(self):
        row_count = 3000
        column_count = 10

        headers = ",".join(["id"] + [f"column_{_}" for _ in range(1, column_count)])
        seed_data = [headers]
        for row in range(1, row_count):
            row_data = [str(row)]
            for column in range(1, column_count):
                row_data += [str(column)]

            row_data = ",".join(row_data)
            seed_data += [row_data]

        large_seed_file = "\n".join(seed_data)
        return large_seed_file

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_data.csv": self.build_large_seed_file(),
            "schema.yml": seed_schema_yml,
        }

    def test_large_seed(self, project):
        run_dbt(["seed"])
