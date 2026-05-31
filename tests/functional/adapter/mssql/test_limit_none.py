import pytest

from dbt.tests.util import run_dbt

seed_csv = """id,name
1,alice
2,bob
3,charlie
"""

schema_yml = """
version: 2
seeds:
  - name: test_seed
    columns:
      - name: id
        tests:
          - unique
          - not_null
"""

schema_with_limit_yml = """
version: 2
seeds:
  - name: test_seed
    columns:
      - name: id
        tests:
          - unique:
              config:
                limit: 10
          - not_null:
              config:
                limit: 10
"""


class TestLimitNone:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"test_seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": schema_yml}

    def test_unique_test_on_seed(self, project):
        run_dbt(["seed"])
        results = run_dbt(["test"])
        assert len(results) == 2
        for result in results:
            assert result.status == "pass"


class TestLimitExplicit:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"test_seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": schema_with_limit_yml}

    def test_unique_test_with_limit(self, project):
        run_dbt(["seed"])
        results = run_dbt(["test"])
        assert len(results) == 2
        for result in results:
            assert result.status == "pass"
