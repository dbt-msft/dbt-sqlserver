import pytest
from dbt.tests.util import run_dbt


class TestAdditionalIncrementalSQLServer:
    #  https://github.com/dbt-msft/dbt-sqlserver/issues/241

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "issue_241", "models": {"+materialized": "incremental"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_unique_array.sql": """
    {{
      config ( unique_key=['A', 'B'] )
    }}
    WITH CTE AS
    (
      SELECT 1 AS A, 2 AS B
      UNION
      SELECT 3 AS A, 4 AS B
    )
    SELECT A, B FROM CTE""",
        }

    def test_build(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
