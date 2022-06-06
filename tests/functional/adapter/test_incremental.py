import pytest
from dbt.tests.util import run_dbt


class TestAdditionalIncrementalSQLServer:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            # https://github.com/dbt-msft/dbt-sqlserver/issues/241
            "incremental_unique_array.sql": """
    {{
      config
      (
        materialized='incremental'
        , unique_key=['A', 'B']
      )
    }}
    WITH CTE AS
    (
      SELECT 1 AS A, 2 AS B
      UNION
      SELECT 3 AS A, 4 AS B
    )
    SELECT
      A
      , B
    FROM
      CTE
            """,
        }

    def test_build(self, project):
        run_dbt(["build"])
