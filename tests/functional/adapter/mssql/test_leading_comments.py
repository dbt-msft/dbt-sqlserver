import pytest

from dbt.tests.util import run_dbt

model_with_line_comment_sql = """
-- This is a comment before the WITH clause
WITH input as (SELECT 1 as id)
SELECT * FROM input
"""

model_with_block_comment_sql = """
/* This is a block comment before the WITH clause */
WITH input as (SELECT 1 as id)
SELECT * FROM input
"""

model_without_cte_sql = """
-- This is a comment before a non-CTE query
SELECT 1 as id
"""

model_yml = """
version: 2
models:
  - name: model_with_line_comment
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
  - name: model_with_block_comment
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
  - name: model_without_cte
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
"""


class TestLeadingComments:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_with_line_comment.sql": model_with_line_comment_sql,
            "model_with_block_comment.sql": model_with_block_comment_sql,
            "model_without_cte.sql": model_without_cte_sql,
            "schema.yml": model_yml,
        }

    def test_comments_before_cte(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3
        for result in results:
            assert result.status == "success", f"{result.node.name} failed"
