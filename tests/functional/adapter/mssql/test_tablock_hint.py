import os

import pytest

from dbt.tests.util import run_dbt

model_sql = """
{{ config(materialized="table") }}
select 1 as id
"""

model_yml = """
version: 2
models:
  - name: contract_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
"""


class TestTablockHint:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "contract_model.sql": model_sql,
            "schema.yml": model_yml,
        }

    def test_contract_table_uses_tablock(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        target_dir = os.path.join(project.project_root, "target", "run")
        path = None
        for root, dirs, files in os.walk(target_dir):
            if "contract_model.sql" in files:
                path = os.path.join(root, "contract_model.sql")
                break

        assert path is not None, "Could not find compiled contract_model.sql"
        with open(path, "r") as f:
            sql = f.read()

        assert "WITH (TABLOCK)" in sql
        assert "INSERT INTO" in sql
