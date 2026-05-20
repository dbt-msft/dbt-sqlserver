"""Interaction coverage for #613 (query_options) + #640 (TABLOCK on contract INSERT).

Each feature is exercised by its own test suite in isolation. This file verifies
that when both are present, a contract-enforced table model with query_options
emits both `WITH (TABLOCK)` (the INSERT target hint) and the `OPTION (...)`
query hint without either path interfering with the other.

The test lives on a dedicated branch that merges both feature branches because
neither individual branch can exercise the combined output: #640 alone ignores
query_options config, and #613 alone uses the non-contract `SELECT * INTO`
path which has no TABLOCK to emit.
"""

import os

import pytest

from dbt.tests.util import run_dbt

model_sql = """
{{ config(materialized='table', query_options={'MAXDOP': 1}) }}
select 1 as id
"""

model_yml = """
version: 2
models:
  - name: contract_with_options
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
"""


class TestContractTableWithQueryOptions:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "contract_with_options.sql": model_sql,
            "schema.yml": model_yml,
        }

    def test_both_hints_in_compiled_sql(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        target_dir = os.path.join(project.project_root, "target", "run")
        for root, _dirs, files in os.walk(target_dir):
            if "contract_with_options.sql" in files:
                with open(os.path.join(root, "contract_with_options.sql"), "r") as f:
                    sql = f.read()
                break
        else:
            raise AssertionError("Could not find compiled contract_with_options.sql")

        # TABLOCK is the INSERT target hint (#640).
        assert "WITH (TABLOCK)" in sql
        # MAXDOP comes from query_options (#613).
        assert "MAXDOP 1" in sql
        # The default LABEL is always present.
        assert "LABEL =" in sql
