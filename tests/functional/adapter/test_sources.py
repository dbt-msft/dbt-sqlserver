import pytest
from dbt.tests.adapter.basic.files import config_materialized_table, config_materialized_view
from dbt.tests.util import run_dbt

source_regular = """
version: 2
sources:
- name: regular
  schema: INFORMATION_SCHEMA
  tables:
  - name: VIEWS
    columns:
    - name: TABLE_NAME
      tests:
      - not_null
"""

source_space_in_name = """
version: 2
sources:
- name: 'space in name'
  schema: INFORMATION_SCHEMA
  tables:
  - name: VIEWS
    columns:
    - name: TABLE_NAME
      tests:
      - not_null
"""

select_from_source_regular = """
select * from {{ source("regular", "VIEWS") }}
"""

select_from_source_space_in_name = """
select * from {{ source("space in name", "VIEWS") }}
"""


class TestSourcesSQLServer:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "source_regular.yml": source_regular,
            "source_space_in_name.yml": source_space_in_name,
            "v_select_from_source_regular.sql": config_materialized_view
            + select_from_source_regular,
            "v_select_from_source_space_in_name.sql": config_materialized_view
            + select_from_source_space_in_name,
            "t_select_from_source_regular.sql": config_materialized_table
            + select_from_source_regular,
            "t_select_from_source_space_in_name.sql": config_materialized_table
            + select_from_source_space_in_name,
        }

    def test_dbt_run(self, project):
        run_dbt(["compile"])

        ls = run_dbt(["list"])
        assert len(ls) == 8
        ls_sources = [src for src in ls if src.startswith("source:")]
        assert len(ls_sources) == 2

        run_dbt(["run"])
        run_dbt(["test"])
