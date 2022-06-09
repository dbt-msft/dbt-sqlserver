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

source_freshness = """
version: 2
sources:
- name: freshness
  error_after:
    count: 1
    period: minute
  schema: INFORMATION_SCHEMA
  tables:
  - name: freshness_timestamp
    loaded_at_field: CAST(current_timestamp AS timestamp)
    identifier: VIEWS
  - name: freshness_datetimeoffset
    identifier: VIEWS
    loaded_at_field: CAST(current_timestamp AS datetimeoffset)
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
            "source_freshness.yml": source_freshness,
            "v_select_from_source_regular.sql": config_materialized_view
            + select_from_source_regular,
            "v_select_from_source_space_in_name.sql": config_materialized_view
            + select_from_source_space_in_name,
            "t_select_from_source_regular.sql": config_materialized_table
            + select_from_source_regular,
            "t_select_from_source_space_in_name.sql": config_materialized_table
            + select_from_source_space_in_name,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_sources",
        }

    def test_dbt_run(self, project):
        run_dbt(["compile"])

        ls = run_dbt(["list"])
        assert len(ls) == 10
        ls_sources = [src for src in ls if src.startswith("source:")]
        assert len(ls_sources) == 3

        run_dbt(["run"])
        run_dbt(["test"])
        run_dbt(["source", "freshness"])
