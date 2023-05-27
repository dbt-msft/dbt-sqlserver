import pytest
from dbt.tests.adapter.basic.files import config_materialized_table, config_materialized_view
from dbt.tests.util import run_dbt

source_regular = """
version: 2
sources:
- name: regular
  schema: sys
  tables:
  - name: tables
    columns:
    - name: name
      tests:
      - not_null
"""

source_space_in_name = """
version: 2
sources:
- name: 'space in name'
  schema: sys
  tables:
  - name: tables
    columns:
    - name: name
      tests:
      - not_null
"""

select_from_source_regular = """
select object_id,schema_id from {{ source("regular", "tables") }}
"""

select_from_source_space_in_name = """
select object_id,schema_id from {{ source("space in name", "tables") }}
"""


# System tables are not supported for data type reasons.
@pytest.mark.skip(
    reason="The query references an object that is not supported in distributed processing mode."
)
class TestSourcesFabric:
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
