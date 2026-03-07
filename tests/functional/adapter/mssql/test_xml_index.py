import pytest

xml_seed = """id_col,xml_data
1,<data><test>1</test></data>"
"""

xml_schema_base_yml = """
version: 2
seeds:
  - name: xml_data
    config:
      column_types:
          id_col: integer
          xml_data: xml
"""

xml_model_yml = """
version: 2
models:
  - name: xml_model
    columns:
     - name: id
     - name: xml_data
"""

xml_sql = """
{{ config(materialized="table") }}
  select * from {{ ref('xml_data') }}
"""


class TestIndex:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "xml_data.csv": xml_seed,
            "schema.yml": xml_schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "xml_model.sql": xml_sql,
            "schema.yml": xml_model_yml,
        }
