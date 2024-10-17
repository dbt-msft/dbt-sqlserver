import pytest
from dbt.tests.util import get_connection, run_dbt

from tests.functional.adapter.mssql.test_index_config import index_count, indexes_def

# flake8: noqa: E501

index_seed_csv = """id_col,data,secondary_data,tertiary_data
1,'a'",122,20
"""

index_schema_base_yml = """
version: 2
seeds:
  - name: raw_data
    config:
      column_types:
          id_col: integer
          data: nvarchar(20)
          secondary_data: integer
          tertiary_data: bigint
"""

model_yml = """
version: 2
models:
  - name: index_model
  - name: index_ccs_model
"""

model_sql = """
{{
  config({
  "materialized": 'table',
  "as_columnstore": False,
        "post-hook": [
            "{{ create_clustered_index(columns = ['id_col'], unique=True) }}",
            "{{ create_nonclustered_index(columns = ['data']) }}",
            "{{ create_nonclustered_index(columns = ['secondary_data'], includes = ['tertiary_data']) }}",
        ]
  })
}}
  select * from {{ ref('raw_data') }}
"""

model_sql_ccs = """
{{
  config({
  "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['data']) }}",
            "{{ create_nonclustered_index(columns = ['secondary_data'], includes = ['tertiary_data']) }}",
        ]
  })
}}
  select * from {{ ref('raw_data') }}
"""

drop_schema_model = """
{{
  config({
  "materialized": 'table',
        "post-hook": [
            "{{ drop_all_indexes_on_table() }}",
        ]
  })
}}
select * from {{ ref('raw_data') }}
"""


class TestIndex:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_data.csv": index_seed_csv,
            "schema.yml": index_schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "index_model.sql": model_sql,
            "index_ccs_model.sql": model_sql_ccs,
            "schema.yml": model_yml,
        }

    def drop_artifacts(self, project):
        with get_connection(project.adapter):
            project.adapter.execute("DROP TABLE IF EXISTS index_model", fetch=True)
            project.adapter.execute("DROP TABLE IF EXISTS index_ccs_model")

    def test_create_index(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        with get_connection(project.adapter):
            result, table = project.adapter.execute(
                index_count.format(schema_name=project.created_schemas[0]), fetch=True
            )
        schema_dict = {_[0]: _[1] for _ in table.rows}
        expected = {
            "Clustered columnstore index": 1,
            "Clustered index": 1,
            "Nonclustered unique index": 4,
        }
        self.drop_artifacts(project)
        assert schema_dict == expected


class TestIndexDropsOnlySchema:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_data.csv": index_seed_csv,
            "schema.yml": index_schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "index_model.sql": drop_schema_model,
            "index_ccs_model.sql": model_sql_ccs,
            "schema.yml": model_yml,
        }

    def create_table_and_index_other_schema(self, project):
        _schema = project.test_schema + "other"
        create_sql = f"""
        USE [{project.database}];
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schema}')
        BEGIN
        EXEC('CREATE SCHEMA [{_schema}]')
        END
        """

        create_table = f"""
        CREATE TABLE {_schema}.index_model (
        IDCOL BIGINT
        )
        """

        create_index = f"""
        CREATE INDEX sample_schema ON {_schema}.index_model (IDCOL)
        """
        with get_connection(project.adapter):
            project.adapter.execute(create_sql, fetch=True)
            project.adapter.execute(create_table)
            project.adapter.execute(create_index)

    def drop_schema_artifacts(self, project):
        _schema = project.test_schema + "other"
        drop_index = f"DROP INDEX IF EXISTS sample_schema ON {_schema}.index_model"
        drop_table = f"DROP TABLE IF EXISTS {_schema}.index_model"
        drop_schema = f"DROP SCHEMA IF EXISTS {_schema}"

        with get_connection(project.adapter):
            project.adapter.execute(drop_index, fetch=True)
            project.adapter.execute(drop_table)
            project.adapter.execute(drop_schema)

    def validate_other_schema(self, project):
        with get_connection(project.adapter):
            result, table = project.adapter.execute(
                indexes_def.format(
                    schema_name=project.test_schema + "other", table_name="index_model"
                ),
                fetch=True,
            )

        assert len(table.rows) == 1

    def test_create_index(self, project):
        self.create_table_and_index_other_schema(project)
        run_dbt(["seed"])
        run_dbt(["run"])
        self.validate_other_schema(project)
        self.drop_schema_artifacts(project)
