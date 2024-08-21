import pytest
from dbt.tests.util import get_connection, run_dbt

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

base_validation = """
with base_query AS (
select i.[name] as index_name,
    substring(column_names, 1, len(column_names)-1) as [columns],
    case when i.[type] = 1 then 'Clustered index'
        when i.[type] = 2 then 'Nonclustered unique index'
        when i.[type] = 3 then 'XML index'
        when i.[type] = 4 then 'Spatial index'
        when i.[type] = 5 then 'Clustered columnstore index'
        when i.[type] = 6 then 'Nonclustered columnstore index'
        when i.[type] = 7 then 'Nonclustered hash index'
        end as index_type,
    case when i.is_unique = 1 then 'Unique'
        else 'Not unique' end as [unique],
    schema_name(t.schema_id) + '.' + t.[name] as table_view,
    case when t.[type] = 'U' then 'Table'
        when t.[type] = 'V' then 'View'
        end as [object_type],
  s.name as schema_name
from sys.objects t
  inner join sys.schemas s
    on
      t.schema_id = s.schema_id
    inner join sys.indexes i
        on t.object_id = i.object_id
    cross apply (select col.[name] + ', '
                    from sys.index_columns ic
                        inner join sys.columns col
                            on ic.object_id = col.object_id
                            and ic.column_id = col.column_id
                    where ic.object_id = t.object_id
                        and ic.index_id = i.index_id
                            order by key_ordinal
                            for xml path ('') ) D (column_names)
where t.is_ms_shipped <> 1
and index_id > 0
)
"""

index_count = (
    base_validation
    + """
select
  index_type,
  count(*) index_count
from
  base_query
WHERE
  schema_name='{schema_name}'
group by index_type
"""
)


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
        assert schema_dict == expected
