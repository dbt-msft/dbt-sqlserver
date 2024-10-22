import re

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

base_validation = """
with base_query AS (
select i.[name] as index_name,
  substring(column_names, 1, len(column_names)-1) as [columns],
  substring(included_column_names, 1, len(included_column_names)-1) as included_columns,
  case when i.[type] = 1 then 'clustered'
    when i.[type] = 2 then 'nonclustered'
    when i.[type] = 3 then 'xml'
    when i.[type] = 4 then 'spatial'
    when i.[type] = 5 then 'clustered columnstore'
    when i.[type] = 6 then 'nonclustered columnstore'
    when i.[type] = 7 then 'nonclustered hash'
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
            and ic.is_included_column = 0
              order by key_ordinal
              for xml path ('') ) D (column_names)
  cross apply (select col.[name] + ', '
          from sys.index_columns ic
            inner join sys.columns col
              on ic.object_id = col.object_id
              and ic.column_id = col.column_id
          where ic.object_id = t.object_id
            and ic.index_id = i.index_id
            and ic.is_included_column = 1
              order by key_ordinal
              for xml path ('') ) E (included_column_names)
where t.is_ms_shipped <> 1
and index_id > 0
)
"""

index_count = (
    base_validation
    + """
select
  index_type + case when [unique] = 'Unique' then ' unique' else '' end as index_type,
  count(*) index_count
from
  base_query
WHERE
  schema_name='{schema_name}'
group by index_type + case when [unique] = 'Unique' then ' unique' else '' end
"""
)

indexes_def = (
    base_validation
    + """
SELECT
  index_name,
  [columns],
  [included_columns],
  index_type,
  [unique],
  table_view,
  [object_type],
  schema_name
FROM
  base_query
WHERE
  schema_name='{schema_name}'
  AND
  table_view='{schema_name}.{table_name}'

"""
)

# Altered from: https://github.com/dbt-labs/dbt-postgres

models__incremental_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a'], 'type': 'nonclustered'},
      {'columns': ['column_a', 'column_b'], 'unique': True},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__columnstore_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a'], 'type': 'columnstore'},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""


models__table_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a']},
      {'columns': ['column_b']},
      {'columns': ['column_a', 'column_b']},
      {'columns': ['column_b', 'column_a'], 'type': 'clustered', 'unique': True},
      {'columns': ['column_a','column_c'],
        'type': 'nonclustered',
        'included_columns': ['column_b']},
    ]
  )
}}

select 1 as column_a, 2 as column_b, 3 as column_c

"""


models__table_included_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a'], 'included_columns': ['column_b']},
      {'columns': ['column_b'], 'type': 'clustered'}
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_columns_type_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': 'column_a, column_b'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_type_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'type': 'non_existent_type'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_unique_config_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'unique': 'yes'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__missing_columns_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'unique': True},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

snapshots__colors_sql = """
{% snapshot colors %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            as_columnstore=False,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=[
              {'columns': ['id'], 'type': 'nonclustered'},
              {'columns': ['id', 'color'], 'unique': True},
            ]
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color union all
        select 2 as id, 'green' as color

    {% else %}

        select 1 as id, 'blue' as color union all
        select 2 as id, 'green' as color

    {% endif %}

{% endsnapshot %}

"""

seeds__seed_csv = """country_code,country_name
US,United States
CA,Canada
GB,United Kingdom
"""


class TestSQLServerIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table.sql": models__table_sql,
            "incremental.sql": models__incremental_sql,
            "columnstore.sql": models__columnstore_sql,
            "table_included.sql": models__table_included_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"colors.sql": snapshots__colors_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seeds": {
                "quote_columns": False,
                "indexes": [
                    {"columns": ["country_code"], "unique": False},
                    {
                        "columns": ["country_code", "country_name"],
                        "unique": True,
                        "type": "clustered",
                    },
                ],
            },
            "vars": {
                "version": 1,
            },
        }

    def test_table(self, project, unique_schema):
        results = run_dbt(["run", "--models", "table"])
        assert len(results) == 1

        indexes = self.get_indexes("table", project, unique_schema)
        indexes = self.sort_indexes(indexes)
        expected = [
            {
                "columns": "column_a",
                "unique": False,
                "type": "nonclustered",
                "included_columns": None,
            },
            {
                "columns": "column_a, column_b",
                "unique": False,
                "type": "nonclustered",
                "included_columns": None,
            },
            {
                "columns": "column_a, column_c",
                "unique": False,
                "type": "nonclustered",
                "included_columns": "column_b",
            },
            {
                "columns": "column_b",
                "unique": False,
                "type": "nonclustered",
                "included_columns": None,
            },
            {
                "columns": "column_b, column_a",
                "unique": True,
                "type": "clustered",
                "included_columns": None,
            },
        ]
        assert indexes == expected

    def test_table_included(self, project, unique_schema):
        results = run_dbt(["run", "--models", "table_included"])
        assert len(results) == 1

        indexes = self.get_indexes("table_included", project, unique_schema)
        indexes = self.sort_indexes(indexes)
        expected = [
            {
                "columns": "column_a",
                "unique": False,
                "type": "nonclustered",
                "included_columns": "column_b",
            },
            {
                "columns": "column_b",
                "unique": False,
                "type": "clustered",
                "included_columns": None,
            },
        ]
        assert indexes == expected

    def test_incremental(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["run", "--models", "incremental"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("incremental", project, unique_schema)
            indexes = self.sort_indexes(indexes)
            expected = [
                {
                    "columns": "column_a",
                    "unique": False,
                    "type": "nonclustered",
                    "included_columns": None,
                },
                {
                    "columns": "column_a, column_b",
                    "unique": True,
                    "type": "nonclustered",
                    "included_columns": None,
                },
            ]
            assert indexes == expected

    def test_columnstore(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["run", "--models", "columnstore"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("columnstore", project, unique_schema)
            expected = [
                {
                    "columns": "column_a",
                    "unique": False,
                    "type": "columnstore",
                    "included_columns": None,
                },
            ]
            assert len(indexes) == len(
                expected
            )  # Nonclustered columnstore indexes meta is different

    def test_seed(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["seed"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("seed", project, unique_schema)
            indexes = self.sort_indexes(indexes)
            expected = [
                {
                    "columns": "country_code",
                    "unique": False,
                    "type": "nonclustered",
                    "included_columns": None,
                },
                {
                    "columns": "country_code, country_name",
                    "unique": True,
                    "type": "clustered",
                    "included_columns": None,
                },
            ]
            assert indexes == expected

    def test_snapshot(self, project, unique_schema):
        for version in [1, 2]:
            results = run_dbt(["snapshot", "--vars", f"version: {version}"])
            assert len(results) == 1

            indexes = self.get_indexes("colors", project, unique_schema)
            indexes = self.sort_indexes(indexes)
            expected = [
                {
                    "columns": "id",
                    "unique": False,
                    "type": "nonclustered",
                    "included_columns": None,
                },
                {
                    "columns": "id, color",
                    "unique": True,
                    "type": "nonclustered",
                    "included_columns": None,
                },
            ]
            assert indexes == expected

    def get_indexes(self, table_name, project, unique_schema):
        sql = indexes_def.format(schema_name=unique_schema, table_name=table_name)
        results = project.run_sql(sql, fetch="all")
        return [self.index_definition_dict(row) for row in results]

    def index_definition_dict(self, index_definition):
        is_unique = index_definition[4] == "Unique"
        return {
            "columns": index_definition[1],
            "included_columns": index_definition[2],
            "unique": is_unique,
            "type": index_definition[3],
        }

    def sort_indexes(self, indexes):
        return sorted(indexes, key=lambda x: (x["columns"], x["type"]))


class TestSQLServerInvalidIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_unique_config.sql": models_invalid__invalid_unique_config_sql,
            "invalid_type.sql": models_invalid__invalid_type_sql,
            "invalid_columns_type.sql": models_invalid__invalid_columns_type_sql,
            "missing_columns.sql": models_invalid__missing_columns_sql,
        }

    def test_invalid_index_configs(self, project):
        results, output = run_dbt_and_capture(expect_pass=False)
        assert len(results) == 4
        assert re.search(r"columns.*is not of type 'array'", output)
        assert re.search(r"unique.*is not of type 'boolean'", output)
        assert re.search(r"'columns' is a required property", output)
        assert re.search(r"'non_existent_type'.*is not one of", output)
