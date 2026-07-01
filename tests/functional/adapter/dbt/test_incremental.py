import pytest

from dbt.tests.adapter.incremental import fixtures
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    TestIncrementalPredicatesDeleteInsert,
    TestPredicatesDeleteInsert,
)
from dbt.tests.util import run_dbt, write_file


def _column_metadata(project, schema, table, column):
    rows = project.run_sql(
        f"""
        select
            t.name,
            c.max_length,
            c.precision,
            c.scale
        from [{project.database}].sys.columns c
        inner join [{project.database}].sys.types t
            on c.user_type_id = t.user_type_id
        where c.object_id = object_id('[{project.database}].[{schema}].[{table}]')
          and c.name = '{column}'
        """,
        fetch="all",
    )
    assert rows, f"Missing column metadata for {schema}.{table}.{column}"

    data_type, max_length, numeric_precision, numeric_scale = rows[0]
    if data_type in ("nchar", "nvarchar", "sysname") and max_length is not None:
        max_length //= 2
        return data_type, max_length, None, None
    return data_type, None, numeric_precision, numeric_scale


_MODELS__INCREMENTAL_IGNORE_SQLServer = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='ignore'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental() %}

SELECT id, field1, field2, field3, field4
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT TOP 3 id, field1, field2 FROM source_data

{% endif %}
"""

_MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET_SQLServer = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = dbt.type_string() %}

select id
       ,cast(field1 as {{string_type}}) as field1

from source_data
"""

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET_SQLServer = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = dbt.type_string() %}

select id
       ,cast(field1 as {{string_type}}) as field1
       --,field2
       ,cast(case when id <= 3 then null else field3 end as {{string_type}}) as field3
       ,cast(case when id <= 3 then null else field4 end as {{string_type}}) as field4

from source_data
"""


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_sync_remove_only.sql": fixtures._MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
            "incremental_ignore.sql": _MODELS__INCREMENTAL_IGNORE_SQLServer,
            "incremental_sync_remove_only_target.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET_SQLServer,  # noqa: E501
            "incremental_ignore_target.sql": fixtures._MODELS__INCREMENTAL_IGNORE_TARGET,
            "incremental_fail.sql": fixtures._MODELS__INCREMENTAL_FAIL,
            "incremental_sync_all_columns.sql": fixtures._MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
            "incremental_append_new_columns_remove_one.sql": fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,  # noqa: E501
            "model_a.sql": fixtures._MODELS__A,
            "incremental_append_new_columns_target.sql": fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,  # noqa: E501
            "incremental_append_new_columns.sql": fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,  # noqa: E501
            "incremental_sync_all_columns_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET_SQLServer,  # noqa: E501
            "incremental_append_new_columns_remove_one_target.sql": fixtures._MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,  # noqa: E501
        }


class TestIncrementalPredicatesDeleteInsert(TestIncrementalPredicatesDeleteInsert):
    pass


class TestPredicatesDeleteInsert(TestPredicatesDeleteInsert):
    pass


_INCREMENTAL__WIDEN_TYPES_SQLServer = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

{% if is_incremental() %}
-- incremental branch: uses larger types and values that would fail if table types were not widened
select
  2 as id,
  cast(40000 as int) as num_int,
  cast('abcdef' as nvarchar(10)) as field1,
  cast(100.25 as decimal(10,4)) as num_decimal,
  cast(999999999999998.9999 as decimal(20,4)) as num_money
{% else %}
-- full-refresh branch: creates the table with smaller types
select
  1 as id,
  cast(1 as smallint) as num_int,
  cast('abc' as varchar(5)) as field1,
  cast(10.5 as decimal(5,2)) as num_decimal,
  cast(1240.14 as money) as num_money
{% endif %}
"""


class TestIncrementalOnSchemaChangeExpands:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_enable_safe_type_expansion": True}}

    def test_run_incremental_widen_types(self, project):
        """Full-refresh to create small types, then incremental to widen types."""
        write_file(_INCREMENTAL__WIDEN_TYPES_SQLServer, "models", "incremental_change_widen.sql")

        # Full-refresh to create table with smallint and varchar(5)
        run_dbt(
            ["run", "--models", "incremental_change_widen", "--full-refresh"]
        )  # creates small types

        # Run again to trigger incremental insert which requires widened types
        # incremental branch inserts larger values
        run_dbt(["run", "--models", "incremental_change_widen"])

        assert _column_metadata(
            project, project.test_schema, "incremental_change_widen", "field1"
        ) == (
            "nvarchar",
            10,
            None,
            None,
        )
        assert _column_metadata(
            project, project.test_schema, "incremental_change_widen", "num_int"
        ) == (
            "int",
            None,
            10,
            0,
        )
        assert _column_metadata(
            project, project.test_schema, "incremental_change_widen", "num_decimal"
        ) == (
            "decimal",
            None,
            10,
            4,
        )
        assert _column_metadata(
            project, project.test_schema, "incremental_change_widen", "num_money"
        ) == (
            "decimal",
            None,
            20,
            4,
        )
