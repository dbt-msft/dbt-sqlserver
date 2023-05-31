import pytest
from dbt.tests.adapter.incremental.fixtures import (
    _MODELS__A,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_FAIL,
    _MODELS__INCREMENTAL_IGNORE_TARGET,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey

_MODELS__INCREMENTAL_IGNORE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='ignore'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental() %}

SELECT
    id,
    field1,
    field2,
    field3,
    field4
FROM source_data
WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT TOP 3 id, field1, field2 FROM source_data

{% endif %}
"""

_MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET = """
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

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET = """
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


@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestBaseIncrementalUniqueKeyFabric(BaseIncrementalUniqueKey):
    pass


@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestIncrementalOnSchemaChangeFabric(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_sync_remove_only.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
            "incremental_ignore.sql": _MODELS__INCREMENTAL_IGNORE,
            "incremental_sync_remove_only_target.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,  # noqa: E501
            "incremental_ignore_target.sql": _MODELS__INCREMENTAL_IGNORE_TARGET,
            "incremental_fail.sql": _MODELS__INCREMENTAL_FAIL,
            "incremental_sync_all_columns.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
            "incremental_append_new_columns_remove_one.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,  # noqa: E501
            "model_a.sql": _MODELS__A,
            "incremental_append_new_columns_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,  # noqa: E501
            "incremental_append_new_columns.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
            "incremental_sync_all_columns_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,  # noqa: E501
            "incremental_append_new_columns_remove_one_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,  # noqa: E501
        }


@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestIncrementalPredicatesDeleteInsertFabric(BaseIncrementalPredicates):
    pass


@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestPredicatesDeleteInsertFabric(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+predicates": ["id != 2"], "+incremental_strategy": "delete+insert"}}
