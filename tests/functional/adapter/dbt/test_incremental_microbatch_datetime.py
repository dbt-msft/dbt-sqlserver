import pytest
from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch

_microbatch_model_no_unique_id_sql_datetime = """
{{ config(materialized='incremental', incremental_strategy='microbatch',
event_time='event_time', batch_size='day', begin='2020-01-01 00:00:00') }}
select * from {{ ref('input_model') }}
"""

_input_model_sql_datetime = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, '2020-01-01 00:00:00' as event_time
union all
select 2 as id, '2020-01-02 00:00:00' as event_time
union all
select 3 as id, '2020-01-03 00:00:00' as event_time
"""


class TestSQLServerMicrobatchDateTime(BaseMicrobatch):
    """
    Setup a version of the microbatch testing that uses a datetime column as the event_time
    This is to test that the microbatch strategy can handle datetime columns when passing in
    event times as UTC strings
    """

    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_model_no_unique_id_sql_datetime

    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        """
        This is the SQL that defines the input model to the microbatch model,
        including any {{ config(..) }}. event_time is a required configuration of this input
        """
        return _input_model_sql_datetime

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        test_schema_relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        return (
            f"insert into {test_schema_relation}.input_model (id, event_time) "
            f"values (4, '2020-01-04 00:00:00'), (5, '2020-01-05 00:00:00')"
        )
