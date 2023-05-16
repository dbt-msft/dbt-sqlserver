import pytest
from dbt.tests.util import run_dbt

schema_yml = """

version: 2

models:
  - name: my_first_dbt_model
    description: "A starter dbt model"
    columns:
      - name: id
        description: "The primary key for this table"
        tests:
          - unique
          - not_null

  - name: my_second_dbt_model
    description: "A starter dbt model"
    columns:
      - name: id
        description: "The primary key for this table"
        tests:
          - unique
          - not_null
"""

my_first_dbt_model_sql = """
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
"""

my_second_dbt_model_sql = """
-- Use the `ref` function to select from other models

select *
from {{ ref('my_first_dbt_model') }}
where id = 1
"""


class TestNewProjectSQLServer:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "my_new_project"}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_first_dbt_model.sql": my_first_dbt_model_sql,
            "my_second_dbt_model.sql": my_second_dbt_model_sql,
            "schema.yml": schema_yml,
        }

    def test_new_project(self, project):
        results = run_dbt(["build"])
        assert len(results) > 0
