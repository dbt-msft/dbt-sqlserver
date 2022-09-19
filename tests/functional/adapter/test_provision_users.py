import pytest
from dbt.adapters.base import BaseAdapter
from dbt.tests.util import run_dbt

my_model_sql = """
  select 1 as fun
"""

cleanup_existing_sql = """
{%- call statement('drop_existing', fetch_result=False) -%}

    if exists(
        select *
        from sys.database_principals
        where name = '{{ env_var('DBT_TEST_AAD_PRINCIPAL_1') }}')
    drop user {{ env_var('DBT_TEST_AAD_PRINCIPAL_1') }}

    if exists(
        select *
        from sys.database_principals
        where name = '{{ env_var('DBT_TEST_AAD_PRINCIPAL_2') }}')
    drop user {{ env_var('DBT_TEST_AAD_PRINCIPAL_2') }}

{%- endcall -%}
"""

model_schema_single_user_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select: ["{{ env_var('DBT_TEST_AAD_PRINCIPAL_1') }}"]
"""

model_schema_multiple_users_yml = """
version: 2
models:
  - name: my_model
    config:
      grants:
        select:
          - "{{ env_var('DBT_TEST_AAD_PRINCIPAL_1') }}"
          - "{{ env_var('DBT_TEST_AAD_PRINCIPAL_2') }}"
"""


class BaseTestProvisionAzureSQL:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cleanup_existing.sql": cleanup_existing_sql,
        }

    def test_auto_provision(self, project, adapter: BaseAdapter):
        adapter.execute_macro("cleanup_existing")
        run_dbt(["run"])


@pytest.mark.only_with_profile("ci_azure_cli", "ci_azure_auto", "ci_azure_environment")
class TestProvisionSingleUserAzureSQL(BaseTestProvisionAzureSQL):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": model_schema_single_user_yml,
        }


@pytest.mark.only_with_profile("ci_azure_cli", "ci_azure_auto", "ci_azure_environment")
class TestProvisionMultipleUsersAzureSQL(BaseTestProvisionAzureSQL):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "schema.yml": model_schema_multiple_users_yml,
        }
