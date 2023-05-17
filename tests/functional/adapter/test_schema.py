import pytest
from dbt.tests.util import run_dbt


class TestSchemaCreation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dummy.sql": """
{{ config(schema='with_custom_auth') }}
select 1 as id
""",
        }

    @staticmethod
    @pytest.fixture(scope="class")
    def profile_extra_options():
        return {"schema_authorization": "{{ env_var('DBT_TEST_USER_1') }}"}

    def test_schema_creation(self, project):
        res = run_dbt(["run", "-s", "dummy"])
        assert len(res) == 1
