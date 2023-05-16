import pytest
from dbt.tests.util import run_dbt


class TestSchemaCreation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dummy.sql": """
{{ config(schema='custom_test_schema') }}
select 1 as id
""",
            "dummy_with_auth.sql": """
{{ config(schema='custom_test_schema', schema_authorization=env_var('DBT_TEST_USER_1')) }}
select 1 as id
""",
        }

    def test_schema_creation(self, project):
        res = run_dbt(["run", "-s", "dummy"])
        assert len(res) == 1

    def test_schema_creation_with_auth(self, project):
        res = run_dbt(["run", "-s", "dummy_with_auth"])
        assert len(res) == 1
