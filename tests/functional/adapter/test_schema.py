import pytest
from dbt.tests.util import run_dbt


class TestSchemaCreation:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "schema_tests"}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dummy.sql": "select 1 as id",
            "dummy_with_auth.sql": """
{{ config(schema_authorization=env_var('DBT_TEST_USER_1')) }}
select 1 as id
""",
        }

    def test_schema_creation(self, project):
        res = run_dbt(["run", "-s", "dummy"])
        assert len(res) == 1

    def test_schema_creation_with_auth(self, project):
        res = run_dbt(["run", "-s", "dummy_with_auth"])
        assert len(res) == 1
