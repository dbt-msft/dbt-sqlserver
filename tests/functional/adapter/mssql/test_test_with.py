import pytest
from dbt.tests.util import run_dbt

sample_model = """
SELECT
    1 as ID,
    'a' as data

UNION ALL

SELECT
    2 as ID,
    'b' as data

UNION ALL

SELECT
    2 as ID,
    'c' as data
"""

pass_model_yml = """
version: 2
models:
- name: sample_model
  data_tests:
  - with_statement_pass:
      field: ID
"""

fail_model_yml = """
version: 2
models:
- name: sample_model
  data_tests:
  - with_statement_fail:
      field: ID
"""

with_test_fail_sql = """
{% test with_statement_fail(model, field) %}

with test_sample AS (
    SELECT {{ field }} FROM {{ model }}
    GROUP BY {{ field }}
    HAVING COUNT(*) > 1
)
SELECT * FROM test_sample

{% endtest %}
"""

with_test_pass_sql = """
{% test with_statement_pass(model, field) %}

with test_sample AS (
    SELECT {{ field }} FROM {{ model }}
    GROUP BY {{ field }}
    HAVING COUNT(*) > 2
)
SELECT * FROM test_sample

{% endtest %}
"""


class BaseSQLTestWith:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "with_statement_pass.sql": with_test_pass_sql,
            "with_statement_fail.sql": with_test_fail_sql,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": sample_model,
            "schema.yml": pass_model_yml,
        }


class TestSQLTestWithPass(BaseSQLTestWith):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": sample_model,
            "schema.yml": pass_model_yml,
        }

    def test_sql_test_contains_with(self, project):
        run_dbt(["run"])
        run_dbt(["test"])


class TestSQLTestWithFail(BaseSQLTestWith):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sample_model.sql": sample_model,
            "schema.yml": fail_model_yml,
        }

    def test_sql_test_contains_with(self, project):
        run_dbt(["run"])
        run_dbt(["test"], expect_pass=False)
