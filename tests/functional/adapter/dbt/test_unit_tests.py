import pytest

from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes

from dbt.tests.util import write_file, run_dbt

my_model_sql = """
select
    tested_column from {{ ref('my_upstream_model')}}
"""

my_upstream_model_sql = """
select
  {sql_value} as tested_column
"""

test_my_model_yml = """
unit_tests:
  - name: test_my_model
    model: my_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {{ tested_column: {yaml_value} }}
    expect:
      rows:
        - {{ tested_column: {yaml_value} }}
"""

class TestUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass

class TestUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass

class TestUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["1", "true"],
            ["CAST('2020-01-02' AS DATE)", "2020-01-02"],
            ["CAST('2013-11-03 00:00:00-0' AS DATETIME2(6))", "2013-11-03 00:00:00-0"],
            ["CAST('2013-11-03 00:00:00-0' AS DATETIME2(6))", "2013-11-03 00:00:00-0"],
            ["CAST('1' AS numeric)", "1"],
        ]

    def test_unit_test_data_type(self, project, data_types):
        for sql_value, yaml_value in data_types:
            # Write parametrized type value to sql files
            write_file(
                my_upstream_model_sql.format(sql_value=sql_value),
                "models",
                "my_upstream_model.sql",
            )

            # Write parametrized type value to unit test yaml definition
            write_file(
                test_my_model_yml.format(yaml_value=yaml_value),
                "models",
                "schema.yml",
            )

            results = run_dbt(["run", "--select", "my_upstream_model"])
            assert len(results) == 1

            try:
                run_dbt(["test", "--select", "my_model"])
            except Exception:
                raise AssertionError(f"unit test failed when testing model with {sql_value}")