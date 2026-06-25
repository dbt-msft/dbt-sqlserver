import pytest

from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.util import run_dbt, write_file

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

my_union_model_sql = """
select tested_column from {{ ref('my_upstream_model') }}
union all
select tested_column from {{ ref('my_other_upstream_model') }}
"""

upstream_model_sql = """
select 1 as tested_column
"""

# `rows: []` must not generate `limit 0`, which is invalid T-SQL (issue #698)
test_empty_fixture_yml = """
unit_tests:
  - name: test_empty_given
    model: my_union_model
    given:
      - input: ref('my_upstream_model')
        rows:
          - {tested_column: 1}
      - input: ref('my_other_upstream_model')
        rows: []
    expect:
      rows:
        - {tested_column: 1}
  - name: test_empty_expect
    model: my_union_model
    given:
      - input: ref('my_upstream_model')
        rows: []
      - input: ref('my_other_upstream_model')
        rows: []
    expect:
      rows: []
"""


class TestUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass


class TestUnitTestEmptyFixture:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_upstream_model.sql": upstream_model_sql,
            "my_other_upstream_model.sql": upstream_model_sql,
            "my_union_model.sql": my_union_model_sql,
            "schema.yml": test_empty_fixture_yml,
        }

    def test_empty_fixture_rows(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        results = run_dbt(["test", "--select", "my_union_model"])
        assert len(results) == 2


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
