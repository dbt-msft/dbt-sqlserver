import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import (
    BaseTypeTimestamp,
    seeds__expected_csv,
)


@pytest.mark.skip(reason="SQL Server shows 'numeric' if you don't explicitly cast it to bigint")
class TestTypeBigIntSQLServer(BaseTypeBigInt):
    pass


class TestTypeFloatSQLServer(BaseTypeFloat):
    pass


class TestTypeIntSQLServer(BaseTypeInt):
    pass


class TestTypeNumericSQLServer(BaseTypeNumeric):
    pass


class TestTypeStringSQLServer(BaseTypeString):
    def assert_columns_equal(self, project, expected_cols, actual_cols):
        #  ignore the size of the varchar since we do
        #  an optimization to not use varchar(max) all the time
        assert (
            expected_cols[:-1] == actual_cols[:-1]
        ), f"Type difference detected: {expected_cols} vs. {actual_cols}"


class TestTypeTimestampSQLServer(BaseTypeTimestamp):
    @pytest.fixture(scope="class")
    def seeds(self):
        seeds__expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      column_types:
        timestamp_col: "datetimeoffset"
        """

        return {
            "expected.csv": seeds__expected_csv,
            "expected.yml": seeds__expected_yml,
        }


class TestTypeBooleanSQLServer(BaseTypeBoolean):
    pass
