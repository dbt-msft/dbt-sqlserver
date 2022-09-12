import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp


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
    pass
