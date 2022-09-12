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
    pass


class TestTypeTimestampSQLServer(BaseTypeTimestamp):
    pass
