from dbt.tests.adapter.simple_copy.test_copy_uppercase import (
    TestSimpleCopyUppercase as BaseSimpleCopyUppercase,
)
from dbt.tests.adapter.simple_copy.test_simple_copy import (
    EmptyModelsArentRunBase,
    SimpleCopyBase,
)


class TestSimpleCopyBaseSQLServer(SimpleCopyBase):
    pass


class TestEmptyModelsArentRunSQLServer(EmptyModelsArentRunBase):
    pass


class TestSimpleCopyUppercaseSQLServer(BaseSimpleCopyUppercase):
    pass
