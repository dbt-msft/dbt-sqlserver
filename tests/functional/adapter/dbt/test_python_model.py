import pytest
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonIncrementalTests,
    BasePythonModelTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


@pytest.mark.skip(reason="Python models are not supported in SQLServer")
class TestPythonModel(BasePythonModelTests):
    pass


@pytest.mark.skip(reason="Python models are not supported in SQLServer")
class TestPythonIncremental(BasePythonIncrementalTests):
    pass


@pytest.mark.skip(reason="Python models are not supported in SQLServer")
class TestPySpark(BasePySparkTests):
    pass
