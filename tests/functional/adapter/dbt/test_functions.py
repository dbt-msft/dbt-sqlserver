import pytest
from dbt_common.events.base_types import EventMsg

from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.adapter.functions import files
from dbt.tests.adapter.functions.test_udafs import (
    BasicPythonUDAF,
    BasicSQLUDAF,
)
from dbt.tests.adapter.functions.test_udfs import (
    ErrorForUnsupportedType,
    PythonUDFNotSupported,
    SqlUDFDefaultArgSupport,
    UDFsBasic,
)
from dbt.tests.util import run_dbt

SQLSERVER_UDF_SQL = """
@price * 2
""".strip()

SQLSERVER_UDF_WITH_DEFAULT_SQL = """
@price * 2
""".strip()

SQLSERVER_UDF_YML = files.MY_UDF_YML

SQLSERVER_UDF_WITH_DEFAULT_ARG_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: float
        description: The price of the standard item
        default_value: 100
    returns:
      data_type: float
      description: The resulting xlarge price
"""

SQLSERVER_MODEL_USING_FUNCTION_SQL = """
SELECT {{ function('price_for_xlarge') }}(100) AS result
"""


class TestUDFsBasic(UDFsBasic):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "CREATE OR ALTER FUNCTION" in event.data.sql
        )

    def check_function_volatility(self, sql: str):
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql
        assert "IMMUTABLE" not in sql


class TestDeterministicUDF(TestUDFsBasic):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "deterministic"},
        }


class TestStableUDF(TestUDFsBasic):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "stable"},
        }


class TestNonDeterministicUDF(TestUDFsBasic):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "functions": {"+volatility": "non-deterministic"},
        }


class TestErrorForUnsupportedType(ErrorForUnsupportedType):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_YML,
        }


class TestPythonUDFNotSupported(PythonUDFNotSupported):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.py": files.MY_UDF_PYTHON,
            "price_for_xlarge.yml": files.MY_UDF_PYTHON_YML,
        }


class TestSqlUDFDefaultArgSupport(SqlUDFDefaultArgSupport):
    expect_default_arg_support = True

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_WITH_DEFAULT_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_WITH_DEFAULT_ARG_YML,
        }

    def is_function_create_event(self, event: EventMsg) -> bool:
        return (
            event.data.node_info.node_name == "price_for_xlarge"
            and "CREATE OR ALTER FUNCTION" in event.data.sql
        )

    def test_udfs(self, project, sql_event_catcher):
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])
        assert len(result.results) == 1

        assert "= 100" in sql_event_catcher.caught_events[0].data.sql

        result = run_dbt(
            ["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(DEFAULT)"]
        )
        assert len(result.results) == 1
        assert result.results[0].agate_table.rows[0].values()[0] == 200


class TestUDFParse:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_YML,
        }

    def test_parse(self, project):
        result = run_dbt(["parse"])
        assert result is not None


class TestUDFList:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_YML,
        }

    def test_list(self, project):
        result = run_dbt(["list", "--select", "resource_type:function"])
        assert len(result) == 1


class TestUDFBuild:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_YML,
        }

    def test_build(self, project):
        result = run_dbt(["build", "--select", "resource_type:function"])
        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success
        assert isinstance(result.results[0].node, FunctionNode)


class TestUDFModelRef:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_using_function.sql": SQLSERVER_MODEL_USING_FUNCTION_SQL,
        }

    def test_model_ref(self, project):
        result = run_dbt(["build"])
        assert len(result.results) == 2

        result = run_dbt(["show", "--inline", "SELECT * FROM {{ ref('model_using_function') }}"])
        assert len(result.results) == 1
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 200


class TestUDFSchemaCreation:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_YML,
        }

    def test_schema_created_before_function(self, project):
        result = run_dbt(["build"])
        assert len(result.results) == 1
        assert result.results[0].status == RunStatus.Success


class TestUDFRebuild:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": SQLSERVER_UDF_SQL,
            "price_for_xlarge.yml": SQLSERVER_UDF_YML,
        }

    def test_rebuild(self, project):
        first_result = run_dbt(["build"])
        assert len(first_result.results) == 1
        assert first_result.results[0].status == RunStatus.Success

        second_result = run_dbt(["build"])
        assert len(second_result.results) == 1
        assert second_result.results[0].status == RunStatus.Success


@pytest.mark.skip(reason="SQL Server does not support user-defined aggregate functions")
class TestBasicSQLUDAF(BasicSQLUDAF):
    pass


@pytest.mark.skip(reason="SQL Server does not support user-defined aggregate functions")
class TestBasicPythonUDAF(BasicPythonUDAF):
    pass
