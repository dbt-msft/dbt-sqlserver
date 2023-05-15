import pytest
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange,
)
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey


class TestBaseIncrementalUniqueKeySQLServer(BaseIncrementalUniqueKey):
    pass


class TestIncrementalOnSchemaChangeSQLServer(BaseIncrementalOnSchemaChange):
    pass


class TestIncrementalPredicatesDeleteInsertSQLServer(BaseIncrementalPredicates):
    pass


class TestPredicatesDeleteInsertSQLServer(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+predicates": ["id != 2"], "+incremental_strategy": "delete+insert"}}
