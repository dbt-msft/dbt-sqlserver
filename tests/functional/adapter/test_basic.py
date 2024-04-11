import pytest
from dbt.tests.adapter.basic.files import incremental_not_schema_change_sql
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestSimpleMaterializationsSQLServer(BaseSimpleMaterializations):
    pass


class TestSingularTestsSQLServer(BaseSingularTests):
    pass


@pytest.mark.skip(reason="ephemeral not supported")
class TestSingularTestsEphemeralSQLServer(BaseSingularTestsEphemeral):
    pass


class TestEmptySQLServer(BaseEmpty):
    pass


class TestEphemeralSQLServer(BaseEphemeral):
    pass


class TestIncrementalSQLServer(BaseIncremental):
    pass


class TestIncrementalNotSchemaChangeSQLServer(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_not_schema_change.sql": incremental_not_schema_change_sql.replace(
                "||", "+"
            )
        }


class TestGenericTestsSQLServer(BaseGenericTests):
    pass


class TestSnapshotCheckColsSQLServer(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampSQLServer(BaseSnapshotTimestamp):
    pass


class TestBaseCachingSQLServer(BaseAdapterMethod):
    pass


class TestValidateConnectionSQLServer(BaseValidateConnection):
    pass


class TestTableMaterializationSQLServer(BaseTableMaterialization):
    ...
