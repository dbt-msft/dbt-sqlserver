import pytest
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
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


# Need help, assertions are failing
class TestSimpleMaterializationsFabric(BaseSimpleMaterializations):
    pass


@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestSingularTestsFabric(BaseSingularTests):
    pass


@pytest.mark.skip(reason="ephemeral not supported")
class TestSingularTestsEphemeralFabric(BaseSingularTestsEphemeral):
    pass


class TestEmptyFabric(BaseEmpty):
    pass


class TestEphemeralFabric(BaseEphemeral):
    pass


class TestIncrementalFabric(BaseIncremental):
    pass


# Modified incremental_not_schema_change.sql file to handle DATETIME compatibility issues.
@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestIncrementalNotSchemaChangeFabric(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_not_schema_change.sql": """
{{ config(materialized="incremental",
unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
CAST(1 + '-' + current_timestamp AS DATETIME2(6)) as user_id_current_time,
{% if is_incremental() %}
'thisis18characters' as platform
{% else %}
'okthisis20characters' as platform
{% endif %}
"""
        }


class TestGenericTestsFabric(BaseGenericTests):
    pass


# Failing due to additional single quotes added
class TestSnapshotCheckColsFabric(BaseSnapshotCheckCols):
    pass


# Failing due to additional single quotes added
class TestSnapshotTimestampFabric(BaseSnapshotTimestamp):
    pass


# Assertion Failed.
class TestBaseCachingFabric(BaseAdapterMethod):
    pass


class TestValidateConnectionFabric(BaseValidateConnection):
    pass
