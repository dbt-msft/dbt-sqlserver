import pytest
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp


class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass


class TestSingularTests(BaseSingularTests):
    pass


@pytest.mark.skip(reason="SQLServer doesn't support nested CTE")
class TestSingularTestsEphemeral(BaseSingularTestsEphemeral):
    pass


class TestEmpty(BaseEmpty):
    pass


@pytest.mark.skip(reason="SQLServer doesn't support nested CTE")
class TestEphemeral(BaseEphemeral):
    pass


class TestIncremental(BaseIncremental):
    pass


class TestGenericTests(BaseGenericTests):
    pass


class TestSnapshotCheckCols(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestamp(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass
