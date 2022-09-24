import pytest
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class TestIncrementalGrantsSQLServer(BaseIncrementalGrants):
    pass


@pytest.mark.only_with_profile("user", "ci_sql_server")
class TestInvalidGrantsSQLServer(BaseInvalidGrants):
    # This test does not run on Azure since the error messages are inconsistent there.
    # Tests have shown errors like "principal x could not be found"
    # as well as "principal x could not be resolved"

    def grantee_does_not_exist_error(self):
        return "Cannot find the user"

    def privilege_does_not_exist_error(self):
        return "Incorrect syntax near"


class TestModelGrantsSQLServer(BaseModelGrants):
    pass


class TestSeedGrantsSQLServer(BaseSeedGrants):
    pass


class TestSnapshotGrantsSQLServer(BaseSnapshotGrants):
    pass
