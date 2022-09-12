from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class TestIncrementalGrantsSQLServer(BaseIncrementalGrants):
    pass


class TestInvalidGrantsSQLServer(BaseInvalidGrants):
    pass


class TestModelGrantsSQLServer(BaseModelGrants):
    pass


class TestSeedGrantsSQLServer(BaseSeedGrants):
    pass


class TestSnapshotGrantsSQLServer(BaseSnapshotGrants):
    pass
