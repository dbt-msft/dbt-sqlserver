from dbt.tests.adapter.hooks.test_model_hooks import (
    TestDuplicateHooksInConfigs as BaseTestDuplicateHooksInConfigs,
)
from dbt.tests.adapter.hooks.test_model_hooks import TestHookRefs as BaseTestHookRefs
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestHooksRefsOnSeeds as BaseTestHooksRefsOnSeeds,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooks as BaseTestPrePostModelHooks,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksInConfig as BaseTestPrePostModelHooksInConfig,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksInConfigKwargs as BaseTestPrePostModelHooksInConfigKwargs,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksInConfigWithCount as BaseTestPrePostModelHooksInConfigWithCount,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksOnSeeds as BaseTestPrePostModelHooksOnSeeds,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksOnSeedsPlusPrefixed as BaseTestPrePostModelHooksOnSeedsPlusPrefixed,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace as BaseTPPMHOSPPrefixedWhitespace,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksOnSnapshots as BaseTestPrePostModelHooksOnSnapshots,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostModelHooksUnderscores as BaseTestPrePostModelHooksUnderscores,
)
from dbt.tests.adapter.hooks.test_model_hooks import (
    TestPrePostSnapshotHooksInConfigKwargs as BaseTestPrePostSnapshotHooksInConfigKwargs,
)
from dbt.tests.adapter.hooks.test_run_hooks import TestAfterRunHooks as BaseTestAfterRunHooks
from dbt.tests.adapter.hooks.test_run_hooks import TestPrePostRunHooks as BaseTestPrePostRunHooks


class TestPrePostRunHooksSQLServer(BaseTestPrePostRunHooks):
    pass


class TestAfterRunHooksSQLServer(BaseTestAfterRunHooks):
    pass


class TestDuplicateHooksInConfigsSQLServer(BaseTestDuplicateHooksInConfigs):
    pass


class TestHookRefsSQLServer(BaseTestHookRefs):
    pass


class TestHooksRefsOnSeedsSQLServer(BaseTestHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksSQLServer(BaseTestPrePostModelHooks):
    pass


class TestPrePostModelHooksInConfigSQLServer(BaseTestPrePostModelHooksInConfig):
    pass


class TestPrePostModelHooksInConfigKwargsSQLServer(BaseTestPrePostModelHooksInConfigKwargs):
    pass


class TestPrePostModelHooksInConfigWithCountSQLServer(BaseTestPrePostModelHooksInConfigWithCount):
    pass


class TestPrePostModelHooksOnSeedsSQLServer(BaseTestPrePostModelHooksOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedSQLServer(
    BaseTestPrePostModelHooksOnSeedsPlusPrefixed
):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespaceSQLServer(BaseTPPMHOSPPrefixedWhitespace):
    pass


class TestPrePostModelHooksOnSnapshotsSQLServer(BaseTestPrePostModelHooksOnSnapshots):
    pass


class TestPrePostModelHooksUnderscoresSQLServer(BaseTestPrePostModelHooksUnderscores):
    pass


class TestPrePostSnapshotHooksInConfigKwargsSQLServer(BaseTestPrePostSnapshotHooksInConfigKwargs):
    pass
