import pytest

from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeral,
    ephemeral_errors__base__base_copy_sql,
    ephemeral_errors__base__base_sql,
    ephemeral_errors__dependent_sql,
)
from dbt.tests.adapter.ephemeral.test_ephemeral import (
    TestEphemeralMulti as BaseTestEphemeralMulti,
)
from dbt.tests.adapter.ephemeral.test_ephemeral import (
    TestEphemeralNested as BaseTestEphemeralNested,
)
from dbt.tests.util import run_dbt


class TestEphemeralErrorHandling(BaseEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dependent.sql": ephemeral_errors__dependent_sql,
            "base": {
                "base.sql": ephemeral_errors__base__base_sql,
                "base_copy.sql": ephemeral_errors__base__base_copy_sql,
            },
        }

    def test_ephemeral_error_handling(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "skipped"
        assert "Compilation Error" in results[0].message


@pytest.mark.skip(reason="Ephemeral not supported")
class TestEphemeralNestedSQLServer(BaseTestEphemeralNested):
    pass


@pytest.mark.skip(reason="Ephemeral not supported")
class TestEphemeralMultiSQLServer(BaseTestEphemeralMulti):
    pass
