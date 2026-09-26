from dbt.tests.adapter.ephemeral.test_ephemeral import (
    BaseEphemeralErrorHandling,
    BaseEphemeralMulti,
    BaseEphemeralNested,
)
from dbt.tests.util import check_relations_equal, run_dbt


# The upstream tests also compare the compiled SQL to Postgres DDL; check the data instead.
class TestEphemeral(BaseEphemeralMulti):
    def test_ephemeral_multi(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 3

        check_relations_equal(project.adapter, ["seed", "dependent"])
        check_relations_equal(project.adapter, ["seed", "double_dependent"])
        check_relations_equal(project.adapter, ["seed", "super_dependent"])


class TestEphemeralNested(BaseEphemeralNested):
    def test_ephemeral_nested(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        check_relations_equal(project.adapter, ["source_table", "root_view"])


class TestEphemeralErrorHandling(BaseEphemeralErrorHandling):
    pass
