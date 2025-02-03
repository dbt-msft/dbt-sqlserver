import pytest
from dbt.tests.adapter.simple_seed.seeds import seeds__expected_sql
from dbt.tests.adapter.simple_seed.test_seed import (
    BaseBasicSeedTests,
    BaseSeedConfigFullRefreshOff,
    BaseSeedConfigFullRefreshOn,
    BaseSeedCustomSchema,
    BaseSeedWithEmptyDelimiter,
    BaseSeedWithUniqueDelimiter,
    BaseSeedWithWrongDelimiter,
    BaseSimpleSeedEnabledViaConfig,
)
from dbt.tests.util import check_table_does_exist, check_table_does_not_exist, run_dbt

seeds__expected_sql = seeds__expected_sql.replace(
    "TIMESTAMP WITHOUT TIME ZONE", "DATETIME2(6)"
).replace("TEXT", "VARCHAR(8000)")

properties__schema_yml = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    data_tests:
    - column_type:
        type: date
  - name: seed_id
    data_tests:
    - column_type:
        type: varchar(8000)

- name: seed_tricky
  columns:
  - name: seed_id
    data_tests:
    - column_type:
        type: integer
  - name: seed_id_str
    data_tests:
    - column_type:
        type: varchar(8000)
  - name: a_bool
    data_tests:
    - column_type:
        type: boolean
  - name: looks_like_a_bool
    data_tests:
    - column_type:
        type: varchar(8000)
  - name: a_date
    data_tests:
    - column_type:
        type: datetime2(6)
  - name: looks_like_a_date
    data_tests:
    - column_type:
        type: varchar(8000)
  - name: relative
    data_tests:
    - column_type:
        type: varchar(8000)
  - name: weekday
    data_tests:
    - column_type:
        type: varchar(8000)
"""


class TestBasicSeedTests(BaseBasicSeedTests):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)

    def test_simple_seed_full_refresh_flag(self, project):
        """
        Drop the seed_actual table and re-create.
        Verifies correct behavior by the absence of the
        model which depends on seed_actual."""
        self._build_relations_for_test(project)
        self._check_relation_end_state(
            run_result=run_dbt(["seed", "--full-refresh"]), project=project, exists=True
        )


class TestSeedConfigFullRefreshOn(BaseSeedConfigFullRefreshOn):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)

    def test_simple_seed_full_refresh_config(self, project):
        """config option should drop current model and cascade drop to downstream models"""
        self._build_relations_for_test(project)
        self._check_relation_end_state(run_result=run_dbt(["seed"]), project=project, exists=True)


class TestSeedConfigFullRefreshOff(BaseSeedConfigFullRefreshOff):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)


@pytest.mark.skip("Unable to inject custom schema to project_config_update")
class TestSeedCustomSchema(BaseSeedCustomSchema):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)


class TestSeedWithUniqueDelimiter(BaseSeedWithUniqueDelimiter):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)


class TestSeedWithWrongDelimiter(BaseSeedWithWrongDelimiter):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)

    def test_seed_with_wrong_delimiter(self, project):
        """Testing failure of running dbt seed with a wrongly configured delimiter"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "incorrect syntax" in seed_result.results[0].message.lower()


class TestSeedWithEmptyDelimiter(BaseSeedWithEmptyDelimiter):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(seeds__expected_sql)


class TestSimpleSeedEnabledViaConfig__seed_with_disabled(BaseSimpleSeedEnabledViaConfig):
    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        yield
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_enabled"
        )
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_disabled"
        )
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_tricky"
        )
        project.run_sql(f"drop view if exists {project.test_schema}.seed_enabled")
        project.run_sql(f"drop view if exists {project.test_schema}.seed_disabled")
        project.run_sql(f"drop view if exists {project.test_schema}.seed_tricky")
        project.run_sql(f"drop schema if exists {project.test_schema}")

    def test_simple_seed_with_disabled(self, clear_test_schema, project):
        results = run_dbt(["seed"])
        assert len(results) == 2
        check_table_does_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_exist(project.adapter, "seed_tricky")

    @pytest.mark.skip(
        reason="""
        Running all the tests in the same schema causes the tests to fail
        as they all share the same schema across the tests
        """
    )
    def test_simple_seed_selection(self, clear_test_schema, project):
        results = run_dbt(["seed", "--select", "seed_enabled"])
        assert len(results) == 1
        check_table_does_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_not_exist(project.adapter, "seed_tricky")

    @pytest.mark.skip(
        reason="""
        Running all the tests in the same schema causes the tests to fail
        as they all share the same schema across the tests
        """
    )
    def test_simple_seed_exclude(self, clear_test_schema, project):
        results = run_dbt(["seed", "--exclude", "seed_enabled"])
        assert len(results) == 1
        check_table_does_not_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_exist(project.adapter, "seed_tricky")


class TestSimpleSeedEnabledViaConfig__seed_selection(BaseSimpleSeedEnabledViaConfig):
    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        yield
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_enabled"
        )
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_disabled"
        )
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_tricky"
        )
        project.run_sql(f"drop view if exists {project.test_schema}.seed_enabled")
        project.run_sql(f"drop view if exists {project.test_schema}.seed_disabled")
        project.run_sql(f"drop view if exists {project.test_schema}.seed_tricky")
        project.run_sql(f"drop schema if exists {project.test_schema}")

    @pytest.mark.skip(
        reason="""
        Running all the tests in the same schema causes the tests to fail
        as they all share the same schema across the tests
        """
    )
    def test_simple_seed_with_disabled(self, clear_test_schema, project):
        results = run_dbt(["seed"])
        assert len(results) == 2
        check_table_does_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_exist(project.adapter, "seed_tricky")

    def test_simple_seed_selection(self, clear_test_schema, project):
        results = run_dbt(["seed", "--select", "seed_enabled"])
        assert len(results) == 1
        check_table_does_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_not_exist(project.adapter, "seed_tricky")

    @pytest.mark.skip(
        reason="""
        Running all the tests in the same schema causes the tests to fail
        as they all share the same schema across the tests
        """
    )
    def test_simple_seed_exclude(self, clear_test_schema, project):
        results = run_dbt(["seed", "--exclude", "seed_enabled"])
        assert len(results) == 1
        check_table_does_not_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_exist(project.adapter, "seed_tricky")


class TestSimpleSeedEnabledViaConfig__seed_exclude(BaseSimpleSeedEnabledViaConfig):
    @pytest.fixture(scope="function")
    def clear_test_schema(self, project):
        yield
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_enabled"
        )
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_disabled"
        )
        project.run_sql(
            f"drop table if exists {project.database}.{project.test_schema}.seed_tricky"
        )
        project.run_sql(f"drop view if exists {project.test_schema}.seed_enabled")
        project.run_sql(f"drop view if exists {project.test_schema}.seed_disabled")
        project.run_sql(f"drop view if exists {project.test_schema}.seed_tricky")
        project.run_sql(f"drop schema if exists {project.test_schema}")

    @pytest.mark.skip(
        reason="""
        Running all the tests in the same schema causes the tests to fail
        as they all share the same schema across the tests
        """
    )
    def test_simple_seed_with_disabled(self, clear_test_schema, project):
        results = run_dbt(["seed"])
        assert len(results) == 2
        check_table_does_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_exist(project.adapter, "seed_tricky")

    @pytest.mark.skip(
        reason="""
        Running all the tests in the same schema causes the tests to fail
        as they all share the same schema across the tests
        """
    )
    def test_simple_seed_selection(self, clear_test_schema, project):
        results = run_dbt(["seed", "--select", "seed_enabled"])
        assert len(results) == 1
        check_table_does_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_not_exist(project.adapter, "seed_tricky")

    def test_simple_seed_exclude(self, clear_test_schema, project):
        results = run_dbt(["seed", "--exclude", "seed_enabled"])
        assert len(results) == 1
        check_table_does_not_exist(project.adapter, "seed_enabled")
        check_table_does_not_exist(project.adapter, "seed_disabled")
        check_table_does_exist(project.adapter, "seed_tricky")
