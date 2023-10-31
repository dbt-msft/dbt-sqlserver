import os

import pytest
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.files import incremental_not_schema_change_sql
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    ref_models__docs_md,
    ref_models__ephemeral_copy_sql,
    ref_models__schema_yml,
    ref_sources__schema_yml,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
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


class TestTableMatSQLServer(BaseTableMaterialization):
    pass


class TestDocsGenerateSQLServer(BaseDocsGenerate):
    @staticmethod
    @pytest.fixture(scope="class")
    def dbt_profile_target_update():
        return {"schema_authorization": "{{ env_var('DBT_TEST_USER_1') }}"}

    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role=os.getenv("DBT_TEST_USER_1"),
            id_type="int",
            text_type="varchar",
            time_type="datetime",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )


class TestDocsGenReferencesSQLServer(BaseDocsGenReferences):
    @staticmethod
    @pytest.fixture(scope="class")
    def dbt_profile_target_update():
        return {"schema_authorization": "{{ env_var('DBT_TEST_USER_1') }}"}

    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_references_catalog(
            project,
            role=os.getenv("DBT_TEST_USER_1"),
            id_type="int",
            text_type="varchar",
            time_type="datetime",
            bigint_type="int",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )

    @pytest.fixture(scope="class")
    def models(self):
        ref_models__ephemeral_summary_sql_no_order_by = """
            {{
              config(
                materialized = "table"
              )
            }}

            select first_name, count(*) as ct from {{ref('ephemeral_copy')}}
            group by first_name
            """

        ref_models__view_summary_sql_no_order_by = """
        {{
          config(
            materialized = "view"
          )
        }}

        select first_name, ct from {{ref('ephemeral_summary')}}
        """

        return {
            "schema.yml": ref_models__schema_yml,
            "sources.yml": ref_sources__schema_yml,
            # order by not allowed in VIEWS
            "view_summary.sql": ref_models__view_summary_sql_no_order_by,
            # order by not allowed in CTEs
            "ephemeral_summary.sql": ref_models__ephemeral_summary_sql_no_order_by,
            "ephemeral_copy.sql": ref_models__ephemeral_copy_sql,
            "docs.md": ref_models__docs_md,
        }
