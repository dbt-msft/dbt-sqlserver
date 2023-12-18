import os

import pytest
<<<<<<< HEAD
from dbt.tests.adapter.basic import (
    expected_catalog,
    files,
    test_base,
    test_adapter_methods,
    test_docs_generate,
    test_empty,
    test_ephemeral,
    test_generic_tests,
    test_incremental,
    test_singular_tests,
    test_singular_tests_ephemeral,
    test_snapshot_check_cols,
    test_snapshot_timestamp,
    test_table_materialization,
    test_validate_connection,
=======
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
>>>>>>> fabric-1.7
)


<<<<<<< HEAD
class TestSimpleMaterializationsSQLServer(test_base.BaseSimpleMaterializations):
    pass


class TestSingularTestsSQLServer(test_singular_tests.BaseSingularTests):
=======
# Need help, assertions are failing
class TestSimpleMaterializationsFabric(BaseSimpleMaterializations):
    pass


@pytest.mark.skip(reason="CTAS is not supported without a table.")
class TestSingularTestsFabric(BaseSingularTests):
>>>>>>> fabric-1.7
    pass


@pytest.mark.skip(reason="ephemeral not supported")
<<<<<<< HEAD
class TestSingularTestsEphemeralSQLServer(
    test_singular_tests_ephemeral.BaseSingularTestsEphemeral
):
    pass


class TestEmptySQLServer(test_empty.BaseEmpty):
    pass


class TestEphemeralSQLServer(test_ephemeral.BaseEphemeral):
    pass


class TestIncrementalSQLServer(test_incremental.BaseIncremental):
    pass


class TestIncrementalNotSchemaChangeSQLServer(
    test_incremental.BaseIncrementalNotSchemaChange
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_not_schema_change.sql": files.incremental_not_schema_change_sql.replace(
                "||", "+"
            )
        }


class TestGenericTestsSQLServer(test_generic_tests.BaseGenericTests):
    pass


class TestSnapshotCheckColsSQLServer(test_snapshot_check_cols.BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampSQLServer(test_snapshot_timestamp.BaseSnapshotTimestamp):
    pass


class TestBaseCachingSQLServer(test_adapter_methods.BaseAdapterMethod):
    pass


class TestValidateConnectionSQLServer(test_validate_connection.BaseValidateConnection):
=======
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


class TestSnapshotCheckColsFabric(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampFabric(BaseSnapshotTimestamp):
    pass


# Assertion Failed.
class TestBaseCachingFabric(BaseAdapterMethod):
    pass


class TestValidateConnectionFabric(BaseValidateConnection):
>>>>>>> fabric-1.7
    pass


class TestTableMatSQLServer(test_table_materialization.BaseTableMaterialization):
    pass


class TestDocsGenerateSQLServer(test_docs_generate.BaseDocsGenerate):
    @staticmethod
    @pytest.fixture(scope="class")
    def dbt_profile_target_update():
        return {"schema_authorization": "{{ env_var('DBT_TEST_USER_1') }}"}

    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_catalog.base_expected_catalog(
            project,
            role=os.getenv("DBT_TEST_USER_1"),
            id_type="int",
            text_type="varchar",
            time_type="datetime",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=expected_catalog.no_stats(),
        )


class TestDocsGenReferencesSQLServer(test_docs_generate.BaseDocsGenReferences):
    @staticmethod
    @pytest.fixture(scope="class")
    def dbt_profile_target_update():
        return {"schema_authorization": "{{ env_var('DBT_TEST_USER_1') }}"}

    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_catalog.expected_references_catalog(
            project,
            role=os.getenv("DBT_TEST_USER_1"),
            id_type="int",
            text_type="varchar",
            time_type="datetime",
            bigint_type="int",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=expected_catalog.no_stats(),
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
            "schema.yml": test_docs_generate.ref_models__schema_yml,
            "sources.yml": test_docs_generate.ref_sources__schema_yml,
            # order by not allowed in VIEWS
            "view_summary.sql": ref_models__view_summary_sql_no_order_by,
            # order by not allowed in CTEs
            "ephemeral_summary.sql": ref_models__ephemeral_summary_sql_no_order_by,
            "ephemeral_copy.sql": test_docs_generate.ref_models__ephemeral_copy_sql,
            "docs.md": test_docs_generate.ref_models__docs_md,
        }
