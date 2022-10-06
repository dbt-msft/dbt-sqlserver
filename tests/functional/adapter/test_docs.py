import pytest
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    ref_models__docs_md,
    ref_models__ephemeral_copy_sql,
    ref_models__schema_yml,
    ref_sources__schema_yml,
)


class TestDocsGenerateSQLServer(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role="dbo",
            id_type="int",
            text_type="varchar",
            time_type="datetime",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )


class TestDocsGenReferencesSQLServer(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return expected_references_catalog(
            project,
            role="dbo",
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
