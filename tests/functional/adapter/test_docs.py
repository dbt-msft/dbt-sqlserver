import os

import pytest
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    expected_references_catalog,
    no_stats,
)
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    get_artifact,
    ref_models__docs_md,
    ref_models__ephemeral_copy_sql,
    ref_models__schema_yml,
    ref_sources__schema_yml,
    run_and_generate,
    verify_metadata,
)


def verify_catalog(project, expected_catalog, start_time, ignore_owner):
    # get the catalog.json
    catalog_path = os.path.join(project.project_root, "target", "catalog.json")
    assert os.path.exists(catalog_path)
    catalog = get_artifact(catalog_path)

    # verify the catalog
    assert set(catalog) == {"errors", "metadata", "nodes", "sources"}
    verify_metadata(
        catalog["metadata"],
        "https://schemas.getdbt.com/dbt/catalog/v1.json",
        start_time,
    )
    assert not catalog["errors"]
    for key in "nodes", "sources":
        for unique_id, expected_node in expected_catalog[key].items():
            found_node = catalog[key][unique_id]
            for node_key in expected_node:
                assert node_key in found_node

                if node_key == "metadata" and ignore_owner:
                    expected_node[node_key]["owner"] = found_node[node_key]["owner"]

                assert (
                    found_node[node_key] == expected_node[node_key]
                ), f"Key '{node_key}' in '{unique_id}' did not match"


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

    # Test "--no-compile" flag works and produces no manifest.json
    def test_run_and_generate_no_compile(self, project, expected_catalog, is_azure: bool):
        start_time = run_and_generate(project, ["--no-compile"])
        assert not os.path.exists(os.path.join(project.project_root, "target", "manifest.json"))
        verify_catalog(project, expected_catalog, start_time, is_azure)

    # Test generic "docs generate" command
    def test_run_and_generate(self, project, expected_catalog, is_azure: bool):
        start_time = run_and_generate(project)
        verify_catalog(project, expected_catalog, start_time, is_azure)

        # Check that assets have been copied to the target directory for use in the docs html page
        assert os.path.exists(os.path.join(".", "target", "assets"))
        assert os.path.exists(os.path.join(".", "target", "assets", "lorem-ipsum.txt"))
        assert not os.path.exists(os.path.join(".", "target", "non-existent-assets"))


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

    def test_references(self, project, expected_catalog, is_azure: bool):
        start_time = run_and_generate(project)
        verify_catalog(project, expected_catalog, start_time, is_azure)
