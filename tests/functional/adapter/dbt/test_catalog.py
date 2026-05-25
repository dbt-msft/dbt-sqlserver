import json
import os

import pytest

from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.tests.adapter.catalog import files
from dbt.tests.adapter.catalog.relation_types import CatalogRelationTypes
from dbt.tests.util import get_connection, run_dbt


class TestRelationTypes(CatalogRelationTypes):
    """
    This is subclassed to remove the references to the materialized views,
    as SQLServer does not support them.

    Likely does not need to be subclassed since we implement everything,
    but prefer keeping it here for clarity.
    """

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": files.MY_TABLE,
            "my_view.sql": files.MY_VIEW,
            # "my_materialized_view.sql": files.MY_MATERIALIZED_VIEW,
        }

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    @pytest.mark.parametrize(
        "node_name,relation_type",
        [
            ("seed.test.my_seed", "BASE TABLE"),
            ("model.test.my_table", "BASE TABLE"),
            ("model.test.my_view", "VIEW"),
            # ("model.test.my_materialized_view", "MATERIALIZED VIEW"),
        ],
    )
    def test_relation_types_populate_correctly(
        self, docs: CatalogArtifact, node_name: str, relation_type: str
    ):
        """
        This test addresses: https://github.com/dbt-labs/dbt-core/issues/8864
        """
        assert node_name in docs.nodes
        node = docs.nodes[node_name]
        assert node.metadata.type == relation_type


class TestCatalogAcrossDatabases:
    SECONDARY_DATABASE = "secondary_db"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "vars": {
                "test_second_database": self.SECONDARY_DATABASE,
            },
        }

    @pytest.fixture(scope="class")
    def schema_yml(self):
        return """
version: 2

models:
  - name: default_db_model
    description: model in the profile/default database
    columns:
      - name: id
        description: id column

  - name: other_db_model
    description: model in a non-default database
    columns:
      - name: id
        description: id column
"""

    @pytest.fixture(scope="class")
    def models(self, schema_yml):
        return {
            "default_db_model.sql": "select 1 as id",
            "other_db_model.sql": """
                {{ config(database=var('test_second_database')) }}
                select 2 as id
            """,
            "schema.yml": schema_yml,
        }

    def create_secondary_db(self, project):
        create_sql = """
        DECLARE @col NVARCHAR(256)
        SET @col = (SELECT CONVERT(varchar(256), SERVERPROPERTY('collation')));

        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{database}')
        BEGIN
            EXEC('CREATE DATABASE [{database}] COLLATE ' + @col)
        END
        """
        with get_connection(project.adapter):
            project.adapter.execute(
                create_sql.format(database=self.SECONDARY_DATABASE),
                fetch=True,
            )

    def cleanup_secondary_database(self, project):
        drop_sql = """
        USE [master]

        IF EXISTS (SELECT * FROM sys.databases WHERE name = '{database}')
        BEGIN
            ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
            DROP DATABASE [{database}]
        END
        """
        with get_connection(project.adapter):
            project.adapter.execute(
                drop_sql.format(database=self.SECONDARY_DATABASE),
                fetch=True,
            )

    def test_docs_generate_includes_non_default_database(self, project):
        self.create_secondary_db(project)
        try:
            run_dbt(["run"])
            run_dbt(["docs", "generate"])

            catalog_path = os.path.join(project.project_root, "target", "catalog.json")
            with open(catalog_path) as fp:
                catalog = json.load(fp)

            nodes = catalog["nodes"]

            default_node = nodes["model.test.default_db_model"]
            other_node = nodes["model.test.other_db_model"]

            assert default_node["metadata"]["name"] == "default_db_model"
            assert other_node["metadata"]["name"] == "other_db_model"

            assert default_node["metadata"]["database"] != other_node["metadata"]["database"]
            assert other_node["metadata"]["database"] == self.SECONDARY_DATABASE

            assert "id" in default_node["columns"]
            assert "id" in other_node["columns"]
        finally:
            self.cleanup_secondary_database(project)
