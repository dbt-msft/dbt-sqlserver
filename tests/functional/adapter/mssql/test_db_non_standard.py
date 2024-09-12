import pytest
from dbt.tests.util import get_connection, run_dbt

database_name = "my-data-base"
schema_name = "mysource"
source_table_name = "my_table"

sources_yml = f"""
version: 2

sources:
  - name: mysource
    database: {database_name}
    tables:
    - name: my_table

"""

model_sql = """
{{ config(database="my-data-base", schema="mysource", materialized="table") }}
SELECT
    *
FROM
    {{ source('mysource', 'my_table') }}
"""


class TestCrossDB:
    def create_db(self, project):
        create_sql = """
        DECLARE @col NVARCHAR(256)
        SET @col = (SELECT CONVERT (varchar(256), SERVERPROPERTY('collation')));

        IF NOT EXISTS (SELECT * FROM sys.databases WHERE name='{database}')
        BEGIN
            EXEC ('CREATE DATABASE [{database}] COLLATE ' + @col)
        END
        """

        with get_connection(project.adapter):
            project.adapter.execute(
                create_sql.format(database=database_name),
                fetch=True,
            )

    def create_source_schema(self, project):
        create_sql = """
        USE [{database}];

        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
        BEGIN
            EXEC('CREATE SCHEMA {schema}')
        END
        """
        with get_connection(project.adapter):
            project.adapter.execute(
                create_sql.format(database=database_name, schema=schema_name),
                fetch=True,
            )

    def create_primary_table(self, project):
        src_query = """
        SELECT *
        INTO
            [{database}].{schema}.{table}
        FROM
            (
                SELECT
                    1 as id,
                    CAST('2024-01-01' as DATETIME2(6)) updated_at

                UNION ALL

                SELECT
                    2 as id,
                    CAST('2024-01-01' as DATETIME2(6)) updated_at

                UNION ALL

                SELECT
                    3 as id,
                    CAST('2024-01-01' as DATETIME2(6)) updated_at
            ) as src_data
        """
        with get_connection(project.adapter):
            project.adapter.execute(
                src_query.format(
                    database=database_name, schema=schema_name, table=source_table_name
                ),
                fetch=True,
            )

    def cleanup_primary_table(self, project):
        drop_sql = "DROP TABLE IF EXISTS [{database}].{schema}.{table}"
        with get_connection(project.adapter):
            project.adapter.execute(
                drop_sql.format(
                    database=database_name, schema=schema_name, table=source_table_name
                ),
                fetch=True,
            )

    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql, "sources.yml": sources_yml}

    def test_cross_db_snapshot(self, project):
        self.create_db(project)

        self.cleanup_primary_table(project)
        self.create_source_schema(project)
        self.create_primary_table(project)

        run_dbt()

        self.cleanup_primary_table(project)
