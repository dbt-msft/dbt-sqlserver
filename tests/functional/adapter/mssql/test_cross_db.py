import pytest
from dbt.tests.util import get_connection, run_dbt

snapshot_sql = """
{% snapshot claims_snapshot %}

{{
   config(
       target_database='TestDB_Secondary',
       target_schema='dbo',
       unique_key='id',

       strategy='timestamp',
       updated_at='updated_at',
   )
}}

select * from {{source('mysource', 'claims')}}

{% endsnapshot %}
"""

source_csv = """id,updated_date
1,2024-01-01
2,2024-01-01
3,2024-01-01
"""

sources_yml = """
version: 2
sources:
  - name: mysource
    database: TestDB
    tables:
      - name: claims
"""


class TestCrossDB:
    def cleanup_primary_table(self, project):
        drop_sql = "DROP TABLE IF EXISTS {database}.mysource.claims"
        with get_connection(project.adapter):
            project.adapter.execute(
                drop_sql.format(database=project.database),
                fetch=True,
            )

    def cleanup_snapshot_table(self, project):
        drop_sql = "DROP TABLE IF EXISTS TestDB_Secondary.dbo.claims_snapshot"
        with get_connection(project.adapter):
            project.adapter.execute(
                drop_sql,
                fetch=True,
            )

    def create_source_schema(self, project):
        create_sql = """
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'mysource')
        BEGIN
            EXEC('CREATE SCHEMA mysource')
        END
        """
        with get_connection(project.adapter):
            project.adapter.execute(
                create_sql,
                fetch=True,
            )

    def create_primary_table(self, project):
        src_query = """
        SELECT *
        INTO
            {database}.mysource.claims
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
                src_query.format(database=project.database, schema=project.test_schema),
                fetch=True,
            )

    def create_secondary_schema(self, project):
        src_query = """
        USE [TestDB_Secondary]
        EXEC ('CREATE SCHEMA {schema}')
        """
        with get_connection(project.adapter):
            project.adapter.execute(
                src_query.format(database=project.database, schema=project.test_schema),
                fetch=True,
            )

    def update_primary_table(self, project):
        sql = """
        UPDATE [{database}].[mysource].[claims]
        SET
        updated_at = CAST('2024-02-01' as datetime2(6))
        WHERE
        id = 3
        """
        with get_connection(project.adapter):
            project.adapter.execute(
                sql.format(database=project.database),
                fetch=True,
            )

    @pytest.fixture(scope="class")
    def models(self):
        return {"sources.yml": sources_yml}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"claims_snapshot.sql": snapshot_sql}

    def test_cross_db_snapshot(self, project):
        self.cleanup_primary_table(project)
        self.cleanup_snapshot_table(project)

        self.create_source_schema(project)
        self.create_primary_table(project)
        # self.create_secondary_schema(project)
        run_dbt(["snapshot"])
        self.update_primary_table(project)
        run_dbt(["snapshot"])
