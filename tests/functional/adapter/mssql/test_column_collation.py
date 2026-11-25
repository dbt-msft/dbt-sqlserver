import pytest

from dbt.adapters.sqlserver.sqlserver_relation import (
    SQLServerRelation,
    SQLServerRelationType,
)
from dbt.tests.util import run_dbt

EXPLICIT_COLLATION = "Latin1_General_100_CI_AS_SC"

MODEL = f"""
{{{{ config(materialized='table') }}}}
select
    cast('a' as varchar(10)) collate {EXPLICIT_COLLATION} as explicit_col,
    cast(N'a' as nvarchar(10)) as default_col,
    cast(1 as int) as int_col
"""


class TestGetColumnsInRelationCollation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"collated.sql": MODEL}

    def test_columns_carry_collation(self, project):
        run_dbt(["run"])

        adapter = project.adapter
        credentials = adapter.config.credentials
        relation = SQLServerRelation.create(
            database=credentials.database,
            schema=credentials.schema,
            identifier="collated",
            type=SQLServerRelationType.Table,
        )

        with adapter.connection_named("__test"):
            columns = {c.name: c for c in adapter.get_columns_in_relation(relation)}
            _, table = adapter.execute(
                "select convert(sysname, databasepropertyex(db_name(), 'Collation')) as collation",
                fetch=True,
            )
        database_collation = table[0][0]

        assert database_collation != EXPLICIT_COLLATION
        assert columns["explicit_col"].collation == EXPLICIT_COLLATION
        assert columns["default_col"].collation == database_collation
        assert columns["int_col"].collation is None
