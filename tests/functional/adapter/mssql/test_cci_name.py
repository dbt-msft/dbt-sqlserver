import pytest

from dbt.tests.util import run_dbt

cci_table_sql = """
{{ config(materialized="table") }}
select 1 as id
"""

cci_incremental_sql = """
{{ config(materialized="incremental", unique_key="id") }}
select 1 as id
"""

cci_snapshot_sql = """
{% snapshot cci_snapshot %}
    {{ config(target_schema=schema, unique_key="id", strategy="check", check_cols="all") }}
    select 1 as id
{% endsnapshot %}
"""


class TestCciNamedAfterFinalTable:
    """Tables built under a tmp name and swapped in keep an index name without __dbt_tmp."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"cci_table.sql": cci_table_sql, "cci_incremental.sql": cci_incremental_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"cci_snapshot.sql": cci_snapshot_sql}

    def _cci_names(self, project):
        rows = project.run_sql(
            f"""select t.name, i.name from sys.indexes i
                join sys.tables t on t.object_id = i.object_id
                where schema_name(t.schema_id) = '{project.test_schema}'
                and i.type_desc = 'CLUSTERED COLUMNSTORE'""",
            fetch="all",
        )
        return dict(rows)

    def test_cci_name(self, project):
        expected = {
            name: f"{project.test_schema}_{name}_cci"
            for name in ("cci_table", "cci_incremental", "cci_snapshot")
        }
        for _ in range(2):
            run_dbt(["run"])
            run_dbt(["snapshot"])
            assert self._cci_names(project) == expected
