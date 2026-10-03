import pytest

from dbt.tests.util import run_dbt

snap_sql = """
{% snapshot snap %}
{{ config(unique_key='id', strategy='check', check_cols='all',
          target_schema=target.schema, as_columnstore=False) }}
select 1 as id{% if var('v2', false) %}, 'x' as [order], 'y' as [two words]{% endif %}
{% endsnapshot %}
"""


class TestSnapshotAddQuotedColumn:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snap.sql": snap_sql}

    def test_snapshot_adds_columns_that_need_quoting(self, project):
        run_dbt(["snapshot"])
        run_dbt(["snapshot", "--vars", "{v2: true}"])

        rows = project.run_sql(
            f"select [order], [two words] from {project.test_schema}.snap "
            "where [order] is not null",
            fetch="all",
        )
        assert [tuple(row) for row in rows] == [("x", "y")]
