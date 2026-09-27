import pytest

from dbt.tests.util import run_dbt

src_sql = """
{{ config(materialized='table', as_columnstore=False) }}
select 1 as id, cast('a' as varchar(10)) as name
"""

snap_cte_sql = """
{% snapshot snap_cte %}
{{ config(unique_key='id', strategy='check', check_cols=['NAME'],
          target_schema=target.schema, as_columnstore=False) }}
with s as (select * from {{ ref('src') }}) select * from s
{% endsnapshot %}
"""

eph_sql = """
{{ config(materialized='ephemeral') }}
select * from {{ ref('src') }}
"""

snap_eph_sql = """
{% snapshot snap_eph %}
{{ config(unique_key='id', strategy='check', check_cols=['name'],
          target_schema=target.schema, as_columnstore=False) }}
select * from {{ ref('eph') }}
{% endsnapshot %}
"""

snap_missing_col_sql = """
{% snapshot snap_missing_col %}
{{ config(unique_key='id', strategy='check', check_cols=['nope'],
          target_schema=target.schema, as_columnstore=False) }}
select * from {{ ref('src') }}
{% endsnapshot %}
"""


class TestSnapshotCheckColsWithQuery:
    @pytest.fixture(scope="class")
    def models(self):
        return {"src.sql": src_sql, "eph.sql": eph_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snap_cte.sql": snap_cte_sql,
            "snap_eph.sql": snap_eph_sql,
            "snap_missing_col.sql": snap_missing_col_sql,
        }

    def test_check_cols_over_with_query(self, project):
        run_dbt(["run"])
        run_dbt(["snapshot"])

        project.run_sql(f"update {project.test_schema}.src set name = 'b'")
        run_dbt(["snapshot", "--select", "snap_cte", "snap_eph"])
        for snapshot in ("snap_cte", "snap_eph"):
            rows = project.run_sql(
                f"select name, case when dbt_valid_to is null then 1 else 0 end as is_current "
                f"from {project.test_schema}.{snapshot} order by name",
                fetch="all",
            )
            assert [tuple(r) for r in rows] == [("a", 0), ("b", 1)], snapshot

        results = run_dbt(["snapshot", "--select", "snap_missing_col"], expect_pass=False)
        assert "check_cols column 'nope' is not in the snapshot query" in results[0].message
