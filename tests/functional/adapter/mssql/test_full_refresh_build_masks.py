"""Intersection of full_refresh_build=prebuilt and Dynamic Data Masking.

The prebuilt rebuild path drops and recreates the table, so — like every other
build path — it must re-apply configured masks or they are silently lost on
every --full-refresh.

A mask cannot be added to an index key column: SQL Server before 2022 rejects
it, and the adapter's check refuses it on every version. prebuilt therefore
masks the empty table before it builds the clustered design, and create_indexes
adds the nonclustered indexes afterwards.

Requires SQL Server 2016+ (DDM). The CI/test server is 2022.
"""

import pytest

from dbt.tests.util import get_connection, run_dbt, run_dbt_and_capture


def masked_columns(project, table_name):
    """Return {column_name: masking_function} from sys.masked_columns."""
    sql = f"""
        select c.name, c.masking_function
        from sys.masked_columns c
        where c.object_id = OBJECT_ID('{project.test_schema}.{table_name}')
    """
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return {row[0]: row[1] for row in table.rows}


def table_exists(project, table_name):
    sql = f"select OBJECT_ID('{project.test_schema}.{table_name}', 'U')"
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return table.rows[0][0] is not None


def index_types(project, table_name):
    """Return the set of index type_desc values on the table (index_id > 0)."""
    sql = f"""
        select i.type_desc
        from sys.indexes i
        where i.object_id = OBJECT_ID('{project.test_schema}.{table_name}')
          and i.index_id > 0
    """
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return {row[0] for row in table.rows}


# ---------------------------------------------------------------------------
# CCI prebuilt + mask on a data column.
# ---------------------------------------------------------------------------

cci_prebuilt_masked_sql = """
{{ config(
    materialized="table",
    full_refresh_build="prebuilt",
    masks={"surname": "default()"}
) }}
select 1 as id, cast('Smith' as varchar(50)) as surname
"""


class TestPrebuiltCCIMasks:
    @pytest.fixture(scope="class")
    def models(self):
        return {"cci_prebuilt_masked.sql": cci_prebuilt_masked_sql}

    def test_mask_applied_and_survives_prebuilt_full_refresh(self, project):
        # First build takes the prebuilt path (existing_relation is none).
        _, output = run_dbt_and_capture(["run", "--full-refresh"])
        assert "full_refresh_build=prebuilt" in output
        assert masked_columns(project, "cci_prebuilt_masked").get("surname") == "default()"

        # A prebuilt rebuild drops & recreates — the mask must be re-applied.
        run_dbt(["run", "--full-refresh"])
        assert masked_columns(project, "cci_prebuilt_masked").get("surname") == "default()"


# ---------------------------------------------------------------------------
# Rowstore prebuilt: clustered on column_b, nonclustered on column_a, and a
# mask on column_a (the nonclustered key column). This only succeeds if
# apply_masks runs BEFORE create_indexes — otherwise column_a is already an
# index key and the mask ADD is rejected.
# ---------------------------------------------------------------------------

rowstore_prebuilt_masked_nc_key_sql = """
{{ config(
    materialized="table",
    as_columnstore=False,
    full_refresh_build="prebuilt",
    indexes=[
      {'columns': ['column_b'], 'type': 'clustered'},
      {'columns': ['column_a'], 'type': 'nonclustered'},
    ],
    masks={"column_a": "default()"}
) }}
select cast('secret' as varchar(50)) as column_a, 2 as column_b
"""


class TestPrebuiltRowstoreMaskOnNonclusteredKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {"rowstore_prebuilt_masked.sql": rowstore_prebuilt_masked_nc_key_sql}

    def test_mask_on_nonclustered_key_lands_before_index(self, project):
        _, output = run_dbt_and_capture(["run", "--full-refresh"])
        assert "full_refresh_build=prebuilt" in output

        # Mask applied to the nonclustered key column...
        assert masked_columns(project, "rowstore_prebuilt_masked").get("column_a") == "default()"
        # ...and the nonclustered index was still built on top of it.
        assert "NONCLUSTERED" in index_types(project, "rowstore_prebuilt_masked")


# ---------------------------------------------------------------------------
# Rowstore prebuilt with masks on both the CLUSTERED key and a nonclustered
# key. prebuilt builds the clustered index itself, so it must mask the empty
# table before that index exists. Covers the table and incremental paths.
# The refuse_mask var swaps in a mask SQL Server rejects (email() on an int).
# ---------------------------------------------------------------------------

clustered_key_masked_config = """
    as_columnstore=False,
    full_refresh_build="prebuilt",
    indexes=[
      {'columns': ['column_a', 'column_b'], 'type': 'clustered'},
      {'columns': ['column_c'], 'type': 'nonclustered', 'included_columns': ['column_a']},
    ],
    masks={"column_a": "email()"} if var("refuse_mask", false)
          else {"column_b": "default()", "column_c": "default()"}
"""

clustered_key_select = """
select 1 as column_a, cast('secret' as varchar(50)) as column_b,
       cast('pseudo' as varchar(50)) as column_c
"""

table_clustered_key_masked_sql = (
    '{{ config(materialized="table",' + clustered_key_masked_config + ") }}" + clustered_key_select
)

incremental_clustered_key_masked_sql = (
    '{{ config(materialized="incremental", unique_key="column_a",'
    ' incremental_strategy="delete+insert",'
    + clustered_key_masked_config
    + ") }}"
    + clustered_key_select
)


class TestPrebuiltRowstoreMaskOnClusteredKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_clustered_key.sql": table_clustered_key_masked_sql,
            "incremental_clustered_key.sql": incremental_clustered_key_masked_sql,
        }

    def test_masks_on_index_keys_survive_rebuilds_and_a_refused_mask_leaves_no_table(
        self, project
    ):
        tables = ("table_clustered_key", "incremental_clustered_key")
        expected = {"column_b": "default()", "column_c": "default()"}
        # fresh build, then a prebuilt rebuild over the existing masked table
        for args in (["run"], ["run", "--full-refresh"]):
            run_dbt(args)
            for table in tables:
                assert masked_columns(project, table) == expected
                assert index_types(project, table) == {"CLUSTERED", "NONCLUSTERED"}

        # A refused mask rolls back the create: no target, rather than a loaded,
        # unmasked one. The old table was already dropped and is not restored.
        results = run_dbt(
            ["run", "--full-refresh", "--vars", "refuse_mask: true"], expect_pass=False
        )
        assert [r.status for r in results] == ["error", "error"]
        for table in tables:
            assert not table_exists(project, table)
