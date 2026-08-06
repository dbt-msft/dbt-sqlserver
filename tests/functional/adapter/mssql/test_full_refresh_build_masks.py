"""Intersection of full_refresh_build=prebuilt and Dynamic Data Masking.

The prebuilt rebuild path drops and recreates the table, so — like every other
build path — it must re-apply configured masks or they are silently lost on
every --full-refresh. It also has an ordering constraint unique to prebuilt:

  * the clustered design (CCI or clustered rowstore index) is built *inside*
    sqlserver__create_table_as_prebuilt, before masks can be applied;
  * the nonclustered indexes are built *after*, by create_indexes.

apply_masks must therefore run after the load but before create_indexes, so a
mask on a nonclustered-index key column lands before that index exists
(mask-then-index; SQL Server rejects adding a mask to an already-indexed key
column). A CCI exposes no key columns, so masks apply freely after it. A mask
on the clustered *rowstore* key column cannot be honoured on this path (the
clustered index already exists by the time we can mask) and must fail clearly.

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
# CCI prebuilt + mask on a data column — maskable after the CCI exists.
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
# Rowstore prebuilt with a mask on the CLUSTERED rowstore key column. The
# clustered index is built inside create_table_as_prebuilt before we can mask,
# so this can't be honoured — apply_masks must fail with the index-key error
# rather than silently dropping the mask.
# ---------------------------------------------------------------------------

rowstore_prebuilt_masked_clustered_key_sql = """
{{ config(
    materialized="table",
    as_columnstore=False,
    full_refresh_build="prebuilt",
    indexes=[
      {'columns': ['column_a'], 'type': 'clustered'},
    ],
    masks={"column_a": "default()"}
) }}
select cast('secret' as varchar(50)) as column_a, 2 as column_b
"""


class TestPrebuiltRowstoreMaskOnClusteredKeyErrors:
    @pytest.fixture(scope="class")
    def models(self):
        return {"rowstore_prebuilt_clustered.sql": rowstore_prebuilt_masked_clustered_key_sql}

    def test_mask_on_clustered_key_raises(self, project):
        results = run_dbt(["run", "--full-refresh"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        assert "index" in str(results[0].message).lower()
