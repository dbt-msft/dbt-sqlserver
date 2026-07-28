"""Functional tests for dbt-sqlserver adapter-specific features under --use-v2-parser.

Exercises column types, index configs, and behaviour flags through the
Rust v2 parser path.

STATUS (2026-07-28): The v2 parser (dbt-core-experimental-parser / dbt Fusion
2.0.0-alpha.5) does NOT recognize the ``sqlserver`` adapter type in
profiles.yml. See test_v2_parser_basic.py for details. These tests are marked
xfail pending that support.
"""

import shutil

import pytest

from dbt.tests.util import run_dbt

_HAS_V2_PARSER = shutil.which("dbt-core-experimental-parser") is not None
skip_if_no_v2_parser = pytest.mark.skipif(
    not _HAS_V2_PARSER,
    reason="dbt-core-experimental-parser binary not on PATH",
)
xfail_no_sqlserver_support = pytest.mark.xfail(
    _HAS_V2_PARSER,
    reason="v2 parser does not yet support sqlserver adapter type",
    strict=True,
)

pytestmark = [pytest.mark.v2_parser, skip_if_no_v2_parser, xfail_no_sqlserver_support]

# ------- Helpers -------


def _get_column_types(project, schema, table):
    """Return {column_name: (data_type_name, max_length)} from sys.columns."""
    rows = project.run_sql(
        f"""
        select c.name, t.name, c.max_length
        from [{project.database}].sys.columns c
        inner join [{project.database}].sys.types t
            on c.user_type_id = t.user_type_id
        where c.object_id = object_id('[{project.database}].[{schema}].[{table}]')
        """,
        fetch="all",
    )
    result = {}
    for name, dtype, max_length in rows:
        if dtype in ("nchar", "nvarchar", "sysname") and max_length != -1:
            char_length = max_length // 2
        else:
            char_length = max_length
        result[name] = (dtype, char_length)
    return result


# ============================================================================
# Column types: varchar, nvarchar, datetime2
# ============================================================================

COLUMN_TYPES_SQL = """
{{ config(materialized='table') }}
select
    cast('hello' as varchar(50)) as varchar_col,
    cast(N'unicode' as nvarchar(100)) as nvarchar_col,
    cast('2025-01-15 13:30:00' as datetime2) as datetime2_col
"""


class TestV2ParserColumnTypes:
    @pytest.fixture(scope="class")
    def models(self):
        return {"column_types.sql": COLUMN_TYPES_SQL}

    def test_column_types_under_v2_parser(self, project):
        """Table with varchar, nvarchar, datetime2 columns builds correctly."""
        results = run_dbt(["--use-v2-parser", "run", "--select", "column_types"])
        assert len(results) == 1
        assert results[0].status == "success"

        types = _get_column_types(project, project.test_schema, "column_types")
        assert types["varchar_col"] == ("varchar", 50)
        assert types["nvarchar_col"] == ("nvarchar", 100)
        assert types["datetime2_col"][0] == "datetime2"


# ============================================================================
# Native string types behaviour flag
# ============================================================================

NATIVE_STRING_SQL = """
{{ config(materialized='table') }}
select
    cast('hello' as varchar(50)) as str_col
"""

NATIVE_STRING_YML = """
version: 2
models:
  - name: native_string
    config:
      contract:
        enforced: true
    columns:
      - name: str_col
        data_type: string
"""


class TestV2ParserNativeStringTypes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "native_string.sql": NATIVE_STRING_SQL,
            "schema.yml": NATIVE_STRING_YML,
        }

    def test_native_string_types_defaul(self, project):
        """Default: STRING maps to VARCHAR(MAX) under --use-v2-parser."""
        results = run_dbt(["--use-v2-parser", "run", "--select", "native_string"])
        assert len(results) == 1
        assert results[0].status == "success"

        types = _get_column_types(project, project.test_schema, "native_string")
        # STRING -> VARCHAR(MAX) -> max_length = -1
        assert types["str_col"] == ("varchar", -1)

    def test_adapter_type_labels_under_v2_parser(self, project):
        """Column.TYPE_LABELS are correct when loaded via --use-v2-parser."""
        labels = project.adapter.Column.TYPE_LABELS
        assert labels["STRING"] == "VARCHAR(MAX)"
        assert labels["NCHAR"] == "NCHAR(1)"
        assert labels["NVARCHAR"] == "NVARCHAR(4000)"


# ============================================================================
# Legacy string types (flag off) under v2 parser
# ============================================================================


class TestV2ParserLegacyStringTypes:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "dbt_sqlserver_use_native_string_types": False,
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "legacy_string.sql": NATIVE_STRING_SQL,
            "schema.yml": NATIVE_STRING_YML,
        }

    def test_legacy_string_types_under_v2_parser(self, project):
        """Flag off: STRING maps to VARCHAR(8000) under --use-v2-parser."""
        results = run_dbt(["--use-v2-parser", "run", "--select", "legacy_string"])
        assert len(results) == 1
        assert results[0].status == "success"

        types = _get_column_types(project, project.test_schema, "legacy_string")
        assert types["str_col"] == ("varchar", 8000)


# ============================================================================
# Index configs: clustered + columnstore
# ============================================================================

INDEX_CLUSTERED_SQL = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {{'columns': ['id'], 'type': 'clustered'}},
      {{'columns': ['name'], 'type': 'nonclustered', 'unique': True}},
    ]
  )
}}

select 1 as id, 'alpha' as name
"""

INDEX_COLUMNSTORE_SQL = """
{{
  config(
    materialized = "table",
    indexes=[
      {{'columns': ['id'], 'type': 'columnstore'}},
    ]
  )
}}

select 1 as id, 'alpha' as name, dateadd(day, 1, getdate()) as ts
"""

INDEX_QUERY = """
select i.type_desc
from sys.indexes i
join sys.objects o on o.object_id = i.object_id
join sys.schemas s on s.schema_id = o.schema_id
where s.name = '{schema_name}'
  and o.name = '{table_name}'
  and i.index_id > 0
"""


class TestV2ParserIndexConfigs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "idx_clustered.sql": INDEX_CLUSTERED_SQL,
            "idx_columnstore.sql": INDEX_COLUMNSTORE_SQL,
        }

    def test_clustered_index_under_v2_parser(self, project):
        """Clustered index is created correctly under --use-v2-parser."""
        results = run_dbt(["--use-v2-parser", "run", "--select", "idx_clustered"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql(
            INDEX_QUERY.format(
                schema_name=project.test_schema,
                table_name="idx_clustered",
            ),
            fetch="all",
        )
        type_descs = {row[0] for row in rows}
        assert "CLUSTERED" in type_descs
        assert "NONCLUSTERED" in type_descs

    def test_columnstore_index_under_v2_parser(self, project):
        """Columnstore index is created correctly under --use-v2-parser."""
        results = run_dbt(["--use-v2-parser", "run", "--select", "idx_columnstore"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = project.run_sql(
            INDEX_QUERY.format(
                schema_name=project.test_schema,
                table_name="idx_columnstore",
            ),
            fetch="all",
        )
        type_descs = {row[0] for row in rows}
        # Columnstore + clustered columnstore (default)
        assert len(type_descs) >= 1
        assert any("COLUMNSTORE" in td for td in type_descs)


# ============================================================================
# Build with multiple materializations + index configs
# ============================================================================

MULTI_MATERIAL_SQL = """
{{ config(materialized='table') }}
select 1 as id, 'hello' as value
"""

MULTI_VIEW_SQL = """
{{ config(materialized='view') }}
select id, upper(value) as value_up from {{ ref('multi_base') }}
"""

MULTI_INCREMENTAL_SQL = """
{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    as_columnstore = False,
    indexes=[
      {{'columns': ['id'], 'type': 'clustered'}},
      {{'columns': ['value_up'], 'type': 'nonclustered'}},
    ]
  )
}}

select * from {{ ref('multi_view') }}

{% if is_incremental() %}
  where id > (select max(id) from {{ this }})
{% endif %}
"""


class TestV2ParserBuildWithAdapterFeatures:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "multi_base.sql": MULTI_MATERIAL_SQL,
            "multi_view.sql": MULTI_VIEW_SQL,
            "multi_incr.sql": MULTI_INCREMENTAL_SQL,
        }

    def test_build_multi_materialization_under_v2_parser(self, project):
        """Full build with table, view, and incremental+indexes under --use-v2-parser."""
        results = run_dbt(["--use-v2-parser", "build"])
        assert len(results) == 3
        assert all(r.status == "success" for r in results)

        # Verify indexes on the incremental model
        rows = project.run_sql(
            INDEX_QUERY.format(
                schema_name=project.test_schema,
                table_name="multi_incr",
            ),
            fetch="all",
        )
        type_descs = {row[0] for row in rows}
        assert "CLUSTERED" in type_descs
        assert "NONCLUSTERED" in type_descs
