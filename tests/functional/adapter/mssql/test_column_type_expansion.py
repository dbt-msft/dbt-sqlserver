"""Functional tests for column type expansion and addition
via the incremental materialization.

Two scenarios tested with default and native string type flags:
  1. Column type expansion via expand_target_column_types
  2. Adding a new nvarchar column via on_schema_change (append / sync_all)
"""

import os

import pytest

from dbt.tests.util import run_dbt


def _column_type(project, schema, table, column):
    rows = project.run_sql(
        f"""
        select t.name, c.max_length
        from [{project.database}].sys.columns c
        inner join [{project.database}].sys.types t
            on c.user_type_id = t.user_type_id
        where c.object_id = object_id('[{project.database}].[{schema}].[{table}]')
          and c.name = '{column}'
        """,
        fetch="all",
    )
    if not rows:
        return None
    dtype, max_length = rows[0]
    if dtype in ("nchar", "nvarchar", "sysname") and max_length != -1:
        return (dtype, max_length // 2)
    return (dtype, max_length)


def write_model(project, filename, contents):
    path = os.path.join(project.project_root, "models", filename)
    with open(path, "w") as f:
        f.write(contents)


# --- Model SQL for expansion test ---

EXPAND_V1 = """
{{ config(materialized='incremental', unique_key='id') }}
select 1 as id, cast('hello' as varchar(10)) as str_col
"""

EXPAND_V2 = """
{{ config(materialized='incremental', unique_key='id') }}
select 1 as id, cast('hello world' as varchar(25)) as str_col
"""

# --- Model SQL for add-column test ---

ADD_COL_V1 = """
{{
    config(materialized='incremental', unique_key='id',
           on_schema_change='append_new_columns')
}}
select 1 as id, cast('hello' as varchar(10)) as str_col
"""

ADD_COL_V2 = """
{{
    config(materialized='incremental', unique_key='id',
           on_schema_change='append_new_columns')
}}
select 1 as id,
       cast('hello' as varchar(10)) as str_col,
       cast('hello' as nvarchar(20)) as new_col
"""

# --- Model SQL for sync-all-columns test ---

SYNC_V1 = """
{{
    config(materialized='incremental', unique_key='id',
           on_schema_change='sync_all_columns')
}}
select 1 as id, cast('hello' as varchar(10)) as str_col
"""

SYNC_V2 = """
{{
    config(materialized='incremental', unique_key='id',
           on_schema_change='sync_all_columns')
}}
select 1 as id,
       cast('hello world' as varchar(25)) as str_col,
       cast('hello' as nvarchar(20)) as new_col
"""


# ============================================================================
# Default string types (dbt_sqlserver_use_native_string_types = false)
# ============================================================================


class TestExpansionDefault:
    @pytest.fixture(scope="class")
    def models(self):
        return {"expand_test.sql": EXPAND_V1}

    def test_varchar_size_expansion(self, project):
        run_dbt(["run", "--full-refresh"])
        write_model(project, "expand_test.sql", EXPAND_V2)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        typ = _column_type(project, project.test_schema, "expand_test", "str_col")
        assert typ == ("varchar", 25), f"Expected varchar(25), got {typ}"


class TestAddColumnDefault:
    """
    This test addresses: https://github.com/dbt-msft/dbt-sqlserver/issues/446
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"add_col_test.sql": ADD_COL_V1}

    def test_add_nvarchar_column(self, project):
        run_dbt(["run", "--full-refresh"])
        write_model(project, "add_col_test.sql", ADD_COL_V2)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        typ = _column_type(project, project.test_schema, "add_col_test", "new_col")
        assert typ == ("nvarchar", 20), f"Expected nvarchar(20), got {typ}"


class TestSyncColumnsDefault:
    @pytest.fixture(scope="class")
    def models(self):
        return {"sync_test.sql": SYNC_V1}

    def test_sync_all_columns(self, project):
        run_dbt(["run", "--full-refresh"])
        write_model(project, "sync_test.sql", SYNC_V2)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        typ = _column_type(project, project.test_schema, "sync_test", "str_col")
        assert typ == ("varchar", 25), f"Expected varchar(25), got {typ}"

        typ = _column_type(project, project.test_schema, "sync_test", "new_col")
        assert typ == ("nvarchar", 20), f"Expected nvarchar(20), got {typ}"


# ============================================================================
# Safe type expansion: varchar -> nvarchar (requires enable_safe flag)
# ============================================================================


NVARCHAR_V1 = """
{{ config(materialized='incremental', unique_key='id',
           column_type_expansion_max_rows=10) }}
select 1 as id, cast('hello' as varchar(10)) as str_col
"""

NVARCHAR_V2 = """
{{ config(materialized='incremental', unique_key='id',
           column_type_expansion_max_rows=10) }}
select 1 as id, cast('hi' as nvarchar(25)) as str_col
"""


class TestVarcharToNvarcharWithoutFlag:
    @pytest.fixture(scope="class")
    def models(self):
        return {"nvarchar_test.sql": NVARCHAR_V1}

    def test_varchar_to_nvarchar_blocked_without_flag(self, project):
        run_dbt(["run", "--full-refresh"])
        write_model(project, "nvarchar_test.sql", NVARCHAR_V2)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        typ = _column_type(project, project.test_schema, "nvarchar_test", "str_col")
        assert typ == ("varchar", 10), f"Expected varchar(10), got {typ}"


class TestVarcharToNvarcharWithFlag:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_enable_safe_type_expansion": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"nvarchar_safe_test.sql": NVARCHAR_V1}

    def test_varchar_to_nvarchar_works_with_flag(self, project):
        run_dbt(["run", "--full-refresh"])
        write_model(project, "nvarchar_safe_test.sql", NVARCHAR_V2)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        typ = _column_type(project, project.test_schema, "nvarchar_safe_test", "str_col")
        assert typ == ("nvarchar", 25), f"Expected nvarchar(25), got {typ}"


# ============================================================================
# Native string types (dbt_sqlserver_use_native_string_types = true)
# ============================================================================


class TestExpansionNative:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_native_string_types": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"expand_test.sql": EXPAND_V1}

    def test_varchar_size_expansion_native(self, project):
        run_dbt(["run", "--full-refresh"])
        write_model(project, "expand_test.sql", EXPAND_V2)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        typ = _column_type(project, project.test_schema, "expand_test", "str_col")
        assert typ == ("varchar", 25), f"Expected varchar(25), got {typ}"


class TestAddColumnNative:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_native_string_types": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"add_col_test.sql": ADD_COL_V1}

    def test_add_nvarchar_column_native(self, project):
        run_dbt(["run", "--full-refresh"])
        write_model(project, "add_col_test.sql", ADD_COL_V2)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        typ = _column_type(project, project.test_schema, "add_col_test", "new_col")
        assert typ == ("nvarchar", 20), f"Expected nvarchar(20), got {typ}"


class TestSyncColumnsNative:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"dbt_sqlserver_use_native_string_types": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {"sync_test.sql": SYNC_V1}

    def test_sync_all_columns_native(self, project):
        run_dbt(["run", "--full-refresh"])
        write_model(project, "sync_test.sql", SYNC_V2)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        typ = _column_type(project, project.test_schema, "sync_test", "str_col")
        assert typ == ("varchar", 25), f"Expected varchar(25), got {typ}"

        typ = _column_type(project, project.test_schema, "sync_test", "new_col")
        assert typ == ("nvarchar", 20), f"Expected nvarchar(20), got {typ}"
