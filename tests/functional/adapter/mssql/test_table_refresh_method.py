import os

import pytest

from dbt.tests.util import get_connection, run_dbt

# -- Model fixtures --

dml_model_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
select 1 as id, 'hello' as val
"""

dml_model_v2_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
select 2 as id, 'world' as val
"""

dml_model_schema_change_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
select 1 as id, 'hello' as val, 42 as new_col
"""

rename_model_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "rename",
    "as_columnstore": False
  })
}}
select 1 as id, 'hello' as val
"""

default_model_sql = """
{{
  config({
    "materialized": "table",
    "as_columnstore": False
  })
}}
select 1 as id, 'hello' as val
"""

invalid_method_model_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "invalid",
    "as_columnstore": False
  })
}}
select 1 as id
"""

dml_with_columnstore_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml"
  })
}}
select 1 as id, 'hello' as val
"""

dml_with_columnstore_v2_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml"
  })
}}
select 2 as id, 'world' as val
"""

view_then_dml_table_sql = """
{{
  config({
    "materialized": "view"
  })
}}
select 1 as id, 'hello' as val
"""

view_then_dml_table_v2_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
select 2 as id, 'world' as val
"""

dml_contract_model_sql = """
{{ config(materialized="table", table_refresh_method="dml", as_columnstore=False) }}
select 1 as id, 'hello' as val
"""

dml_contract_model_v2_sql = """
{{ config(materialized="table", table_refresh_method="dml", as_columnstore=False) }}
select 2 as id, 'world' as val
"""

dml_cte_model_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
with cte as (
  select 1 as id, 'hello' as val
)
select * from cte
"""

dml_cte_model_v2_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
with cte as (
  select 2 as id, 'world' as val
)
select * from cte
"""

dml_contract_schema_yml = """
version: 2
models:
  - name: dml_contract_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: int
      - name: val
        data_type: varchar(5)
"""


def write_model(project, filename, contents):
    """Write a model file into the project's models directory."""
    path = os.path.join(project.project_root, "models", filename)
    with open(path, "w") as f:
        f.write(contents)


def query_table(project, table_name):
    """Query all rows from a table, return as list of tuples."""
    sql = f"SELECT * FROM {project.test_schema}.{table_name} ORDER BY id"
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return table.rows


def table_exists(project, table_name):
    """Check if a table exists in the test schema."""
    sql = (
        f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{project.test_schema}' "
        f"AND TABLE_NAME = '{table_name}'"
    )
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return table.rows[0][0] == 1


def get_column_names(project, table_name):
    """Get column names for a table in order."""
    sql = (
        f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{project.test_schema}' "
        f"AND TABLE_NAME = '{table_name}' "
        f"ORDER BY ORDINAL_POSITION"
    )
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return [row[0] for row in table.rows]


def has_columnstore_index(project, table_name):
    """Check if a table has a clustered columnstore index."""
    sql = (
        f"SELECT COUNT(*) FROM sys.indexes i "
        f"JOIN sys.tables t ON i.object_id = t.object_id "
        f"JOIN sys.schemas s ON t.schema_id = s.schema_id "
        f"WHERE s.name = '{project.test_schema}' "
        f"AND t.name = '{table_name}' "
        f"AND i.type = 5"
    )
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return table.rows[0][0] > 0


# -- Test: First run uses standard CREATE path (table doesn't exist yet) --


class TestDmlRefreshFirstRun:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_model.sql": dml_model_sql}

    def test_first_run_creates_table(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "dml_model")
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "hello"


# -- Test: Second run with same schema uses DML refresh (DELETE + INSERT) --


class TestDmlRefreshSubsequentRun:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_model.sql": dml_model_sql}

    def test_dml_refresh_updates_data(self, project):
        # First run — creates the table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "dml_model")
        assert len(rows) == 1
        assert rows[0][0] == 1

        # Swap in the v2 model with different data but same schema
        write_model(project, "dml_model.sql", dml_model_v2_sql)

        # Second run — should use DML refresh
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "dml_model")
        assert len(rows) == 1
        assert rows[0][0] == 2
        assert rows[0][1] == "world"

        # Scratch table should be cleaned up
        assert not table_exists(project, "dml_model__dbt_refresh")


# -- Test: Schema change triggers rename-swap fallback --


class TestDmlRefreshSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_model.sql": dml_model_sql}

    def test_schema_change_falls_back_to_rename(self, project):
        # First run — creates the table
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        cols = get_column_names(project, "dml_model")
        assert cols == ["id", "val"]

        # Swap in model with an extra column
        write_model(project, "dml_model.sql", dml_model_schema_change_sql)

        # Second run — schema changed, should fall back to rename-swap
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        cols = get_column_names(project, "dml_model")
        assert "new_col" in cols

        rows = query_table(project, "dml_model")
        assert len(rows) == 1

        # Scratch table should be cleaned up
        assert not table_exists(project, "dml_model__dbt_refresh")


# -- Test: Default config uses rename-swap (backwards compatible) --


class TestDefaultMethodUsesRename:
    @pytest.fixture(scope="class")
    def models(self):
        return {"default_model.sql": default_model_sql}

    def test_default_uses_rename(self, project):
        # First run
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run — should use rename-swap (no scratch table created)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "default_model")
        assert len(rows) == 1


# -- Test: Explicit rename method works --


class TestExplicitRenameMethod:
    @pytest.fixture(scope="class")
    def models(self):
        return {"rename_model.sql": rename_model_sql}

    def test_explicit_rename(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "rename_model")
        assert len(rows) == 1


# -- Test: Invalid config value raises compiler error --


class TestInvalidRefreshMethod:
    @pytest.fixture(scope="class")
    def models(self):
        return {"invalid_model.sql": invalid_method_model_sql}

    def test_invalid_method_raises_error(self, project):
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"


# -- Test: DML refresh with as_columnstore (CCI survives DML) --


class TestDmlRefreshWithColumnstore:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_cci_model.sql": dml_with_columnstore_sql}

    def test_cci_survives_dml_refresh(self, project):
        # First run — creates table with CCI
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        assert has_columnstore_index(project, "dml_cci_model")

        # Swap in v2 data
        write_model(project, "dml_cci_model.sql", dml_with_columnstore_v2_sql)

        # Second run — DML refresh, CCI should survive
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        assert has_columnstore_index(project, "dml_cci_model")

        rows = query_table(project, "dml_cci_model")
        assert len(rows) == 1
        assert rows[0][0] == 2


# -- Test: DML refresh with contract enforced --


class TestDmlRefreshWithContract:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dml_contract_model.sql": dml_contract_model_sql,
            "schema.yml": dml_contract_schema_yml,
        }

    def test_contract_with_dml_refresh(self, project):
        # First run — contract creates table with explicit types
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "dml_contract_model")
        assert len(rows) == 1
        assert rows[0][0] == 1

        # Swap in v2 data (same schema)
        write_model(project, "dml_contract_model.sql", dml_contract_model_v2_sql)

        # Second run — should use DML refresh since schema matches
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "dml_contract_model")
        assert len(rows) == 1
        assert rows[0][0] == 2


# -- Test: DML refresh works with CTEs in model SQL --


class TestDmlRefreshWithCTE:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_cte_model.sql": dml_cte_model_sql}

    def test_cte_model_dml_refresh(self, project):
        # First run — creates the table (uses CREATE path, no DML refresh)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "dml_cte_model")
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "hello"

        # Swap in v2 model with CTE but different data
        write_model(project, "dml_cte_model.sql", dml_cte_model_v2_sql)

        # Second run — DML refresh with CTE-based SQL
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "dml_cte_model")
        assert len(rows) == 1
        assert rows[0][0] == 2
        assert rows[0][1] == "world"

        # Scratch table should be cleaned up
        assert not table_exists(project, "dml_cte_model__dbt_refresh")


# -- Test: Existing view with DML refresh falls back to rename-swap --


class TestDmlRefreshExistingViewFallback:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_view_model.sql": view_then_dml_table_sql}

    def test_view_to_table_with_dml_config(self, project):
        # First run — creates a view
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify it's a view
        sql = (
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS "
            f"WHERE TABLE_SCHEMA = '{project.test_schema}' "
            f"AND TABLE_NAME = 'dml_view_model'"
        )
        with get_connection(project.adapter):
            _, result = project.adapter.execute(sql, fetch=True)
        assert result.rows[0][0] == 1

        # Swap in v2 model that materializes as table with dml refresh
        write_model(project, "dml_view_model.sql", view_then_dml_table_v2_sql)

        # Second run — existing relation is a view, DML refresh should
        # skip the DELETE+INSERT path and fall back to rename-swap
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Verify it's now a table, not a view
        assert table_exists(project, "dml_view_model")

        rows = query_table(project, "dml_view_model")
        assert len(rows) == 1
        assert rows[0][0] == 2
        assert rows[0][1] == "world"


# -- Test: DML refresh preserves target column order when scratch SELECT reorders --

dml_model_reorder_v1_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
select 1 as id, 'hello' as val
"""

# Same columns, reversed projection order — SELECT INTO scratch will have
# (val, id) physical order while target keeps (id, val).
dml_model_reorder_v2_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
select 'world' as val, 2 as id
"""


class TestDmlRefreshColumnOrderMismatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_reorder_model.sql": dml_model_reorder_v1_sql}

    def test_column_order_mismatch_inserts_by_name(self, project):
        # First run — creates target with physical order (id, val)
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Swap in a model that selects the same columns in reversed order
        write_model(project, "dml_reorder_model.sql", dml_model_reorder_v2_sql)

        # Second run — DML refresh path. Without the explicit column list,
        # `INSERT INTO target SELECT * FROM scratch` would map scratch.val ->
        # target.id and scratch.id -> target.val, producing wrong values.
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        rows = query_table(project, "dml_reorder_model")
        assert len(rows) == 1
        # id column still holds an int, val column still holds the string —
        # the named INSERT is what makes this work.
        assert rows[0][0] == 2
        assert rows[0][1] == "world"

        # Scratch cleaned up
        assert not table_exists(project, "dml_reorder_model__dbt_refresh")


# -- Test: DML refresh handles a model that produces zero rows --

dml_model_empty_sql = """
{{
  config({
    "materialized": "table",
    "table_refresh_method": "dml",
    "as_columnstore": False
  })
}}
select 1 as id, 'hello' as val where 1 = 0
"""


class TestDmlRefreshEmptyModel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_model.sql": dml_model_sql}

    def test_empty_model_swap(self, project):
        # First run — one row
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"
        assert len(query_table(project, "dml_model")) == 1

        # Swap in an empty-projection version
        write_model(project, "dml_model.sql", dml_model_empty_sql)

        # Second run — DELETE removes the original row, INSERT inserts nothing
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        assert query_table(project, "dml_model") == []
        assert not table_exists(project, "dml_model__dbt_refresh")


# -- Test: leftover scratch table from a prior failed run is cleaned up --


class TestDmlRefreshLeftoverScratchCleanup:
    @pytest.fixture(scope="class")
    def models(self):
        return {"dml_model.sql": dml_model_sql}

    def test_leftover_scratch_does_not_block_refresh(self, project):
        # First run — establishes the target
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Simulate a prior failed run by manually creating a leftover scratch table
        with get_connection(project.adapter):
            project.adapter.execute(
                f"SELECT CAST(99 AS INT) AS id, CAST('stale' AS VARCHAR(5)) AS val "
                f"INTO {project.test_schema}.dml_model__dbt_refresh"
            )
        assert table_exists(project, "dml_model__dbt_refresh")

        # Second run — the macro's pre-DROP TABLE IF EXISTS should clean up the
        # leftover before re-creating it; the run should succeed normally.
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Target has the live row (not the stale one)
        rows = query_table(project, "dml_model")
        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == "hello"

        # Scratch fully cleaned up after the refresh
        assert not table_exists(project, "dml_model__dbt_refresh")
