"""Functional coverage for the dbt_sqlserver_use_native_string_types behaviour flag (#626).

Default (flag off): preserves the pre-existing mappings
  STRING -> VARCHAR(8000), NCHAR -> CHAR(1), NVARCHAR -> VARCHAR(8000).

Flag on: switches to SQL Server-native mappings
  STRING -> VARCHAR(MAX), NCHAR -> NCHAR(1), NVARCHAR -> NVARCHAR(4000).

Each path is exercised at two levels:
 - dict assertion on adapter.Column.TYPE_LABELS (cheap sanity check),
 - end-to-end: a contract-enforced model with `data_type: string|nchar|nvarchar`
   is materialised and the resulting column type read back from
   sys.columns + sys.types.
"""

import pytest

from dbt.tests.util import run_dbt

contract_model_sql = """
{{ config(materialized='table') }}
select
    cast('hello' as varchar(50)) as str_col,
    cast('h' as char(1)) as nchar_col,
    cast('hello' as varchar(50)) as nvarchar_col
"""

contract_model_yml = """
version: 2
models:
  - name: types_model
    config:
      contract:
        enforced: true
    columns:
      - name: str_col
        data_type: string
      - name: nchar_col
        data_type: nchar
      - name: nvarchar_col
        data_type: nvarchar
"""


def _column_types(project, schema: str, table: str) -> dict:
    """Return {column_name: (data_type, character_maximum_length)} for a table.

    Queries sys.columns + sys.types with a three-part OBJECT_ID so we don't
    depend on the connection's current-database context.
    """
    # sys.columns.max_length is in bytes; for unicode (n*) types it's two bytes
    # per character, so we halve to get the declared character length. MAX is
    # reported as -1 in both cases.
    rows = project.run_sql(
        f"""
        select c.name, t.name, c.max_length
        from [{project.database}].sys.columns c
        inner join [{project.database}].sys.types t on c.user_type_id = t.user_type_id
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


# ---------------------------------------------------------------------------
# Default behaviour — flag absent, mappings unchanged from pre-#626
# ---------------------------------------------------------------------------


class TestDefaultStringTypes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "types_model.sql": contract_model_sql,
            "schema.yml": contract_model_yml,
        }

    def test_type_labels_dict_default(self, project):
        labels = project.adapter.Column.TYPE_LABELS
        assert labels["STRING"] == "VARCHAR(8000)"
        assert labels["NCHAR"] == "CHAR(1)"
        assert labels["NVARCHAR"] == "VARCHAR(8000)"

    def test_column_types_in_database_default(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        types = _column_types(project, project.test_schema, "types_model")
        # STRING -> VARCHAR(8000)
        assert types["str_col"] == ("varchar", 8000)
        # NCHAR -> CHAR(1) (non-unicode under legacy)
        assert types["nchar_col"] == ("char", 1)
        # NVARCHAR -> VARCHAR(8000) (non-unicode under legacy)
        assert types["nvarchar_col"] == ("varchar", 8000)


# ---------------------------------------------------------------------------
# Native behaviour — flag enabled
# ---------------------------------------------------------------------------


class TestNativeStringTypes:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "dbt_sqlserver_use_native_string_types": True,
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "types_model.sql": contract_model_sql,
            "schema.yml": contract_model_yml,
        }

    def test_type_labels_dict_native(self, project):
        labels = project.adapter.Column.TYPE_LABELS
        assert labels["STRING"] == "VARCHAR(MAX)"
        assert labels["NCHAR"] == "NCHAR(1)"
        assert labels["NVARCHAR"] == "NVARCHAR(4000)"

    def test_column_types_in_database_native(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        types = _column_types(project, project.test_schema, "types_model")
        # STRING -> VARCHAR(MAX), reported as character_maximum_length = -1
        assert types["str_col"] == ("varchar", -1)
        # NCHAR -> NCHAR(1) (unicode)
        assert types["nchar_col"] == ("nchar", 1)
        # NVARCHAR -> NVARCHAR(4000) (unicode, max fixed-length)
        assert types["nvarchar_col"] == ("nvarchar", 4000)
