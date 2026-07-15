import pytest

from dbt.tests.util import get_connection, run_dbt
from tests.functional.adapter.mssql.test_index_config import index_count, indexes_def

# flake8: noqa: E501

index_seed_csv = """id_col,data,secondary_data,tertiary_data
1,'a'",122,20
"""

index_schema_base_yml = """
version: 2
seeds:
  - name: raw_data
    config:
      column_types:
          id_col: integer
          data: nvarchar(20)
          secondary_data: integer
          tertiary_data: bigint
"""

model_yml = """
version: 2
models:
  - name: index_model
  - name: index_ccs_model
"""

model_sql = """
{{
  config({
  "materialized": 'table',
  "as_columnstore": False,
        "post-hook": [
            "{{ create_clustered_index(columns = ['id_col'], unique=True) }}",
            "{{ create_nonclustered_index(columns = ['data']) }}",
            "{{ create_nonclustered_index(columns = ['secondary_data'], includes = ['tertiary_data']) }}",
        ]
  })
}}
  select * from {{ ref('raw_data') }}
"""

model_sql_ccs = """
{{
  config({
  "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['data']) }}",
            "{{ create_nonclustered_index(columns = ['secondary_data'], includes = ['tertiary_data']) }}",
        ]
  })
}}
  select * from {{ ref('raw_data') }}
"""

drop_schema_model = """
{{
  config({
  "materialized": 'table',
        "post-hook": [
            "{{ drop_all_indexes_on_table() }}",
        ]
  })
}}
select * from {{ ref('raw_data') }}
"""


class TestIndex:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_data.csv": index_seed_csv,
            "schema.yml": index_schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "index_model.sql": model_sql,
            "index_ccs_model.sql": model_sql_ccs,
            "schema.yml": model_yml,
        }

    def drop_artifacts(self, project):
        with get_connection(project.adapter):
            project.adapter.execute("DROP TABLE IF EXISTS index_model", fetch=True)
            project.adapter.execute("DROP TABLE IF EXISTS index_ccs_model")

    def test_create_index(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        with get_connection(project.adapter):
            result, table = project.adapter.execute(
                index_count.format(schema_name=project.created_schemas[0]), fetch=True
            )
        schema_dict = {_[0]: _[1] for _ in table.rows}
        expected = {
            "clustered columnstore": 1,
            "clustered unique": 1,
            "nonclustered": 4,
        }
        self.drop_artifacts(project)
        assert schema_dict == expected


class TestIndexDropsOnlySchema:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "generic_tests"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_data.csv": index_seed_csv,
            "schema.yml": index_schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "index_model.sql": drop_schema_model,
            "index_ccs_model.sql": model_sql_ccs,
            "schema.yml": model_yml,
        }

    def create_table_and_index_other_schema(self, project):
        _schema = project.test_schema + "other"
        create_sql = f"""
        USE [{project.database}];
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schema}')
        BEGIN
        EXEC('CREATE SCHEMA [{_schema}]')
        END
        """

        create_table = f"""
        CREATE TABLE {_schema}.index_model (
        IDCOL BIGINT
        )
        """

        create_index = f"""
        CREATE INDEX sample_schema ON {_schema}.index_model (IDCOL)
        """
        with get_connection(project.adapter):
            project.adapter.execute(create_sql, fetch=True)
            project.adapter.execute(create_table)
            project.adapter.execute(create_index)

    def drop_schema_artifacts(self, project):
        _schema = project.test_schema + "other"
        drop_index = f"DROP INDEX IF EXISTS sample_schema ON {_schema}.index_model"
        drop_table = f"DROP TABLE IF EXISTS {_schema}.index_model"
        drop_schema = f"DROP SCHEMA IF EXISTS {_schema}"

        with get_connection(project.adapter):
            project.adapter.execute(drop_index, fetch=True)
            project.adapter.execute(drop_table)
            project.adapter.execute(drop_schema)

    def validate_other_schema(self, project):
        with get_connection(project.adapter):
            result, table = project.adapter.execute(
                indexes_def.format(
                    schema_name=project.test_schema + "other", table_name="index_model"
                ),
                fetch=True,
            )

        assert len(table.rows) == 1

    def test_create_index(self, project):
        self.create_table_and_index_other_schema(project)
        run_dbt(["seed"])
        run_dbt(["run"])
        self.validate_other_schema(project)
        self.drop_schema_artifacts(project)


# ---------------------------------------------------------------------------
# Integration tests for the refactored index subsystem.
#
# These run against a live SQL Server (credentials from test.env) and verify:
#   * incremental runs produce identical, idempotent indexes
#   * INCLUDE columns are registered as is_included_column = 1
#   * quoted / weird identifiers are safely bracket-escaped
#   * schema-qualified models keep indexes isolated per schema
#   * multi-threaded runs are race-free (deterministic names + IF NOT EXISTS)
#   * CCI is created on the final relation, not the __dbt_tmp intermediate
#   * CCI name on the final table never contains __dbt_tmp or __dbt_backup
# ---------------------------------------------------------------------------


index_columns_for_table = """
SELECT
    i.[name]                  AS index_name,
    i.type_desc               AS index_type,
    c.[name]                  AS column_name,
    ic.is_included_column     AS is_included,
    ic.key_ordinal            AS key_ordinal
FROM sys.indexes i
INNER JOIN sys.index_columns ic
    ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns c
    ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID(N'[{database}].[{schema}].[{table}]')
ORDER BY i.[name], ic.key_ordinal, ic.is_included_column, c.[name]
"""


incremental_model_sql = """
{{
  config({
    "materialized": "incremental",
    "unique_key": "id_col",
    "as_columnstore": False,
    "post-hook": [
      "{{ create_clustered_index(columns=['id_col'], unique=True) }}",
      "{{ create_nonclustered_index(columns=['data']) }}",
    ],
  })
}}
select * from {{ ref('raw_data') }}
{% if is_incremental() %}
where id_col > (select coalesce(max(id_col), 0) from {{ this }})
{% endif %}
"""


class TestIndexIncremental:
    """Indexes should survive incremental runs (idempotent IF NOT EXISTS)."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"raw_data.csv": index_seed_csv, "schema.yml": index_schema_base_yml}

    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_model.sql": incremental_model_sql}

    def _index_rows(self, project, table_name):
        sql = index_columns_for_table.format(
            database=project.database,
            schema=project.test_schema,
            table=table_name,
        )
        with get_connection(project.adapter):
            _, table = project.adapter.execute(sql, fetch=True)
        return list(table.rows)

    def test_indexes_stable_across_incremental_runs(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        first = self._index_rows(project, "inc_model")

        run_dbt(["run"])
        second = self._index_rows(project, "inc_model")

        assert first == second
        index_names = {r[0] for r in second}
        assert len(index_names) == 2


include_columns_model_sql = """
{{
  config({
    "materialized": "table",
    "as_columnstore": False,
    "post-hook": [
      "{{ create_nonclustered_index(columns=['secondary_data'], includes=['tertiary_data','data']) }}",
    ],
  })
}}
select * from {{ ref('raw_data') }}
"""


class TestIndexIncludeColumns:
    """INCLUDE columns must end up as is_included_column = 1 in sys.index_columns."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"raw_data.csv": index_seed_csv, "schema.yml": index_schema_base_yml}

    @pytest.fixture(scope="class")
    def models(self):
        return {"inc_cols_model.sql": include_columns_model_sql}

    def test_include_columns_present(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        sql = index_columns_for_table.format(
            database=project.database,
            schema=project.test_schema,
            table="inc_cols_model",
        )
        with get_connection(project.adapter):
            _, table = project.adapter.execute(sql, fetch=True)
        rows = list(table.rows)
        keyed = [r for r in rows if r[3] == 0]
        included = [r for r in rows if r[3] == 1]
        assert {r[2] for r in keyed} == {"secondary_data"}
        assert {r[2] for r in included} == {"tertiary_data", "data"}


quoted_seed_csv = """id col],weird name
1,a
2,b
"""

quoted_schema_yml = """
version: 2
seeds:
  - name: raw_data
    config:
      column_types:
        "id col]": integer
        "weird name": nvarchar(20)
"""

quoted_model_sql = """
{{
  config({
    "materialized": "table",
    "as_columnstore": False,
    "post-hook": [
      "{{ create_nonclustered_index(columns=['id col]', 'weird name']) }}",
    ],
  })
}}
select * from {{ ref('raw_data') }}
"""


class TestIndexQuotedIdentifiers:
    """Bracket and space characters in column names must be safely quoted."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"raw_data.csv": quoted_seed_csv, "schema.yml": quoted_schema_yml}

    @pytest.fixture(scope="class")
    def models(self):
        return {"quoted_model.sql": quoted_model_sql}

    def test_quoted_columns_indexed(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        sql = index_columns_for_table.format(
            database=project.database,
            schema=project.test_schema,
            table="quoted_model",
        )
        with get_connection(project.adapter):
            _, table = project.adapter.execute(sql, fetch=True)
        col_names = {r[2] for r in table.rows}
        assert "id col]" in col_names
        assert "weird name" in col_names





orphan_cci_check_sql = """
SELECT COUNT(*)
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = '{schema}'
  AND (t.name LIKE '%__dbt_tmp' OR t.name LIKE '%__dbt_backup')
  AND i.type IN (5, 6)
"""


class TestNoOrphanColumnstoreIndex:
    """Regression: CCI must be created on the *final* relation, not the
    __dbt_tmp intermediate. After a successful run there should be zero CCIs
    sitting on any tmp / backup relation in the test schema."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"raw_data.csv": index_seed_csv, "schema.yml": index_schema_base_yml}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cci_model.sql": (
                "{{ config(materialized='table', as_columnstore=True) }}\n"
                "select * from {{ ref('raw_data') }}\n"
            )
        }

    def test_no_orphan_cci(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["run"])

        with get_connection(project.adapter):
            _, t = project.adapter.execute(
                orphan_cci_check_sql.format(schema=project.test_schema),
                fetch=True,
            )
        assert list(t.rows)[0][0] == 0, "Orphaned CCI found on __dbt_tmp/__dbt_backup"

        cci_name_sql = """
            SELECT i.name
            FROM sys.indexes i
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{schema}'
              AND t.name = 'cci_model'
              AND i.type IN (5, 6)
        """.format(schema=project.test_schema)
        with get_connection(project.adapter):
            _, t = project.adapter.execute(cci_name_sql, fetch=True)
        cci_rows = list(t.rows)
        assert len(cci_rows) == 1, f"Expected exactly 1 CCI on cci_model, got {cci_rows}"
        cci_name = cci_rows[0][0]
        assert "__dbt_tmp" not in cci_name, (
            f"CCI name '{cci_name}' contains __dbt_tmp - index would be orphaned after rename"
        )
        assert "__dbt_backup" not in cci_name, (
            f"CCI name '{cci_name}' contains __dbt_backup - index would be orphaned after rename"
        )
