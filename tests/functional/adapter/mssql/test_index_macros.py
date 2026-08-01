import pytest

from dbt.tests.util import get_connection, run_dbt
from tests.functional.adapter.mssql.test_index_config import indexes_def

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

other_schema_pk_count = """
select count(*)
from sys.key_constraints kc
inner join sys.tables t on kc.parent_object_id = t.object_id
where kc.[type] = 'PK'
  and schema_name(t.schema_id) = '{schema_name}'
  and t.[name] = '{table_name}'
"""

other_schema_fk_count = """
select count(*)
from sys.foreign_keys fk
inner join sys.tables t on fk.referenced_object_id = t.object_id
where schema_name(t.schema_id) = '{schema_name}'
  and t.[name] = '{table_name}'
"""

# Foreign keys touching a table in either direction: inbound (some other table
# references it) and outbound (it references some other table).
fk_count_both_directions = """
select
    sum(case when fk.referenced_object_id = t.object_id then 1 else 0 end) as inbound,
    sum(case when fk.parent_object_id = t.object_id then 1 else 0 end) as outbound
from sys.tables t
left join sys.foreign_keys fk
    on fk.referenced_object_id = t.object_id
    or fk.parent_object_id = t.object_id
where schema_name(t.schema_id) = '{schema_name}'
  and t.[name] = '{table_name}'
"""

# The model owns a foreign key in each direction by the time
# drop_all_indexes_on_table() runs: fk_child points at it, and it points at
# fk_target. Both must be gone afterwards (issue #632).
drop_both_fk_directions_model = """
{{
  config({
  "materialized": 'table',
  "as_columnstore": False,
        "post-hook": [
            "alter table {{ this.schema }}.{{ this.identifier }} alter column id_col int not null",
            "alter table {{ this.schema }}.{{ this.identifier }} add constraint pk_fk_model primary key (id_col)",
            "if object_id('{{ this.schema }}.fk_target', 'U') is null create table {{ this.schema }}.fk_target (target_id int not null constraint pk_fk_target primary key)",
            "if object_id('{{ this.schema }}.fk_child', 'U') is null create table {{ this.schema }}.fk_child (child_id int not null, constraint fk_child_to_model foreign key (child_id) references {{ this.schema }}.{{ this.identifier }} (id_col))",
            "alter table {{ this.schema }}.{{ this.identifier }} with nocheck add constraint fk_model_to_target foreign key (secondary_data) references {{ this.schema }}.fk_target (target_id)",
            "{{ drop_all_indexes_on_table() }}",
        ]
  })
}}
select * from {{ ref('raw_data') }}
"""

# A schema name needing delimiters: the backslash reaches the generated CCI
# name (#409), the dot and the quote reach identifiers built inside string
# literals. Raw string: what is written here is exactly what Jinja parses.
backslash_schema_model = r"""
{{ config(materialized='table', schema=target.schema ~ '_dom\\usr.x\"q') }}
select 1 as id
"""


class TestIndexMacros:
    """Every index-macro assertion in this module shares one project and one
    dbt invocation. The models are independent of each other, so each test
    reads only the tables it owns:

    create_index_model / index_ccs_model  index creation
    index_model                           drops stay inside the model's schema
    fk_model                              drops reach both foreign key directions
    """

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
        # index_model keeps its name deliberately: it has to collide with the
        # decoy table created in the other schema.
        return {
            "create_index_model.sql": model_sql,
            "index_model.sql": drop_schema_model,
            "index_ccs_model.sql": model_sql_ccs,
            "fk_model.sql": drop_both_fk_directions_model,
            "backslash_schema_model.sql": backslash_schema_model,
            "schema.yml": model_yml,
        }

    @pytest.fixture(scope="class", autouse=True)
    def build_once(self, project):
        # The decoy table in the other schema has to exist before the run; the
        # single `build` then seeds and builds every model in this class.
        self.create_table_and_index_other_schema(project)
        run_dbt(["build"])
        yield
        self.drop_fk_artifacts(project)
        self.drop_schema_artifacts(project)

    def create_table_and_index_other_schema(self, project):
        _schema = project.test_schema + "other"
        create_sql = f"""
        USE [{project.database}];
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schema}')
        BEGIN
        EXEC('CREATE SCHEMA [{_schema}]')
        END
        """

        # Same table name as the dbt model, in a different schema. The primary
        # key and the inbound foreign key are what the drop_pk_constraints() /
        # drop_fk_constraints() legs of drop_all_indexes_on_table() reach.
        create_table = f"""
        CREATE TABLE {_schema}.index_model (
        IDCOL BIGINT NOT NULL,
        CONSTRAINT pk_other_index_model PRIMARY KEY (IDCOL)
        )
        """

        create_index = f"""
        CREATE INDEX sample_schema ON {_schema}.index_model (IDCOL)
        """

        create_child_table = f"""
        CREATE TABLE {_schema}.index_model_child (
        IDCOL BIGINT NOT NULL,
        CONSTRAINT fk_other_index_model FOREIGN KEY (IDCOL)
            REFERENCES {_schema}.index_model (IDCOL)
        )
        """
        with get_connection(project.adapter):
            project.adapter.execute(create_sql, fetch=True)
            project.adapter.execute(create_table)
            project.adapter.execute(create_index)
            project.adapter.execute(create_child_table)

    def drop_schema_artifacts(self, project):
        _schema = project.test_schema + "other"
        drop_child_table = f"DROP TABLE IF EXISTS {_schema}.index_model_child"
        drop_index = f"DROP INDEX IF EXISTS sample_schema ON {_schema}.index_model"
        drop_table = f"DROP TABLE IF EXISTS {_schema}.index_model"
        drop_schema = f"DROP SCHEMA IF EXISTS {_schema}"
        bs_schema = (project.test_schema + '_dom\\usr.x"q').replace('"', '""')  # single backslash

        with get_connection(project.adapter):
            project.adapter.execute(drop_child_table, fetch=True)
            project.adapter.execute(drop_index)
            project.adapter.execute(drop_table)
            project.adapter.execute(drop_schema)
            project.adapter.execute(f'DROP TABLE IF EXISTS "{bs_schema}".backslash_schema_model')
            project.adapter.execute(f'DROP SCHEMA IF EXISTS "{bs_schema}"')

    def drop_fk_artifacts(self, project):
        # Ordered so teardown still works when the macro under test leaves a
        # key behind: each table goes before the one it points at.
        with get_connection(project.adapter):
            project.adapter.execute(
                f"DROP TABLE IF EXISTS {project.test_schema}.fk_child", fetch=True
            )
            project.adapter.execute(f"DROP TABLE IF EXISTS {project.test_schema}.fk_model")
            project.adapter.execute(f"DROP TABLE IF EXISTS {project.test_schema}.fk_target")

    def validate_other_schema(self, project):
        _schema = project.test_schema + "other"
        with get_connection(project.adapter):
            result, table = project.adapter.execute(
                indexes_def.format(schema_name=_schema, table_name="index_model"),
                fetch=True,
            )

            _, pk_table = project.adapter.execute(
                other_schema_pk_count.format(schema_name=_schema, table_name="index_model"),
                fetch=True,
            )

            _, fk_table = project.adapter.execute(
                other_schema_fk_count.format(schema_name=_schema, table_name="index_model"),
                fetch=True,
            )

        # The nonclustered index plus the clustered index backing the primary key.
        assert len(table.rows) == 2
        assert pk_table.rows[0][0] == 1
        assert fk_table.rows[0][0] == 1

    def test_create_index(self, project):
        # Counted over the two index-building models by name rather than over
        # the whole schema: the drop models share this schema, so a schema-wide
        # count would make this assertion fail whenever a drop macro breaks.
        index_types = {}
        with get_connection(project.adapter):
            for table_name in ("create_index_model", "index_ccs_model"):
                _, table = project.adapter.execute(
                    indexes_def.format(schema_name=project.test_schema, table_name=table_name),
                    fetch=True,
                )
                for row in table.rows:
                    key = row["index_type"] + (" unique" if row["unique"] == "Unique" else "")
                    index_types[key] = index_types.get(key, 0) + 1

        assert index_types == {
            "clustered columnstore": 1,
            "clustered unique": 1,
            "nonclustered": 4,
        }

    def test_builds_in_a_schema_needing_delimiters(self, project):
        """A backslash in the schema reaches the CCI name (#409)."""
        with get_connection(project.adapter):
            _, table = project.adapter.execute(
                f"""
                select i.type_desc
                from sys.indexes i
                join sys.tables t on t.object_id = i.object_id
                join sys.schemas s on s.schema_id = t.schema_id
                where s.name like '{project.test_schema}%' and s.name like '%\\%'
                  and t.name = 'backslash_schema_model'
                  and i.type_desc = 'CLUSTERED COLUMNSTORE'
                """,
                fetch=True,
            )
        assert len(table.rows) == 1, f"expected a clustered columnstore index, got {table.rows}"

    def test_leaves_other_schemas_alone(self, project):
        self.validate_other_schema(project)

    def test_drops_inbound_and_outbound_fks(self, project):
        """Both directions, not just the keys pointing at the model (#632)."""
        with get_connection(project.adapter):
            _, model_fks = project.adapter.execute(
                fk_count_both_directions.format(
                    schema_name=project.test_schema, table_name="fk_model"
                ),
                fetch=True,
            )
            # The counterparties keep their own constraints: fk_target's
            # primary key is untouched by a drop scoped to fk_model.
            _, target_pk = project.adapter.execute(
                other_schema_pk_count.format(
                    schema_name=project.test_schema, table_name="fk_target"
                ),
                fetch=True,
            )

        inbound, outbound = model_fks.rows[0][0], model_fks.rows[0][1]
        assert inbound == 0, "inbound foreign key survived drop_fk_constraints()"
        assert outbound == 0, "outbound foreign key survived drop_fk_constraints()"
        assert target_pk.rows[0][0] == 1
