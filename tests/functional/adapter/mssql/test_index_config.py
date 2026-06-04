import re

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

base_validation = """
with base_query AS (
select i.[name] as index_name,
  substring(column_names, 1, len(column_names)-1) as [columns],
  substring(included_column_names, 1, len(included_column_names)-1) as included_columns,
  case when i.[type] = 1 then 'clustered'
    when i.[type] = 2 then 'nonclustered'
    when i.[type] = 3 then 'xml'
    when i.[type] = 4 then 'spatial'
    when i.[type] = 5 then 'clustered columnstore'
    when i.[type] = 6 then 'nonclustered columnstore'
    when i.[type] = 7 then 'nonclustered hash'
    end as index_type,
  case when i.is_unique = 1 then 'Unique'
    else 'Not unique' end as [unique],
  schema_name(t.schema_id) + '.' + t.[name] as table_view,
  case when t.[type] = 'U' then 'Table'
    when t.[type] = 'V' then 'View'
    end as [object_type],
  s.name as schema_name
from sys.objects t
  inner join sys.schemas s
  on
    t.schema_id = s.schema_id
  inner join sys.indexes i
    on t.object_id = i.object_id
  cross apply (select col.[name] + ', '
          from sys.index_columns ic
            inner join sys.columns col
              on ic.object_id = col.object_id
              and ic.column_id = col.column_id
          where ic.object_id = t.object_id
            and ic.index_id = i.index_id
            and ic.is_included_column = 0
              order by key_ordinal
              for xml path ('') ) D (column_names)
  cross apply (select col.[name] + ', '
          from sys.index_columns ic
            inner join sys.columns col
              on ic.object_id = col.object_id
              and ic.column_id = col.column_id
          where ic.object_id = t.object_id
            and ic.index_id = i.index_id
            and ic.is_included_column = 1
              order by key_ordinal
              for xml path ('') ) E (included_column_names)
where t.is_ms_shipped <> 1
and index_id > 0
)
"""

index_count = base_validation + """
select
  index_type + case when [unique] = 'Unique' then ' unique' else '' end as index_type,
  count(*) index_count
from
  base_query
WHERE
  schema_name='{schema_name}'
group by index_type + case when [unique] = 'Unique' then ' unique' else '' end
"""

indexes_def = base_validation + """
SELECT
  index_name,
  [columns],
  [included_columns],
  index_type,
  [unique],
  table_view,
  [object_type],
  schema_name
FROM
  base_query
WHERE
  schema_name='{schema_name}'
  AND
  table_view='{schema_name}.{table_name}'

"""

# Altered from: https://github.com/dbt-labs/dbt-postgres

models__incremental_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a'], 'type': 'nonclustered'},
      {'columns': ['column_a', 'column_b'], 'unique': True},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__columnstore_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a'], 'type': 'columnstore'},
    ]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""


models__table_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a']},
      {'columns': ['column_b']},
      {'columns': ['column_a', 'column_b']},
      {'columns': ['column_b', 'column_a'], 'type': 'clustered', 'unique': True},
      {'columns': ['column_a','column_c'],
        'type': 'nonclustered',
        'included_columns': ['column_b']},
    ]
  )
}}

select 1 as column_a, 2 as column_b, 3 as column_c

"""


models__table_compressed_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_b', 'column_a'], 'type': 'clustered', 'data_compression': 'page'},
      {'columns': ['column_a'], 'data_compression': 'row', 'sort_in_tempdb': True},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""


models__table_reserved_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {'columns': ['order'], 'included_columns': ['select']},
    ]
  )
}}

select 1 as [order], 2 as [select]

"""


models__table_included_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a'], 'included_columns': ['column_b']},
      {'columns': ['column_b'], 'type': 'clustered'}
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_columns_type_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': 'column_a, column_b'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_type_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'type': 'non_existent_type'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__invalid_unique_config_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'unique': 'yes'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models_invalid__missing_columns_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'unique': True},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

snapshots__colors_sql = """
{% snapshot colors %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            as_columnstore=False,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=[
              {'columns': ['id'], 'type': 'nonclustered'},
              {'columns': ['id', 'color'], 'unique': True},
            ]
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color union all
        select 2 as id, 'green' as color

    {% else %}

        select 1 as id, 'blue' as color union all
        select 2 as id, 'green' as color

    {% endif %}

{% endsnapshot %}

"""

seeds__seed_csv = """country_code,country_name
US,United States
CA,Canada
GB,United Kingdom
"""


class TestSQLServerIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table.sql": models__table_sql,
            "incremental.sql": models__incremental_sql,
            "columnstore.sql": models__columnstore_sql,
            "table_included.sql": models__table_included_sql,
            "table_reserved.sql": models__table_reserved_sql,
            "table_compressed.sql": models__table_compressed_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seeds__seed_csv}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"colors.sql": snapshots__colors_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seeds": {
                "quote_columns": False,
                "indexes": [
                    {"columns": ["country_code"], "unique": False},
                    {
                        "columns": ["country_code", "country_name"],
                        "unique": True,
                        "type": "clustered",
                    },
                ],
            },
            "vars": {
                "version": 1,
            },
        }

    def test_table(self, project, unique_schema):
        results = run_dbt(["run", "--models", "table"])
        assert len(results) == 1

        indexes = self.get_indexes("table", project, unique_schema)
        indexes = self.sort_indexes(indexes)
        expected = [
            {
                "columns": "column_a",
                "unique": False,
                "type": "nonclustered",
                "included_columns": None,
            },
            {
                "columns": "column_a, column_b",
                "unique": False,
                "type": "nonclustered",
                "included_columns": None,
            },
            {
                "columns": "column_a, column_c",
                "unique": False,
                "type": "nonclustered",
                "included_columns": "column_b",
            },
            {
                "columns": "column_b",
                "unique": False,
                "type": "nonclustered",
                "included_columns": None,
            },
            {
                "columns": "column_b, column_a",
                "unique": True,
                "type": "clustered",
                "included_columns": None,
            },
        ]
        assert indexes == expected

    def test_table_included(self, project, unique_schema):
        results = run_dbt(["run", "--models", "table_included"])
        assert len(results) == 1

        indexes = self.get_indexes("table_included", project, unique_schema)
        indexes = self.sort_indexes(indexes)
        expected = [
            {
                "columns": "column_a",
                "unique": False,
                "type": "nonclustered",
                "included_columns": "column_b",
            },
            {
                "columns": "column_b",
                "unique": False,
                "type": "clustered",
                "included_columns": None,
            },
        ]
        assert indexes == expected

    def test_incremental(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["run", "--models", "incremental"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("incremental", project, unique_schema)
            indexes = self.sort_indexes(indexes)
            expected = [
                {
                    "columns": "column_a",
                    "unique": False,
                    "type": "nonclustered",
                    "included_columns": None,
                },
                {
                    "columns": "column_a, column_b",
                    "unique": True,
                    "type": "nonclustered",
                    "included_columns": None,
                },
            ]
            assert indexes == expected

    def test_columnstore(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["run", "--models", "columnstore"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("columnstore", project, unique_schema)
            expected = [
                {
                    "columns": "column_a",
                    "unique": False,
                    "type": "columnstore",
                    "included_columns": None,
                },
            ]
            assert len(indexes) == len(
                expected
            )  # Nonclustered columnstore indexes meta is different

    def test_seed(self, project, unique_schema):
        for additional_argument in [[], [], ["--full-refresh"]]:
            results = run_dbt(["seed"] + additional_argument)
            assert len(results) == 1

            indexes = self.get_indexes("seed", project, unique_schema)
            indexes = self.sort_indexes(indexes)
            expected = [
                {
                    "columns": "country_code",
                    "unique": False,
                    "type": "nonclustered",
                    "included_columns": None,
                },
                {
                    "columns": "country_code, country_name",
                    "unique": True,
                    "type": "clustered",
                    "included_columns": None,
                },
            ]
            assert indexes == expected

    def test_snapshot(self, project, unique_schema):
        for version in [1, 2]:
            results = run_dbt(["snapshot", "--vars", f"version: {version}"])
            assert len(results) == 1

            indexes = self.get_indexes("colors", project, unique_schema)
            indexes = self.sort_indexes(indexes)
            expected = [
                {
                    "columns": "id",
                    "unique": False,
                    "type": "nonclustered",
                    "included_columns": None,
                },
                {
                    "columns": "id, color",
                    "unique": True,
                    "type": "nonclustered",
                    "included_columns": None,
                },
            ]
            assert indexes == expected

    def test_table_indexes_stable_across_runs(self, project, unique_schema):
        # Deterministic naming: a rebuild must produce the same index *names*
        # (reconciliation relies on name equality <=> definition equality),
        # and the definition set must never accumulate.
        results = run_dbt(["run", "--models", "table"])
        assert len(results) == 1
        first_names = self.get_index_names("table", project, unique_schema)
        first_defs = self.sort_indexes(self.get_indexes("table", project, unique_schema))

        results = run_dbt(["run", "--models", "table"])
        assert len(results) == 1
        second_names = self.get_index_names("table", project, unique_schema)
        second_defs = self.sort_indexes(self.get_indexes("table", project, unique_schema))

        assert first_names == second_names
        assert first_defs == second_defs
        assert len(first_defs) == 5

    def test_table_reserved_word_columns(self, project, unique_schema):
        # Index key and included columns must be bracket-quoted: this model
        # indexes columns named after T-SQL reserved words.
        results = run_dbt(["run", "--models", "table_reserved"])
        assert len(results) == 1

        indexes = self.sort_indexes(self.get_indexes("table_reserved", project, unique_schema))
        expected = [
            {
                "columns": "order",
                "unique": False,
                "type": "nonclustered",
                "included_columns": "select",
            },
        ]
        assert indexes == expected

    def test_table_data_compression(self, project, unique_schema):
        results = run_dbt(["run", "--models", "table_compressed"])
        assert len(results) == 1

        sql = f"""
        select i.type_desc, p.data_compression_desc
        from sys.indexes i
        join sys.partitions p
          on p.object_id = i.object_id and p.index_id = i.index_id
        where i.object_id = OBJECT_ID('{unique_schema}.table_compressed')
          and i.index_id > 0
        order by i.type_desc
        """
        rows = project.run_sql(sql, fetch="all")
        compression = {row[0]: row[1] for row in rows}
        assert compression == {"CLUSTERED": "PAGE", "NONCLUSTERED": "ROW"}

    def get_index_names(self, table_name, project, unique_schema):
        sql = indexes_def.format(schema_name=unique_schema, table_name=table_name)
        results = project.run_sql(sql, fetch="all")
        return sorted(row[0] for row in results)

    def get_indexes(self, table_name, project, unique_schema):
        sql = indexes_def.format(schema_name=unique_schema, table_name=table_name)
        results = project.run_sql(sql, fetch="all")
        return [self.index_definition_dict(row) for row in results]

    def index_definition_dict(self, index_definition):
        is_unique = index_definition[4] == "Unique"
        return {
            "columns": index_definition[1],
            "included_columns": index_definition[2],
            "unique": is_unique,
            "type": index_definition[3],
        }

    def sort_indexes(self, indexes):
        return sorted(indexes, key=lambda x: (x["columns"], x["type"]))


class TestSQLServerInvalidIndex:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_unique_config.sql": models_invalid__invalid_unique_config_sql,
            "invalid_type.sql": models_invalid__invalid_type_sql,
            "invalid_columns_type.sql": models_invalid__invalid_columns_type_sql,
            "missing_columns.sql": models_invalid__missing_columns_sql,
        }

    def test_invalid_index_configs(self, project):
        results, output = run_dbt_and_capture(expect_pass=False)
        assert len(results) == 4
        assert re.search(r"columns.*is not of type 'array'", output)
        assert re.search(r"unique.*is not of type 'boolean'", output)
        assert re.search(r"'columns' is a required property", output)
        assert re.search(r"'non_existent_type'.*is not one of", output)


models__reconcile_incremental_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    indexes = var('reconcile_indexes', [{'columns': ['column_a'], 'type': 'nonclustered'}])
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__reconcile_dml_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    table_refresh_method = "dml",
    indexes = var('reconcile_indexes', [{'columns': ['column_a'], 'type': 'nonclustered'}])
  )
}}

select 1 as column_a, 2 as column_b

"""

snapshots__reconcile_snapshot_sql = """
{% snapshot reconcile_colors %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            as_columnstore=False,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=var('reconcile_indexes', [{'columns': ['id'], 'type': 'nonclustered'}])
        )
    }}

    select 1 as id, 'red' as color

{% endsnapshot %}

"""

models__drop_unmanaged_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    drop_unmanaged_indexes = var('dum', False),
    indexes = [{'columns': ['column_a'], 'type': 'nonclustered'}]
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

models__collision_sql = """
{{
  config(
    materialized = "incremental",
    as_columnstore = False,
    indexes = var('coll_idx', [])
  )
}}

select *
from (
  select 1 as column_a, 2 as column_b
) t

{% if is_incremental() %}
    where column_a > (select max(column_a) from {{this}})
{% endif %}

"""

SET_B = "[{'columns': ['column_b'], 'type': 'nonclustered'}]"


def get_index_rows(project, unique_schema, table_name):
    sql = indexes_def.format(schema_name=unique_schema, table_name=table_name)
    return project.run_sql(sql, fetch="all")


def index_summary(rows):
    return sorted((row[1], row[3]) for row in rows)  # (columns, type)


class TestSQLServerIndexReconciliation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"reconcile_incremental.sql": models__reconcile_incremental_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"reconcile_colors.sql": snapshots__reconcile_snapshot_sql}

    def test_incremental_reconciles_definition_change(self, project, unique_schema):
        run_dbt(["run", "--models", "reconcile_incremental"])
        first = get_index_rows(project, unique_schema, "reconcile_incremental")
        assert index_summary(first) == [("column_a", "nonclustered")]
        first_names = sorted(row[0] for row in first)

        # Non-full-refresh rerun with unchanged config: no churn, names stable.
        _, output = run_dbt_and_capture(["run", "--models", "reconcile_incremental"])
        second = get_index_rows(project, unique_schema, "reconcile_incremental")
        assert sorted(row[0] for row in second) == first_names
        assert "Dropping index" not in output

        # Non-full-refresh rerun with a changed definition: reconciled to set B.
        run_dbt(
            ["run", "--models", "reconcile_incremental", "--vars", f"reconcile_indexes: {SET_B}"]
        )
        third = get_index_rows(project, unique_schema, "reconcile_incremental")
        assert index_summary(third) == [("column_b", "nonclustered")]

    def test_snapshot_reconciles_definition_change(self, project, unique_schema):
        run_dbt(["snapshot"])
        first = get_index_rows(project, unique_schema, "reconcile_colors")
        assert index_summary(first) == [("id", "nonclustered")]

        run_dbt(["snapshot", "--vars", "reconcile_indexes: [{'columns': ['color']}]"])
        second = get_index_rows(project, unique_schema, "reconcile_colors")
        assert index_summary(second) == [("color", "nonclustered")]


class TestSQLServerIndexReconciliationDML:
    @pytest.fixture(scope="class")
    def models(self):
        return {"reconcile_dml.sql": models__reconcile_dml_sql}

    def test_dml_refresh_reconciles_definition_change(self, project, unique_schema):
        run_dbt(["run", "--models", "reconcile_dml"])
        first = get_index_rows(project, unique_schema, "reconcile_dml")
        assert index_summary(first) == [("column_a", "nonclustered")]
        first_names = sorted(row[0] for row in first)

        # Unchanged pure-DML rerun: no churn, names stable.
        _, output = run_dbt_and_capture(["run", "--models", "reconcile_dml"])
        assert "Dropping index" not in output
        assert (
            sorted(row[0] for row in get_index_rows(project, unique_schema, "reconcile_dml"))
            == first_names
        )

        # Pure-DML refresh keeps the table; reconcile must converge it.
        run_dbt(["run", "--models", "reconcile_dml", "--vars", f"reconcile_indexes: {SET_B}"])
        second = get_index_rows(project, unique_schema, "reconcile_dml")
        assert index_summary(second) == [("column_b", "nonclustered")]


class TestSQLServerDropUnmanagedIndexes:
    @pytest.fixture(scope="class")
    def models(self):
        return {"drop_unmanaged.sql": models__drop_unmanaged_sql}

    def seed_out_of_band_indexes(self, project, unique_schema):
        table = f"{unique_schema}.drop_unmanaged"
        project.run_sql(f"CREATE NONCLUSTERED INDEX dbt_idx_orphan ON {table} (column_b)")
        project.run_sql(f"CREATE NONCLUSTERED INDEX ix_dba_tuning ON {table} (column_b)")
        project.run_sql(f"CREATE NONCLUSTERED INDEX nonclustered_legacy1 ON {table} (column_b)")
        project.run_sql(f"ALTER TABLE {table} ADD CONSTRAINT uq_drop_unmanaged UNIQUE (column_a)")

    def names(self, project, unique_schema):
        return sorted(row[0] for row in get_index_rows(project, unique_schema, "drop_unmanaged"))

    def test_drop_unmanaged_modes(self, project, unique_schema):
        run_dbt(["run", "--models", "drop_unmanaged"])
        self.seed_out_of_band_indexes(project, unique_schema)

        # Default (false): managed orphan swept, everything else kept.
        run_dbt(["run", "--models", "drop_unmanaged"])
        names = self.names(project, unique_schema)
        assert "dbt_idx_orphan" not in names
        assert "ix_dba_tuning" in names
        assert "nonclustered_legacy1" in names
        assert "uq_drop_unmanaged" in names

        # warn: unmanaged listed, nothing else dropped.
        _, output = run_dbt_and_capture(
            ["run", "--models", "drop_unmanaged", "--vars", "dum: warn"]
        )
        assert "ix_dba_tuning" in output
        assert "ix_dba_tuning" in self.names(project, unique_schema)

        # true: unmanaged dropped; constraint-backing and legacy names survive.
        run_dbt(["run", "--models", "drop_unmanaged", "--vars", "dum: true"])
        names = self.names(project, unique_schema)
        assert "ix_dba_tuning" not in names
        assert "nonclustered_legacy1" in names
        assert "uq_drop_unmanaged" in names


class TestSQLServerClusteredCollision:
    @pytest.fixture(scope="class")
    def models(self):
        return {"collision.sql": models__collision_sql}

    def test_clustered_collision_fails_with_clear_error(self, project, unique_schema):
        run_dbt(["run", "--models", "collision"])
        project.run_sql(
            f"CREATE CLUSTERED INDEX ix_dba_clustered " f"ON {unique_schema}.collision (column_a)"
        )

        _, output = run_dbt_and_capture(
            [
                "run",
                "--models",
                "collision",
                "--vars",
                "coll_idx: [{'columns': ['column_b'], 'type': 'clustered'}]",
            ],
            expect_pass=False,
        )
        assert "ix_dba_clustered" in output
        assert "will not be dropped" in output


models__multiple_clustered_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a'], 'type': 'clustered'},
      {'columns': ['column_b'], 'type': 'clustered'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""

models__clustered_with_cci_sql = """
{{
  config(
    materialized = "table",
    indexes=[
      {'columns': ['column_a'], 'type': 'clustered'},
    ]
  )
}}

select 1 as column_a, 2 as column_b

"""


class TestSQLServerCrossConfigValidation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "multiple_clustered.sql": models__multiple_clustered_sql,
            "clustered_with_cci.sql": models__clustered_with_cci_sql,
        }

    def test_multiple_clustered_rejected(self, project):
        _, output = run_dbt_and_capture(
            ["run", "--models", "multiple_clustered"], expect_pass=False
        )
        assert "at most one clustered index" in output

    def test_clustered_with_default_columnstore_rejected(self, project):
        # as_columnstore deliberately omitted: it defaults to TRUE, so the
        # table is built with a clustered columnstore index and a clustered
        # rowstore index cannot also exist.
        _, output = run_dbt_and_capture(
            ["run", "--models", "clustered_with_cci"], expect_pass=False
        )
        assert "as_columnstore" in output


models__full_options_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    indexes=[
      {'columns': ['column_a', {'column': 'column_b', 'desc': True}],
       'type': 'clustered', 'data_compression': 'page', 'fillfactor': 90},
      {'columns': ['column_a'], 'unique': True, 'ignore_dup_key': True,
       'build_options': {'maxdop': 1, 'allow_page_locks': False}},
      {'columns': ['column_b'], 'included_columns': ['column_c'],
       'where': 'column_b is not null',
       'optimize_for_sequential_key': True},
      {'columns': ['column_c'], 'type': 'columnstore',
       'data_compression': 'columnstore_archive'},
    ]
  )
}}

select 1 as column_a, 2 as column_b, 3 as column_c

"""


class TestSQLServerIndexFullOptions:
    @pytest.fixture(scope="class")
    def models(self):
        return {"full_options.sql": models__full_options_sql}

    def introspect(self, project, unique_schema):
        sql = f"""
        select i.[name], i.type_desc, i.is_unique, i.ignore_dup_key,
               i.fill_factor, i.has_filter, i.filter_definition,
               isnull(desc_cols.cols, '') as descending_columns,
               isnull(part.data_compression_desc, '') as data_compression
        from sys.indexes i
        outer apply (
            select string_agg(col.[name], ', ') as cols
            from sys.index_columns ic
            join sys.columns col
              on col.object_id = ic.object_id and col.column_id = ic.column_id
            where ic.object_id = i.object_id and ic.index_id = i.index_id
              and ic.is_descending_key = 1
        ) desc_cols
        outer apply (
            select max(p.data_compression_desc) as data_compression_desc
            from sys.partitions p
            where p.object_id = i.object_id and p.index_id = i.index_id
        ) part
        where i.object_id = OBJECT_ID('{unique_schema}.full_options')
          and i.index_id > 0
        """
        rows = project.run_sql(sql, fetch="all")
        return {(row[1], bool(row[2]), bool(row[5])): row for row in rows}

    def test_full_option_surface(self, project, unique_schema):
        results = run_dbt(["run", "--models", "full_options"])
        assert len(results) == 1

        by_key = self.introspect(project, unique_schema)
        assert set(by_key) == {
            ("CLUSTERED", False, False),
            ("NONCLUSTERED", True, False),
            ("NONCLUSTERED", False, True),
            ("NONCLUSTERED COLUMNSTORE", False, False),
        }

        clustered = by_key[("CLUSTERED", False, False)]
        assert clustered[4] == 90  # fill_factor
        assert clustered[7] == "column_b"  # descending key
        assert clustered[8] == "PAGE"

        unique_idx = by_key[("NONCLUSTERED", True, False)]
        assert unique_idx[3] in (True, 1)  # ignore_dup_key

        filtered_idx = by_key[("NONCLUSTERED", False, True)]
        assert "column_b" in filtered_idx[6]  # filter_definition

        columnstore = by_key[("NONCLUSTERED COLUMNSTORE", False, False)]
        assert columnstore[8] == "COLUMNSTORE_ARCHIVE"

        # Stability: rebuild produces identical names - the new fields hash
        # deterministically and reconcile/create has nothing to churn.
        first_names = sorted(by_key[k][0] for k in by_key)
        results = run_dbt(["run", "--models", "full_options"])
        assert len(results) == 1
        second = self.introspect(project, unique_schema)
        assert sorted(second[k][0] for k in second) == first_names


models__project_level_sql = """
select 1 as column_a, 2 as column_b
"""

models__project_level_override_sql = """
{{
  config(
    indexes=[{'columns': ['column_b']}]
  )
}}

select 1 as column_a, 2 as column_b
"""


class TestSQLServerProjectLevelIndexes:
    """indexes set as keys in dbt_project.yml (models scope) must behave the
    same as in-model config, and a model-level indexes config must fully
    replace (clobber, not merge) the project-level list."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "project_level.sql": models__project_level_sql,
            "project_level_override.sql": models__project_level_override_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+materialized": "table",
                    "+as_columnstore": False,
                    "+indexes": [{"columns": ["column_a"], "type": "nonclustered"}],
                }
            }
        }

    def test_project_level_indexes_apply(self, project, unique_schema):
        results = run_dbt(["run"])
        assert len(results) == 2

        from_project = get_index_rows(project, unique_schema, "project_level")
        assert index_summary(from_project) == [("column_a", "nonclustered")]

        # model-level config clobbers the project-level list entirely
        overridden = get_index_rows(project, unique_schema, "project_level_override")
        assert index_summary(overridden) == [("column_b", "nonclustered")]


models__invalid_dum_sql = """
{{
  config(
    materialized = "table",
    as_columnstore = False,
    drop_unmanaged_indexes = "yes please",
    indexes=[{'columns': ['column_a']}]
  )
}}

select 1 as column_a

"""

snapshots__clustered_cci_snapshot_sql = """
{% snapshot clustered_cci_snapshot %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=[{'columns': ['id'], 'type': 'clustered'}]
        )
    }}

    select 1 as id, 'red' as color

{% endsnapshot %}

"""


class TestSQLServerEarlyValidation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"invalid_dum.sql": models__invalid_dum_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"clustered_cci_snapshot.sql": snapshots__clustered_cci_snapshot_sql}

    def test_invalid_drop_unmanaged_fails_on_first_build(self, project):
        # Must fail on the FIRST build (create path), not only when
        # reconciliation first runs on a later build.
        _, output = run_dbt_and_capture(["run", "--models", "invalid_dum"], expect_pass=False)
        assert "drop_unmanaged_indexes" in output

    def test_snapshot_clustered_with_default_columnstore_rejected(self, project):
        # Snapshots build through create_table_as, which honors the
        # as_columnstore default (true) - a clustered rowstore index in the
        # snapshot config must be rejected with the clear cross-config error.
        _, output = run_dbt_and_capture(["snapshot"], expect_pass=False)
        assert "as_columnstore" in output
