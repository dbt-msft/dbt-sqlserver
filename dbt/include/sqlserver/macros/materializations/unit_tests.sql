{#-
    Copy of dbt-adapters' `materialization unit, default` (as of 1.24.5). The
    changes: the fixture table is dropped straight after its column types are
    read, so an error later cannot leak it, and main passes auto_begin=False,
    which tests/unit/adapters/mssql/test_probe_auto_begin.py requires.
-#}
{%- materialization unit, adapter='sqlserver' -%}

  {% set relations = [] %}
  {% set sql_header = config.get('sql_header') if flags.REQUIRE_SQL_HEADER_IN_TEST_CONFIGS else none %}

  {% set expected_rows = config.get('expected_rows') %}
  {% set expected_sql = config.get('expected_sql') %}
  {% set tested_expected_column_names = expected_rows[0].keys() if (expected_rows | length ) > 0 else get_columns_in_query(sql) %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}
  {% do run_query(get_create_table_as_sql(True, temp_relation, get_empty_subquery_sql(sql))) %}
  {%- set columns_in_relation = adapter.get_columns_in_relation(temp_relation) -%}
  {% do adapter.drop_relation(temp_relation) %}
  {%- set column_name_to_data_types = {} -%}
  {%- set column_name_to_quoted = {} -%}
  {%- for column in columns_in_relation -%}
  {%-   do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
  {%-   do column_name_to_quoted.update({column.name|lower: column.quoted}) -%}
  {%- endfor -%}

  {%- set expected_column_names_quoted = [] -%}
  {%- for column_name in tested_expected_column_names -%}
  {%-   do expected_column_names_quoted.append(column_name_to_quoted[column_name|lower]) -%}
  {%- endfor -%}

  {% if not expected_sql %}
  {%   set expected_sql = get_expected_sql(expected_rows, column_name_to_data_types, column_name_to_quoted) %}
  {% endif %}
  {% set unit_test_sql = get_unit_test_sql(sql, expected_sql, expected_column_names_quoted) %}

  {% call statement('main', fetch_result=True, auto_begin=False) -%}

    {% if sql_header %}{{ sql_header }}{% endif %}
    {{ unit_test_sql }}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}

{% macro sqlserver__get_unit_test_sql(main_sql, expected_fixture_sql, expected_column_names) -%}

  {{ get_use_database_sql(target.database) }}
  {{ create_schema_if_not_exists(target.schema) }}

  {% set test_view_name = "testview_" ~ local_md5(main_sql) ~ "_" ~ (range(1300, 19000) | random) %}
  {% set test_view %}
      {{ adapter.quote(target.schema) }}.{{ adapter.quote(test_view_name) }}
  {% endset %}
  {% set test_sql = main_sql.replace("'", "''")%}
  {#- An error in the model aborts the batch before the drops at the end. Main
      runs outside a transaction, so nothing rolls the views back and the CATCH
      must drop them. The XACT_STATE() check covers a sql_header that opens
      one: XACT_ABORT dooms it, and a drop there would fail and replace the
      model's error. -#}
  BEGIN TRY
  EXEC('create view {{test_view}} as {{ test_sql }};')

  {% set expected_view_name = "expectedview_" ~ local_md5(expected_fixture_sql) ~ "_" ~ (range(1300, 19000) | random) %}
  {% set expected_view %}
      {{ adapter.quote(target.schema) }}.{{ adapter.quote(expected_view_name) }}
  {% endset %}
  {% set expected_sql = expected_fixture_sql.replace("'", "''")%}
  EXEC('create view {{expected_view}} as {{ expected_sql }};')

  -- Build actual result given inputs
  {% set unittest_sql %}
  with dbt_internal_unit_test_actual as (
    select
      {% for expected_column_name in expected_column_names %}{{expected_column_name}}{% if not loop.last -%},{% endif %}{%- endfor -%}, {{ dbt.string_literal("actual") }} as {{ adapter.quote("actual_or_expected") }}
    from
      {{ test_view }}
  ),
  -- Build expected result
  dbt_internal_unit_test_expected as (
    select
      {% for expected_column_name in expected_column_names %}{{expected_column_name}}{% if not loop.last -%}, {% endif %}{%- endfor -%}, {{ dbt.string_literal("expected") }} as {{ adapter.quote("actual_or_expected") }}
    from
      {{ expected_view }}
  )
  -- Union actual and expected results
  select * from dbt_internal_unit_test_actual
  union all
  select * from dbt_internal_unit_test_expected
  {% endset %}

  EXEC('{{- escape_single_quotes(unittest_sql) -}}')
  END TRY
  BEGIN CATCH
    IF XACT_STATE() = 0
    BEGIN
      EXEC('drop view if exists {{test_view}};')
      EXEC('drop view if exists {{expected_view}};')
    END;
    THROW;
  END CATCH

  EXEC('drop view {{test_view}};')
  EXEC('drop view {{expected_view}};')

{%- endmacro %}
