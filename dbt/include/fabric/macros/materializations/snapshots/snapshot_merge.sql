{% macro fabric__snapshot_merge_sql(target, source, insert_cols) %}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}

  {%- set target_table = target.include(database=False) -%}
  {%- set source_table = source.include(database=False) -%}

  {% set target_columns_list = [] %}

  {% for column in insert_cols %}
    {% set target_columns_list = target_columns_list.append("DBT_INTERNAL_SOURCE."+column)  %}
  {% endfor %}

  {%- set target_columns = target_columns_list | join(', ') -%}

  UPDATE DBT_INTERNAL_DEST
  SET dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
  FROM {{ target_table }} as DBT_INTERNAL_DEST
  INNER JOIN {{ source_table }} as DBT_INTERNAL_SOURCE
  on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
  WHERE DBT_INTERNAL_DEST.dbt_valid_to is null
  AND DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete');

  INSERT INTO {{ target_table }} ({{ insert_cols_csv }})
  SELECT {{target_columns}} FROM {{ source_table }} as DBT_INTERNAL_SOURCE
  WHERE  DBT_INTERNAL_SOURCE.dbt_change_type = 'insert';
{% endmacro %}
