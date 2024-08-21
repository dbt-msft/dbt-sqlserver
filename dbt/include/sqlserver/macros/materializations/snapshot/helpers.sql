{% macro sqlserver__create_columns(relation, columns) %}
  {% set column_list %}
    {% for column_entry in columns %}
      {{column_entry.name}} {{column_entry.data_type}}{{ ", " if not loop.last }}
    {% endfor %}
  {% endset %}

  {% set alter_sql %}
    ALTER TABLE {{ relation }}
    ADD {{ column_list }}
  {% endset %}

  {% set results = run_query(alter_sql) %}

{% endmacro %}

{% macro build_snapshot_staging_table(strategy, temp_snapshot_relation, target_relation) %}
    {% set temp_relation = make_temp_relation(target_relation) %}
    {{ adapter.drop_relation(temp_relation) }}

    {% set select = snapshot_staging_table(strategy, temp_snapshot_relation, target_relation) %}

    {% set tmp_tble_vw_relation = temp_relation.incorporate(path={"identifier": temp_relation.identifier ~ '__dbt_tmp_vw'}, type='view')-%}
    -- Dropping temp view relation if it exists
    {{ adapter.drop_relation(tmp_tble_vw_relation) }}

    {% call statement('build_snapshot_staging_relation') %}
        {{ get_create_table_as_sql(True, temp_relation, select) }}
    {% endcall %}

    -- Dropping temp view relation if it exists
    {{ adapter.drop_relation(tmp_tble_vw_relation) }}

    {% do return(temp_relation) %}
{% endmacro %}
