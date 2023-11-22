{% macro fabric__post_snapshot(staging_relation) %}
  -- Clean up the snapshot temp table
  {% do drop_relation(staging_relation) %}
{% endmacro %}

{% macro fabric__create_columns(relation, columns) %}
  {# default__ macro uses "add column"
     TSQL preferes just "add"
  #}

  {% set columns %}
    {% for column in columns %}
      , CAST(NULL AS {{column.data_type}}) AS {{column_name}}
    {% endfor %}
  {% endset %}

  {% set tempTableName %}
    [{{relation.database}}].[{{ relation.schema }}].[{{ relation.identifier }}_{{ range(1300, 19000) | random }}]
  {% endset %}

  {% set tempTable %}
      CREATE TABLE {{tempTableName}}
      AS SELECT * {{columns}} FROM [{{relation.database}}].[{{ relation.schema }}].[{{ relation.identifier }}] {{ information_schema_hints() }}
  {% endset %}

  {% call statement('create_temp_table') -%}
      {{ tempTable }}
  {%- endcall %}

  {% set dropTable %}
      DROP TABLE [{{relation.database}}].[{{ relation.schema }}].[{{ relation.identifier }}]
  {% endset %}

  {% call statement('drop_table') -%}
      {{ dropTable }}
  {%- endcall %}

  {% set createTable %}
      CREATE TABLE {{ relation }}
      AS SELECT * FROM {{tempTableName}} {{ information_schema_hints() }}
  {% endset %}

  {% call statement('create_Table') -%}
      {{ createTable }}
  {%- endcall %}

  {% set dropTempTable %}
      DROP TABLE {{tempTableName}}
  {% endset %}

  {% call statement('drop_temp_table') -%}
      {{ dropTempTable }}
  {%- endcall %}
{% endmacro %}

{% macro fabric__get_true_sql() %}
  {{ return('1=1') }}
{% endmacro %}
