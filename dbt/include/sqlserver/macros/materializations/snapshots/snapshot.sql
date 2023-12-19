{#
Fabric uses the 'CREATE TABLE XYZ AS SELECT * FROM ABC' syntax to create tables.
SQL Server doesnt support this, so we use the 'SELECT * INTO XYZ FROM ABC' syntax instead.
#}

{% macro sqlserver__create_columns(relation, columns) %}
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
        SELECT * INTO {{tempTableName}} {{columns}} FROM [{{relation.database}}].[{{ relation.schema }}].[{{ relation.identifier }}] {{ information_schema_hints() }}
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
      SELECT * INTO {{ relation }} FROM {{tempTableName}} {{ information_schema_hints() }}
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
