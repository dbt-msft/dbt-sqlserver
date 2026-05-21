{% macro sqlserver__create_schema(relation) -%}
  {% call statement('create_schema') -%}
    USE [{{ relation.database }}];
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ relation.schema }}')
    BEGIN
    EXEC('CREATE SCHEMA [{{ relation.schema }}]')
    END
  {% endcall %}
{% endmacro %}

{% macro sqlserver__create_schema_with_authorization(relation, schema_authorization) -%}
  {% call statement('create_schema') -%}
    {{ get_use_database_sql(relation.database) }}
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ relation.schema }}')
    BEGIN
    EXEC('CREATE SCHEMA [{{ relation.schema }}] AUTHORIZATION [{{ schema_authorization }}]')
    END
  {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_schema(relation) -%}
  {%- set relations_in_schema = list_relations_without_caching(relation) %}

  {% for row in relations_in_schema %}
    {%- set schema_relation = api.Relation.create(database=relation.database,
                                               schema=relation.schema,
                                               identifier=row[1],
                                               type=row[3]
                                               ) -%}
    {% do adapter.drop_relation(schema_relation) %}
  {%- endfor %}

  {% call statement('drop_schema') -%}
    {{ get_use_database_sql(relation.database) }}
    EXEC('DROP SCHEMA IF EXISTS {{ relation.schema }}')
  {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_schema_named(schema_name) %}
  {% set schema_relation = api.Relation.create(schema=schema_name) %}
  {{ adapter.drop_schema(schema_relation) }}
{% endmacro %}

{#
    Generates a schema name for a model.

    By default (legacy adapter behaviour), the schema name is:
      - `target.schema`              when no custom schema is set
      - `custom_schema_name` (trim)  when a custom schema is set

    When the `dbt_sqlserver_use_default_schema_concat` variable is set to true,
    this delegates to dbt-core's `default__generate_schema_name`, which concatenates
    `target.schema` + `_` + `custom_schema_name`.

    Set the flag in dbt_project.yml:
      vars:
        dbt_sqlserver_use_default_schema_concat: true
#}
{% macro sqlserver__generate_schema_name(custom_schema_name, node) -%}
    {%- if var('dbt_sqlserver_use_default_schema_concat', false) -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- else -%}
        {%- set default_schema = target.schema -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
