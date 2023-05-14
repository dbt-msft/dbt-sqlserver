{% macro sqlserver__create_view_as(relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {{ sql_header if sql_header is not none }}
    USE [{{ relation.database }}];
    {{ sqlserver__create_view_exec(relation, sql) }}
{% endmacro %}

{% macro sqlserver__create_view_exec(relation, sql) -%}
    {#- TODO: add contracts here when in dbt 1.5 -#}
    {%- set temp_view_sql = sql.replace("'", "''") -%}
    execute('create view {{ relation.include(database=False) }} as
        {{ temp_view_sql }}
    ');
{% endmacro %}
