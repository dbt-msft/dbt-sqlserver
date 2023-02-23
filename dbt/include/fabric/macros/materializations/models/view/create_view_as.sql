{% macro fabric__create_view_as(relation, sql) -%}
    USE [{{ relation.database }}];
    {{ fabric__create_view_exec(relation, sql) }}
{% endmacro %}

{% macro fabric__create_view_exec(relation, sql) -%}
    {#- TODO: add contracts here when in dbt 1.5 -#}
    {%- set temp_view_sql = sql.replace("'", "''") -%}
    execute('create view {{ relation.include(database=False) }} as
        {{ temp_view_sql }}
    ');
{% endmacro %}
