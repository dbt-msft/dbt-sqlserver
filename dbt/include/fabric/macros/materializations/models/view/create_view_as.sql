{% macro fabric__create_view_as(relation, sql) -%}
    {{ fabric__create_view_exec(relation, sql) }}
{% endmacro %}

{% macro fabric__create_view_exec(relation, sql) -%}
    {#- TODO: add contracts here when in dbt 1.5 -#}
    {%- set temp_view_sql = sql.replace("'", "''") -%}
    {{ use_database_hint() }}
    EXEC('create view {{ relation.include(database=False) }} as {{ temp_view_sql }};');
{% endmacro %}
