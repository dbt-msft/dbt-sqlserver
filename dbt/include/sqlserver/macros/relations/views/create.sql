{% macro sqlserver__create_view_as(relation, sql) -%}

    {{ get_use_database_sql(relation.database) }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
        {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}

    {% set query %}
        create view {{ relation.include(database=False) }} as {{ sql }};
    {% endset %}

    {% set tst %}
    SELECT '1' as col
    {% endset %}
    USE [{{ relation.database }}];
    EXEC('{{- escape_single_quotes(query) -}}')

{% endmacro %}
