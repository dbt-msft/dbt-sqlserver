{% macro sqlserver__create_view_as(relation, sql) -%}
    {#- Only guard against user-configured view materializations; this macro is also
        called for intermediate temp views during table/snapshot materializations,
        where query_options is intended for the *outer* DML and shouldn't trip a guard here. -#}
    {%- if config.get('materialized') == 'view' -%}
        {{ raise_if_query_options_set('view materializations (SQL Server does not accept OPTION on CREATE VIEW)') }}
    {%- endif -%}

    {{ get_use_database_sql(relation.database) }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
        {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}

    {% set query %}
        CREATE OR ALTER VIEW {{ relation.include(database=False) }} AS {{ sql }};
    {% endset %}

    {% set tst %}
    SELECT '1' as col
    {% endset %}
    USE [{{ relation.database }}];
    EXEC('{{- escape_single_quotes(query) -}}')

{% endmacro %}
