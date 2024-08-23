{% macro apply_label() %}
    {{ log (config.get('query_tag','dbt-sqlserver'))}}
    {%- set query_label = config.get('query_tag','dbt-sqlserver') -%}
    OPTION (LABEL = '{{query_label}}');
{% endmacro %}
