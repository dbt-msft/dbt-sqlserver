{% macro sqlserver__concat(fields) -%}
    concat({{ fields|join(', ') }}, '')
{%- endmacro %}
