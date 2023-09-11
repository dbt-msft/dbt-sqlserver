{% macro sqlserver__concat(fields) -%}
    {%- if fields|length == 1 -%}
        {{ fields[0] }}
    {%- else -%}
        concat({{ fields|join(', ') }})
    {%- endif -%}
{%- endmacro %}
