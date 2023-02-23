{% macro fabric__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
