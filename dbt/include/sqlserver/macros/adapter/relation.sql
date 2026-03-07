{% macro sqlserver__truncate_relation(relation) %}
    {% call statement('truncate_relation') -%}
        truncate table {{ relation }}
    {%- endcall %}
{% endmacro %}
