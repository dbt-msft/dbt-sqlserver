{% macro sqlserver__current_timestamp() -%}
  SYSDATETIME()
{%- endmacro %}
