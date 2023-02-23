{% macro fabric__current_timestamp() -%}
  SYSDATETIME()
{%- endmacro %}

{% macro fabric__snapshot_string_as_time(timestamp) -%}
    {%- set result = "CONVERT(DATETIME2, '" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}
