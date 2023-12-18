{% macro sqlserver__snapshot_merge_sql(target, source, insert_cols) %}
  {{ default__snapshot_merge_sql(target, source, insert_cols) }};
{% endmacro %}
