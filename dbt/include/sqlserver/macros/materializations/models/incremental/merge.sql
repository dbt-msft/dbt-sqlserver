  {# global project no longer includes semi-colons in merge statements, so
     default macro are invoked below w/ a semi-colons after it.
     more context:
     https://github.com/dbt-labs/dbt-core/pull/3510
     https://getdbt.slack.com/archives/C50NEBJGG/p1636045535056600
  #}

{% macro sqlserver__get_merge_sql(target, source, unique_key, dest_columns, predicates) %}
  {{ default__get_merge_sql(target, source, unique_key, dest_columns, predicates) }};
{% endmacro %}

{% macro sqlserver__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) %}
  {{ default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) }};
{% endmacro %}

{% macro sqlserver__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
  {{ default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) }};
{% endmacro %}
