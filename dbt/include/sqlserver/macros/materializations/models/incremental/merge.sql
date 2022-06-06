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

      {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

      {% if unique_key %}
      {% if unique_key is sequence and unique_key is not string %}
      delete from {{ target }}
          where exists (
                SELECT NULL
                FROM
                  {{ source }}
                WHERE
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = {{ target }}.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}
            );
      {% else %}
      delete from {{ target }}
          where (
                {{ unique_key }}) in (
      select ({{ unique_key }})
      from {{ source }}
          );

      {% endif %}
      {% endif %}

      insert into {{ target }} ({{ dest_cols_csv }})
          (
      select {{ dest_cols_csv }}
      from {{ source }}
          )

{% endmacro %}

{% macro sqlserver__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
  {{ default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) }};
{% endmacro %}
