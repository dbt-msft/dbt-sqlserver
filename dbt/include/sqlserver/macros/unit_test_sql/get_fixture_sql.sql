{% macro get_expected_sql(rows, column_name_to_data_types, column_name_to_quoted) %}

{%- if (rows | length) == 0 -%}
    select top 0 * from dbt_internal_unit_test_actual where 1=0
{%- else -%}
{%- for row in rows -%}
{%- set formatted_row = format_row(row, column_name_to_data_types) -%}
select
{%- for column_name, column_value in formatted_row.items() %} {{ column_value }} as {{ column_name_to_quoted[column_name] }}{% if not loop.last -%}, {%- endif %}
{%- endfor %}
{%- if not loop.last %}
union all
{% endif %}
{%- endfor -%}
{%- endif -%}

{% endmacro %}
