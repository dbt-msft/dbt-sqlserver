{% macro calc_batch_size(num_columns,max_batch_size) %}
    {#
        SQL Server allows for a max of 2100 parameters in a single statement.
        Check if the max_batch_size fits with the number of columns, otherwise
        reduce the batch size so it fits.
    #}
    {% if num_columns * max_batch_size < 2100 %}
    {% set batch_size = max_batch_size %}
    {% else %}
    {% set batch_size = (2100 / num_columns)|int %}
    {% endif %}

    {{ return(batch_size) }}
{%  endmacro %}

{% macro sqlserver__get_binding_char() %}
  {{ return('?') }}
{% endmacro %}


{% macro sqlserver__load_csv_rows(model, agate_table) %}
  {% set max_batch_size = var("max_batch_size", 400) %}

  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}

  {% set batch_size = calc_batch_size(cols_sql|length, max_batch_size) %}
  {{ return(basic_load_csv_rows(model, batch_size, agate_table) )}}
{% endmacro %}