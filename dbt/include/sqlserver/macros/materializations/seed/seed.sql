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

{% macro sqlserver__basic_load_csv_rows(model, max_batch_size, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}

    {% set batch_size = calc_batch_size(cols_sql|length, max_batch_size) %}
    {% set bindings = [] %}
    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% set _ = bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ this.render() }} ({{ cols_sql }}) values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    ?
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% set _ = adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% set _ = statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{% macro sqlserver__load_csv_rows(model, agate_table) %}
  {% set max_batch_size = var("max_batch_size", 400) %}
  {{ return(sqlserver__basic_load_csv_rows(model, max_batch_size, agate_table) )}}
{% endmacro %}