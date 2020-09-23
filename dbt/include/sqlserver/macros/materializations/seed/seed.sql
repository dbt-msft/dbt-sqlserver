{% macro sqlserver__basic_load_csv_rows(model, batch_size, agate_table) %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% do bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ this.render() }} ({{ cols_sql }})
            {% for row in chunk -%}
                {{'SELECT'+' '}}
                {%- for column in agate_table.column_names -%}
                    '{{row[column]}}'
                    {%- if not loop.last%}, {%- endif -%}
                {%- endfor -%}
                {%- if not loop.last-%} {{' '+'UNION ALL'+'\n'}} {%- endif -%}
            {%- endfor -%}
        {% endset %}

        {% do adapter.add_query(sql, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{% macro sqlserver__load_csv_rows(model, agate_table) %}
  {{ return(sqlserver__basic_load_csv_rows(model, 200, agate_table) )}}
{% endmacro %}