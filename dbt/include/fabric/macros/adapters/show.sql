{% macro fabric__get_limit_subquery_sql(sql, limit) %}

    {% if sql.strip().lower().startswith('with') %}
        {{ sql }} order by (select null)
    offset 0 rows fetch first {{ limit }} rows only
    {% else -%}
        select *
        from (
            {{ sql }}
        ) as model_limit_subq order by (select null)
    offset 0 rows fetch first {{ limit }} rows only
    {%- endif -%}

{% endmacro %}
