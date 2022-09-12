{% macro synapse__datediff(first_date, second_date, datepart) %}
    datediff(
        {{ datepart }},
        cast({{first_date}} as datetime),
        cast({{second_date}} as datetime)
        )
{% endmacro %}
