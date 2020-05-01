{% macro sqlserver__test_unique(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

select count(*) as validation_errors
from (

    select
        {{ column_name }} as validation_errors

    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(*) > 1

) validation_errors

{% endmacro %}


{% macro sqlserver__test_not_null(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

select count(*) as validation_errors
from {{ model }}
where {{ column_name }} is null

{% endmacro %}


{% macro sqlserver__test_accepted_values(model, values) %}

{% set column_name = kwargs.get('column_name', kwargs.get('field')) %}

with all_values as (

    select distinct
        {{ column_name }} as value_field

    from {{ model }}

),

validation_errors as (

    select
        value_field

    from all_values
    where value_field not in (
        {% for value in values -%}

            '{{ value }}' {% if not loop.last -%} , {%- endif %}

        {%- endfor %}
    )
)

select count(*) as validation_errors
from validation_errors

{% endmacro %}


{% macro sqlserver__test_relationships(model, to, field) %}

{% set column_name = kwargs.get('column_name', kwargs.get('from')) %}


select count(*) as validation_errors
from (
    select {{ column_name }} as id from {{ model }}
) as child
left join (
    select {{ field }} as id from {{ to }}
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null

{% endmacro %}