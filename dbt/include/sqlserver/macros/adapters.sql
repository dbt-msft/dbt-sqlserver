{# {% macro sqlserver__insert_into_from(to_relation, from_relation) -%}
  SELECT * INTO {{ to_relation }} FROM {{ from_relation }}
{% endmacro %} #}
