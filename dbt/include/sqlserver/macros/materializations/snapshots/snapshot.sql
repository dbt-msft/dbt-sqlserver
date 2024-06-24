{% macro sqlserver__create_columns(relation, columns) %}
  {# default__ macro uses "add column"
     TSQL preferes just "add"
  #}
  {% for column in columns %}
    {% call statement() %}
      alter table {{ relation }} add "{{ column.name }}" {{ column.data_type }};
    {% endcall %}
  {% endfor %}
{% endmacro %}
