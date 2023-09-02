{% macro fabric__table_columns_and_constraints(relation) %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_column_constraints = adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    {%- set raw_model_constraints = adapter.render_raw_model_constraints(raw_constraints=model['constraints']) -%}
    (
      {% for c in raw_column_constraints -%}
        {{ c }}{{ "," if not loop.last }}
      {% endfor %}
    )
    {% for c in raw_model_constraints -%}
      {% set alter_table_script %}
        alter table {{ relation.include(database=False) }} {{c}};
      {%endset%}
      EXEC('{{alter_table_script}};')
    {% endfor -%}
{% endmacro %}
