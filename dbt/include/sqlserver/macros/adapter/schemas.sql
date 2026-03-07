
{% macro sqlserver__drop_schema_named(schema_name) %}
  {% set schema_relation = api.Relation.create(schema=schema_name) %}
  {{ adapter.drop_schema(schema_relation) }}
{% endmacro %}
