{% macro fabric__create_clustered_columnstore_index(relation) -%}
  {# {% exceptions.raise_compiler_error('Indexes are not supported') %} #}
{% endmacro %}

{% macro drop_xml_indexes() -%}
  {# {% exceptions.raise_compiler_error('Indexes are not supported') %} #}
{% endmacro %}

{% macro drop_spatial_indexes() -%}
  {# {% exceptions.raise_compiler_error('Indexes are not supported') %} #}
{% endmacro %}

{% macro drop_fk_constraints() -%}
  {# {% exceptions.raise_compiler_error('Indexes are not supported') %} #}
{% endmacro %}

{% macro drop_pk_constraints() -%}
  {# {% exceptions.raise_compiler_error('Indexes are not supported') %} #}
{% endmacro %}

{% macro drop_all_indexes_on_table() -%}
  {# {% exceptions.raise_compiler_error('Indexes are not supported') %} #}
{% endmacro %}

{% macro create_clustered_index(columns, unique=False) -%}
  {# {% exceptions.raise_compiler_error('Indexes are not supported') %} #}
{% endmacro %}

{% macro create_nonclustered_index(columns, includes=False) %}
  {# {% exceptions.raise_compiler_error('Indexes are not supported') %} #}
{% endmacro %}
