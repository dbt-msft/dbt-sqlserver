{% macro fabric__create_table_as(temporary, relation, sql) -%}

   {% set tmp_relation = relation.incorporate(
   path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
   type='view')-%}

   {%- set temp_view_sql = sql.replace("'", "''") -%}

   {% do run_query(fabric__drop_relation_script(tmp_relation)) %}
   {% do run_query(fabric__drop_relation_script(relation)) %}

   {{ use_database_hint() }}
   {{ fabric__create_view_as(tmp_relation, temp_view_sql) }}
   {# EXEC('create view {{ tmp_relation.include(database=False) }} as {{ temp_view_sql }}'); #}

   CREATE TABLE {{ relation }}
   AS (SELECT * FROM {{ tmp_relation }})

   {{ fabric__drop_relation_script(tmp_relation) }}

{% endmacro %}
