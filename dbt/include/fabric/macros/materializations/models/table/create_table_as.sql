{% macro fabric__create_table_as(temporary, relation, sql) -%}

   {% set tmp_relation = relation.incorporate(
   path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
   type='view')-%}

   {%- set temp_view_sql = sql.replace("'", "''") -%}

   {% do run_query(fabric__drop_relation_script(tmp_relation)) %}
   {% do run_query(fabric__drop_relation_script(relation)) %}

   USE [{{ relation.database }}];
   EXEC('create view {{ tmp_relation.include(database=False) }} as {{ temp_view_sql }}');

   {{ log(relation.include(database=False), info=True) }}

   CREATE TABLE {{ relation.include(database=(not temporary), schema=(not temporary)) }}
   AS (SELECT * FROM {{ tmp_relation.include(database=False) }})

   {{ fabric__drop_relation_script(tmp_relation) }}

{% endmacro %}
