{% macro fabric__create_table_as(temporary, relation, sql) -%}

   {% set tmp_relation = relation.incorporate(
   path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
   type='view')-%}
   {% do run_query(fabric__drop_relation_script(tmp_relation)) %}
   {% do run_query(fabric__drop_relation_script(relation)) %}
   {{ fabric__create_view_as(tmp_relation, sql) }}
   EXEC('CREATE TABLE {{ relation.include(database=False) }} AS (SELECT * FROM {{ tmp_relation.include(database=False) }});');
   {{ fabric__drop_relation_script(tmp_relation) }}

{% endmacro %}
