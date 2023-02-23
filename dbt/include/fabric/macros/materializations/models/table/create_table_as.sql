{% macro fabric__create_table_as(temporary, relation, sql) -%}

   {{ log("In CREATE_TABLE_AS Macro. " ~ relation, info=True) }}

   {%- set as_columnstore = config.get('as_columnstore', default=false) -%}
   {% set tmp_relation = relation.incorporate(
   path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
   type='view')-%}

   {%- set temp_view_sql = sql.replace("'", "''") -%}

   {% do run_query(fabric__drop_relation_script(tmp_relation)) %}
   {% do run_query(fabric__drop_relation_script(relation)) %}

   USE [{{ relation.database }}];
   EXEC('create view {{ tmp_relation.include(database=False) }} as {{ temp_view_sql }}');

   {{ log(relation.include(database=False), info=True) }}

   CREATE TABLE {{ relation.include(database=False) }}
   AS (SELECT * FROM {{ tmp_relation.include(database=False) }})

   {{ fabric__drop_relation_script(tmp_relation) }}

{% endmacro %}
