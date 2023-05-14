{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
   {#- TODO: add contracts here when in dbt 1.5 -#}
   {%- set sql_header = config.get('sql_header', none) -%}
   {%- set as_columnstore = config.get('as_columnstore', default=true) -%}
   {%- set temp_view_sql = sql.replace("'", "''") -%}
   {%- set tmp_relation = relation.incorporate(
        path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
        type='view') -%}

   {{- sql_header if sql_header is not none -}}

    -- drop previous temp view
   {{- sqlserver__drop_relation_script(tmp_relation) }}

    -- create temp view
   USE [{{ relation.database }}];
   EXEC('create view {{ tmp_relation.include(database=False) }} as
    {{ temp_view_sql }}
    ');

   -- select into the table and create it that way
   {# TempDB schema is ignored, always goes to dbo #}
   SELECT *
   INTO {{ relation.include(database=False, schema=(not temporary))  }}
   FROM {{ tmp_relation }}

   -- drop temp view
   {{ sqlserver__drop_relation_script(tmp_relation) }}

   {%- if not temporary and as_columnstore -%}
        -- add columnstore index
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
   {%- endif -%}

{% endmacro %}
