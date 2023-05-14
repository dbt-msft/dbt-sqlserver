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
    {{- temp_view_sql -}}
    ');

    -- drop current version of the table
   {{- sqlserver__drop_relation_script(relation) -}}

   -- select into the table and create it that way
   {#- see https://learn.microsoft.com/en-us/sql/t-sql/queries/select-into-clause-transact-sql?view=sql-server-ver16#b-inserting-rows-using-minimal-logging #}
   ALTER DATABASE [{{ relation.database }}] SET RECOVERY BULK_LOGGED

   SELECT *
   INTO {% if temporary %}#{% endif %}{{ relation.include(database=(not temporary), schema=(not temporary)) }}
   FROM {{ tmp_relation }}

   {#- see https://learn.microsoft.com/en-us/sql/t-sql/queries/select-into-clause-transact-sql?view=sql-server-ver16#b-inserting-rows-using-minimal-logging #}
   ALTER DATABASE [{{ relation.database }}] SET RECOVERY FULL

   -- drop temp view
   {{ sqlserver__drop_relation_script(tmp_relation) }}

   {%- if not temporary and as_columnstore -%}
        -- add columnstore index
        {{ sqlserver__create_clustered_columnstore_index(relation) }}
   {%- endif -%}

{% endmacro %}
