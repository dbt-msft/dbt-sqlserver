
{% macro sqlserver__get_catalog(information_schema, schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}

    with tabs as (
      select
        TABLE_CATALOG as table_database,
        TABLE_SCHEMA as table_schema,
        TABLE_NAME as table_name,
        TABLE_TYPE as table_type,
        TABLE_SCHEMA as table_owner,
        null as table_comment
      from INFORMATION_SCHEMA.TABLES
    ),

    cols as (
      select
          table_catalog as table_database,
          table_schema,
          table_name,
          column_name,
          ordinal_position as column_index,
          data_type as column_type,
          null as column_comment
      from information_schema.columns
    )

    select
      tabs.table_database,
      tabs.table_schema,
      tabs.table_name,
      tabs.table_type,
      tabs.table_comment,
      tabs.table_owner,
      cols.column_name,
      cols.column_index,
      cols.column_type,
      cols.column_comment
    from tabs
    join cols on
      tabs.table_database = cols.table_database
      and tabs.table_schema = cols.table_schema
      and tabs.table_name = cols.table_name
    order by column_index

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
