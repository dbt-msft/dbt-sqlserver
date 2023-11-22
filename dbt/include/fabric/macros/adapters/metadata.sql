{% macro information_schema_hints() %}
    {{ return(adapter.dispatch('information_schema_hints')()) }}
{% endmacro %}

{% macro default__information_schema_hints() %}{% endmacro %}
{% macro fabric__information_schema_hints() %}{% endmacro %}

{% macro fabric__get_catalog(information_schemas, schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}

    with
    principals as (
        select
            name as principal_name,
            principal_id as principal_id
        from
            sys.database_principals {{ information_schema_hints() }}
    ),

    schemas as (
        select
            name as schema_name,
            schema_id as schema_id,
            principal_id as principal_id
        from
            sys.schemas {{ information_schema_hints() }}
    ),

    tables as (
        select
            name as table_name,
            schema_id as schema_id,
            principal_id as principal_id,
            'BASE TABLE' as table_type
        from
            sys.tables {{ information_schema_hints() }}
    ),

    tables_with_metadata as (
        select
            table_name,
            schema_name,
            coalesce(tables.principal_id, schemas.principal_id) as owner_principal_id,
            table_type
        from
            tables
        join schemas on tables.schema_id = schemas.schema_id
    ),

    views as (
        select
            name as table_name,
            schema_id as schema_id,
            principal_id as principal_id,
            'VIEW' as table_type
        from
            sys.views {{ information_schema_hints() }}
    ),

    views_with_metadata as (
        select
            table_name,
            schema_name,
            coalesce(views.principal_id, schemas.principal_id) as owner_principal_id,
            table_type
        from
            views
        join schemas on views.schema_id = schemas.schema_id
    ),

    tables_and_views as (
        select
            table_name,
            schema_name,
            principal_name,
            table_type
        from
            tables_with_metadata
        join principals on tables_with_metadata.owner_principal_id = principals.principal_id
        union all
        select
            table_name,
            schema_name,
            principal_name,
            table_type
        from
            views_with_metadata
        join principals on views_with_metadata.owner_principal_id = principals.principal_id
    ),

    cols as (

        select
            table_catalog as table_database,
            table_schema,
            table_name,
            column_name,
            ordinal_position as column_index,
            data_type as column_type
        from INFORMATION_SCHEMA.COLUMNS {{ information_schema_hints() }}

    )

    select
        cols.table_database,
        tv.schema_name as table_schema,
        tv.table_name,
        tv.table_type,
        null as table_comment,
        tv.principal_name as table_owner,
        cols.column_name,
        cols.column_index,
        cols.column_type,
        null as column_comment
    from tables_and_views tv
             join cols on tv.schema_name = cols.table_schema and tv.table_name = cols.table_name
    order by column_index

    {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}

{% macro fabric__information_schema_name(database) -%}
  information_schema
{%- endmacro %}

{% macro fabric__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}

    select  name as [schema]
    from sys.schemas {{ information_schema_hints() }}
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro fabric__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}

    SELECT count(*) as schema_exist FROM sys.schemas WHERE name = '{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro fabric__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as [database],
      table_name as [name],
      table_schema as [schema],
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type

    from [{{ schema_relation.database }}].INFORMATION_SCHEMA.TABLES {{ information_schema_hints() }}
    where table_schema like '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
