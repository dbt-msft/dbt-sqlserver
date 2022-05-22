
{% macro sqlserver__get_catalog(information_schemas, schemas) -%}

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
        from INFORMATION_SCHEMA.COLUMNS

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
    join cols on tabs.table_database = cols.table_database and tabs.table_schema = cols.table_schema and tabs.table_name = cols.table_name
    order by column_index

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}

{% macro sqlserver__information_schema_name(database) -%}
  information_schema
{%- endmacro %}

{% macro sqlserver__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
    USE {{ database }};
    select  name as [schema]
    from sys.schemas
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro sqlserver__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    --USE {{ database_name }}
    SELECT count(*) as schema_exist FROM sys.schemas WHERE name = '{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro sqlserver__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as [database],
      table_name as [name],
      table_schema as [schema],
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type

    from [{{ schema_relation.database }}].INFORMATION_SCHEMA.TABLES
    where table_schema like '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
