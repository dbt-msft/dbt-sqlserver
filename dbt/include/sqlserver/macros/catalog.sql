
{% macro sqlserver__get_catalog(information_schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}

    with table_owners as (

		select
			TABLE_CATALOG as table_database,
			TABLE_SCHEMA as table_schema,
			TABLE_NAME as table_name,
			TABLE_SCHEMA as table_owner
		from INFORMATION_SCHEMA.TABLES

    ),

    tabs as (

		select
			TABLE_CATALOG as table_database,
			TABLE_SCHEMA as table_schema,
			TABLE_NAME as table_name,
			TABLE_SCHEMA as table_owner
		from INFORMATION_SCHEMA.TABLES

    ),

    cols as (

        select
            table_catalog as table_database,
            table_schema,
            table_name,
            null as table_comment,
            column_name,
            ordinal_position as column_index,
            data_type as column_type,
            null as column_comment
        from information_schema.columns

    )

    select *
    from tabs
    join cols on tabs.table_database = cols.table_database and tabs.table_schema = cols.table_schema and tabs.table_name = cols.table_name
    join table_owners on tabs.table_database = table_owners.table_database and tabs.table_schema = table_owners.table_schema and tabs.table_name = table_owners.table_name
    order by column_index

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
