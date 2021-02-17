{% macro sqlserver__information_schema_name(database) -%}
  information_schema
{%- endmacro %}


{% macro sqlserver__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        select TOP 0 * from (
            {{ select_sql }}
        ) as __dbt_sbq
        where 0 = 1
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
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

    from information_schema.tables
    where table_schema like '{{ schema_relation.schema }}'
      and table_catalog like '{{ schema_relation.database }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro sqlserver__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
    select  name as [schema]
    from sys.schemas
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro sqlserver__create_schema(relation) -%}
  {% call statement('create_schema') -%}
    USE [{{ relation.database }}]
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ relation.without_identifier().schema }}')
    BEGIN
    EXEC('CREATE SCHEMA {{ relation.without_identifier().schema }}')
    END
  {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_schema(relation) -%}
  {%- set tables_in_schema_query %}
      SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '{{ relation.schema }}'
  {% endset %}
  {% set tables_to_drop = run_query(tables_in_schema_query).columns[0].values() %}
  {% for table in tables_to_drop %}
    {%- set schema_relation = adapter.get_relation(database=relation.database,
                                               schema=relation.schema,
                                               identifier=table) -%}
    {% do drop_relation(schema_relation) %}
  {%- endfor %}

  {% call statement('drop_schema') -%}
      IF EXISTS (SELECT * FROM sys.schemas WHERE name = '{{ relation.schema }}')
      BEGIN
      EXEC('DROP SCHEMA {{ relation.schema }}')
      END  {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) %}
    USE [{{ relation.database }}]
    DROP VIEW IF EXISTS {{ relation.include(database=False) }}
    DROP TABLE IF EXISTS {{ relation.include(database=False) }}
  {%- endcall %}
{% endmacro %}

{% macro sqlserver__drop_relation_script(relation) -%}
  DROP VIEW IF EXISTS {{ relation.include(database=False) }}
  DROP TABLE IF EXISTS {{ relation.include(database=False) }}
{% endmacro %}

{% macro sqlserver__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    --USE {{ database_name }}
    SELECT count(*) as schema_exist FROM sys.schemas WHERE name = '{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro sqlserver__create_view_exec(relation, sql) -%}
   {%- set temp_view_sql = sql.replace("'", "''") -%}

   EXEC('create view {{ relation.schema }}.{{ relation.identifier }} as
    {{ temp_view_sql }}
    ');
{% endmacro %}

{% macro sqlserver__create_view_as(relation, sql) -%}
  USE [{{ relation.database }}]
  {{ sqlserver__create_view_exec(relation, sql) }}
{% endmacro %}

{% macro sqlserver__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    {% if from_relation.database != to_relation.database %}
      {{ exceptions.raise_compiler_error("Can't do cross-database rename {} -> {}".format(from_relation, to_relation)) }}
    {% endif %}
    USE [{{ from_relation.database }}]
    {{ sqlserver__drop_relation_script(to_relation) }}
    EXEC sp_rename '{{ from_relation.schema }}.{{ from_relation.identifier }}', '{{ to_relation.identifier }}'
    IF EXISTS(
    SELECT *
    FROM sys.indexes
    WHERE name='{{ from_relation.schema }}_{{ from_relation.identifier }}_cci' and object_id = OBJECT_ID('{{ from_relation.schema }}.{{ to_relation.identifier }}'))
    EXEC sp_rename N'{{ from_relation.schema }}.{{ to_relation.identifier }}.{{ from_relation.schema }}_{{ from_relation.identifier }}_cci', N'{{ from_relation.schema }}_{{ to_relation.identifier }}_cci', N'INDEX'
  {%- endcall %}
{% endmacro %}

{% macro sqlserver__create_clustered_columnstore_index(relation) -%}
  {%- set cci_name = relation.schema ~ '_' ~ relation.identifier ~ '_cci' -%}
  {%- set relation_name = relation.schema ~ '_' ~ relation.identifier -%}
  {%- set full_relation = relation.schema ~ '.' ~ relation.identifier -%}
  if EXISTS (
        SELECT * FROM
        sys.indexes WHERE name = '{{cci_name}}'
        AND object_id=object_id('{{relation_name}}')
    )
  DROP index {{full_relation}}.{{cci_name}}
  CREATE CLUSTERED COLUMNSTORE INDEX {{cci_name}}
    ON {{full_relation}}
{% endmacro %}

{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
   {%- set as_columnstore = config.get('as_columnstore', default=true) -%}
   {% set tmp_relation = relation.incorporate(
   path={"identifier": relation.identifier.replace("#", "") ~ '_temp_view'},
   type='view')-%}

   USE [{{ relation.database }}]
   
   {{ sqlserver__drop_relation_script(tmp_relation) }}

   {{ sqlserver__drop_relation_script(relation) }}
    
   {{ sqlserver__create_view_exec(tmp_relation, sql) }}

   SELECT * INTO {{ relation.schema }}.{{ relation.identifier }} FROM
    {{ tmp_relation.schema }}.{{ tmp_relation.identifier }}

   {{ sqlserver__drop_relation_script(tmp_relation) }}
    
   {% if not temporary and as_columnstore -%}
   {{ sqlserver__create_clustered_columnstore_index(relation) }}
   {% endif %}

{% endmacro %}_

{% macro sqlserver__insert_into_from(to_relation, from_relation) -%}
  {%- set full_to_relation = to_relation.schema ~ '.' ~ to_relation.identifier -%}
  {%- set full_from_relation = from_relation.schema ~ '.' ~ from_relation.identifier -%}

  SELECT * INTO {{full_to_relation}} FROM {{full_from_relation}}

{% endmacro %}

{% macro sqlserver__current_timestamp() -%}
  getdate()
{%- endmacro %}

{% macro sqlserver__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      SELECT
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      FROM
          (select
              ordinal_position,
              column_name,
              data_type,
              character_maximum_length,
              numeric_precision,
              numeric_scale
          from INFORMATION_SCHEMA.COLUMNS
          where table_name = '{{ relation.identifier }}'
            and table_schema = '{{ relation.schema }}'
          UNION ALL
          select
              ordinal_position,
              column_name collate database_default,
              data_type collate database_default,
              character_maximum_length,
              numeric_precision,
              numeric_scale
          from tempdb.INFORMATION_SCHEMA.COLUMNS
          where table_name like '{{ relation.identifier }}%') cols
      order by ordinal_position


  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro sqlserver__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = '#' ~  base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier}) -%}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro sqlserver__snapshot_string_as_time(timestamp) -%}
    {%- set result = "CONVERT(DATETIME2, '" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}
