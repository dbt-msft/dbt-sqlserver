{% macro sqlserver__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as [database],
      table_name as [name],
      table_schema as [schema],
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type
    from {{ information_schema }}.tables
    where table_schema like '{{ schema }}'
      and table_catalog like '{{ information_schema.database.lower() }}'
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

{% macro sqlserver__create_schema(database_name, schema_name) -%}
  {% call statement('create_schema') -%}
    USE {{ database_name }}
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = {{ schema_name | replace('"', "'") }})
    BEGIN
    EXEC('CREATE SCHEMA {{ schema_name | replace('"', "") }}')
    END
  {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_schema(database_name, schema_name) -%}
  {% call statement('drop_schema') -%}
    drop schema if exists {{database_name}}.{{schema_name}}
  {% endcall %}
{% endmacro %}

{% macro sqlserver__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation.schema }}.{{ relation.identifier }}
  {%- endcall %}
{% endmacro %}

{% macro sqlserver__check_schema_exists(database, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    --USE {{ database_name }}
    SELECT count(*) as schema_exist FROM sys.schemas WHERE name = '{{ schema }}'
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro sqlserver__create_view_as(relation, sql) -%}
  create view {{ relation.schema }}.{{ relation.identifier }} as 
    {{ sql }}
{% endmacro %}

{% macro sqlserver__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    EXEC sp_rename '{{ from_relation.schema }}.{{ from_relation.identifier }}', '{{ to_relation.identifier }}'
  {%- endcall %}
{% endmacro %}

{% macro sqlserver__create_table_as(temporary, relation, sql) -%}
   {%- set as_columnstore = config.get('as_columnstore', default=true) -%}
    SELECT * INTO {{ relation.schema }}.{% if temporary: -%}#{%- endif %}{{ relation.identifier }} FROM (
    {{ sql }}
    ) as a
   {% if not temporary and as_columnstore -%}
    DROP INDEX IF EXISTS {{ relation.schema }}.{{ relation.identifier }}.{{ relation.schema }}_{{ relation.identifier }}_cci
    CREATE CLUSTERED COLUMNSTORE INDEX {{ relation.schema }}_{{ relation.identifier }}_cci
    ON {{ relation.schema }}.{{ relation.identifier }}
   {% endif %}
{% endmacro %}_

{% macro sqlserver__current_timestamp() -%}
  getdate()
{%- endmacro %}

{% macro sqlserver__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale

      from INFORMATION_SCHEMA.COLUMNS
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro sqlserver__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier},
                                table_name=tmp_identifier) -%}

    {% do return(tmp_relation) %}
{% endmacro %}