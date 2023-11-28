{% macro information_schema_hints() %}
    {{ return(adapter.dispatch('information_schema_hints')()) }}
{% endmacro %}

{% macro default__information_schema_hints() %}{% endmacro %}
{% macro fabric__information_schema_hints() %}{% endmacro %}

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

{% macro fabric__list_relations_without_caching(schema_relation) -%}
{{ log("schema_relation in fabric__list_relations_without_caching is " ~ schema_relation.schema, info=True) }}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      table_catalog as [database],
      table_name as [name],
      table_schema as [schema],
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type

    from INFORMATION_SCHEMA.TABLES {{ information_schema_hints() }}
    where table_schema like '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro fabric__get_relation_last_modified(information_schema, relations) -%}
{{ log("information_schema - "~ information_schema, info=True) }}
  {%- call statement('last_modified', fetch_result=True) -%}
        select
            o.name as [identifier]
            , s.name as [schema]
            , o.modify_date as last_modified
            , current_timestamp as snapshotted_at
        from sys.objects o
        inner join sys.schemas s on o.schema_id = s.schema_id and [type] = 'U'
        where (
            {%- for relation in relations -%}
            (upper(s.name) = upper('{{ relation.schema }}') and
                upper(o.name) = upper('{{ relation.identifier }}')){%- if not loop.last %} or {% endif -%}
            {%- endfor -%}
        )
  {%- endcall -%}
  {{ return(load_result('last_modified')) }}

{% endmacro %}
