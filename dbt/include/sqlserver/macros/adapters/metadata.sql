{% macro apply_label() %}
    {{ log (config.get('query_tag','dbt-sqlserver'))}}
    {%- set query_label = config.get('query_tag','dbt-sqlserver') -%}
    OPTION (LABEL = '{{query_label}}');
{% endmacro %}

{% macro default__information_schema_hints() %}{% endmacro %}
{% macro sqlserver__information_schema_hints() %}with (nolock){% endmacro %}

{% macro information_schema_hints() %}
    {{ return(adapter.dispatch('information_schema_hints')()) }}
{% endmacro %}

{% macro sqlserver__information_schema_name(database) -%}
  information_schema
{%- endmacro %}

{% macro get_use_database_sql(database) %}
    {{ return(adapter.dispatch('get_use_database_sql', 'dbt')(database)) }}
{% endmacro %}

{%- macro sqlserver__get_use_database_sql(database) -%}
  USE [{{database | replace('"', '')}}];
{%- endmacro -%}

{% macro sqlserver__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
    {{ get_use_database_sql(database) }}
    select  name as [schema]
    from sys.schemas {{ information_schema_hints() }} {{ apply_label() }}
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro sqlserver__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    SELECT count(*) as schema_exist FROM sys.schemas WHERE name = '{{ schema }}' {{ apply_label() }}
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro sqlserver__list_relations_without_caching(schema_relation) -%}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    {{ get_use_database_sql(schema_relation.database) }}
    declare @schema_id int = schema_id('{{ schema_relation.schema }}');
    select
      DB_NAME() as [database],
      t.name as [name],
      '{{ schema_relation.schema }}' as [schema],
      'table' as table_type
    from sys.tables as t {{ information_schema_hints() }}
    where t.schema_id = @schema_id
    union all
    select
      DB_NAME() as [database],
      v.name as [name],
      '{{ schema_relation.schema }}' as [schema],
      'view' as table_type
    from sys.views as v {{ information_schema_hints() }}
    where v.schema_id = @schema_id
    {{ apply_label() }}
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro sqlserver__get_relation_without_caching(schema_relation) -%}
  {% call statement('get_relation_without_caching', fetch_result=True) -%}
    {{ get_use_database_sql(schema_relation.database) }}
    declare @schema_id int = schema_id('{{ schema_relation.schema }}');
    select
      DB_NAME() as [database],
      t.name as [name],
      '{{ schema_relation.schema }}' as [schema],
      'table' as table_type
    from sys.tables as t {{ information_schema_hints() }}
    where t.schema_id = @schema_id and t.name = '{{ schema_relation.identifier }}'
    union all
    select
      DB_NAME() as [database],
      v.name as [name],
      '{{ schema_relation.schema }}' as [schema],
      'view' as table_type
    from sys.views as v {{ information_schema_hints() }}
    where v.schema_id = @schema_id and v.name = '{{ schema_relation.identifier }}'
    {{ apply_label() }}
  {% endcall %}
  {{ return(load_result('get_relation_without_caching').table) }}
{% endmacro %}

{% macro get_view_definition_sql(relation) %}
    {{ return(adapter.dispatch('get_view_definition_sql')(relation)) }}
{% endmacro %}

{% macro sqlserver__get_view_definition_sql(relation) -%}
  {%- set object_name = "quotename('" ~ relation.schema ~ "') + '.' + quotename('" ~ relation.identifier ~ "')" -%}
  {{ get_use_database_sql(relation.database) }}
  select object_definition(object_id({{ object_name }}, 'V')) as definition
  where object_id({{ object_name }}, 'V') is not null
{% endmacro %}

{% macro sqlserver__get_relation_last_modified(information_schema, relations) -%}
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
        {{ apply_label() }}
  {%- endcall -%}
  {{ return(load_result('last_modified')) }}

{% endmacro %}
