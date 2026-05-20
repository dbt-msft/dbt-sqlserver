{% macro get_query_options(parse_options=False) %}
    {{ log (config.get('query_tag','dbt-sqlserver'))}}
    {%- set query_label = config.get('query_tag','dbt-sqlserver') -%}
    {%- set query_options = config.get('query_options', {}) -%}
    {%- set query_options_raw = config.get('query_options_raw', []) -%}

    {%- set options_list = ["LABEL = '" ~ query_label ~ "'"] -%}

    {%- if parse_options -%}
        {%- set valid_options = [
            'HASH GROUP', 'ORDER GROUP',
            'CONCAT UNION', 'HASH UNION', 'MERGE UNION',
            'LOOP JOIN', 'MERGE JOIN', 'HASH JOIN',
            'DISABLE_OPTIMIZED_PLAN_FORCING',
            'EXPAND VIEWS',
            'FAST',
            'FORCE ORDER',
            'FORCE EXTERNALPUSHDOWN', 'DISABLE EXTERNALPUSHDOWN',
            'FORCE SCALEOUTEXECUTION', 'DISABLE SCALEOUTEXECUTION',
            'IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX',
            'KEEP PLAN',
            'KEEPFIXED PLAN',
            'MAX_GRANT_PERCENT',
            'MIN_GRANT_PERCENT',
            'MAXDOP',
            'MAXRECURSION',
            'NO_PERFORMANCE_SPOOL',
            'OPTIMIZE FOR UNKNOWN',
            'PARAMETERIZATION',
            'QUERYTRACEON',
            'RECOMPILE',
            'ROBUST PLAN',
        ] -%}
        {#- SQL Server uses `OPTION (X = N)` for grant-percent hints, not `OPTION (X N)`. -#}
        {%- set equals_syntax_options = ['MAX_GRANT_PERCENT', 'MIN_GRANT_PERCENT'] -%}

        {%- for key, value in query_options.items() -%}
            {%- if key | upper not in valid_options -%}
                {{ exceptions.raise_compiler_error("Invalid query option: '" ~ key ~ "'. Use query_options_raw for non-standard hints. Allowed: " ~ valid_options | join(', ')) }}
            {%- endif -%}

            {%- if value is none -%}
                {%- do options_list.append(key | upper) -%}
            {%- else -%}
                {%- if value is not number -%}
                    {{ exceptions.raise_compiler_error("Query option '" ~ key ~ "' value must be a number, got: '" ~ value ~ "'") }}
                {%- endif -%}
                {%- set separator = ' = ' if key | upper in equals_syntax_options else ' ' -%}
                {%- do options_list.append(key | upper ~ separator ~ value | int) -%}
            {%- endif -%}
        {%- endfor -%}

        {#- query_options_raw bypasses the allowlist; users opt in to writing valid SQL Server syntax themselves. -#}
        {%- for raw in query_options_raw -%}
            {%- do options_list.append(raw) -%}
        {%- endfor -%}
    {%- endif -%}

    OPTION ({{ options_list | join(', ') }});
{% endmacro %}

{#- Backward-compat alias for the pre-1.10 macro. Emits only the LABEL hint
    and ignores query_options / query_options_raw. New adapter code should
    call get_query_options() directly.

    Note: this preserves non-breaking *consumption* of apply_label (user
    macros calling `{{ apply_label() }}` still resolve), but does NOT
    preserve non-breaking *override*: adapter macros no longer call
    apply_label internally, so a project that overrides apply_label in its
    own macros directory will find that override has no effect on adapter
    behaviour. To customise the OPTION clause emitted by adapter macros,
    override get_query_options instead. -#}
{% macro apply_label() %}
    {{ log (config.get('query_tag','dbt-sqlserver'))}}
    {%- set query_label = config.get('query_tag','dbt-sqlserver') -%}
    OPTION (LABEL = '{{query_label}}');
{% endmacro %}

{#- Guard for materializations and incremental strategies that cannot emit OPTION clauses.
    Raises a compiler error if the user has configured query_options/query_options_raw. -#}
{% macro raise_if_query_options_set(context_label) %}
    {%- if config.get('query_options') or config.get('query_options_raw') -%}
        {{ exceptions.raise_compiler_error(
            "query_options/query_options_raw is not supported on " ~ context_label
            ~ ". Remove the config or switch to a supported materialization (table, incremental delete+insert, snapshot, unit_test)."
        ) }}
    {%- endif -%}
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
    from sys.schemas {{ information_schema_hints() }} {{ get_query_options() }}
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro sqlserver__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    SELECT count(*) as schema_exist FROM sys.schemas WHERE name = '{{ schema }}' {{ get_query_options() }}
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}

{% macro sqlserver__list_relations_without_caching(schema_relation) -%}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    {{ get_use_database_sql(schema_relation.database) }}
    with base as (
      select
        DB_NAME() as [database],
        t.name as [name],
        SCHEMA_NAME(t.schema_id) as [schema],
        'table' as table_type
      from sys.tables as t {{ information_schema_hints() }}
      union all
      select
        DB_NAME() as [database],
        v.name as [name],
        SCHEMA_NAME(v.schema_id) as [schema],
        'view' as table_type
      from sys.views as v {{ information_schema_hints() }}
    )
    select * from base
    where [schema] like '{{ schema_relation.schema }}'
    {{ get_query_options() }}
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro sqlserver__get_relation_without_caching(schema_relation) -%}
  {% call statement('get_relation_without_caching', fetch_result=True) -%}
    {{ get_use_database_sql(schema_relation.database) }}
    with base as (
      select
        DB_NAME() as [database],
        t.name as [name],
        SCHEMA_NAME(t.schema_id) as [schema],
        'table' as table_type
      from sys.tables as t {{ information_schema_hints() }}
      union all
      select
        DB_NAME() as [database],
        v.name as [name],
        SCHEMA_NAME(v.schema_id) as [schema],
        'view' as table_type
      from sys.views as v {{ information_schema_hints() }}
    )
    select * from base
    where [schema] like '{{ schema_relation.schema }}'
    and [name] like '{{ schema_relation.identifier }}'
    {{ get_query_options() }}
  {% endcall %}
  {{ return(load_result('get_relation_without_caching').table) }}
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
        {{ get_query_options() }}
  {%- endcall -%}
  {{ return(load_result('last_modified')) }}

{% endmacro %}
