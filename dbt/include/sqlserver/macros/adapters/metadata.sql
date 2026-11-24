{% macro get_query_options(parse_options=False) %}
    {{ log (config.get('query_tag','dbt-sqlserver'))}}
    {#- Escape single quotes so a query_tag like "it's" can't break out of the LABEL literal. -#}
    {%- set query_label = escape_single_quotes(config.get('query_tag','dbt-sqlserver')) -%}
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
                {#- Render the value verbatim: ints become "1", floats become "12.5".
                    MAX_GRANT_PERCENT / MIN_GRANT_PERCENT accept decimals 0.0–100.0; integer-only
                    options will surface a clear SQL Server parse error on invalid decimals. -#}
                {%- do options_list.append(key | upper ~ separator ~ value) -%}
            {%- endif -%}
        {%- endfor -%}

        {#- query_options_raw bypasses the allowlist; users opt in to writing valid SQL Server syntax themselves.
            Shape-check only: a plain string would be iterated character-by-character into garbage. -#}
        {%- if query_options_raw is string or query_options_raw is mapping -%}
            {{ exceptions.raise_compiler_error("query_options_raw must be a list of strings, got: '" ~ query_options_raw ~ "'") }}
        {%- endif -%}
        {%- for raw in query_options_raw -%}
            {%- do options_list.append(raw) -%}
        {%- endfor -%}
    {%- endif -%}

    OPTION ({{ options_list | join(', ') }});
{% endmacro %}

{#- DEPRECATED: backward-compat alias for the pre-1.10 macro.

    Calls to `{{ apply_label() }}` from user macros still resolve and emit
    a LABEL-only OPTION clause — but apply_label() is no longer the
    extension point. Adapter macros now call get_query_options() instead,
    so overriding apply_label() in a project's macros directory will have
    no effect on adapter-emitted SQL.

    To customise the OPTION clause emitted by adapter macros (table,
    incremental, snapshot, unit_test), override get_query_options instead. -#}
{% macro apply_label() %}
    {{ log (config.get('query_tag','dbt-sqlserver'))}}
    {%- set query_label = escape_single_quotes(config.get('query_tag','dbt-sqlserver')) -%}
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
  USE {{ adapter.quote(database | replace('"', '')) }};
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
    {{ get_query_options() }}
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
    {{ get_query_options() }}
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

{% macro get_view_skip_check_sql(relation, compiled_sql) %}
    {{ return(adapter.dispatch('get_view_skip_check_sql')(relation, compiled_sql)) }}
{% endmacro %}

{#- `definition` for the skip decision, and `needs_refresh` when the cached column
    metadata no longer matches what the body resolves to. Refreshing unconditionally
    would advance modify_date on every unchanged view.
    - dm_exec_describe_first_result_set is referenced once: each reference compiles the body.
    - Its error row is kept, so a body it cannot describe is refreshed.
    - COLLATE DATABASE_DEFAULT, or the join fails where catalog and database collations differ.
    - Run through sp_executesql, so the statement text is the same for every view and SQL
      Server caches one plan for all of them.
    - The values are inlined, typed by the parameter list: the function needs nvarchar and
      mssql-python binds str as varchar. -#}
{% macro sqlserver__get_view_skip_check_sql(relation, compiled_sql) -%}
  {%- set object_name = "quotename(@schema) + '.' + quotename(@identifier)" -%}
  {%- set check_sql -%}
  select
      object_definition(object_id({{ object_name }}, 'V')) as definition,
      case when exists (
          select 1
          from (
              select column_ordinal, name, system_type_id, user_type_id,
                     max_length, precision, scale, collation_name
              from sys.dm_exec_describe_first_result_set(
                  @dbt_sqlserver_skip_check_body, null, 0)
              where is_hidden = 0 or error_number is not null
          ) as described
          full outer join (
              select row_number() over (order by column_id) as column_ordinal,
                     name, system_type_id, user_type_id,
                     max_length, precision, scale, collation_name
              from sys.columns {{ information_schema_hints() }}
              where object_id = object_id({{ object_name }}, 'V')
          ) as cached
            on described.column_ordinal = cached.column_ordinal
          where described.column_ordinal is null
             or cached.column_ordinal is null
             or described.name collate database_default
                    <> cached.name collate database_default
             or described.system_type_id <> cached.system_type_id
             or described.user_type_id <> cached.user_type_id
             or described.max_length <> cached.max_length
             or described.precision <> cached.precision
             or described.scale <> cached.scale
             or isnull(described.collation_name, '') collate database_default
                    <> isnull(cached.collation_name, '') collate database_default
      ) then 1 else 0 end as needs_refresh
  where object_id({{ object_name }}, 'V') is not null
  {%- endset -%}
  {{ get_use_database_sql(relation.database) }}
  exec sp_executesql N'{{ escape_single_quotes(check_sql) }}',
      N'@schema sysname, @identifier sysname, @dbt_sqlserver_skip_check_body nvarchar(max)',
      @schema = N'{{ escape_single_quotes(relation.schema) }}',
      @identifier = N'{{ escape_single_quotes(relation.identifier) }}',
      @dbt_sqlserver_skip_check_body = N'{{ escape_single_quotes(compiled_sql) }}';
{% endmacro %}

{% macro get_view_needs_check_sql(relation, compiled_sql) %}
    {{ return(adapter.dispatch('get_view_needs_check_sql')(relation, compiled_sql)) }}
{% endmacro %}

{#- `definition` for the skip decision, and `needs_check` when the metadata check has to run.
    The check compiles the whole stack under the body, and between two runs almost nothing
    under a view changes, so it only runs when an object below the view was modified after
    the view was written. sys.objects.modify_date records that: ALTER TABLE, ALTER VIEW,
    sp_refreshview and ALTER AUTHORIZATION all move it, as do an index drop, an indexed
    view's index drop and a change-tracking toggle. A grant, a deny and an extended-property
    write do not, and none of them changes a column.
    - The closure is walked with sys.sql_expression_dependencies. option (maxrecursion 0)
      because the default stops at 100 levels.
    - needs_check is also 1 when the walk cannot vouch for an input, so the describe keeps
      its answer: an unresolved or cross-database reference, a linked server, a synonym, an
      encrypted module, a caller-dependent name, or text naming a system view of another
      database. Those leave no dependency row, so the body is flattened and searched.
    - Run through sp_executesql, so the statement text is the same for every view and SQL
      Server caches one plan for all of them. -#}
{% macro sqlserver__get_view_needs_check_sql(relation, compiled_sql) -%}
  {%- set object_name = "quotename(@schema) + '.' + quotename(@identifier)" -%}
  {%- set check_sql -%}
  set nocount on;
  declare @root int = object_id({{ object_name }}, 'V');
  declare @needs_check bit = 1;
  declare @nodes table (node_id int primary key);
  declare @flat nvarchar(max);

  if @root is not null
  begin
      ;with closure as (
          select @root as node_id
          union all
          select d.referenced_id
          from closure as c
          join sys.sql_expression_dependencies as d {{ information_schema_hints() }}
            on d.referencing_id = c.node_id
          where d.referenced_id is not null
      )
      insert @nodes (node_id) select distinct node_id from closure option (maxrecursion 0);

      {#- the body without white space, brackets or quotes, so `otherdb . sys . tables`
          reads as `otherdb.sys.tables`. -#}
      set @flat = replace(translate(
          @dbt_sqlserver_body,
          nchar(9) + nchar(10) + nchar(13) + nchar(32) + nchar(160) + N'[]"',
          replicate(nchar(1), 8)), nchar(1), N'');

      {#- Matched with the collation on each comparison: a variable takes the database's
          collation, so a COLLATE on the assignment is lost and a case-sensitive database
          would miss an upper-case OPENQUERY. -#}
      set @needs_check = case when
          isnull(has_perms_by_name(N'sys.sql_expression_dependencies', N'OBJECT', N'SELECT'), 0) <> 1
          or isnull(has_perms_by_name(db_name(), N'DATABASE', N'VIEW DEFINITION'), 0) <> 1
          or exists (
              select 1
              from @nodes as n
              left join sys.objects as o {{ information_schema_hints() }} on o.object_id = n.node_id
              left join sys.sql_modules as m {{ information_schema_hints() }} on m.object_id = n.node_id
              where o.object_id is null
                 or o.type = 'SN'
                 or (o.type in ('V', 'P', 'FN', 'IF', 'TF') and m.definition is null))
          or exists (
              select 1
              from @nodes as n
              join sys.sql_expression_dependencies as d {{ information_schema_hints() }}
                on d.referencing_id = n.node_id
              where d.referenced_class <> 1
                 or d.referenced_server_name is not null
                 or d.is_caller_dependent = 1
                 or d.is_ambiguous = 1
                 or d.referenced_id is null
                 or (d.referenced_database_name is not null
                     and d.referenced_database_name collate database_default
                         <> db_name() collate database_default))
          or charindex(N'openquery', @flat collate Latin1_General_100_CI_AS) > 0
          or charindex(N'openrowset', @flat collate Latin1_General_100_CI_AS) > 0
          or charindex(N'opendatasource', @flat collate Latin1_General_100_CI_AS) > 0
          or charindex(N'.sys.', @flat collate Latin1_General_100_CI_AS) > 0
          or charindex(N'.dbo.sys', @flat collate Latin1_General_100_CI_AS) > 0
          or charindex(N'.information_schema.', @flat collate Latin1_General_100_CI_AS) > 0
          or @flat collate Latin1_General_100_CI_AS like N'%[^.]..[^.]%'
          then 1 else 0 end;

      if @needs_check = 0 and exists (
          select 1
          from @nodes as n
          join sys.objects as o {{ information_schema_hints() }} on o.object_id = n.node_id
          where n.node_id <> @root
            and o.modify_date > (select modify_date from sys.objects {{ information_schema_hints() }}
                                 where object_id = @root))
          set @needs_check = 1;

      select object_definition(@root) as definition, @needs_check as needs_check;
  end
  {%- endset -%}
  {{ get_use_database_sql(relation.database) }}
  exec sp_executesql N'{{ escape_single_quotes(check_sql) }}',
      N'@schema sysname, @identifier sysname, @dbt_sqlserver_body nvarchar(max)',
      @schema = N'{{ escape_single_quotes(relation.schema) }}',
      @identifier = N'{{ escape_single_quotes(relation.identifier) }}',
      @dbt_sqlserver_body = N'{{ escape_single_quotes(compiled_sql) }}';
{% endmacro %}

{% macro sqlserver__get_relation_last_modified(information_schema, relations) -%}
  {%- call statement('last_modified', fetch_result=True) -%}
        select
            o.name as [identifier]
            , s.name as [schema]
            , o.modify_date as last_modified
            , current_timestamp as snapshotted_at
        from sys.objects o {{ information_schema_hints() }}
        inner join sys.schemas s {{ information_schema_hints() }} on o.schema_id = s.schema_id and [type] = 'U'
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
