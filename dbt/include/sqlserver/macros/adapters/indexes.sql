{% macro sqlserver__strip_dbt_suffix(identifier) -%}
  {%- set ns = namespace(result=identifier) -%}
  {%- for suffix in ['__dbt_tmp_vw', '__dbt_backup', '__dbt_tmp'] -%}
    {%- if ns.result.endswith(suffix) -%}
      {%- set ns.result = ns.result[:(ns.result | length) - (suffix | length)] -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(ns.result) }}
{%- endmacro %}


{% macro sqlserver__index_name(rel, type, columns, unique=False, includes=false) -%}
  {%- set cols = [columns] if columns is string else columns -%}
  {%- set incs = [] if not includes else ([includes] if includes is string else includes) -%}
  {%- set stripped = sqlserver__strip_dbt_suffix(rel.identifier) | replace('[', '') | replace(']', '') -%}
  {%- set sig = type ~ '|' ~ (cols | join(',')) ~ '|' ~ (unique | string) ~ '|' ~ (incs | join(',')) -%}
  {%- set hash = local_md5(sig) -%}
  {%- set prefix = type ~ '_' ~ stripped -%}
  {%- set max_prefix = 83 -%}
  {%- if prefix | length > max_prefix -%}
    {%- set prefix = (prefix | list)[:max_prefix] | join -%}
  {%- endif -%}
  {{ return(prefix ~ '_' ~ hash) }}
{%- endmacro %}


{% macro sqlserver__index_exists(rel, index_name) -%}
  EXISTS (
    SELECT 1
    FROM sys.indexes {{ information_schema_hints() }}
    WHERE name = N'{{ escape_single_quotes(index_name) }}'
    AND object_id = OBJECT_ID(N'{{ escape_single_quotes(rel) }}')
  )
{%- endmacro %}


{% macro sqlserver__create_clustered_columnstore_index(relation) -%}
    {%- set stripped_id = sqlserver__strip_dbt_suffix(relation.identifier) | replace('[', '') | replace(']', '') -%}
    {%- set cci_name = relation.schema | replace('[', '') | replace(']', '') ~ '_' ~ stripped_id ~ '_cci' -%}
    {{ get_use_database_sql(relation.database) }}
    if EXISTS (
        SELECT *
        FROM sys.indexes {{ information_schema_hints() }}
        WHERE name = N'{{ escape_single_quotes(cci_name) }}'
        AND object_id = OBJECT_ID(N'{{ escape_single_quotes(relation) }}')
    )
    DROP INDEX {{ adapter.quote(cci_name) }} ON {{ relation }}
    CREATE CLUSTERED COLUMNSTORE INDEX {{ adapter.quote(cci_name) }}
    ON {{ relation }}
{% endmacro %}

{% macro drop_xml_indexes() -%}
    {{ log("Running drop_xml_indexes() macro...") }}

    declare @drop_xml_indexes nvarchar(max);
    select @drop_xml_indexes = (
    select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ' + QUOTENAME(sys.indexes.[name], '''') + ', ''IndexId'') IS NOT NULL DROP INDEX ' + QUOTENAME(sys.indexes.[name]) + ' ON ' + QUOTENAME(SCHEMA_NAME(sys.tables.[schema_id])) + '.' + QUOTENAME(OBJECT_NAME(sys.tables.[object_id])) + '; '
    from sys.indexes {{ information_schema_hints() }}
    inner join sys.tables {{ information_schema_hints() }}
    on sys.indexes.object_id = sys.tables.object_id
    where sys.indexes.[name] is not null
        and sys.indexes.type_desc = 'XML'
        and SCHEMA_NAME(sys.tables.[schema_id]) = '{{ this.schema }}'
        and sys.tables.[name] = '{{ this.table }}'
    for xml path('')
    ); exec sp_executesql @drop_xml_indexes;
{%- endmacro %}


{% macro drop_spatial_indexes() -%}
    {# Altered from https://stackoverflow.com/q/1344401/10415173 #}
    {# and https://stackoverflow.com/a/33785833/10415173         #}

    {{ log("Running drop_spatial_indexes() macro...") }}

    declare @drop_spatial_indexes nvarchar(max);
    select @drop_spatial_indexes = (
        select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ' + QUOTENAME(sys.indexes.[name], '''') + ', ''IndexId'') IS NOT NULL DROP INDEX ' + QUOTENAME(sys.indexes.[name]) + ' ON ' + QUOTENAME(SCHEMA_NAME(sys.tables.[schema_id])) + '.' + QUOTENAME(OBJECT_NAME(sys.tables.[object_id])) + '; '
        from sys.indexes {{ information_schema_hints() }}
        inner join sys.tables {{ information_schema_hints() }}
        on sys.indexes.object_id = sys.tables.object_id
        where sys.indexes.[name] is not null
        and sys.indexes.type_desc = 'Spatial'
        and SCHEMA_NAME(sys.tables.[schema_id]) = '{{ this.schema }}'
        and sys.tables.[name] = '{{ this.table }}'
        for xml path('')
    ); exec sp_executesql @drop_spatial_indexes;
{%- endmacro %}


{% macro drop_fk_constraints() -%}
    {# Altered from https://stackoverflow.com/q/1344401/10415173 #}

    {{ log("Running drop_fk_constraints() macro...") }}

    {# The schema filter applies to the referenced table (this model). The
       constraint itself is dropped from the referencing (parent) table, which
       may legitimately live in another schema, so it is not filtered. #}

    declare @drop_fk_constraints nvarchar(max);
    select @drop_fk_constraints = (
        select 'IF OBJECT_ID(''' + REPLACE(QUOTENAME(SCHEMA_NAME(sys.foreign_keys.[schema_id])) + '.' + QUOTENAME(sys.foreign_keys.[name]), '''', '''''') + ''', ''F'') IS NOT NULL ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(sys.foreign_keys.[schema_id])) + '.' + QUOTENAME(OBJECT_NAME(sys.foreign_keys.[parent_object_id])) + ' DROP CONSTRAINT ' + QUOTENAME(sys.foreign_keys.[name]) + ';'
        from sys.foreign_keys
        inner join sys.tables on sys.foreign_keys.[referenced_object_id] = sys.tables.[object_id]
        where SCHEMA_NAME(sys.tables.[schema_id]) = '{{ this.schema }}'
        and sys.tables.[name] = '{{ this.table }}'
        for xml path('')
    ); exec sp_executesql @drop_fk_constraints;

{%- endmacro %}


{% macro drop_pk_constraints() -%}
    {# Altered from https://stackoverflow.com/q/1344401/10415173 #}
    {# and https://stackoverflow.com/a/33785833/10415173         #}

    {{ drop_xml_indexes() }}

    {{ drop_spatial_indexes() }}

    {{ drop_fk_constraints() }}

    {{ log("Running drop_pk_constraints() macro...") }}

    declare @drop_pk_constraints nvarchar(max);
    select @drop_pk_constraints = (
        select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ' + QUOTENAME(sys.indexes.[name], '''') + ', ''IndexId'') IS NOT NULL ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(sys.tables.[schema_id])) + '.' + QUOTENAME(sys.tables.[name]) + ' DROP CONSTRAINT ' + QUOTENAME(sys.indexes.[name]) + ';'
        from sys.indexes
        inner join sys.tables on sys.indexes.[object_id] = sys.tables.[object_id]
        where sys.indexes.is_primary_key = 1
        and SCHEMA_NAME(sys.tables.[schema_id]) = '{{ this.schema }}'
        and sys.tables.[name] = '{{ this.table }}'
        for xml path('')
    ); exec sp_executesql @drop_pk_constraints;

{%- endmacro %}


{% macro drop_all_indexes_on_table() -%}
    {# Altered from https://stackoverflow.com/q/1344401/10415173 #}
    {# and https://stackoverflow.com/a/33785833/10415173         #}

    {{ drop_pk_constraints() }}

    {{ log("Dropping remaining indexes...") }}

    declare @drop_remaining_indexes_last nvarchar(max);
    select @drop_remaining_indexes_last = (
        select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ' + QUOTENAME(sys.indexes.[name], '''') + ', ''IndexId'') IS NOT NULL DROP INDEX ' + QUOTENAME(sys.indexes.[name]) + ' ON ' + QUOTENAME(SCHEMA_NAME(sys.tables.[schema_id])) + '.' + QUOTENAME(OBJECT_NAME(sys.tables.[object_id])) + '; '
        from sys.indexes {{ information_schema_hints() }}
        inner join sys.tables {{ information_schema_hints() }}
        on sys.indexes.object_id = sys.tables.object_id
        where sys.indexes.[name] is not null
        and SCHEMA_NAME(sys.tables.[schema_id]) = '{{ this.schema }}'
        and sys.tables.[name] = '{{ this.table }}'
        for xml path('')
    ); exec sp_executesql @drop_remaining_indexes_last;

{%- endmacro %}


{% macro create_clustered_index(columns, unique=False, relation=none) -%}
    {%- set _relation = relation if relation is not none else this -%}
    {%- set cols = [columns] if columns is string else columns -%}
    {%- set idx_name = sqlserver__index_name(_relation, 'cidx', cols, unique=unique) -%}
    {%- set quoted_cols = [] -%}
    {%- for col in cols -%}
        {%- do quoted_cols.append(adapter.quote(col)) -%}
    {%- endfor -%}
    {{ log("Creating clustered index...") }}

    if not exists(select 1
                    from sys.indexes {{ information_schema_hints() }}
                    where name = N'{{ escape_single_quotes(idx_name) }}'
                    and object_id = OBJECT_ID(N'{{ escape_single_quotes(_relation) }}')
    )
    begin

    create
    {% if unique -%}
    unique
    {% endif %}
    clustered index
        {{ adapter.quote(idx_name) }}
        on {{ _relation }} ({{ quoted_cols | join(', ') }})
    end
{%- endmacro %}


{% macro create_nonclustered_index(columns, includes=False, relation=none) %}

    {%- set _relation = relation if relation is not none else this -%}
    {%- set cols = [columns] if columns is string else columns -%}
    {%- set incs = [] if not includes else ([includes] if includes is string else includes) -%}
    {%- set idx_name = sqlserver__index_name(_relation, 'nidx', cols, includes=incs) -%}
    {%- set quoted_cols = [] -%}
    {%- for col in cols -%}
        {%- do quoted_cols.append(adapter.quote(col)) -%}
    {%- endfor -%}
    {%- set quoted_incs = [] -%}
    {%- for inc in incs -%}
        {%- do quoted_incs.append(adapter.quote(inc)) -%}
    {%- endfor -%}
    {{ log("Creating nonclustered index...") }}

    if not exists(select 1
                    from sys.indexes {{ information_schema_hints() }}
                    where name = N'{{ escape_single_quotes(idx_name) }}'
                    and object_id = OBJECT_ID(N'{{ escape_single_quotes(_relation) }}')
    )
    begin
    create nonclustered index
        {{ adapter.quote(idx_name) }}
        on {{ _relation }} ({{ quoted_cols | join(', ') }})
        {% if quoted_incs -%}
            include ({{ quoted_incs | join(', ') }})
        {% endif %}
    end
{% endmacro %}


{% macro drop_fk_indexes_on_table(relation) -%}
  {% call statement('find_references', fetch_result=true) %}
      USE [{{ relation.database }}];
      SELECT  obj.name AS FK_NAME,
      sch.name AS [schema_name],
      tab1.name AS [table],
      col1.name AS [column],
      tab2.name AS [referenced_table],
      col2.name AS [referenced_column]
      FROM sys.foreign_key_columns fkc
      INNER JOIN sys.objects obj
          ON obj.object_id = fkc.constraint_object_id
      INNER JOIN sys.tables tab1
          ON tab1.object_id = fkc.parent_object_id
      INNER JOIN sys.schemas sch
          ON tab1.schema_id = sch.schema_id
      INNER JOIN sys.columns col1
          ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
      INNER JOIN sys.tables tab2
          ON tab2.object_id = fkc.referenced_object_id
      INNER JOIN sys.columns col2
          ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
      WHERE sch.name = '{{ relation.schema }}' and tab2.name = '{{ relation.identifier }}'
  {% endcall %}
      {% set references = load_result('find_references')['data'] %}
      {% for reference in references -%}
        {% call statement('main') -%}
           alter table [{{reference[1]}}].[{{reference[2]}}] drop constraint [{{reference[0]}}]
        {%- endcall %}
      {% endfor %}
{% endmacro %}

{% macro sqlserver__list_nonclustered_rowstore_indexes(relation) -%}
  {% call statement('list_nonclustered_rowstore_indexes', fetch_result=True) -%}

    SELECT i.name AS index_name
    , i.name + '__dbt_backup' as index_new_name
    , COL_NAME(ic.object_id,ic.column_id) AS column_name
    FROM sys.indexes AS i
    INNER JOIN sys.index_columns AS ic
        ON i.object_id = ic.object_id AND i.index_id = ic.index_id and i.type <> 5
    WHERE i.object_id = OBJECT_ID('{{ relation.schema }}.{{ relation.identifier }}')

    UNION ALL

    SELECT  obj.name AS index_name
    , obj.name + '__dbt_backup' as index_new_name
    , col1.name AS column_name
    FROM sys.foreign_key_columns fkc
    INNER JOIN sys.objects obj
        ON obj.object_id = fkc.constraint_object_id
    INNER JOIN sys.tables tab1
        ON tab1.object_id = fkc.parent_object_id
    INNER JOIN sys.schemas sch
        ON tab1.schema_id = sch.schema_id
    INNER JOIN sys.columns col1
        ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
    INNER JOIN sys.tables tab2
        ON tab2.object_id = fkc.referenced_object_id
    INNER JOIN sys.columns col2
        ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
    WHERE sch.name = '{{ relation.schema }}' and tab1.name = '{{ relation.identifier }}'

  {% endcall %}
  {{ return(load_result('list_nonclustered_rowstore_indexes').table) }}
{% endmacro %}


{% macro sqlserver__server_major_version() -%}
  {#- Detected engine major version: 13 = 2016, 14 = 2017, 15 = 2019,
      16 = 2022, 17 = 2025. parsename() on the always-4-part productversion
      works on every supported release, unlike
      SERVERPROPERTY('ProductMajorVersion') which is 2014+ only. Returns none
      outside an executing context (e.g. parse time). -#}
  {%- if not execute -%}{{ return(none) }}{%- endif -%}
  {%- set result = run_query(
      "select cast(parsename(cast(serverproperty('productversion') as varchar(128)), 4) as int) as major_version"
  ) -%}
  {{ return(result.columns[0].values()[0]) }}
{%- endmacro %}


{% macro sqlserver__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set index_name = index_config.render(relation) -%}

  {# Validations are made on the adapter class SQLServerIndexConfig to control resulting sql #}
  {# Names are a deterministic hash of the full definition, so an existing #}
  {# index with this name is already the index we want: skip, don't fail.  #}
  if not exists(select *
                  from sys.indexes {{ information_schema_hints() }}
                  where name = '{{ index_name }}'
                  and object_id = OBJECT_ID('{{ relation }}')
  )
  begin
  {# key columns: bracket-quoted (with ]] escaping) plus per-column direction #}
  {%- set key_columns = [] -%}
  {%- for column in index_config.columns -%}
    {%- do key_columns.append(
        '[' ~ column | replace(']', ']]') ~ ']'
        ~ (' desc' if column in index_config.descending_columns else '')
    ) -%}
  {%- endfor -%}
  {%- set include_columns = [] -%}
  {%- for column in index_config.included_columns -%}
    {%- do include_columns.append('[' ~ column | replace(']', ']]') ~ ']') -%}
  {%- endfor %}
  create
  {% if index_config.unique -%} unique {% endif %}{{ index_config.type }}
  index [{{ index_name }}]
  on {{ relation }}
  ({{ key_columns | join(', ') }})
    {% if include_columns -%}
        include ({{ include_columns | join(', ') }})
    {% endif %}
    {% if index_config.where -%}
        where {{ index_config.where }}
    {% endif %}
    {#- optimize_for_sequential_key, RESUMABLE and RESUMABLE's MAX_DURATION are
        CREATE INDEX options that SQL Server only recognizes on 2019 (major 15)
        and newer. This adapter still supports 2017/2016, so on older engines
        they are dropped (with a warning) and the index is built without them,
        rather than failing with "is not a recognized CREATE INDEX option".
        ONLINE is intentionally kept: it is recognized on 2017 (edition-gated,
        not version-gated). The server version is only queried when one of these
        options is actually requested, so the common path adds no round-trip. -#}
    {%- set v2019_only_build_options = ['resumable', 'max_duration'] -%}
    {%- set _build_options = index_config.build_options or {} -%}
    {%- set _wants_v2019_option = index_config.optimize_for_sequential_key
          or _build_options.get('resumable') or _build_options.get('max_duration') -%}
    {%- set drop_v2019_options = false -%}
    {%- if _wants_v2019_option -%}
        {%- set _major = sqlserver__server_major_version() -%}
        {%- if _major is not none and _major < 15 -%}
            {%- set drop_v2019_options = true -%}
            {%- do log(
                "Index [" ~ index_name ~ "] on " ~ relation ~ ": optimize_for_sequential_key"
                ~ " / resumable require SQL Server 2019 (15.x) or newer; building the index"
                ~ " without them on detected major version " ~ _major ~ ".", info=true
            ) -%}
        {%- endif -%}
    {%- endif -%}
    {%- set with_options = [] -%}
    {%- if index_config.data_compression -%}
        {%- do with_options.append('data_compression = ' ~ index_config.data_compression | upper) -%}
    {%- endif -%}
    {%- if index_config.fillfactor -%}
        {%- do with_options.append('fillfactor = ' ~ index_config.fillfactor) -%}
    {%- endif -%}
    {%- if index_config.pad_index -%}
        {%- do with_options.append('pad_index = on') -%}
    {%- endif -%}
    {%- if index_config.ignore_dup_key -%}
        {%- do with_options.append('ignore_dup_key = on') -%}
    {%- endif -%}
    {%- if index_config.optimize_for_sequential_key and not drop_v2019_options -%}
        {%- do with_options.append('optimize_for_sequential_key = on') -%}
    {%- endif -%}
    {%- if index_config.sort_in_tempdb -%}
        {%- do with_options.append('sort_in_tempdb = on') -%}
    {%- endif -%}
    {%- for option_key, option_value in _build_options.items() -%}
        {%- if drop_v2019_options and option_key in v2019_only_build_options -%}
            {#- skipped: not recognized before SQL Server 2019 -#}
        {%- elif option_value is sameas true -%}
            {%- do with_options.append(option_key ~ ' = on') -%}
        {%- elif option_value is sameas false -%}
            {%- do with_options.append(option_key ~ ' = off') -%}
        {%- elif option_key == 'max_duration' -%}
            {%- do with_options.append('max_duration = ' ~ option_value ~ ' minutes') -%}
        {%- else -%}
            {%- do with_options.append(option_key ~ ' = ' ~ option_value) -%}
        {%- endif -%}
    {%- endfor -%}
    {% if with_options %}
        with ({{ with_options | join(', ') }})
    {% endif %}
  end
{%- endmacro %}


{% macro sqlserver__describe_indexes(relation) %}
  {% call statement('describe_indexes', fetch_result=True) -%}
    select
        i.[name] as [name],
        case when i.[type] = 1 then 'clustered'
             when i.[type] = 2 then 'nonclustered'
             when i.[type] = 5 then 'clustered columnstore'
             when i.[type] = 6 then 'columnstore'
        end as [type],
        i.is_unique as [unique],
        i.is_primary_key as is_primary_key,
        i.is_unique_constraint as is_unique_constraint,
        isnull(key_cols.cols, '') as [columns],
        isnull(incl_cols.cols, '') as included_columns,
        case when i.[type] in (1, 2, 6)
             then isnull(part.data_compression_desc, 'NONE')
        end as data_compression,
        isnull(desc_cols.cols, '') as descending_columns,
        i.filter_definition as [where],
        i.fill_factor as [fillfactor],
        i.ignore_dup_key as [ignore_dup_key]
        /* optimize_for_sequential_key is deliberately not selected: the
           sys.indexes column only exists on SQL Server 2019+ and managed
           comparisons are name-based, so it isn't needed here */
    from sys.indexes i {{ information_schema_hints() }}
    outer apply (
        /* STRING_AGG ... WITHIN GROUP requires SQL Server 2017+, the floor
           of this adapter's CI matrix */
        select string_agg(cast(col.[name] as nvarchar(max)), ', ') within group (order by ic.key_ordinal) as cols
        from sys.index_columns ic {{ information_schema_hints() }}
        inner join sys.columns col {{ information_schema_hints() }}
            on col.object_id = ic.object_id and col.column_id = ic.column_id
        where ic.object_id = i.object_id and ic.index_id = i.index_id
          and ic.is_included_column = 0
    ) key_cols
    outer apply (
        select string_agg(cast(col.[name] as nvarchar(max)), ', ') as cols
        from sys.index_columns ic {{ information_schema_hints() }}
        inner join sys.columns col {{ information_schema_hints() }}
            on col.object_id = ic.object_id and col.column_id = ic.column_id
        where ic.object_id = i.object_id and ic.index_id = i.index_id
          and ic.is_included_column = 1
    ) incl_cols
    outer apply (
        select string_agg(cast(col.[name] as nvarchar(max)), ', ') as cols
        from sys.index_columns ic {{ information_schema_hints() }}
        inner join sys.columns col {{ information_schema_hints() }}
            on col.object_id = ic.object_id and col.column_id = ic.column_id
        where ic.object_id = i.object_id and ic.index_id = i.index_id
          and ic.is_descending_key = 1
    ) desc_cols
    outer apply (
        /* MAX() rather than TOP 1: deterministic if partitions ever carry
           mixed compression (the adapter doesn't manage partitioning today) */
        select max(p.data_compression_desc) as data_compression_desc
        from sys.partitions p {{ information_schema_hints() }}
        where p.object_id = i.object_id and p.index_id = i.index_id
    ) part
    where i.object_id = OBJECT_ID('{{ relation.schema }}.{{ relation.identifier }}')
      and i.index_id > 0
      and i.[type] not in (3, 4, 7)  /* xml, spatial, memory-optimized hash */
  {%- endcall %}
  {{ return(load_result('describe_indexes').table) }}
{% endmacro %}


{% macro sqlserver__get_drop_index_sql(relation, index_name) -%}
    drop index [{{ index_name }}] on {{ relation }}
{%- endmacro %}


{% macro sqlserver__create_indexes(relation) %}
  {#-
    Override of the dbt-adapters default to validate the index set as a whole
    (at most one clustered; clustered rowstore vs as_columnstore conflict)
    before creating anything. as_columnstore is only honored by
    create_table_as, so it is irrelevant for seeds despite defaulting true.
  -#}
  {%- set raw_indexes = config.get('indexes', default=[]) -%}
  {%- set materialized = config.get('materialized') -%}
  {%- set as_columnstore = config.get('as_columnstore', default=true)
      if materialized in ('table', 'incremental', 'snapshot') else false -%}
  {%- do adapter.validate_indexes(
      raw_indexes, as_columnstore, config.get('drop_unmanaged_indexes', default=false)
  ) -%}
  {%- set creates_no_txn = [] -%}
  {%- for _index_dict in raw_indexes %}
    {%- set create_index_sql = get_create_index_sql(relation, _index_dict) -%}
    {% if create_index_sql %}
      {% if adapter.index_needs_own_batch(_index_dict) %}
        {%- do creates_no_txn.append(_index_dict) -%}
      {% else %}
        {% do run_query(create_index_sql) %}
      {% endif %}
    {% endif %}
  {%- endfor %}
  {% do sqlserver__create_indexes_no_txn(relation, creates_no_txn) %}
{% endmacro %}


{% macro sqlserver__create_indexes_no_txn(relation, index_dicts) %}
  {#-
    Build ONLINE / RESUMABLE indexes as standalone (non-transactional)
    statements. SQL Server forbids RESUMABLE in a user transaction, and an
    ONLINE build wrapped in one holds its locks until commit, negating the
    non-blocking intent.

    With dbt_sqlserver_use_dbt_transactions on (default), the
    materialization's whole build - from the first statement through its own
    trailing adapter.commit() - runs inside one continuous ambient
    transaction, so run_query's auto_begin=false is not enough on its own to
    escape it: commit it first (see adapter.commit_if_open), before ANY of
    these run - committing between them would just reopen one and land the
    next create right back inside it - then reopen once after the whole
    batch (adapter.begin_if_closed), which always leaves one open afterward
    even if none was open before, so later code (grants, persist_docs, the
    materialization's own trailing adapter.commit(), which raises if it
    finds nothing open) sees one exactly as it would without this macro.
    When the flag is off, or no earlier statement this run has opened one,
    commit_if_open is a no-op and these still run standalone via run_query's
    auto_begin=false.
  -#}
  {% if index_dicts %}
    {{ adapter.commit_if_open() }}
    {% for index_dict in index_dicts %}
      {% do log("Creating ONLINE/RESUMABLE index outside the transaction on " ~ relation, info=true) %}
      {% do run_query(get_create_index_sql(relation, index_dict)) %}
    {% endfor %}
    {{ adapter.begin_if_closed() }}
  {% endif %}
{% endmacro %}


{% macro sqlserver__reconcile_indexes(relation) %}
  {#-
    Converge an existing relation on its configured index set. Called on the
    paths where the relation persists across runs (incremental non-full-
    refresh, dml table refresh, snapshot updates), where create_indexes alone
    would let config changes drift.
  -#}
  {%- set raw_indexes = config.get('indexes', default=[]) -%}
  {%- set drop_unmanaged = config.get('drop_unmanaged_indexes', default=false) -%}
  {#- all three callers (incremental, dml refresh, snapshot) honor as_columnstore -#}
  {%- do adapter.validate_indexes(
      raw_indexes, config.get('as_columnstore', default=true), drop_unmanaged
  ) -%}
  {%- set existing = sqlserver__describe_indexes(relation) -%}
  {%- set result = adapter.index_changes(existing, raw_indexes, relation, drop_unmanaged) -%}
  {%- for warning in result['warnings'] %}
    {% do log("Index reconcile on " ~ relation ~ ": " ~ warning, info=true) %}
  {%- endfor %}
  {#- Apply all drops and creates in ONE transactional batch: a definition
      change is then atomic, with no window where a replacement index (or the
      uniqueness it enforces) is missing for concurrent readers. The
      session-level SET XACT_ABORT ON applied at connection open (see #718)
      guarantees rollback if any statement fails mid-batch. -#}
  {%- set reconcile_statements = [] -%}
  {%- for index_name in result['drops'] %}
    {% do log("Dropping index " ~ index_name ~ " on " ~ relation, info=true) %}
    {%- do reconcile_statements.append(sqlserver__get_drop_index_sql(relation, index_name)) -%}
  {%- endfor %}
  {%- for index_dict in result['creates'] %}
    {%- do reconcile_statements.append(sqlserver__get_create_index_sql(relation, index_dict)) -%}
  {%- endfor %}
  {% if reconcile_statements %}
    {% do run_query(
        "begin transaction;\n"
        ~ reconcile_statements | join(";\n")
        ~ ";\ncommit transaction;"
    ) %}
  {% endif %}
  {#- ONLINE / RESUMABLE creates cannot run inside the transaction above, nor
      inside the ambient dbt-managed transaction that wraps the whole
      materialization when dbt_sqlserver_use_dbt_transactions is on - see
      sqlserver__create_indexes_no_txn. The drops (and any transactional
      creates) above have already committed by the time these run, so a
      replacement index still builds after its predecessor is gone; these are
      not part of the atomic batch, so there is a brief window where the new
      index is absent for readers. -#}
  {% do sqlserver__create_indexes_no_txn(relation, result['creates_no_txn']) %}
{% endmacro %}
