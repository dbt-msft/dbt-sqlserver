{% macro sqlserver__create_clustered_columnstore_index(relation) -%}
    {%- set cci_name = (relation.schema ~ '_' ~ relation.identifier ~ '_cci') | replace(".", "") | replace(" ", "") -%}
    {%- set relation_name = relation.include(database=False) -%}
    {%- set full_relation = '"' ~ relation.schema ~ '"."' ~ relation.identifier ~ '"' -%}
    use [{{ relation.database }}];
    if EXISTS (
        SELECT *
        FROM sys.indexes {{ information_schema_hints() }}
        WHERE name = '{{cci_name}}'
        AND object_id=object_id('{{relation_name}}')
    )
    DROP index {{full_relation}}.{{cci_name}}
    CREATE CLUSTERED COLUMNSTORE INDEX {{cci_name}}
    ON {{full_relation}}
{% endmacro %}

{% macro drop_xml_indexes() -%}
    {{ log("Running drop_xml_indexes() macro...") }}

    declare @drop_xml_indexes nvarchar(max);
    select @drop_xml_indexes = (
    select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL DROP INDEX [' + sys.indexes.[name] + '] ON ' + '[' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + OBJECT_NAME(sys.tables.[object_id]) + ']; '
    from sys.indexes {{ information_schema_hints() }}
    inner join sys.tables {{ information_schema_hints() }}
    on sys.indexes.object_id = sys.tables.object_id
    where sys.indexes.[name] is not null
        and sys.indexes.type_desc = 'XML'
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
        select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL DROP INDEX [' + sys.indexes.[name] + '] ON ' + '[' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + OBJECT_NAME(sys.tables.[object_id]) + ']; '
        from sys.indexes {{ information_schema_hints() }}
        inner join sys.tables {{ information_schema_hints() }}
        on sys.indexes.object_id = sys.tables.object_id
        where sys.indexes.[name] is not null
        and sys.indexes.type_desc = 'Spatial'
        and sys.tables.[name] = '{{ this.table }}'
        for xml path('')
    ); exec sp_executesql @drop_spatial_indexes;
{%- endmacro %}


{% macro drop_fk_constraints() -%}
    {# Altered from https://stackoverflow.com/q/1344401/10415173 #}

    {{ log("Running drop_fk_constraints() macro...") }}

    declare @drop_fk_constraints nvarchar(max);
    select @drop_fk_constraints = (
        select 'IF OBJECT_ID(''' + SCHEMA_NAME(CONVERT(VARCHAR(MAX), sys.foreign_keys.[schema_id])) + '.' + sys.foreign_keys.[name] + ''', ''F'') IS NOT NULL ALTER TABLE [' + SCHEMA_NAME(sys.foreign_keys.[schema_id]) + '].[' + OBJECT_NAME(sys.foreign_keys.[parent_object_id]) + '] DROP CONSTRAINT [' + sys.foreign_keys.[name]+ '];'
        from sys.foreign_keys
        inner join sys.tables on sys.foreign_keys.[referenced_object_id] = sys.tables.[object_id]
        where sys.tables.[name] = '{{ this.table }}'
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
        select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL ALTER TABLE [' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + sys.tables.[name] + '] DROP CONSTRAINT [' + sys.indexes.[name]+ '];'
        from sys.indexes
        inner join sys.tables on sys.indexes.[object_id] = sys.tables.[object_id]
        where sys.indexes.is_primary_key = 1
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
        select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL DROP INDEX [' + sys.indexes.[name] + '] ON ' + '[' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + OBJECT_NAME(sys.tables.[object_id]) + ']; '
        from sys.indexes {{ information_schema_hints() }}
        inner join sys.tables {{ information_schema_hints() }}
        on sys.indexes.object_id = sys.tables.object_id
        where sys.indexes.[name] is not null
        and SCHEMA_NAME(sys.tables.schema_id) = '{{ this.schema }}'
        and sys.tables.[name] = '{{ this.table }}'
        for xml path('')
    ); exec sp_executesql @drop_remaining_indexes_last;

{%- endmacro %}


{% macro create_clustered_index(columns, unique=False) -%}
    {{ log("Creating clustered index...") }}

    {% set idx_name = "clustered_" + local_md5(columns | join("_")) %}

    if not exists(select *
                    from sys.indexes {{ information_schema_hints() }}
                    where name = '{{ idx_name }}'
                    and object_id = OBJECT_ID('{{ this }}')
    )
    begin

    create
    {% if unique -%}
    unique
    {% endif %}
    clustered index
        {{ idx_name }}
        on {{ this }} ({{ '[' + columns|join("], [") + ']' }})
    end
{%- endmacro %}


{% macro create_nonclustered_index(columns, includes=False) %}

    {{ log("Creating nonclustered index...") }}

    {% if includes -%}
        {% set idx_name = (
            "nonclustered_"
            + local_md5(columns | join("_"))
            + "_incl_"
            + local_md5(includes | join("_"))
        ) %}
    {% else -%}
        {% set idx_name = "nonclustered_" + local_md5(columns | join("_")) %}
    {% endif %}

    if not exists(select *
                    from sys.indexes {{ information_schema_hints() }}
                    where name = '{{ idx_name }}'
                    and object_id = OBJECT_ID('{{ this }}')
    )
    begin
    create nonclustered index
        {{ idx_name }}
        on {{ this }} ({{ '[' + columns|join("], [") + ']' }})
        {% if includes -%}
            include ({{ '[' + includes|join("], [") + ']' }})
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
    {%- if index_config.optimize_for_sequential_key -%}
        {%- do with_options.append('optimize_for_sequential_key = on') -%}
    {%- endif -%}
    {%- if index_config.sort_in_tempdb -%}
        {%- do with_options.append('sort_in_tempdb = on') -%}
    {%- endif -%}
    {%- for option_key, option_value in (index_config.build_options or {}).items() -%}
        {%- if option_value is sameas true -%}
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
        select string_agg(col.[name], ', ') within group (order by ic.key_ordinal) as cols
        from sys.index_columns ic
        inner join sys.columns col
            on col.object_id = ic.object_id and col.column_id = ic.column_id
        where ic.object_id = i.object_id and ic.index_id = i.index_id
          and ic.is_included_column = 0
    ) key_cols
    outer apply (
        select string_agg(col.[name], ', ') as cols
        from sys.index_columns ic
        inner join sys.columns col
            on col.object_id = ic.object_id and col.column_id = ic.column_id
        where ic.object_id = i.object_id and ic.index_id = i.index_id
          and ic.is_included_column = 1
    ) incl_cols
    outer apply (
        select string_agg(col.[name], ', ') as cols
        from sys.index_columns ic
        inner join sys.columns col
            on col.object_id = ic.object_id and col.column_id = ic.column_id
        where ic.object_id = i.object_id and ic.index_id = i.index_id
          and ic.is_descending_key = 1
    ) desc_cols
    outer apply (
        /* MAX() rather than TOP 1: deterministic if partitions ever carry
           mixed compression (the adapter doesn't manage partitioning today) */
        select max(p.data_compression_desc) as data_compression_desc
        from sys.partitions p
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
  {%- do adapter.validate_indexes(raw_indexes, as_columnstore) -%}
  {%- for _index_dict in raw_indexes %}
    {%- set create_index_sql = get_create_index_sql(relation, _index_dict) -%}
    {% if create_index_sql %}
      {% do run_query(create_index_sql) %}
    {% endif %}
  {%- endfor %}
{% endmacro %}


{% macro sqlserver__reconcile_indexes(relation) %}
  {#-
    Converge an existing relation on its configured index set. Called on the
    paths where the relation persists across runs (incremental non-full-
    refresh, dml table refresh, snapshot updates), where create_indexes alone
    would let config changes drift.
  -#}
  {%- set raw_indexes = config.get('indexes', default=[]) -%}
  {#- all three callers (incremental, dml refresh, snapshot) honor as_columnstore -#}
  {%- do adapter.validate_indexes(raw_indexes, config.get('as_columnstore', default=true)) -%}
  {%- set drop_unmanaged = config.get('drop_unmanaged_indexes', default=false) -%}
  {%- set existing = sqlserver__describe_indexes(relation) -%}
  {%- set result = adapter.index_changes(existing, raw_indexes, relation, drop_unmanaged) -%}
  {%- for warning in result['warnings'] %}
    {% do log("Index reconcile on " ~ relation ~ ": " ~ warning, info=true) %}
  {%- endfor %}
  {%- for index_name in result['drops'] %}
    {% do log("Dropping index " ~ index_name ~ " on " ~ relation, info=true) %}
    {% do run_query(sqlserver__get_drop_index_sql(relation, index_name)) %}
  {%- endfor %}
  {%- for index_dict in result['creates'] %}
    {% do run_query(sqlserver__get_create_index_sql(relation, index_dict)) %}
  {%- endfor %}
{% endmacro %}
