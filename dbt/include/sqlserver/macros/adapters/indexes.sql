{% macro sqlserver__create_clustered_columnstore_index(relation) -%}
    {%- set cci_name = (relation.schema ~ '_' ~ relation.identifier ~ '_cci') | replace(".", "") | replace(" ", "") -%}
    {%- set relation_name = relation.schema ~ '_' ~ relation.identifier -%}
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
