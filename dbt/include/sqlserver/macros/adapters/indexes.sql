{% macro sqlserver__create_clustered_columnstore_index(relation) -%}
  {%- set cci_name = relation.schema ~ '_' ~ relation.identifier ~ '_cci' -%}
  {%- set relation_name = relation.schema ~ '_' ~ relation.identifier -%}
  {%- set full_relation = relation.schema ~ '.' ~ relation.identifier -%}
  use [{{ relation.database }}];
  if EXISTS (
        SELECT * FROM
        sys.indexes WHERE name = '{{cci_name}}'
        AND object_id=object_id('{{relation_name}}')
    )
  DROP index {{full_relation}}.{{cci_name}}
  CREATE CLUSTERED COLUMNSTORE INDEX {{cci_name}}
    ON {{full_relation}}
{% endmacro %}


{% macro sqlserver__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set index_name = index_config.render(relation) -%}
  {{ log("Create index (" ~ index_config.type ~ ") [" ~ index_name ~ "] on table " ~ this) }}
  create {% if index_config.unique -%}
    unique
  {%- endif %} {{ index_config.type }} index [{{ index_name }}]
    on {{ relation }}
    {% if index_config.columns -%}
      ({{ "[" + index_config.columns|join("], [") + "]" }})
    {%- endif %}
    {% if index_config.include_columns -%}
      include ({{ "[" + index_config.include_columns|join("], [") + "]" }})
    {%- endif %}
    {% if index_config.data_compression -%}
      with (data_compression = {{ index_config.data_compression }})
    {%- endif %}
    {% if index_config.partition_schema and index_config.partition_column -%}
      on [{{ index_config.partition_schema }}] ({{ "[" + index_config.partition_column + "]" }})
    {%- endif %};
{%- endmacro %}


{# most of this code is from https://github.com/jacobm001/dbt-mssql/blob/master/dbt/include/mssql/macros/indexes.sql        #}


{% macro drop_xml_indexes() -%}
{# Altered from https://stackoverflow.com/q/1344401/10415173 #}
{# and https://stackoverflow.com/a/33785833/10415173         #}


{{ log("Running drop_xml_indexes() macro...") }}

declare @drop_xml_indexes nvarchar(max);
select @drop_xml_indexes = (
    select 'IF INDEXPROPERTY(' + CONVERT(VARCHAR(MAX), sys.tables.[object_id]) + ', ''' + sys.indexes.[name] + ''', ''IndexId'') IS NOT NULL DROP INDEX [' + sys.indexes.[name] + '] ON ' + '[' + SCHEMA_NAME(sys.tables.[schema_id]) + '].[' + OBJECT_NAME(sys.tables.[object_id]) + ']; '
	from sys.indexes
    inner join sys.tables on sys.indexes.object_id = sys.tables.object_id
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
    from sys.indexes
    inner join sys.tables on sys.indexes.object_id = sys.tables.object_id
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
    from sys.indexes
    inner join sys.tables on sys.indexes.object_id = sys.tables.object_id
    where sys.indexes.[name] is not null
      and sys.tables.[name] = '{{ this.table }}'
    for xml path('')
); exec sp_executesql @drop_remaining_indexes_last;

{%- endmacro %}


{% macro create_clustered_index(columns, unique=False) -%}

{{ log("Creating clustered index...") }}

{% set idx_name = this.table + '__clustered_index_on_' + columns|join('_') %}

if not exists(select * from sys.indexes 
                where 
                name = '{{ idx_name }}' and 
                object_id = OBJECT_ID('{{ this }}')
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

{% set idx_name = this.table + '__index_on_' + columns|join('_')|replace(" ", "_") %}

if not exists(select * from sys.indexes 
                where 
                name = '{{ idx_name }}' and 
                object_id = OBJECT_ID('{{ this }}')
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
