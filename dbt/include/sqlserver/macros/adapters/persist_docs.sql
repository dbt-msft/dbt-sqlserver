{% macro sqlserver__alter_column_comment(relation, column_dict) -%}
  {%- set existing_columns = adapter.get_columns_in_relation(relation)|map(attribute="name")|list %}
  {%- for column_name in column_dict if (column_name in existing_columns) %}
    {{ log('Alter extended property "MS_Description" to "' ~ column_dict[column_name]['description'] ~ '" for ' ~ relation ~ ' column "' ~ column_name ~ '"') }}
    if not exists (
        select 1
        from
            sys.extended_properties as ep
            inner join sys.all_columns as cols
                on cols.object_id = ep.major_id
                    and cols.column_id = ep.minor_id
        where
            ep.major_id = object_id('{{ relation }}') 
            and ep.name = N'MS_Description'
            and cols.name = N'{{ column_name }}'
    )
        execute sp_addextendedproperty @name = N'MS_Description', @value = N'{{ column_dict[column_name]['description'] }}'
                                            , @level0type = N'SCHEMA', @level0name = N'{{ relation.schema }}'
                                            , @level1type = N'{{ relation.type }}', @level1name = N'{{ relation.identifier }}'
                                            , @level2type = N'COLUMN', @level2name = N'{{ column_name }}';
    else
        execute sp_updateextendedproperty @name = N'MS_Description', @value = N'{{ column_dict[column_name]['description'] }}'
                                            , @level0type = N'SCHEMA', @level0name = N'{{ relation.schema }}'
                                            , @level1type = N'{{ relation.type }}', @level1name = N'{{ relation.identifier }}'
                                            , @level2type = N'COLUMN', @level2name = N'{{ column_name }}';
  {%- endfor %}
{%- endmacro %}
