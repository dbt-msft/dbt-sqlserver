{% macro sqlserver__make_temp_relation(base_relation, suffix) %}
    {%- set temp_identifier = '#' ~ base_relation.identifier ~ suffix -%}
    {%- set temp_relation = base_relation.incorporate(
                                path={"identifier": temp_identifier}) -%}

    {{ return(temp_relation) }}
{% endmacro %}

{% macro sqlserver__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {{ sqlserver__drop_relation_script(relation) }}
  {%- endcall %}
{% endmacro %}

{% macro sqlserver__drop_relation_script(relation) -%}
    {% call statement('find_references', fetch_result=true) %}
        USE [{{ relation.database }}];
        SELECT referencing_schema_name, referencing_entity_name
        FROM sys.dm_sql_referencing_entities ('{{ relation.include(database=false) }}', 'object')
    {% endcall %}
    {% set references = load_result('find_references')['data'] %}
    {% for reference in references -%}
        -- dropping referenced view {{ reference[0] }}.{{ reference[1] }}
        {{ sqlserver__drop_relation_script(relation.incorporate(
            type="view",
            path={"schema": reference[0], "identifier": reference[1]})) }}
    {% endfor %}
    {% if relation.type == 'view' -%}
        {% set object_id_type = 'V' %}
    {% elif relation.type == 'table'%}
        {% set object_id_type = 'U' %}
    {%- else -%}
        {{ exceptions.raise_not_implemented('Invalid relation being dropped: ' ~ relation) }}
    {% endif %}
    USE [{{ relation.database }}];
    if object_id ('{{ relation.include(database=False) }}','{{ object_id_type }}') is not null
        begin
            drop {{ relation.type }} {{ relation.include(database=False) }}
        end
{% endmacro %}

{% macro sqlserver__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    USE [{{ to_relation.database }}];
    EXEC sp_rename '{{ from_relation.schema }}.{{ from_relation.identifier }}', '{{ to_relation.identifier }}'
    IF EXISTS(
    SELECT *
    FROM sys.indexes
    WHERE name='{{ from_relation.schema }}_{{ from_relation.identifier }}_cci' and object_id = OBJECT_ID('{{ from_relation.schema }}.{{ to_relation.identifier }}'))
    EXEC sp_rename N'{{ from_relation.schema }}.{{ to_relation.identifier }}.{{ from_relation.schema }}_{{ from_relation.identifier }}_cci', N'{{ from_relation.schema }}_{{ to_relation.identifier }}_cci', N'INDEX'
  {%- endcall %}
{% endmacro %}
