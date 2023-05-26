{% macro fabric__make_temp_relation(base_relation, suffix) %}
    {%- set temp_identifier = base_relation.identifier ~ suffix -%}
    {%- set temp_relation = base_relation.incorporate(
                                path={"identifier": temp_identifier}) -%}

    {{ return(temp_relation) }}
{% endmacro %}

{% macro fabric__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {{ fabric__drop_relation_script(relation) }}
  {%- endcall %}
{% endmacro %}

{% macro fabric__drop_relation_script(relation) -%}
    {% call statement('find_references', fetch_result=true) %}
        {{ use_database_hint() }}
        select
            sch.name as schema_name,
            obj.name as view_name
        from sys.sql_expression_dependencies refs
        inner join sys.objects obj
        on refs.referencing_id = obj.object_id
        inner join sys.schemas sch
        on obj.schema_id = sch.schema_id
        where refs.referenced_database_name = '{{ relation.database }}'
        and refs.referenced_schema_name = '{{ relation.schema }}'
        and refs.referenced_entity_name = '{{ relation.identifier }}'
        and refs.referencing_class = 1
        and obj.type = 'V'
    {% endcall %}
    {% set references = load_result('find_references')['data'] %}
    {% for reference in references -%}
        -- dropping referenced view {{ reference[0] }}.{{ reference[1] }}
        {{ fabric__drop_relation_script(relation.incorporate(
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

    {{ use_database_hint() }}
    DROP {{ relation.type }} IF EXISTS {{ relation.include(database=False) }}

{% endmacro %}

{% macro fabric__rename_relation(from_relation, to_relation) -%}
  {% if from_relation.type == 'view' %}
    {% call statement('get_view_definition', fetch_result=True) %}
        select VIEW_DEFINITION
        from INFORMATION_SCHEMA.VIEWS
        where TABLE_CATALOG = '{{ from_relation.database }}'
        and TABLE_SCHEMA = '{{ from_relation.schema }}'
        and TABLE_NAME = '{{ from_relation.identifier }}'
    {% endcall %}
    {% set view_def_full = load_result('get_view_definition')['data'][0][0] %}

    {% set view_def_sql_matches = modules.re.match('^create\\s+view\\s+[0-9a-z.\\"\\[\\]_]+\\s+as\\s+\\(?(.*)\\)?\\s+;?\\s+$', view_def_full, modules.re.I) %}
    {% if not view_def_sql_matches %}
        {{ exceptions.raise_compiler_error("Could not extract view definition to rename") }}
    {% endif %}
    {% set view_def_sql = view_def_sql_matches.group(1) %}

    {% call statement('create_new_view') %}
        {{ create_view_as(to_relation, view_def_sql) }}
    {% endcall %}
    {% call statement('drop_old_view') %}
        DROP VIEW IF EXISTS {{ from_relation.include(database=False) }};
    {% endcall %}
  {% endif %}
  {% if from_relation.type == 'table' %}
      {% call statement('rename_relation') %}
        create table {{ to_relation.include(database=False) }} as select * from {{ from_relation.include(database=False) }}
      {%- endcall %}
      {{ fabric__drop_relation(from_relation) }}
  {% endif %}
{% endmacro %}

-- DROP synapsevnext__truncate_relation when TRUNCATE TABLE is supported
{% macro fabric__truncate_relation(relation) -%}

  {% set tempTableName %}
    {{ relation.identifier.replace("#", "") }}_{{ range(21000, 109000) | random }}
  {% endset %}

  {% call statement('truncate_relation') -%}
    CREATE TABLE {{ tempTableName }} AS SELECT * FROM {{ relation }} WHERE 1=2
    DROP TABLE IF EXISTS {{ relation }}
    CREATE TABLE {{ relation }} AS SELECT * FROM {{ tempTableName }}
    DROP TABLE IF EXISTS {{ tempTableName }}
  {%- endcall %}

{% endmacro %}
