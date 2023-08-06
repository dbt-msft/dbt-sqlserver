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

    {% if relation.type == 'view' -%}
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
      {% elif relation.type == 'table'%}
      {%- else -%}
          {{ exceptions.raise_not_implemented('Invalid relation being dropped: ' ~ relation) }}
      {% endif %}

    {{ use_database_hint() }}
    EXEC('DROP {{ relation.type }} IF EXISTS {{ relation.include(database=False) }};');

{% endmacro %}

{% macro fabric__rename_relation(from_relation, to_relation) -%}
  {% if to_relation.type == 'view' %}
    {% call statement('get_view_definition', fetch_result=True) %}
        SELECT m.[definition] AS VIEW_DEFINITION
        FROM sys.objects o
        INNER JOIN sys.sql_modules m
            ON m.[object_id] = o.[object_id]
        INNER JOIN sys.views v
            ON o.[object_id] = v.[object_id]
        INNER JOIN sys.schemas s
            ON o.schema_id = s.schema_id
            AND s.schema_id = v.schema_id
        WHERE s.name = '{{ from_relation.schema }}'
            AND v.name = '{{ from_relation.identifier }}'
            AND o.[type] = 'V';
    {% endcall %}

    {% set view_def_full = load_result('get_view_definition')['data'][0][0] %}
    {# Jinja does not allow bitwise operators and we need re.I | re.M here. So calculated manually this becomes 10. #}
    {% set final_view_sql = modules.re.sub("create\s+view\s+.*?\s+as\s+","",view_def_full, 10) %}

    {% call statement('create_new_view') %}
        {{ create_view_as(to_relation, final_view_sql) }}
    {% endcall %}
    {% call statement('drop_old_view') %}
        EXEC('DROP VIEW IF EXISTS {{ from_relation.include(database=False) }};');
    {% endcall %}
  {% endif %}
  {% if to_relation.type == 'table' %}
      {% call statement('rename_relation') %}
        {{ log("renaming relation", info=True) }}
        EXEC('create table {{ to_relation.include(database=False) }} as select * from {{ from_relation.include(database=False) }}');
      {%- endcall %}

      -- Getting constraints from the old table
      {% call statement('get_table_constraints', fetch_result=True) %}
        SELECT Contraint_statement FROM
        (
          SELECT
          CASE
              WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  THEN 'ALTER TABLE <<REPLACE TABLE>> ADD CONSTRAINT ' + tc.CONSTRAINT_NAME + ' PRIMARY KEY NONCLUSTERED('+ccu.COLUMN_NAME+') NOT ENFORCED'
              WHEN tc.CONSTRAINT_TYPE = 'UNIQUE'
                  THEN 'ALTER TABLE <<REPLACE TABLE>> ADD CONSTRAINT ' + tc.CONSTRAINT_NAME + ' UNIQUE NONCLUSTERED('+ccu.COLUMN_NAME+') NOT ENFORCED'
              END AS Contraint_statement
          FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc INNER JOIN
              INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                  ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
          WHERE tc.TABLE_NAME = '{{ from_relation.identifier }}' and tc.TABLE_SCHEMA = '{{ from_relation.schema }}'
          UNION ALL
          SELECT
              'ALTER TABLE <<REPLACE TABLE>> ADD CONSTRAINT ' + ccu.CONSTRAINT_NAME + ' FOREIGN KEY('+ccu.COLUMN_NAME+') references '+kcu.TABLE_SCHEMA+'.'+kcu.TABLE_NAME+' ('+kcu.COLUMN_NAME+') not enforced'   AS Contraint_statement
          FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
              INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                  ON ccu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
              INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON kcu.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
          WHERE ccu.TABLE_NAME = '{{ from_relation.identifier }}' and ccu.TABLE_SCHEMA = '{{ from_relation.schema }}'
        ) T WHERE Contraint_statement IS NOT NULL
      {% endcall %}

      {% set references = load_result('get_table_constraints')['data'] %}
      {{ fabric__drop_relation(from_relation) }}

      {% set tempTableName %}
        {{to_relation.include(database=False)}}
      {% endset %}

      {% for reference in references -%}
        {% set alter_table_script %}
          {{reference[0].replace("<<REPLACE TABLE>>", tempTableName)}}
        {% endset %}

        --EXEC('alter_table_script;')
        {% call statement('Execute_Constraints') %}
          EXEC('{{alter_table_script}};');
        {% endcall %}
      {% endfor %}
  {% endif %}
{% endmacro %}

-- DROP fabric__truncate_relation when TRUNCATE TABLE is supported
{% macro fabric__truncate_relation(relation) -%}

  {% set tempTableName %}
    {{ relation.include(database=False).identifier.replace("#", "") }}_{{ range(21000, 109000) | random }}
  {% endset %}

  {% call statement('truncate_relation') -%}
    CREATE TABLE {{ tempTableName }} AS SELECT * FROM {{ relation }} WHERE 1=2
    EXEC('DROP TABLE IF EXISTS {{ relation.include(database=False) }};');
    EXEC('CREATE TABLE {{ relation.include(database=False) }} AS SELECT * FROM {{ tempTableName }};');
    EXEC('DROP TABLE IF EXISTS {{ tempTableName }};');
  {%- endcall %}

{% endmacro %}
